package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the client does with a server that sends rubbish: random bytes, valid answers with bytes flipped, cut, repeated or interleaved, and random sequences of message types. The
 * connection ends after the bytes, so every iteration must end: the query fails with a {@link PgException} (or, if the mutation left a valid answer, succeeds), never hangs, and the
 * failure is a clean protocol error, not an exception that escaped from the parser ("internal error"). The seeds are fixed, so a finding reproduces.
 */
class PgFuzzTest {

    private EventLoopGroup group;
    private ServerSocket server;

    @BeforeEach
    void setUp() throws IOException {
        group = new EventLoopGroup(1);
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    }

    @AfterEach
    void tearDown() throws IOException {
        server.close();
        group.close();
    }

    private static byte[] msg(char type, byte[] body) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(type);
        int len = body.length + 4;
        o.write(len >> 24);
        o.write(len >> 16);
        o.write(len >> 8);
        o.write(len);
        o.writeBytes(body);
        return o.toByteArray();
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    private static byte[] i32(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] i16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static byte[] cstr(String s) {
        return cat(s.getBytes(StandardCharsets.UTF_8), new byte[]{0});
    }

    /** What a server answers to {@code SELECT n}: the baseline the mutations start from. */
    private static byte[] validAnswer() {
        return cat(msg('1', new byte[0]), msg('t', i16(0)),
                msg('T', cat(i16(2), cstr("n"), i32(0), i16(0), i32(23), i16(4), i32(-1), i16(0), cstr("s"), i32(0), i16(0), i32(25), i16(-1), i32(-1), i16(0))),
                msg('2', new byte[0]), msg('D', cat(i16(2), i32(2), "42".getBytes(), i32(-1))), msg('D', cat(i16(2), i32(1), "7".getBytes(), i32(3), "abc".getBytes())),
                msg('N', cat(new byte[]{'S'}, cstr("NOTICE"), new byte[]{'M'}, cstr("hello"), new byte[]{0})),
                msg('C', cstr("SELECT 2")), msg('Z', new byte[]{'I'}));
    }

    private static final char[] BACKEND_TYPES = "RSKZ123CDTtnIEsNAGHWdcv".toCharArray();

    private static byte[] mutate(Random rnd, int mode) {
        byte[] base = validAnswer();
        switch (mode) {
            case 0: {                                                   // pure noise
                byte[] b = new byte[rnd.nextInt(300)];
                rnd.nextBytes(b);
                return b;
            }
            case 1: {                                                   // a few flipped bytes
                byte[] b = base.clone();
                for (int i = 0, n = 1 + rnd.nextInt(6); i < n; i++) {
                    b[rnd.nextInt(b.length)] = (byte) rnd.nextInt(256);
                }
                return b;
            }
            case 2:                                                     // cut anywhere
                return java.util.Arrays.copyOf(base, rnd.nextInt(base.length));
            case 3: {                                                   // a piece of it repeated
                int from = rnd.nextInt(base.length), to = from + rnd.nextInt(base.length - from);
                return cat(java.util.Arrays.copyOf(base, to), java.util.Arrays.copyOfRange(base, from, to), java.util.Arrays.copyOfRange(base, to, base.length));
            }
            case 4: {                                                   // random message types with random bodies and honest lengths
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                for (int i = 0, n = 1 + rnd.nextInt(12); i < n; i++) {
                    byte[] body = new byte[rnd.nextInt(40)];
                    rnd.nextBytes(body);
                    o.writeBytes(msg(BACKEND_TYPES[rnd.nextInt(BACKEND_TYPES.length)], body));
                }
                return o.toByteArray();
            }
            default: {                                                  // valid messages with lengths that lie
                byte[] b = base.clone();
                int pos = 0;
                List<Integer> starts = new ArrayList<>();
                while (pos + 5 <= b.length) {
                    starts.add(pos);
                    pos += 1 + (((b[pos + 1] & 0xff) << 24) | ((b[pos + 2] & 0xff) << 16) | ((b[pos + 3] & 0xff) << 8) | (b[pos + 4] & 0xff));
                }
                int at = starts.get(rnd.nextInt(starts.size()));
                int[] lies = {0, 3, 4, 5, 1000, -1, Integer.MAX_VALUE, Integer.MIN_VALUE};
                byte[] lie = i32(lies[rnd.nextInt(lies.length)]);
                System.arraycopy(lie, 0, b, at + 1, 4);
                return b;
            }
        }
    }

    private void serveOnce(byte[] answer) {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                in.readFully(new byte[in.readInt() - 4]);
                s.getOutputStream().write(cat(msg('R', i32(0)), msg('K', cat(i32(7), i32(8))), msg('Z', new byte[]{'I'})));
                s.getOutputStream().flush();
                // wait for the query to arrive (its Sync), then answer with the rubbish and hang up
                for (;;) {
                    int type = in.readUnsignedByte();
                    int len = in.readInt();
                    in.readFully(new byte[len - 4]);
                    if (type == 'S') {
                        break;
                    }
                }
                s.getOutputStream().write(answer);
                s.getOutputStream().flush();
            } catch (IOException e) {
                // the client hung up
            }
        });
    }

    @Test
    void rubbishFromTheServerEndsEveryQueryCleanly() throws Exception {
        PgConfig config = PgConfig.of("127.0.0.1", server.getLocalPort(), "u", "d").withLimits(100, 4096).withResultLimits(1000, 1 << 20);
        List<String> findings = new ArrayList<>();
        java.util.Map<String, Integer> outcomes = new java.util.TreeMap<>();
        int iterations = 900;
        for (int i = 0; i < iterations; i++) {
            long seed = 20_261_028L * 1000 + i;
            Random rnd = new Random(seed);
            int mode = i % 6;
            byte[] answer = mutate(rnd, mode);
            serveOnce(answer);
            PgClient c = PgClient.connect(group, config).get(10, TimeUnit.SECONDS);
            try {
                CompletableFuture<PgResult> q = c.query("SELECT n");
                try {
                    q.get(10, TimeUnit.SECONDS);
                    outcomes.merge("answered", 1, Integer::sum);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof PgException pe0) {
                        String m = pe0.getMessage();
                        outcomes.merge(m.startsWith("protocol error: malformed") ? "malformed message" : m.startsWith("protocol error") ? "other protocol error"
                                : m.contains("lost") ? "connection lost" : pe0.sqlState() != null ? "server error" : "other: " + m.substring(0, Math.min(40, m.length())), 1, Integer::sum);
                    }
                    if (!(e.getCause() instanceof PgException pe)) {
                        findings.add("seed " + seed + " mode " + mode + ": " + e.getCause());
                    } else if (pe.getMessage().startsWith("internal error")) {
                        findings.add("seed " + seed + " mode " + mode + ": escaped the parser: " + pe.getMessage());
                    }
                } catch (java.util.concurrent.TimeoutException e) {
                    findings.add("seed " + seed + " mode " + mode + ": the query never ended");
                }
            } finally {
                c.close();
            }
        }
        System.out.println("fuzz outcomes over " + iterations + " iterations: " + outcomes);
        assertTrue(outcomes.getOrDefault("malformed message", 0) >= 10, "the parser's bounds checks were exercised: " + outcomes);
        assertTrue(outcomes.getOrDefault("other protocol error", 0) >= 100, "and the other protocol checks: " + outcomes);
        assertTrue(outcomes.getOrDefault("answered", 0) >= 10, "and some mutations left a valid answer: " + outcomes);
        assertTrue(findings.isEmpty(), findings.size() + " finding(s):\n" + String.join("\n", findings.subList(0, Math.min(15, findings.size()))));
    }
}
