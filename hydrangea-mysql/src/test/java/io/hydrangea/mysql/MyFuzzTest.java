package io.hydrangea.mysql;

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
 * What the client does with a server that sends rubbish after a good login: random bytes, a valid answer with bytes flipped, cut, repeated, random packets with honest lengths, and
 * valid packets whose lengths lie. The connection ends after the bytes, so every iteration must end: the call fails with a {@link MyException} (or, if the mutation left a valid
 * answer, succeeds), never hangs, and the failure is a clean protocol error, not an exception that escaped from the parser ("internal error"). The seeds are fixed, so a finding reproduces.
 */
class MyFuzzTest {

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

    private static byte[] packet(int seq, byte[] body) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(body.length);
        o.write(body.length >> 8);
        o.write(body.length >> 16);
        o.write(seq);
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

    private static byte[] lenenc(String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        return cat(new byte[]{(byte) b.length}, b);
    }

    private static final int CAPS = 0x8 | 0x200 | 0x8000 | 0x80000 | 0x200000 | 0x1000000;   // CONNECT_WITH_DB, PROTOCOL_41, SECURE_CONNECTION, PLUGIN_AUTH, PLUGIN_AUTH_LENENC, DEPRECATE_EOF

    private static byte[] handshake() {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(10);
        o.writeBytes("8.4.0-fake".getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        o.writeBytes(new byte[]{7, 0, 0, 0});                          // thread id
        o.writeBytes("12345678".getBytes(StandardCharsets.US_ASCII));   // scramble part 1
        o.write(0);
        o.write(CAPS & 0xFF);
        o.write((CAPS >> 8) & 0xFF);
        o.write(45);                                                    // charset
        o.write(2);                                                     // status: autocommit
        o.write(0);
        o.write((CAPS >> 16) & 0xFF);
        o.write((CAPS >> 24) & 0xFF);
        o.write(21);                                                    // auth data length
        o.writeBytes(new byte[10]);
        o.writeBytes("abcdefghijkl".getBytes(StandardCharsets.US_ASCII));   // scramble part 2
        o.write(0);
        o.writeBytes("mysql_native_password".getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        return packet(0, o.toByteArray());
    }

    private static final byte[] OK = {0, 0, 0, 2, 0, 0, 0};

    /** What a server answers to {@code SELECT n} (text protocol, DEPRECATE_EOF): the baseline the mutations start from. Sequence numbers from 1. */
    private static byte[] validAnswer() {
        byte[] column = cat(lenenc("def"), lenenc(""), lenenc("t"), lenenc("t"), lenenc("n"), lenenc("n"), new byte[]{0x0c, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0});
        return cat(packet(1, new byte[]{1}), packet(2, column), packet(3, lenenc("42")), packet(4, lenenc("7")), packet(5, cat(new byte[]{(byte) 0xFE}, new byte[]{0, 0, 2, 0, 0, 0})));
    }

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
            case 4: {                                                   // random packets with random bodies and honest lengths, some starting with the marker bytes
                ByteArrayOutputStream o = new ByteArrayOutputStream();
                byte[] markers = {0, (byte) 0xFE, (byte) 0xFF, (byte) 0xFB, (byte) 0xFC, (byte) 0xFD, 1, 5, 20};
                for (int i = 0, n = 1 + rnd.nextInt(12); i < n; i++) {
                    byte[] body = new byte[1 + rnd.nextInt(40)];
                    rnd.nextBytes(body);
                    if (rnd.nextBoolean()) {
                        body[0] = markers[rnd.nextInt(markers.length)];
                    }
                    o.writeBytes(packet(i + 1, body));
                }
                return o.toByteArray();
            }
            default: {                                                  // valid packets with lengths that lie
                byte[] b = base.clone();
                int pos = 0;
                List<Integer> starts = new ArrayList<>();
                while (pos + 4 <= b.length) {
                    starts.add(pos);
                    pos += 4 + ((b[pos] & 0xff) | (b[pos + 1] & 0xff) << 8 | (b[pos + 2] & 0xff) << 16);
                }
                int at = starts.get(rnd.nextInt(starts.size()));
                int[] lies = {0, 1, 2, 3, 200, 0xFFFFFF, 0xFFFFFE, 0x7FFFFF};
                int lie = lies[rnd.nextInt(lies.length)];
                b[at] = (byte) lie;
                b[at + 1] = (byte) (lie >> 8);
                b[at + 2] = (byte) (lie >> 16);
                return b;
            }
        }
    }

    private void serveOnce(byte[] answer) {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                s.getOutputStream().write(handshake());
                s.getOutputStream().flush();
                readPacket(in);                                         // the login
                s.getOutputStream().write(packet(2, OK));
                s.getOutputStream().flush();
                readPacket(in);                                         // the query
                s.getOutputStream().write(answer);
                s.getOutputStream().flush();
            } catch (IOException e) {
                // the client hung up
            }
        });
    }

    private static void readPacket(DataInputStream in) throws IOException {
        int len = in.readUnsignedByte() | in.readUnsignedByte() << 8 | in.readUnsignedByte() << 16;
        in.readUnsignedByte();
        in.readFully(new byte[len]);
    }

    @Test
    void rubbishFromTheServerEndsEveryQueryCleanly() throws Exception {
        MyConfig config = MyConfig.of("127.0.0.1", server.getLocalPort(), "u", "d").withPassword("p").withResultLimits(1000, 1 << 20);
        List<String> findings = new ArrayList<>();
        java.util.Map<String, Integer> outcomes = new java.util.TreeMap<>();
        int iterations = 900;
        for (int i = 0; i < iterations; i++) {
            long seed = 20_261_029L * 1000 + i;
            Random rnd = new Random(seed);
            int mode = i % 6;
            byte[] answer = mutate(rnd, mode);
            serveOnce(answer);
            MyClient c = MyClient.connect(group, config).get(10, TimeUnit.SECONDS);
            try {
                CompletableFuture<MyResult> q = c.query("SELECT n");
                try {
                    q.get(10, TimeUnit.SECONDS);
                    outcomes.merge("answered", 1, Integer::sum);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof MyException me0) {
                        String m = me0.getMessage();
                        outcomes.merge(m.startsWith("protocol error: malformed") ? "malformed packet" : m.startsWith("protocol error") ? "other protocol error"
                                : m.contains("lost") ? "connection lost" : me0.errorCode() != 0 ? "server error" : "other: " + m.substring(0, Math.min(40, m.length())), 1, Integer::sum);
                    }
                    if (!(e.getCause() instanceof MyException me)) {
                        findings.add("seed " + seed + " mode " + mode + ": " + e.getCause());
                    } else if (me.getMessage().startsWith("internal error")) {
                        findings.add("seed " + seed + " mode " + mode + ": escaped the parser: " + me.getMessage());
                    }
                } catch (java.util.concurrent.TimeoutException e) {
                    findings.add("seed " + seed + " mode " + mode + ": the query never ended");
                }
            } finally {
                c.close();
            }
        }
        System.out.println("fuzz outcomes over " + iterations + " iterations: " + outcomes);
        assertTrue(outcomes.getOrDefault("malformed packet", 0) >= 10, "the parser's bounds checks were exercised: " + outcomes);
        assertTrue(outcomes.getOrDefault("other protocol error", 0) + outcomes.getOrDefault("connection lost", 0) >= 100, "and the other checks: " + outcomes);
        assertTrue(outcomes.getOrDefault("answered", 0) >= 10, "and some mutations left a valid answer: " + outcomes);
        assertTrue(findings.isEmpty(), findings.size() + " finding(s):\n" + String.join("\n", findings.subList(0, Math.min(15, findings.size()))));
    }
}
