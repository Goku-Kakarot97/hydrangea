package io.hydrangea.mongodb;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** A hostile server (or a man in the middle on a connection without TLS) against the client: it must not get the password, nor crash the client. */
class MongoAttackTest {

    private EventLoopGroup group;
    private ServerSocket server;
    /** Every command the client sent, decoded, in order. */
    private final List<Map<String, Object>> received = java.util.Collections.synchronizedList(new ArrayList<>());
    private final List<byte[]> rawReceived = java.util.Collections.synchronizedList(new ArrayList<>());

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

    interface Script {
        /** Answers one command; null closes the connection. */
        byte[] answer(int requestId, Map<String, Object> command) throws IOException;
    }

    /** Reads one OP_MSG from the client; its request id and body. */
    private Object[] read(DataInputStream in) throws IOException {
        byte[] head = new byte[16];
        in.readFully(head);
        ByteBuffer h = ByteBuffer.wrap(head).order(ByteOrder.LITTLE_ENDIAN);
        int length = h.getInt(), requestId = h.getInt();
        byte[] rest = new byte[length - 16];
        in.readFully(rest);
        rawReceived.add(rest);
        ByteBuffer b = ByteBuffer.wrap(rest).order(ByteOrder.LITTLE_ENDIAN);
        b.getInt();                                                // flags
        b.get();                                                   // kind 0
        Map<String, Object> body = Bson.decode(b);
        received.add(body);
        return new Object[]{requestId, body};
    }

    static byte[] reply(int responseTo, Map<String, Object> body) {
        byte[] doc = Bson.encode(body);
        ByteBuffer b = ByteBuffer.allocate(21 + doc.length).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(21 + doc.length).putInt(99).putInt(responseTo).putInt(2013).putInt(0).put((byte) 0).put(doc);
        return b.array();
    }

    /** Serves every connection with the script (the client makes two: a hello-only probe for discovery, then the real one). The latch counts the second's end. */
    private CountDownLatch serve(Script script) {
        CountDownLatch done = new CountDownLatch(2);
        Thread.ofVirtual().start(() -> {
            while (!server.isClosed()) {
                Socket s;
                try {
                    s = server.accept();
                } catch (IOException e) {
                    return;
                }
                Thread.ofVirtual().start(() -> {
                    try (s) {
                        s.setSoTimeout(3000);
                        DataInputStream in = new DataInputStream(s.getInputStream());
                        OutputStream out = s.getOutputStream();
                        for (;;) {
                            Object[] r = read(in);
                            @SuppressWarnings("unchecked") byte[] answer = script.answer((Integer) r[0], (Map<String, Object>) r[1]);
                            if (answer == null) {
                                return;
                            }
                            out.write(answer);
                            out.flush();
                        }
                    } catch (IOException e) {
                        // the client hung up, or the read timed out
                    } finally {
                        done.countDown();
                    }
                });
            }
        });
        return done;
    }

    private static final Map<String, Object> HELLO = Bson.doc("isWritablePrimary", true, "maxBsonObjectSize", 16777216, "maxMessageSizeBytes", 48000000, "maxWriteBatchSize", 100000,
            "maxWireVersion", 21, "minWireVersion", 0, "saslSupportedMechs", List.of("SCRAM-SHA-256"), "ok", 1.0);

    private MongoConfig config() {
        return MongoConfig.of("127.0.0.1", server.getLocalPort(), "db").withCredentials("victim", "s3cret-password").withConnectTimeout(java.time.Duration.ofSeconds(3)).withServerSelectionTimeout(java.time.Duration.ofSeconds(2));
    }

    private MongoException connectFails(MongoConfig config) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> MongoClient.connect(group, config).get(5, TimeUnit.SECONDS));
        return assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
    }

    private void assertNeverSent(String secret) {
        byte[] s = secret.getBytes(StandardCharsets.UTF_8);
        for (byte[] p : rawReceived) {
            outer:
            for (int i = 0; i + s.length <= p.length; i++) {
                for (int j = 0; j < s.length; j++) {
                    if (p[i + j] != s[j]) {
                        continue outer;
                    }
                }
                fail("the secret '" + secret + "' went over the wire");
            }
        }
    }

    @Test
    void aServerThatFakesTheScramSignatureIsRejectedAndNeverSeesThePassword() throws Exception {
        CountDownLatch done = serve((id, cmd) -> {
            if (cmd.containsKey("hello")) {
                return reply(id, HELLO);
            }
            if (cmd.containsKey("saslStart")) {
                String clientFirst = new String((byte[]) cmd.get("payload"), StandardCharsets.UTF_8);
                String nonce = clientFirst.substring(clientFirst.indexOf("r=") + 2);
                String serverFirst = "r=" + nonce + "fakeNonce,s=" + Base64.getEncoder().encodeToString("salt".getBytes()) + ",i=4096";
                return reply(id, Bson.doc("conversationId", 1, "done", false, "payload", serverFirst.getBytes(StandardCharsets.UTF_8), "ok", 1.0));
            }
            if (cmd.containsKey("saslContinue")) {           // a signature it made up: it does not know the password
                return reply(id, Bson.doc("conversationId", 1, "done", true, "payload", "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".getBytes(StandardCharsets.UTF_8), "ok", 1.0));
            }
            return reply(id, Bson.doc("ok", 1.0));
        });
        MongoException e = connectFails(config());
        done.await(5, TimeUnit.SECONDS);
        assertTrue(e.getMessage().toLowerCase().contains("signature"), e.getMessage());
        assertNeverSent("s3cret-password");
        assertEquals(4, received.size(), "the probe's hello, then hello, saslStart, saslContinue; nothing after the bad signature");
    }

    @Test
    void tooFewIterationsOrAForeignNonceAreRefused() throws Exception {
        for (String serverFirstTail : List.of(",i=1", "fake,i=4096")) {
            received.clear();
            boolean fewIterations = serverFirstTail.startsWith(",");
            CountDownLatch done = serve((id, cmd) -> {
                if (cmd.containsKey("hello")) {
                    return reply(id, HELLO);
                }
                if (cmd.containsKey("saslStart")) {
                    String clientFirst = new String((byte[]) cmd.get("payload"), StandardCharsets.UTF_8);
                    String nonce = clientFirst.substring(clientFirst.indexOf("r=") + 2);
                    String serverFirst = fewIterations ? "r=" + nonce + "x,s=c2FsdA==" + serverFirstTail : "r=" + serverFirstTail.replace(",i=4096", "") + ",s=c2FsdA==,i=4096";
                    return reply(id, Bson.doc("conversationId", 1, "done", false, "payload", serverFirst.getBytes(StandardCharsets.UTF_8), "ok", 1.0));
                }
                return reply(id, Bson.doc("ok", 1.0));
            });
            MongoException e = connectFails(config());
            done.await(5, TimeUnit.SECONDS);
            assertTrue(e.getMessage().contains("iterations") || e.getMessage().contains("nonce"), e.getMessage());
            assertEquals(3, received.size(), "no proof was sent: " + received);
        }
    }

    @Test
    void aServerOfferingOnlyPlainGetsNothing() throws Exception {
        CountDownLatch done = serve((id, cmd) -> {
            if (cmd.containsKey("hello")) {
                Map<String, Object> h = Bson.doc();
                h.putAll(HELLO);
                h.put("saslSupportedMechs", List.of("PLAIN"));
                return reply(id, h);
            }
            return reply(id, Bson.doc("ok", 1.0));
        });
        MongoException e = connectFails(config());
        done.await(5, TimeUnit.SECONDS);
        assertTrue(e.getMessage().contains("PLAIN"), e.getMessage());
        assertEquals(2, received.size(), "the probe's hello, the hello, and nothing more");
        assertNeverSent("s3cret-password");
    }

    @Test
    void anOversizedOrMalformedReplyFailsTheConnectionWithoutAllocatingIt() throws Exception {
        for (int variant = 0; variant < 3; variant++) {
            int v = variant;
            received.clear();
            CountDownLatch done = serve((id, cmd) -> {
                byte[] ok = reply(id, HELLO);
                ByteBuffer b = ByteBuffer.wrap(ok).order(ByteOrder.LITTLE_ENDIAN);
                switch (v) {
                    case 0 -> b.putInt(0, 0x7FFFFFF0);                        // 2 GB announced
                    case 1 -> b.putInt(12, 1);                                // OP_REPLY instead of OP_MSG
                    default -> b.putInt(21, 0x7FFFFFFF);                      // the BSON document says it is 2 GB long
                }
                return ok;
            });
            MongoException e = connectFails(config());
            done.await(5, TimeUnit.SECONDS);
            assertEquals(0, e.code());
            assertTrue(e.retriable(), e.getMessage());
            assertTrue(e.getMessage().contains("no primary found"), "the probe failed, so no server was found: " + e.getMessage());
            assertNeverSent("s3cret-password");
        }
    }

    @Test
    void aReplyToNothingAndAConnectionDropMidCommand() throws Exception {
        MongoConfig noAuth = MongoConfig.of("127.0.0.1", server.getLocalPort(), "db").withConnectTimeout(java.time.Duration.ofSeconds(3)).withServerSelectionTimeout(java.time.Duration.ofSeconds(2));
        CountDownLatch done = serve((id, cmd) -> {
            if (cmd.containsKey("hello")) {
                return reply(id, HELLO);
            }
            if (cmd.containsKey("ping")) {
                byte[] stray = reply(id + 1000, Bson.doc("ok", 1.0));      // a reply to a request never made: dropped
                byte[] real = reply(id, Bson.doc("ok", 1.0));
                byte[] both = new byte[stray.length + real.length];
                System.arraycopy(stray, 0, both, 0, stray.length);
                System.arraycopy(real, 0, both, stray.length, real.length);
                return both;
            }
            return null;                                           // any other command: hang up without an answer
        });
        try (MongoClient c = MongoClient.connect(group, noAuth).get(5, TimeUnit.SECONDS)) {
            c.ping().get(5, TimeUnit.SECONDS);
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.collection("x").count(null).get(5, TimeUnit.SECONDS));
            MongoException m = assertInstanceOf(MongoException.class, e.getCause());
            assertEquals(0, m.code());
            assertTrue(m.connectionLost(), m.getMessage());
            assertTrue(m.retriable(), "the command may or may not have run: the caller decides");
            assertEquals(2, group.metrics().counter("mongo.connections.lost", null).value(), "a read is sent once more on a fresh connection, which the server also drops");
        }
        done.await(5, TimeUnit.SECONDS);
    }

    @Test
    void theConfigDoesNotPrintThePassword() {
        String s = config().toString();
        assertFalse(s.contains("s3cret-password"), s);
        assertTrue(s.contains("victim"), s);
        assertThrows(IllegalArgumentException.class, () -> MongoConfig.of("h", 1, "bad.db"));
        assertThrows(IllegalArgumentException.class, () -> MongoConfig.parse("mongodb+srv://x/y"));
    }
}
