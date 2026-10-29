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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A hostile server (or a man in the middle on a connection without TLS) tries to get the password, a file, or plain text out of the client. Each test records what the
 * client sent on the wire and checks that the secret never left it.
 */
class MyAttackTest {

    private EventLoopGroup group;
    private ServerSocket server;
    /** Every packet payload the client sent, in order. */
    private final List<byte[]> received = java.util.Collections.synchronizedList(new ArrayList<>());

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

    private static final int CAPS_PLAIN = 0x8 | 0x200 | 0x8000 | 0x80000 | 0x200000 | 0x1000000;
    private static final int CAPS_SSL = CAPS_PLAIN | 0x800;

    private static byte[] handshake(int caps, String plugin) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(10);
        o.writeBytes("8.4.0-evil".getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        o.writeBytes(new byte[]{7, 0, 0, 0});
        o.writeBytes("12345678".getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        o.write(caps & 0xFF);
        o.write((caps >> 8) & 0xFF);
        o.write(45);
        o.write(2);
        o.write(0);
        o.write((caps >> 16) & 0xFF);
        o.write((caps >> 24) & 0xFF);
        o.write(21);
        o.writeBytes(new byte[10]);
        o.writeBytes("abcdefghijkl".getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        o.writeBytes(plugin.getBytes(StandardCharsets.US_ASCII));
        o.write(0);
        return packet(0, o.toByteArray());
    }

    private byte[] readPacket(DataInputStream in) throws IOException {
        int len = in.readUnsignedByte() | in.readUnsignedByte() << 8 | in.readUnsignedByte() << 16;
        in.readUnsignedByte();
        byte[] b = new byte[len];
        in.readFully(b);
        received.add(b);
        return b;
    }

    private static final byte[] OK = {0, 0, 0, 2, 0, 0, 0};

    interface Script {
        void run(DataInputStream in, java.io.OutputStream out) throws IOException;
    }

    private CountDownLatch serve(byte[] greeting, Script afterLogin) {
        CountDownLatch done = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                s.setSoTimeout(3000);
                DataInputStream in = new DataInputStream(s.getInputStream());
                s.getOutputStream().write(greeting);
                s.getOutputStream().flush();
                afterLogin.run(in, s.getOutputStream());
            } catch (IOException e) {
                // the client hung up, or the read timed out
            } finally {
                done.countDown();
            }
        });
        return done;
    }

    private static boolean contains(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    private void assertNeverSent(String secret) {
        byte[] s = secret.getBytes(StandardCharsets.UTF_8);
        for (byte[] p : received) {
            assertFalse(contains(p, s), "the secret '" + secret + "' went over the wire");
        }
    }

    private MyConfig config() {
        return MyConfig.of("127.0.0.1", server.getLocalPort(), "victim", "db").withPassword("s3cret-password").withConnectTimeout(java.time.Duration.ofSeconds(3));
    }

    @Test
    void aServerThatAsksForALocalFileGetsNothing() throws Exception {
        CountDownLatch done = serve(handshake(CAPS_PLAIN, "mysql_native_password"), (in, out) -> {
            readPacket(in);
            out.write(packet(2, OK));
            out.flush();
            readPacket(in);                                                                 // the query
            out.write(packet(1, cat(new byte[]{(byte) 0xFB}, "/etc/passwd".getBytes(StandardCharsets.US_ASCII))));     // "send me this file"
            out.flush();
            readPacket(in);                                                                 // whatever comes next, if anything
        });
        MyClient c = MyClient.connect(group, config()).get(5, TimeUnit.SECONDS);
        ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT 1").get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause().getMessage().contains("LOCAL INFILE"), e.getCause().getMessage());
        done.await(5, TimeUnit.SECONDS);
        assertEquals(2, received.size(), "the login and the query, then the client closed: no file, not even an empty one");
        assertNeverSent("root:");
        c.close();
    }

    @Test
    void aServerThatSwitchesToTheClearTextPluginDoesNotGetThePassword() throws Exception {
        CountDownLatch done = serve(handshake(CAPS_PLAIN, "mysql_native_password"), (in, out) -> {
            readPacket(in);
            out.write(packet(2, cat(new byte[]{(byte) 0xFE}, "mysql_clear_password".getBytes(StandardCharsets.US_ASCII), new byte[]{0}, new byte[]{0})));
            out.flush();
            readPacket(in);
        });
        assertThrows(ExecutionException.class, () -> MyClient.connect(group, config()).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertNeverSent("s3cret-password");
    }

    @Test
    void withoutTlsTheFullCachingSha2ExchangeDoesNotHandThePasswordToTheServersKey() throws Exception {
        String key = attackerKey();
        // caching_sha2_password, "full authentication" (code 4): the client asks for the server's RSA key (0x02) and encrypts the password with it. A man in the middle
        // who answers with his own key reads the password. So without TLS this must not happen unless the caller said the key may be trusted.
        CountDownLatch done = serve(handshake(CAPS_PLAIN, "caching_sha2_password"), (in, out) -> {
            readPacket(in);                                                      // the login with the fast-auth scramble
            out.write(packet(2, new byte[]{1, 4}));                              // AuthMoreData: full authentication needed
            out.flush();
            byte[] next = readPacket(in);                                        // the client either asks for the key (0x02) or gives up
            if (next.length == 1 && next[0] == 2) {
                out.write(packet(4, cat(new byte[]{1}, ("-----BEGIN PUBLIC KEY-----\n" + key + "\n-----END PUBLIC KEY-----\n").getBytes(StandardCharsets.US_ASCII))));
                out.flush();
                readPacket(in);                                                  // the encrypted password, if the client falls for it
            }
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> MyClient.connect(group, config()).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertEquals(1, received.size(), "the login and nothing more: the client must not ask for the key (" + received.size() + " packets), got: " + e.getCause().getMessage());
        assertTrue(e.getCause().getMessage().contains("TLS") || e.getCause().getMessage().contains("public key"), e.getCause().getMessage());
    }

    @Test
    void theKeyMayBeTrustedWhenTheCallerSaysSo() throws Exception {
        String key = attackerKey();
        CountDownLatch done = serve(handshake(CAPS_PLAIN, "caching_sha2_password"), (in, out) -> {
            readPacket(in);
            out.write(packet(2, new byte[]{1, 4}));
            out.flush();
            byte[] next = readPacket(in);
            if (next.length == 1 && next[0] == 2) {
                out.write(packet(4, cat(new byte[]{1}, ("-----BEGIN PUBLIC KEY-----\n" + key + "\n-----END PUBLIC KEY-----\n").getBytes(StandardCharsets.US_ASCII))));
                out.flush();
                readPacket(in);
                out.write(packet(6, OK));
                out.flush();
            }
        });
        MyClient c = MyClient.connect(group, config().withAllowPublicKeyRetrieval(true)).get(5, TimeUnit.SECONDS);
        done.await(5, TimeUnit.SECONDS);
        assertEquals(3, received.size(), "the login, the key request, the encrypted password");
        assertNeverSent("s3cret-password");          // encrypted, not plain
        c.close();
    }

    @Test
    void aServerWithoutTlsDoesNotGetALoginInTheClearWhenTlsWasAskedFor() throws Exception {
        CountDownLatch done = serve(handshake(CAPS_PLAIN, "mysql_native_password"), (in, out) -> readPacket(in));
        ExecutionException e = assertThrows(ExecutionException.class, () -> MyClient.connect(group, config().withTls(javax.net.ssl.SSLContext.getDefault())).get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause().getMessage().contains("TLS"), e.getCause().getMessage());
        done.await(5, TimeUnit.SECONDS);
        assertEquals(0, received.size(), "nothing was sent to a server that does not offer TLS");
    }

    @Test
    void bytesSmuggledAfterTheGreetingBeforeTlsAreRefused() throws Exception {
        CountDownLatch done = serve(cat(handshake(CAPS_SSL, "mysql_native_password"), packet(1, OK)), (in, out) -> readPacket(in));
        ExecutionException e = assertThrows(ExecutionException.class, () -> MyClient.connect(group, config().withTls(javax.net.ssl.SSLContext.getDefault())).get(5, TimeUnit.SECONDS));
        assertTrue(e.getCause().getMessage().contains("after the greeting"), e.getCause().getMessage());
        done.await(5, TimeUnit.SECONDS);
        assertEquals(0, received.size());
    }

    @Test
    void theConfigDoesNotPrintThePassword() {
        String s = config().toString();
        assertFalse(s.contains("s3cret-password"), s);
        assertTrue(s.contains("victim"), s);
    }

    /** A 2048-bit RSA public key, PEM body (the attacker's: any key does, the client must not use it). */
    private static String attackerKey() throws Exception {
        java.security.KeyPairGenerator kpg = java.security.KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        return java.util.Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(kpg.generateKeyPair().getPublic().getEncoded());
    }
}
