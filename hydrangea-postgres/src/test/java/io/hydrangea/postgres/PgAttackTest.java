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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** A hostile server (or a man in the middle on a connection without TLS) tries to get the password out of the client, or to pass for the real server. */
class PgAttackTest {

    private EventLoopGroup group;
    private ServerSocket server;
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

    private static byte[] i32(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    /** Reads one client message (the startup message has no type byte) and records its body. */
    private byte[] readStartup(DataInputStream in) throws IOException {
        byte[] b = new byte[in.readInt() - 4];
        in.readFully(b);
        received.add(b);
        return b;
    }

    private char readTyped(DataInputStream in) throws IOException {
        char type = (char) in.readUnsignedByte();
        byte[] b = new byte[in.readInt() - 4];
        in.readFully(b);
        received.add(b);
        return type;
    }

    interface Script {
        void run(DataInputStream in, java.io.OutputStream out) throws IOException;
    }

    private CountDownLatch serve(Script script) {
        CountDownLatch done = new CountDownLatch(1);
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                s.setSoTimeout(3000);
                script.run(new DataInputStream(s.getInputStream()), s.getOutputStream());
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

    private PgConfig config() {
        return PgConfig.of("127.0.0.1", server.getLocalPort(), "victim", "db").withPassword("s3cret-password").withConnectTimeout(java.time.Duration.ofSeconds(3));
    }

    @Test
    void withoutTlsAServerThatAsksForTheClearTextPasswordDoesNotGetIt() throws Exception {
        // AuthenticationCleartextPassword (type R, code 3): the real server may be configured that way, but so may a man in the middle; without TLS the password would travel in
        // the clear, so the client must refuse unless the caller allowed it.
        CountDownLatch done = serve((in, out) -> {
            readStartup(in);
            out.write(msg('R', i32(3)));
            out.flush();
            readTyped(in);                                           // a PasswordMessage, if the client falls for it
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> PgClient.connect(group, config()).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertEquals(1, received.size(), "the startup message and nothing more: " + e.getCause().getMessage());
        assertNeverSent("s3cret-password");
        assertTrue(e.getCause().getMessage().contains("clear"), e.getCause().getMessage());
    }

    @Test
    void theClearTextPasswordMayBeSentWhenTheCallerAllowsIt() throws Exception {
        CountDownLatch done = serve((in, out) -> {
            readStartup(in);
            out.write(msg('R', i32(3)));
            out.flush();
            readTyped(in);
            out.write(cat(msg('R', i32(0)), msg('K', cat(i32(7), i32(8))), msg('Z', new byte[]{'I'})));
            out.flush();
            readTyped(in);
        });
        PgClient c = PgClient.connect(group, config().withAllowClearTextPassword(true)).get(5, TimeUnit.SECONDS);
        done.await(1, TimeUnit.SECONDS);
        assertEquals(2, received.size());
        assertTrue(contains(received.get(1), "s3cret-password".getBytes(StandardCharsets.UTF_8)), "the password went, as allowed");
        c.close();
    }

    @Test
    void aServerThatCannotProveItKnowsThePasswordIsRejectedBySCRAM() throws Exception {
        // SCRAM-SHA-256: the client proves it knows the password without sending it; the server must prove it too (the server signature). A fake server that
        // sends a wrong signature must be rejected, so that it cannot pass for the real one and feed the application made-up data.
        CountDownLatch done = serve((in, out) -> {
            readStartup(in);
            out.write(msg('R', cat(i32(10), "SCRAM-SHA-256".getBytes(StandardCharsets.US_ASCII), new byte[]{0, 0})));
            out.flush();
            readTyped(in);                                                       // SASLInitialResponse with the client-first-message
            byte[] clientFirst = received.get(received.size() - 1);
            String cf = new String(clientFirst, StandardCharsets.UTF_8);
            String nonce = cf.substring(cf.indexOf("r=") + 2);
            String serverFirst = "r=" + nonce + "fakeNonce,s=" + java.util.Base64.getEncoder().encodeToString("salt".getBytes()) + ",i=4096";
            out.write(msg('R', cat(i32(11), serverFirst.getBytes(StandardCharsets.US_ASCII))));
            out.flush();
            readTyped(in);                                                       // the client-final-message with its proof (useless without the password: the fake cannot check it)
            out.write(msg('R', cat(i32(12), "v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".getBytes(StandardCharsets.US_ASCII))));     // a signature it made up
            out.write(cat(msg('R', i32(0)), msg('K', cat(i32(7), i32(8))), msg('Z', new byte[]{'I'})));
            out.flush();
            readTyped(in);
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> PgClient.connect(group, config()).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertNeverSent("s3cret-password");
        assertTrue(e.getCause().getMessage().toLowerCase().contains("signature") || e.getCause().getMessage().toLowerCase().contains("server"), e.getCause().getMessage());
    }

    @Test
    void aServerThatRefusesTlsDoesNotGetTheStartupInTheClear() throws Exception {
        CountDownLatch done = serve((in, out) -> {
            readStartup(in);                                                     // the SSLRequest (8 bytes, no type)
            out.write('N');
            out.flush();
            readStartup(in);                                                     // a startup message in the clear, if the client falls back
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> PgClient.connect(group, config().withTls(javax.net.ssl.SSLContext.getDefault())).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertEquals(1, received.size(), "only the SSLRequest went");
        assertTrue(e.getCause().getMessage().contains("TLS"), e.getCause().getMessage());
    }

    @Test
    void bytesSmuggledAfterTheTlsAnswerAreRefused() throws Exception {
        CountDownLatch done = serve((in, out) -> {
            readStartup(in);
            out.write(cat(new byte[]{'S'}, msg('R', i32(0))));                   // CVE-2021-23222: plain text after the S, meant to be taken as part of the TLS session
            out.flush();
            readStartup(in);
        });
        ExecutionException e = assertThrows(ExecutionException.class, () -> PgClient.connect(group, config().withTls(javax.net.ssl.SSLContext.getDefault())).get(5, TimeUnit.SECONDS));
        done.await(5, TimeUnit.SECONDS);
        assertEquals(1, received.size());
        assertTrue(e.getCause().getMessage().contains("SSLRequest") || e.getCause().getMessage().contains("TLS"), e.getCause().getMessage());
    }

    @Test
    void theConfigDoesNotPrintThePassword() {
        String s = config().toString();
        assertFalse(s.contains("s3cret-password"), s);
        assertTrue(s.contains("victim"), s);
    }
}
