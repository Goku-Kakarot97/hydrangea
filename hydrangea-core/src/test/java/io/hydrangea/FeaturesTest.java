package io.hydrangea;

import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.LengthPrefixStage;
import io.hydrangea.pipeline.LineStage;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FeaturesTest {

    private static final InetSocketAddress ANY = new InetSocketAddress("127.0.0.1", 0);
    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Stage that records inbound messages and exposes its context for writing. */
    private static final class Probe implements Stage {
        final List<Object> received = Collections.synchronizedList(new ArrayList<>());
        final CompletableFuture<Context> active = new CompletableFuture<>();
        final CountDownLatch messages;

        Probe(int expected) {
            messages = new CountDownLatch(expected);
        }

        @Override
        public void onActive(Context ctx) {
            active.complete(ctx);
        }

        @Override
        public void onRead(Context ctx, Object msg) {
            received.add(msg);
            messages.countDown();
        }
    }

    // ---- pipeline ----

    @Test
    void pipelineLineStageBothDirections() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> Pipeline.of(new LineStage(1024), new Stage() {
            @Override
            public void onRead(Context ctx, Object msg) throws Exception {
                ctx.write("echo:" + msg); // goes back through LineStage's encoder
            }
        }))) {
            Probe probe = new Probe(2);
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(),
                    Pipeline.of(new LineStage(1024), probe)).get(5, TimeUnit.SECONDS);
            Context ctx = probe.active.get(5, TimeUnit.SECONDS);
            c.eventLoop().execute(() -> {
                try {
                    ctx.write("one");
                    ctx.write("two");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(probe.messages.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("echo:one", "echo:two"), probe.received);
        }
    }

    @Test
    void pipelineLengthPrefixStage() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> Pipeline.of(new LengthPrefixStage(1024), new Stage() {
            @Override
            public void onRead(Context ctx, Object msg) throws Exception {
                ctx.write(msg);
            }
        }))) {
            Probe probe = new Probe(1);
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(),
                    Pipeline.of(new LengthPrefixStage(1024), probe)).get(5, TimeUnit.SECONDS);
            Context ctx = probe.active.get(5, TimeUnit.SECONDS);
            c.eventLoop().execute(() -> {
                try {
                    ctx.write(bytes("framed"));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(probe.messages.await(5, TimeUnit.SECONDS));
            assertEquals("framed", StandardCharsets.UTF_8.decode((ByteBuffer) probe.received.get(0)).toString());
        }
    }

    // ---- TLS ----

    private record TlsMaterial(SSLContext server, SSLContext client) {
    }

    private static TlsMaterial tls(Path dir) throws Exception {
        Path ks = dir.resolve("test.p12");
        Process p = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair", "-alias", "t", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
                "-dname", "CN=localhost", "-ext", "san=dns:localhost,ip:127.0.0.1",
                "-storetype", "PKCS12", "-keystore", ks.toString(), "-storepass", "changeit")
                .redirectErrorStream(true).start();
        try (InputStream in = p.getInputStream()) {
            in.readAllBytes();
        }
        assertEquals(0, p.waitFor(), "keytool failed");
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(ks)) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(store);
        SSLContext serverCtx = SSLContext.getInstance("TLS");
        serverCtx.init(kmf.getKeyManagers(), null, null);
        SSLContext clientCtx = SSLContext.getInstance("TLS");
        clientCtx.init(null, tmf.getTrustManagers(), null);
        return new TlsMaterial(serverCtx, clientCtx);
    }

    @Test
    void tlsEchoWithLines(@TempDir Path dir) throws Exception {
        TlsMaterial m = tls(dir);
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> Pipeline.of(
                TlsStage.server(m.server()), new LineStage(1 << 16), new Stage() {
                    @Override
                    public void onRead(Context ctx, Object msg) throws Exception {
                        ctx.write("secure:" + msg);
                    }
                }))) {
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            Probe probe = new Probe(3);
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), Pipeline.of(
                    TlsStage.client(m.client(), "localhost", port), new LineStage(1 << 16), probe)).get(5, TimeUnit.SECONDS);
            Context ctx = probe.active.get(10, TimeUnit.SECONDS); // only fires after the handshake
            String big = "x".repeat(40_000); // spans several TLS records
            c.eventLoop().execute(() -> {
                try {
                    ctx.write("a");
                    ctx.write("b");
                    ctx.write(big);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(probe.messages.await(10, TimeUnit.SECONDS));
            assertEquals(List.of("secure:a", "secure:b", "secure:" + big), probe.received);
        }
    }

    @Test
    void tlsRejectsUntrustedServer(@TempDir Path dir) throws Exception {
        TlsMaterial m = tls(dir);
        SSLContext defaultCtx = SSLContext.getDefault(); // does not trust the self-signed cert
        var closed = new CompletableFuture<Throwable>();
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> Pipeline.of(TlsStage.server(m.server()), new Stage() {}))) {
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            Probe probe = new Probe(0);
            Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                final Handler inner = Pipeline.of(TlsStage.client(defaultCtx, "localhost", port), probe);

                @Override
                public void onActive(Connection c) throws Exception {
                    inner.onActive(c);
                }

                @Override
                public void onRead(Connection c, ByteBuffer d) throws Exception {
                    inner.onRead(c, d);
                }

                @Override
                public void onClose(Connection c, Throwable cause) {
                    closed.complete(cause);
                }
            }).get(5, TimeUnit.SECONDS);
            Throwable cause = closed.get(10, TimeUnit.SECONDS);
            assertInstanceOf(javax.net.ssl.SSLException.class, cause);
            assertFalse(probe.active.isDone(), "app stage must never activate on a failed handshake");
        }
    }

    // ---- UDP ----

    @Test
    void udpPingPong() throws Exception {
        UdpSocket server = UdpSocket.bind(group, ANY, (s, from, data) -> s.send(ByteBuffer.allocate(data.remaining()).put(data).flip(), from));
        var reply = new CompletableFuture<String>();
        UdpSocket client = UdpSocket.bind(group, ANY, (s, from, data) -> reply.complete(StandardCharsets.UTF_8.decode(data).toString()));
        try {
            client.send(bytes("ping"), server.localAddress());
            assertEquals("ping", reply.get(5, TimeUnit.SECONDS));
            Metrics.Snapshot m = group.metrics().snapshot();
            assertTrue(m.datagramsReceived() >= 2 && m.datagramsSent() >= 2);
        } finally {
            server.close();
            client.close();
        }
    }

    @Test
    void udpBadListenerDoesNotKillSocket() throws Exception {
        var errors = new AtomicInteger();
        var got = new CompletableFuture<String>();
        UdpSocket server = new UdpSocketHolder().open(group, errors, got);
        UdpSocket client = UdpSocket.bind(group, ANY, (s, f, d) -> {});
        try {
            client.send(bytes("bad"), server.localAddress());
            client.send(bytes("good"), server.localAddress());
            assertEquals("good", got.get(5, TimeUnit.SECONDS));
            assertEquals(1, errors.get());
        } finally {
            server.close();
            client.close();
        }
    }

    private static final class UdpSocketHolder {
        UdpSocket open(EventLoopGroup g, AtomicInteger errors, CompletableFuture<String> got) throws IOException {
            return UdpSocket.bind(g, ANY, new UdpSocket.Listener() {
                @Override
                public void onDatagram(UdpSocket s, SocketAddress from, ByteBuffer d) {
                    String v = StandardCharsets.UTF_8.decode(d).toString();
                    if (v.equals("bad")) throw new IllegalStateException("bad datagram");
                    got.complete(v);
                }

                @Override
                public void onError(UdpSocket s, Throwable t) {
                    errors.incrementAndGet();
                }
            });
        }
    }

    // ---- pool, write callback, metrics, selector rebuild ----

    @Test
    void bufferPoolReusesAndRoundsUp() {
        BufferPool pool = BufferPool.heap();
        ByteBuffer a = pool.acquire(300);
        assertEquals(512, a.capacity());
        pool.release(a);
        ByteBuffer b = pool.acquire(500);
        assertSame(a, b);
        assertEquals(1, pool.hits());
        assertEquals(1, pool.misses());
        ByteBuffer huge = pool.acquire(4 << 20);
        assertEquals(4 << 20, huge.capacity());
        pool.release(huge); // too big: silently not pooled
        assertNotSame(huge, pool.acquire(4 << 20));
        pool.release(ByteBuffer.allocate(1000)); // not a power of two: ignored
    }

    @Test
    void writeCallbackRunsAfterSendAndOnDiscard() throws Exception {
        BufferPool pool = BufferPool.heap();
        var received = new CompletableFuture<Integer>();
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            int n;

            @Override
            public void onRead(Connection c, ByteBuffer d) {
                n += d.remaining();
                if (n >= 5) received.complete(n);
            }
        })) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            ByteBuffer buf = pool.acquire(5);
            buf.put(bytes("hello")).flip();
            var released = new CompletableFuture<Void>();
            c.write(buf, () -> {
                pool.release(buf);
                released.complete(null);
            });
            released.get(5, TimeUnit.SECONDS);
            assertEquals(5, received.get(5, TimeUnit.SECONDS));
            assertSame(buf, pool.acquire(5), "buffer was returned to the pool");

            c.close();
            c.closeFuture().get(5, TimeUnit.SECONDS);
            var discarded = new CompletableFuture<Void>();
            c.write(bytes("late"), () -> discarded.complete(null));
            discarded.get(5, TimeUnit.SECONDS); // callback still fires when the write is dropped
        }
    }

    @Test
    void metricsCountTraffic() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer d) {
                c.write(ByteBuffer.allocate(d.remaining()).put(d).flip());
            }
        })) {
            var echoed = new CompletableFuture<Void>();
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer d) {
                    echoed.complete(null);
                }
            }).get(5, TimeUnit.SECONDS);
            c.write(bytes("12345678"));
            echoed.get(5, TimeUnit.SECONDS);
            Metrics.Snapshot m = group.metrics().snapshot();
            assertEquals(2, m.connectionsOpened());
            assertEquals(2, m.activeConnections());
            assertTrue(m.bytesRead() >= 16, "client and server each read 8 bytes: " + m);
            assertTrue(m.bytesWritten() >= 16);
            assertTrue(m.tasksExecuted() > 0);
            c.close();
            c.closeFuture().get(5, TimeUnit.SECONDS);
            Thread.sleep(100);
            assertTrue(group.metrics().snapshot().connectionsClosed() >= 1);
        }
    }

    @Test
    void connectionsSurviveSelectorRebuild() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer d) {
                c.write(ByteBuffer.allocate(d.remaining()).put(d).flip());
            }
        })) {
            var first = new CompletableFuture<String>();
            var second = new CompletableFuture<String>();
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer d) {
                    String s = StandardCharsets.UTF_8.decode(d).toString();
                    if (!first.isDone()) first.complete(s); else second.complete(s);
                }
            }).get(5, TimeUnit.SECONDS);
            c.write(bytes("before"));
            assertEquals("before", first.get(5, TimeUnit.SECONDS));

            // rebuild every loop's selector (the acceptor and both connection ends live on these loops)
            for (int i = 0; i < group.size(); i++) {
                var done = new CompletableFuture<Void>();
                EventLoop loop = group.next();
                loop.execute(() -> {
                    try {
                        loop.rebuildSelector();
                        done.complete(null);
                    } catch (Throwable t) {
                        done.completeExceptionally(t);
                    }
                });
                done.get(5, TimeUnit.SECONDS);
            }
            assertTrue(group.metrics().snapshot().selectorRebuilds() >= 2);

            c.write(bytes("after"));
            assertEquals("after", second.get(5, TimeUnit.SECONDS));
            // a new connection must also work through the acceptor registered on the new selector
            var third = new CompletableFuture<String>();
            Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onActive(Connection c) {
                    c.write(bytes("fresh"));
                }

                @Override
                public void onRead(Connection c, ByteBuffer d) {
                    third.complete(StandardCharsets.UTF_8.decode(d).toString());
                }
            }).get(5, TimeUnit.SECONDS);
            assertEquals("fresh", third.get(5, TimeUnit.SECONDS));
        }
    }
}
