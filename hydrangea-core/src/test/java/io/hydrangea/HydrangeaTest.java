package io.hydrangea;

import io.hydrangea.codec.LengthPrefixedHandler;
import io.hydrangea.codec.LineHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class HydrangeaTest {

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

    private static Handler echo() {
        return new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                c.write(ByteBuffer.allocate(data.remaining()).put(data).flip());
            }
        };
    }

    @Test
    void echoRoundTrip() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), HydrangeaTest::echo)) {
            var received = new CompletableFuture<String>();
            var sb = new StringBuilder();
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer data) {
                    sb.append(StandardCharsets.UTF_8.decode(data));
                    if (sb.length() >= 5) received.complete(sb.toString());
                }
            }).get(5, TimeUnit.SECONDS);
            c.write(bytes("hello"));
            assertEquals("hello", received.get(5, TimeUnit.SECONDS));
            c.close();
            c.closeFuture().get(5, TimeUnit.SECONDS);
            assertFalse(c.isOpen());
        }
    }

    @Test
    void largePayloadSurvivesBackpressure() throws Exception {
        int size = 8 * 1024 * 1024;
        try (Server server = Server.bind(group, ANY, Options.defaults(), HydrangeaTest::echo)) {
            var total = new java.util.concurrent.atomic.AtomicLong();
            var done = new CompletableFuture<Void>();
            var sawNonWritable = new AtomicBoolean();
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer data) {
                    if (total.addAndGet(data.remaining()) == size) done.complete(null);
                }

                @Override
                public void onWritabilityChanged(Connection c) {
                    if (!c.isWritable()) sawNonWritable.set(true);
                }
            }).get(5, TimeUnit.SECONDS);
            c.write(ByteBuffer.allocate(size)); // one huge write far above the high-water mark
            done.get(20, TimeUnit.SECONDS);
            assertTrue(sawNonWritable.get(), "high-water mark should have flipped writability");
            assertTrue(c.isWritable(), "writable again after draining");
        }
    }

    @Test
    void whenWritableCompletesAtOnceWhenIdleAndAfterDrainWhenBacklogged() throws Exception {
        int size = 8 * 1024 * 1024;
        try (Server server = Server.bind(group, ANY, Options.defaults(), HydrangeaTest::echo)) {
            var total = new java.util.concurrent.atomic.AtomicLong();
            var done = new CompletableFuture<Void>();
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer data) {
                    if (total.addAndGet(data.remaining()) == size) done.complete(null);
                }
            }).get(5, TimeUnit.SECONDS);
            c.whenWritable().get(5, TimeUnit.SECONDS);          // idle: immediate
            c.write(ByteBuffer.allocate(size));
            var waited = c.whenWritable();                        // backlogged: must wait for the drain
            waited.get(20, TimeUnit.SECONDS);
            assertTrue(c.isWritable());
            done.get(20, TimeUnit.SECONDS);
        }
    }

    @Test
    void whenWritableFailsWhenTheConnectionCloses() throws Exception {
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {})) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            c.close();
            c.closeFuture().get(5, TimeUnit.SECONDS);
            var f = c.whenWritable();
            assertThrows(java.util.concurrent.ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void lineFramingAcrossFragmentedWrites() throws Exception {
        List<String> lines = Collections.synchronizedList(new ArrayList<>());
        var got = new CountDownLatch(3);
        try (Server server = Server.bind(group, ANY, Options.defaults(),
                () -> new LineHandler(1024, (c, line) -> { lines.add(line); got.countDown(); }))) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            for (String part : new String[]{"al", "pha\r\nbe", "ta\ngam", "ma\n"}) {
                c.write(bytes(part));
                Thread.sleep(30); // force separate TCP reads
            }
            assertTrue(got.await(5, TimeUnit.SECONDS));
            assertEquals(List.of("alpha", "beta", "gamma"), lines);
        }
    }

    @Test
    void oversizedFrameClosesConnection() throws Exception {
        var closeCause = new AtomicReference<Throwable>();
        var closed = new CountDownLatch(1);
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new LengthPrefixedHandler(16, (c, m) -> {}) {
            @Override
            public void onClose(Connection c, Throwable cause) {
                closeCause.set(cause);
                closed.countDown();
            }
        })) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            c.write(ByteBuffer.allocate(4).putInt(1_000_000).flip());
            assertTrue(closed.await(5, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, closeCause.get());
        }
    }

    @Test
    void lengthPrefixedRoundTrip() throws Exception {
        var got = new CompletableFuture<String>();
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new LengthPrefixedHandler(1024,
                (c, m) -> got.complete(StandardCharsets.UTF_8.decode(m).toString())))) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            c.write(LengthPrefixedHandler.encode(bytes("payload")));
            assertEquals("payload", got.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void idleConnectionIsClosed() throws Exception {
        var cause = new CompletableFuture<Throwable>();
        Options opts = Options.defaults().withIdleTimeout(Duration.ofMillis(100));
        try (Server server = Server.bind(group, ANY, opts, () -> new Handler() {
            @Override
            public void onClose(Connection c, Throwable t) {
                cause.complete(t);
            }
        })) {
            Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            assertInstanceOf(IdleTimeoutException.class, cause.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void handlerExceptionClosesConnectionWithCause() throws Exception {
        var cause = new CompletableFuture<Throwable>();
        try (Server server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer d) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void onClose(Connection c, Throwable t) {
                cause.complete(t);
            }
        })) {
            Connection c = Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
            c.write(bytes("x"));
            assertEquals("boom", cause.get(5, TimeUnit.SECONDS).getMessage());
        }
    }

    @Test
    void connectToClosedPortFails() throws Exception {
        SocketAddress dead;
        try (Server s = Server.bind(group, ANY, Options.defaults(), HydrangeaTest::echo)) {
            dead = s.localAddress();
        }
        var f = Client.connect(group, dead, Options.defaults(), new Handler() {});
        var ex = assertThrows(java.util.concurrent.ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IOException.class, ex.getCause());
    }

    @Test
    void bindFailureIsReportedSynchronously() throws Exception {
        try (Server s = Server.bind(group, ANY, Options.defaults(), HydrangeaTest::echo)) {
            assertThrows(IOException.class, () -> Server.bind(group, s.localAddress(), Options.defaults(), HydrangeaTest::echo));
        }
    }

    @Test
    void timersRunInOrderAndCanBeCancelled() throws Exception {
        EventLoop loop = group.next();
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        var last = new CountDownLatch(1);
        loop.schedule(() -> { order.add(2); last.countDown(); }, 60, TimeUnit.MILLISECONDS);
        loop.schedule(() -> order.add(1), 20, TimeUnit.MILLISECONDS);
        Scheduled cancelled = loop.schedule(() -> order.add(99), 40, TimeUnit.MILLISECONDS);
        cancelled.cancel();
        assertTrue(last.await(5, TimeUnit.SECONDS));
        assertEquals(List.of(1, 2), order);
    }

    @Test
    void shutdownClosesOpenConnections() throws Exception {
        var closed = new CompletableFuture<Void>();
        var server = Server.bind(group, ANY, Options.defaults(), () -> new Handler() {
            @Override
            public void onClose(Connection c, Throwable t) {
                closed.complete(null);
            }
        });
        Client.connect(group, server.localAddress(), Options.defaults(), new Handler() {}).get(5, TimeUnit.SECONDS);
        Thread.sleep(100);
        group.shutdown().get(5, TimeUnit.SECONDS);
        closed.get(5, TimeUnit.SECONDS);
    }
}
