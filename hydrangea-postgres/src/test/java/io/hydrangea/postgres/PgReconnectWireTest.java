package io.hydrangea.postgres;

import io.hydrangea.Backoff;
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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reconnecting against a server that comes and goes as the test says: it can refuse connections, refuse logins, or never answer one. */
class PgReconnectWireTest {

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

    /** A server that logs clients in (or does not), and that the test can stop (connections are refused), restart on the same port and drop clients from. */
    private static final class FakeServer implements AutoCloseable {
        final int port;
        private volatile ServerSocket socket;
        private final Queue<String> behaviours = new ArrayDeque<>();      // one per accepted connection: ok, error, silent; "ok" when the queue is empty
        private final List<Socket> clients = new CopyOnWriteArrayList<>();
        final List<String> accepted = new CopyOnWriteArrayList<>();
        private volatile boolean closed;

        FakeServer() throws IOException {
            socket = new ServerSocket(0, 20, InetAddress.getLoopbackAddress());
            port = socket.getLocalPort();
            listen(socket);
        }

        void script(String... next) {
            synchronized (behaviours) {
                behaviours.addAll(List.of(next));
            }
        }

        void stop() throws IOException {
            socket.close();
        }

        void start() throws IOException {
            ServerSocket s = new ServerSocket();
            s.setReuseAddress(true);
            s.bind(new java.net.InetSocketAddress(InetAddress.getLoopbackAddress(), port), 20);
            socket = s;
            listen(s);
        }

        void drop() {
            for (Socket c : clients) {
                try {
                    c.close();
                } catch (IOException ignored) {
                    // gone already
                }
            }
            clients.clear();
        }

        private void listen(ServerSocket s) {
            Thread.ofVirtual().start(() -> {
                while (!closed) {
                    try {
                        Socket c = s.accept();
                        String how;
                        synchronized (behaviours) {
                            how = behaviours.isEmpty() ? "ok" : behaviours.poll();
                        }
                        accepted.add(how);
                        clients.add(c);
                        Thread.ofVirtual().start(() -> serve(c, how));
                    } catch (IOException e) {
                        return;                // the listening socket was closed
                    }
                }
            });
        }

        private void serve(Socket c, String how) {
            try {
                DataInputStream in = new DataInputStream(c.getInputStream());
                in.readFully(new byte[in.readInt() - 4]);                      // the startup message
                switch (how) {
                    case "ok" -> {
                        c.getOutputStream().write(cat(msg('R', new byte[]{0, 0, 0, 0}), msg('K', new byte[]{0, 0, 0, 7, 0, 0, 0, 8}), msg('Z', new byte[]{'I'})));
                        c.getOutputStream().flush();
                        while (in.read() >= 0) {
                            // takes in what the client sends
                        }
                    }
                    case "error" -> {
                        c.getOutputStream().write(msg('E', cat("SFATAL\0".getBytes(), "C28P01\0".getBytes(), "Mpassword authentication failed\0".getBytes(), new byte[]{0})));
                        c.getOutputStream().flush();
                        c.close();
                    }
                    default -> {
                        while (in.read() >= 0) {
                            // says nothing
                        }
                    }
                }
            } catch (IOException e) {
                // the client or the test closed it
            }
        }

        private static byte[] cat(byte[]... parts) {
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            for (byte[] p : parts) {
                o.writeBytes(p);
            }
            return o.toByteArray();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            socket.close();
            drop();
        }
    }

    private static final class Events implements PgListener {
        final List<String> log = new CopyOnWriteArrayList<>();

        @Override
        public void onClose(Throwable cause) {
            log.add(cause == null ? "close" : "lost");
        }

        @Override
        public void onReconnecting(int attempt, Duration delay) {
            log.add("reconnecting " + attempt);
        }

        @Override
        public void onReconnected() {
            log.add("back");
        }

        @Override
        public void onGaveUp(Throwable cause) {
            log.add("gave up");
        }
    }

    private EventLoopGroup group;
    private FakeServer server;

    @BeforeEach
    void setUp() throws IOException {
        group = new EventLoopGroup(1);
        server = new FakeServer();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.close();
        group.close();
    }

    private PgConfig config(Backoff backoff) {
        return PgConfig.of("127.0.0.1", server.port, "u", "d").withReconnect(backoff).withConnectTimeout(Duration.ofMillis(400));
    }

    private static void waitFor(java.util.function.BooleanSupplier ok, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void theClientGivesUpWhenTheBackOffRunsOutAndStaysClosed() throws Exception {
        Events ev = new Events();
        PgClient c = PgClient.connect(group, config(new Backoff(Duration.ofMillis(50), Duration.ofMillis(50), 1.0, 0, 3)), ev).get(10, TimeUnit.SECONDS);
        server.stop();                 // from now on connections are refused
        server.drop();
        waitFor(() -> ev.log.contains("gave up"), "the client to give up");
        assertEquals(List.of("lost", "reconnecting 1", "reconnecting 2", "reconnecting 3", "gave up"), ev.log);
        assertFalse(c.isOpen());
        ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT 1").get(5, TimeUnit.SECONDS));
        assertFalse(((PgException) e.getCause()).retriable(), "closed for good: a retry will not help");
        Thread.sleep(300);
        assertEquals(List.of("lost", "reconnecting 1", "reconnecting 2", "reconnecting 3", "gave up"), ev.log, "and it does not try again");
        assertTrue(c.disconnect().isDone());
    }

    @Test
    void theClientComesBackWhenTheServerReturns() throws Exception {
        Events ev = new Events();
        PgClient c = PgClient.connect(group, config(new Backoff(Duration.ofMillis(100), Duration.ofMillis(200), 2.0, 0, 0)), ev).get(10, TimeUnit.SECONDS);
        server.stop();
        server.drop();
        waitFor(() -> ev.log.stream().filter(s -> s.startsWith("reconnecting")).count() >= 3, "a few failed attempts");
        assertFalse(c.isOpen());
        server.start();
        waitFor(c::isOpen, "the client to come back");
        assertEquals("back", ev.log.get(ev.log.size() - 1));
        assertEquals("lost", ev.log.get(0));
        c.close();
    }

    @Test
    void aRefusedLoginAndAConnectionThatNeverAnswersAreFailedAttemptsNotTheEnd() throws Exception {
        Events ev = new Events();
        PgClient c = PgClient.connect(group, config(new Backoff(Duration.ofMillis(50), Duration.ofMillis(50), 1.0, 0, 0)), ev).get(10, TimeUnit.SECONDS);
        server.script("error", "silent", "ok");            // the reconnects: the login is refused, then nothing is answered, then it works
        server.drop();
        waitFor(() -> ev.log.contains("lost"), "the client to notice the loss");     // before isOpen can mean "back": it is still true until the loss is noticed
        waitFor(c::isOpen, "the client to come back");
        assertEquals(List.of("ok", "error", "silent", "ok"), server.accepted, "four connections in all: the first login and three attempts");
        assertEquals(List.of("lost", "reconnecting 1", "reconnecting 2", "reconnecting 3", "back"), ev.log);
        c.close();
    }

    @Test
    void withoutABackOffTheClientCloses() throws Exception {
        Events ev = new Events();
        PgClient c = PgClient.connect(group, PgConfig.of("127.0.0.1", server.port, "u", "d"), ev).get(10, TimeUnit.SECONDS);
        server.drop();
        waitFor(() -> !c.isOpen(), "the loss");
        waitFor(() -> c.disconnect().isDone(), "the client to be closed");
        Thread.sleep(300);
        assertEquals(List.of("lost"), ev.log);
        assertEquals(1, server.accepted.size(), "it did not reconnect");
    }

    @Test
    void closingWhileAnAttemptIsInProgressLeavesNothingBehind() throws Exception {
        Events ev = new Events();
        PgClient c = PgClient.connect(group, config(new Backoff(Duration.ofMillis(50), Duration.ofMillis(50), 1.0, 0, 0)), ev).get(10, TimeUnit.SECONDS);
        server.script("silent");
        server.drop();
        waitFor(() -> server.accepted.size() == 2, "the attempt");
        CompletableFuture<Void> closed = c.disconnect();
        closed.get(5, TimeUnit.SECONDS);
        Thread.sleep(400);
        assertEquals(2, server.accepted.size(), "no attempt after the close");
        assertFalse(c.isOpen());
    }
}
