package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** The pool's ping against a server that logs in and then says nothing: the dead-but-open connection a firewall leaves behind. */
class PgPoolPingTest {

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

    @Test
    void anIdleConnectionThatDoesNotAnswerAPingIsClosedAndReplaced() throws Exception {
        try (ServerSocket server = new ServerSocket(0, 5, InetAddress.getLoopbackAddress()); EventLoopGroup group = new EventLoopGroup(1)) {
            AtomicInteger connections = new AtomicInteger();
            CompletableFuture<Long> firstClosedAfter = new CompletableFuture<>();
            Thread acceptor = Thread.ofVirtual().start(() -> {
                try {
                    while (true) {
                        Socket s = server.accept();
                        int n = connections.incrementAndGet();
                        Thread.ofVirtual().start(() -> {
                            try (s) {
                                DataInputStream in = new DataInputStream(s.getInputStream());
                                in.readFully(new byte[in.readInt() - 4]);      // the startup message
                                s.getOutputStream().write(msg('R', new byte[]{0, 0, 0, 0}));
                                s.getOutputStream().write(msg('K', new byte[]{0, 0, 0, 7, 0, 0, 0, 8}));
                                s.getOutputStream().write(msg('Z', new byte[]{'I'}));
                                s.getOutputStream().flush();
                                long t0 = System.nanoTime();
                                while (in.read() >= 0) {
                                    // takes in what the client sends (the pings) and answers nothing
                                }
                                if (n == 1) {
                                    firstClosedAfter.complete((System.nanoTime() - t0) / 1_000_000);
                                }
                            } catch (IOException e) {
                                if (n == 1) {
                                    firstClosedAfter.complete(-1L);
                                }
                            }
                        });
                    }
                } catch (IOException e) {
                    // the test is over
                }
            });
            PgConfig config = PgConfig.of("127.0.0.1", server.getLocalPort(), "u", "d");
            PgPoolOptions opts = PgPoolOptions.defaults().withMaxLifetime(null).withPing(Duration.ofMillis(200), Duration.ofMillis(300));
            try (PgPool pool = PgPool.create(group, config, 1, opts, null).get(10, TimeUnit.SECONDS)) {
                long closedAfter = firstClosedAfter.get(10, TimeUnit.SECONDS);
                assertTrue(closedAfter >= 200 && closedAfter < 5000, "the silent connection was closed after about interval + timeout: " + closedAfter + " ms");
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (connections.get() < 2 && System.nanoTime() < end) {
                    Thread.sleep(20);
                }
                assertTrue(connections.get() >= 2, "and a new connection was made in its place");
            }
            server.close();
            acceptor.join(2000);
        }
    }
}
