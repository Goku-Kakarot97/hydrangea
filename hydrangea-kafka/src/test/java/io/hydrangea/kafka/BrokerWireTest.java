package io.hydrangea.kafka;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** One broker connection against a scripted server: well-formed and hostile responses. No Kafka needed. */
class BrokerWireTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(1);
    }

    @AfterAll
    static void stop() throws Exception {
        group.shutdown().get(5, TimeUnit.SECONDS);
    }

    private interface Script {
        void run(DataInputStream in, OutputStream out) throws IOException;
    }

    private record Fake(ServerSocket server) implements AutoCloseable {
        public void close() throws IOException {
            server.close();
        }
    }

    private static Fake serve(Script script) throws IOException {
        ServerSocket ss = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
        Thread.ofPlatform().daemon().start(() -> {
            try (Socket s = ss.accept()) {
                script.run(new DataInputStream(s.getInputStream()), s.getOutputStream());
            } catch (IOException ignored) {
                // closed by the test
            }
        });
        return new Fake(ss);
    }

    /** Reads one request and returns its correlation id. */
    private static int readRequest(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = in.readNBytes(len);
        return ((b[4] & 0xFF) << 24) | ((b[5] & 0xFF) << 16) | ((b[6] & 0xFF) << 8) | (b[7] & 0xFF);
    }

    private static void respond(OutputStream out, int correlation, byte[] body) throws IOException {
        Wire.Writer w = new Wire.Writer(16);
        w.i32(4 + body.length).i32(correlation).raw(body, 0, body.length);
        out.write(w.buf, 0, w.len);
        out.flush();
    }

    private static Broker connect(Fake f, KafkaConfig cfg) throws Exception {
        EventLoop loop = group.next();
        CompletableFuture<Broker> b = new CompletableFuture<>();
        loop.execute(() -> {
            Broker br = new Broker(1, "127.0.0.1", f.server().getLocalPort(), cfg, loop);
            br.connect().whenComplete((x, e) -> {
                if (e != null) b.completeExceptionally(e); else b.complete(br);
            });
        });
        return b.get(5, TimeUnit.SECONDS);
    }

    private static CompletableFuture<Wire.Reader> send(Broker b) {
        CompletableFuture<CompletableFuture<Wire.Reader>> f = new CompletableFuture<>();
        group.next().execute(() -> f.complete(b.send(Broker.Api.METADATA, 4, w -> w.i32(-1).i8(0))));
        return f.join();
    }

    private static KafkaException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return assertInstanceOf(KafkaException.class, e.getCause());
        }
        return fail("expected a failure");
    }

    private final KafkaConfig cfg = KafkaConfig.of("x:1").withMaxResponseBytes(1 << 20).withTimeouts(Duration.ofSeconds(2), Duration.ofMillis(700));

    @Test
    void responsesAreMatchedToRequestsInOrder() throws Exception {
        try (Fake f = serve((in, out) -> {
            int a = readRequest(in);
            int b = readRequest(in);
            respond(out, a, new byte[] {0, 0, 0, 1});
            respond(out, b, new byte[] {0, 0, 0, 2});
        })) {
            Broker b = connect(f, cfg);
            CompletableFuture<Wire.Reader> r1 = send(b);
            CompletableFuture<Wire.Reader> r2 = send(b);
            assertEquals(1, r1.get(5, TimeUnit.SECONDS).i32());
            assertEquals(2, r2.get(5, TimeUnit.SECONDS).i32());
        }
    }

    @Test
    void aResponseSplitIntoBytesIsStillOneResponse() throws Exception {
        try (Fake f = serve((in, out) -> {
            int a = readRequest(in);
            Wire.Writer w = new Wire.Writer(16);
            w.i32(8).i32(a).i32(77);
            for (int i = 0; i < w.len; i++) {
                out.write(w.buf[i]);
                out.flush();
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        })) {
            assertEquals(77, send(connect(f, cfg)).get(5, TimeUnit.SECONDS).i32());
        }
    }

    @Test
    void aWrongCorrelationIdEndsTheConnection() throws Exception {
        try (Fake f = serve((in, out) -> respond(out, readRequest(in) + 5, new byte[4]))) {
            KafkaException e = failure(send(connect(f, cfg)));
            assertTrue(e.getMessage().contains("correlation"), e.getMessage());
            assertFalse(e.retriable());
        }
    }

    @Test
    void anOversizedResponseIsRefusedBeforeItIsBuffered() throws Exception {
        try (Fake f = serve((in, out) -> {
            readRequest(in);
            out.write(new byte[] {0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0, 0});   // a frame of 2 GB
            out.flush();
        })) {
            KafkaException e = failure(send(connect(f, cfg)));
            assertTrue(e.getMessage().contains("limit"), e.getMessage());
        }
    }

    @Test
    void aNegativeOrTinyLengthIsAProtocolError() throws Exception {
        for (int len : new int[] {-1, 0, 3}) {
            try (Fake f = serve((in, out) -> {
                readRequest(in);
                Wire.Writer w = new Wire.Writer(8);
                w.i32(len).i32(0);
                out.write(w.buf, 0, w.len);
                out.flush();
            })) {
                assertFalse(failure(send(connect(f, cfg))).retriable(), "length " + len);
            }
        }
    }

    @Test
    void aResponseNobodyAskedForEndsTheConnection() throws Exception {
        try (Fake f = serve((in, out) -> respond(out, 99, new byte[4]))) {
            Broker b = connect(f, cfg);
            for (int i = 0; i < 100 && b.isOnline(); i++) Thread.sleep(20);
            assertFalse(b.isOnline());
        }
    }

    @Test
    void aServerThatNeverAnswersTimesOutAndTheConnectionIsDropped() throws Exception {
        try (Fake f = serve((in, out) -> {
            readRequest(in);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ignored) {
                // test over
            }
        })) {
            Broker b = connect(f, cfg);
            KafkaException e = failure(send(b));
            assertTrue(e.retriable());
            assertTrue(e.getMessage().contains("no response"), e.getMessage());
            for (int i = 0; i < 50 && b.isOnline(); i++) Thread.sleep(20);
            assertFalse(b.isOnline(), "a late answer would be taken for the next one");
        }
    }

    @Test
    void aClosedConnectionFailsWhatWaitsRetriably() throws Exception {
        try (Fake f = serve((in, out) -> readRequest(in))) {
            KafkaException e = failure(send(connect(f, cfg)));
            assertTrue(e.retriable());
        }
    }

    @Test
    void connectingToNothingFails() throws Exception {
        EventLoop loop = group.next();
        CompletableFuture<Broker> b = new CompletableFuture<>();
        loop.execute(() -> new Broker(1, "127.0.0.1", 1, cfg, loop).connect().whenComplete((x, e) -> b.completeExceptionally(e)));
        assertTrue(failure(b).retriable());
    }
}
