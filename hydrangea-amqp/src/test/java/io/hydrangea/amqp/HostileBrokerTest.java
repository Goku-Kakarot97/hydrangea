package io.hydrangea.amqp;

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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The client against a scripted broker that misbehaves: it must close, fail what is pending and never hang or buffer without limit.
 * No real RabbitMQ needed.
 */
class HostileBrokerTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(1);
    }

    @AfterAll
    static void stop() throws Exception {
        group.shutdown().get(5, TimeUnit.SECONDS);
    }

    /** What the scripted broker does once the client is connected. */
    interface Script {
        void run(Socket s, DataInputStream in, OutputStream out) throws Exception;
    }

    private static final class Fake implements AutoCloseable {
        final ServerSocket server;
        final Thread thread;

        Fake(Script script) throws IOException {
            server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            thread = new Thread(() -> {
                try (Socket s = server.accept()) {
                    script.run(s, new DataInputStream(s.getInputStream()), s.getOutputStream());
                } catch (Exception ignored) {
                    // the client hung up: that is what several scripts expect
                }
            }, "fake-broker");
            thread.setDaemon(true);
            thread.start();
        }

        AmqpConfig config() {
            return AmqpConfig.of("127.0.0.1", server.getLocalPort()).withHeartbeat(Duration.ofSeconds(1));
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    // ---- scripted broker helpers ----

    static void frame(OutputStream out, int type, int channel, byte[] payload) throws IOException {
        Wire.Writer w = new Wire.Writer(payload.length + 8);
        w.octet(type).shortInt(channel).longInt(payload.length).raw(payload, 0, payload.length).octet(0xCE);
        out.write(w.toByteArray());
        out.flush();
    }

    static byte[] readFrame(DataInputStream in) throws IOException {
        int type = in.readUnsignedByte();
        in.readUnsignedShort();
        int size = in.readInt();
        byte[] payload = new byte[size];
        in.readFully(payload);
        in.readUnsignedByte();
        return payload;
    }

    /** Protocol header, Start, StartOk, Tune, TuneOk, Open, OpenOk. */
    static void handshake(DataInputStream in, OutputStream out, int frameMax, int heartbeat) throws IOException {
        in.readFully(new byte[8]);
        Map<String, Object> props = Map.of("product", "Fake");
        frame(out, 1, 0, new Wire.Writer(64).shortInt(10).shortInt(10).octet(0).octet(9).table(props).longStr("PLAIN").longStr("en_US").toByteArray());
        readFrame(in); // StartOk
        frame(out, 1, 0, new Wire.Writer(16).shortInt(10).shortInt(30).shortInt(0).longInt(frameMax).shortInt(heartbeat).toByteArray());
        readFrame(in); // TuneOk
        readFrame(in); // Open
        frame(out, 1, 0, new Wire.Writer(16).shortInt(10).shortInt(41).shortStr("").toByteArray());
    }

    static void openChannelOk(DataInputStream in, OutputStream out, int channel) throws IOException {
        readFrame(in);
        frame(out, 1, channel, new Wire.Writer(16).shortInt(20).shortInt(11).longStr("").toByteArray());
    }

    private static void assertConnectionGone(AmqpConnection c) throws Exception {
        c.closeFuture().get(5, TimeUnit.SECONDS);
        assertFalse(c.isOpen());
    }

    // ---- tests ----

    @Test
    void aFrameWithoutTheEndMarkerClosesTheConnection() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            out.write(new byte[] {1, 0, 0, 0, 0, 0, 2, 0, 0, 0x00}); // a frame whose last byte is not 0xCE
            out.flush();
            in.read(); // wait for the client to hang up
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            assertConnectionGone(c);
        }
    }

    @Test
    void aFrameLargerThanTheNegotiatedMaximumClosesTheConnection() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 8192, 0);
            out.write(new byte[] {3, 0, 1, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff}); // claims a 2 GB body frame
            out.flush();
            in.read();
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            assertConnectionGone(c);
        }
    }

    @Test
    void aFrameForAChannelThatWasNeverOpenedClosesTheConnection() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            frame(out, 1, 7, new Wire.Writer(16).shortInt(20).shortInt(11).longStr("").toByteArray());
            in.read();
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            assertConnectionGone(c);
        }
    }

    @Test
    void aMessageSizeBeyondTheLimitClosesTheConnectionWithoutAllocatingIt() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            openChannelOk(in, out, 1);
            readFrame(in); // Basic.Get
            frame(out, 1, 1, new Wire.Writer(32).shortInt(60).shortInt(71).longLong(1).octet(0).shortStr("").shortStr("q").longInt(0).toByteArray());
            frame(out, 2, 1, new Wire.Writer(32).shortInt(60).shortInt(0).longLong(1L << 40).shortInt(0).toByteArray()); // one terabyte
            in.read();
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            CompletableFuture<Delivery> get = ch.basicGet("q", true);
            CompletionException e = assertThrows(CompletionException.class, get::join);
            assertInstanceOf(AmqpException.class, e.getCause());
            assertConnectionGone(c);
        }
    }

    @Test
    void aBodyLongerThanAnnouncedClosesTheConnection() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            openChannelOk(in, out, 1);
            readFrame(in);
            frame(out, 1, 1, new Wire.Writer(32).shortInt(60).shortInt(71).longLong(1).octet(0).shortStr("").shortStr("q").longInt(0).toByteArray());
            frame(out, 2, 1, new Wire.Writer(32).shortInt(60).shortInt(0).longLong(3).shortInt(0).toByteArray());
            frame(out, 3, 1, new byte[10]);
            in.read();
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            assertThrows(CompletionException.class, () -> ch.basicGet("q", true).join());
            assertConnectionGone(c);
        }
    }

    @Test
    void anAnswerThatDoesNotMatchTheRequestClosesTheConnection() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            openChannelOk(in, out, 1);
            readFrame(in); // Queue.Declare
            frame(out, 1, 1, new Wire.Writer(16).shortInt(40).shortInt(11).toByteArray()); // Exchange.DeclareOk instead
            in.read();
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            assertThrows(CompletionException.class, () -> ch.queueDeclare().join());
            assertConnectionGone(c);
        }
    }

    @Test
    void aBrokerThatOffersAnotherProtocolVersionIsRefused() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            in.readFully(new byte[8]);
            out.write(new byte[] {'A', 'M', 'Q', 'P', 0, 1, 0, 0}); // AMQP 1.0
            out.flush();
            in.read();
        })) {
            CompletionException e = assertThrows(CompletionException.class, () -> AmqpConnection.connect(group, f.config()).join());
            assertEquals(AmqpException.NOT_IMPLEMENTED, ((AmqpException) e.getCause()).replyCode());
        }
    }

    @Test
    void aBrokerThatNeverAnswersTheHandshakeTimesOut() throws Exception {
        try (Fake f = new Fake((s, in, out) -> in.read())) {
            AmqpConfig cfg = f.config();
            AmqpConfig fast = new AmqpConfig(cfg.host(), cfg.port(), "guest", "guest", "/", null, 131072, 0, 1 << 20, Duration.ofMillis(700), null, null);
            long t0 = System.nanoTime();
            assertThrows(CompletionException.class, () -> AmqpConnection.connect(group, fast).join());
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 3000, "gave up after " + ms + " ms");
        }
    }

    @Test
    void aBrokerThatGoesSilentIsDetectedByTheHeartbeat() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 1); // one second heartbeat, then nothing (the client's own heartbeats are read and ignored)
            while (in.read() >= 0) {
                // stay silent
            }
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config()).get(5, TimeUnit.SECONDS);
            assertEquals(1, c.heartbeat().toSeconds());
            long t0 = System.nanoTime();
            c.closeFuture().get(10, TimeUnit.SECONDS);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms >= 1500 && ms < 6000, "noticed after " + ms + " ms (two intervals expected)");
        }
    }

    @Test
    void whenWritableWaitsForABrokerThatStopsReadingAndFailsWhenTheConnectionDies() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            openChannelOk(in, out, 1);
            Thread.sleep(2500); // answers the channel, then reads nothing: the socket buffers fill up
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            AmqpChannel ch = c.openChannel().get(5, TimeUnit.SECONDS);
            byte[] mb = new byte[1 << 20];
            for (int i = 0; i < 64; i++) {
                ch.basicPublish("", "q", false, null, mb);
            }
            Thread.sleep(300);
            assertFalse(c.isWritable(), "64 MiB cannot be in the socket buffers");
            CompletableFuture<Void> w = c.whenWritable();
            Thread.sleep(300);
            assertFalse(w.isDone(), "still backed up, so still waiting");
            CompletionException e = assertThrows(CompletionException.class, () -> w.orTimeout(10, TimeUnit.SECONDS).join());
            assertInstanceOf(AmqpException.class, e.getCause());
            assertTrue(((AmqpException) e.getCause()).isConnectionError());
            assertThrows(CompletionException.class, () -> c.whenWritable().join(), "a closed connection is not writable, and says so");
        }
    }

    @Test
    void theBrokerClosingTheConnectionIsAnsweredAndReported() throws Exception {
        try (Fake f = new Fake((s, in, out) -> {
            handshake(in, out, 131072, 0);
            frame(out, 1, 0, new Wire.Writer(64).shortInt(10).shortInt(50).shortInt(320).shortStr("CONNECTION_FORCED - shutdown").shortInt(0).shortInt(0).toByteArray());
            byte[] answer = readFrame(in);
            assertEquals(10, ((answer[0] & 0xff) << 8) | (answer[1] & 0xff));
            assertEquals(51, ((answer[2] & 0xff) << 8) | (answer[3] & 0xff)); // Connection.CloseOk
        })) {
            AmqpConnection c = AmqpConnection.connect(group, f.config().withHeartbeat(null)).get(5, TimeUnit.SECONDS);
            assertConnectionGone(c);
        }
    }
}
