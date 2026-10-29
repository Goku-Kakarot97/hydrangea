package io.hydrangea;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/** Writes made inside a read callback are sent together once it returns; none may be lost, whatever the handler does next. */
class WriteCoalescingTest {

    private static byte[] roundTrip(Handler handler, String request) throws Exception {
        EventLoopGroup group = new EventLoopGroup(1);
        try (Server server = Server.bind(group, new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), Options.defaults(), () -> handler);
             Socket s = new Socket(InetAddress.getLoopbackAddress(), ((InetSocketAddress) server.localAddress()).getPort())) {
            s.setSoTimeout(5000);
            s.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
            ByteArrayOutputStream got = new ByteArrayOutputStream();
            s.getInputStream().transferTo(got);   // until the server closes
            return got.toByteArray();
        } finally {
            group.shutdown();
        }
    }

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void manyWritesFollowedByCloseAfterFlushAllArrive() throws Exception {
        byte[] got = roundTrip(new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                for (int i = 0; i < 50; i++) {
                    c.write(bytes("line" + i + "\n"));
                }
                c.closeAfterFlush();
            }
        }, "go");
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            expected.append("line").append(i).append('\n');
        }
        assertEquals(expected.toString(), new String(got, StandardCharsets.UTF_8));
    }

    /** A heap buffer above the per-call write limit is sent in slices; what is queued behind it must not slip in between the slices. */
    @Test
    void aLargeBufferIsNotInterruptedByTheOneQueuedBehindIt() throws Exception {
        byte[] big = new byte[1_500_000];
        new java.util.Random(7).nextBytes(big);
        byte[] got = roundTrip(new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                c.write(ByteBuffer.wrap(big));
                c.write(bytes("TRAILER"));
                c.write(ByteBuffer.wrap(big));
                c.closeAfterFlush();
            }
        }, "go");
        byte[] expected = new byte[big.length * 2 + 7];
        System.arraycopy(big, 0, expected, 0, big.length);
        System.arraycopy("TRAILER".getBytes(StandardCharsets.UTF_8), 0, expected, big.length, 7);
        System.arraycopy(big, 0, expected, big.length + 7, big.length);
        assertEquals(expected.length, got.length);
        assertArrayEquals(expected, got);
    }

    @Test
    void aWriteFollowedByAnImmediateCloseStillGoesOut() throws Exception {
        byte[] got = roundTrip(new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                c.write(bytes("goodbye"));
                c.close();
            }
        }, "go");
        assertEquals("goodbye", new String(got, StandardCharsets.UTF_8));
    }

    @Test
    void writesMadeLaterFromTheLoopAreNotHeldBack() throws Exception {
        byte[] got = roundTrip(new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                c.eventLoop().execute(() -> {   // not inside the read callback any more
                    c.write(bytes("late"));
                    c.closeAfterFlush();
                });
            }
        }, "go");
        assertEquals("late", new String(got, StandardCharsets.UTF_8));
    }
}
