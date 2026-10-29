package io.hydrangea.codec;

import io.hydrangea.Connection;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

/**
 * Frames made of a 4-byte big-endian length followed by that many payload bytes.
 * Use {@link #encode(ByteBuffer)} to build outgoing frames.
 */
public class LengthPrefixedHandler extends FramingHandler {

    private final int maxFrameSize;
    private final BiConsumer<Connection, ByteBuffer> onMessage;

    public LengthPrefixedHandler(int maxFrameSize, BiConsumer<Connection, ByteBuffer> onMessage) {
        super(maxFrameSize);
        this.maxFrameSize = maxFrameSize;
        this.onMessage = onMessage;
    }

    public static ByteBuffer encode(ByteBuffer payload) {
        ByteBuffer out = ByteBuffer.allocate(4 + payload.remaining());
        out.putInt(payload.remaining()).put(payload).flip();
        return out;
    }

    @Override
    protected ByteBuffer extract(ByteBuffer in) throws FrameTooLargeException {
        if (in.remaining() < 4) {
            return null;
        }
        int len = in.getInt(in.position());
        if (len < 0 || len > maxFrameSize) {
            throw new FrameTooLargeException("frame length " + len + " exceeds " + maxFrameSize);
        }
        if (in.remaining() < 4 + len) {
            return null;
        }
        ByteBuffer frame = ByteBuffer.allocate(len);
        frame.put(in.slice(in.position() + 4, len)).flip();
        in.position(in.position() + 4 + len);
        return frame;
    }

    @Override
    protected void onFrame(Connection connection, ByteBuffer frame) {
        onMessage.accept(connection, frame);
    }
}
