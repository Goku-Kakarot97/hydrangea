package io.hydrangea.pipeline;

import io.hydrangea.codec.Accumulator;
import io.hydrangea.codec.FrameTooLargeException;

import java.nio.ByteBuffer;

/**
 * Inbound: bytes → {@link ByteBuffer} frames (4-byte big-endian length + payload).
 * Outbound: {@link ByteBuffer} payload → length-prefixed bytes.
 */
public final class LengthPrefixStage implements Stage {

    private final int maxFrameSize;
    private final Accumulator acc = new Accumulator();

    public LengthPrefixStage(int maxFrameSize) {
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        acc.append((ByteBuffer) msg);
        ByteBuffer in = acc.buffer();
        while (in.remaining() >= 4) {
            int len = in.getInt(in.position());
            if (len < 0 || len > maxFrameSize) {
                throw new FrameTooLargeException("frame length " + len + " exceeds " + maxFrameSize);
            }
            if (in.remaining() < 4 + len) {
                return;
            }
            ByteBuffer frame = ByteBuffer.allocate(len);
            frame.put(in.slice(in.position() + 4, len)).flip();
            in.position(in.position() + 4 + len);
            ctx.fireRead(frame);
        }
    }

    @Override
    public void onWrite(Context ctx, Object msg) throws Exception {
        ByteBuffer payload = (ByteBuffer) msg;
        ByteBuffer out = ByteBuffer.allocate(4 + payload.remaining());
        out.putInt(payload.remaining()).put(payload).flip();
        ctx.write(out);
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        acc.release();
        ctx.fireClose(cause);
    }
}
