package io.hydrangea.codec;

import io.hydrangea.BufferPool;
import io.hydrangea.Connection;
import io.hydrangea.Handler;

import java.nio.ByteBuffer;

/**
 * Turns a byte stream into discrete frames. Bytes are accumulated across reads (the buffer handed to
 * {@link #onRead} is only valid during the call), so subclasses only describe where a frame ends.
 * A frame larger than {@code maxFrameSize} closes the connection, so a peer cannot make us buffer forever.
 */
public abstract class FramingHandler implements Handler {

    private final int maxFrameSize;
    private final Accumulator acc;

    protected FramingHandler(int maxFrameSize) {
        this(maxFrameSize, null);
    }

    /** @param pool where the accumulation buffer comes from / goes back to on close; may be null */
    protected FramingHandler(int maxFrameSize, BufferPool pool) {
        if (maxFrameSize < 1) {
            throw new IllegalArgumentException("maxFrameSize must be >= 1");
        }
        this.maxFrameSize = maxFrameSize;
        this.acc = new Accumulator(pool);
    }

    /**
     * Tries to cut one frame off the front of {@code in} (position..limit), advancing its position past the
     * frame. Return a buffer holding the frame payload, or {@code null} if more bytes are needed
     * (in which case the position must be left unchanged).
     */
    protected abstract ByteBuffer extract(ByteBuffer in) throws FrameTooLargeException;

    /** Called for each complete frame. The buffer is owned by the callee. */
    protected abstract void onFrame(Connection connection, ByteBuffer frame) throws Exception;

    @Override
    public final void onRead(Connection connection, ByteBuffer data) throws Exception {
        acc.append(data);
        ByteBuffer in = acc.buffer();
        ByteBuffer frame;
        while (in.hasRemaining() && (frame = extract(in)) != null) {
            onFrame(connection, frame);
        }
        if (in.remaining() > maxFrameSize) {
            throw new FrameTooLargeException("incomplete frame exceeds " + maxFrameSize + " bytes");
        }
    }

    /** Releases the accumulation buffer. Subclasses overriding this should call {@code super.onClose}. */
    @Override
    public void onClose(Connection connection, Throwable cause) {
        acc.release();
    }
}
