package io.hydrangea.pipeline;

import io.hydrangea.Connection;

import java.lang.System.Logger;
import java.nio.ByteBuffer;

/** A stage's view of the pipeline. Loop-thread only (use {@code connection().eventLoop().execute} from elsewhere). */
public final class Context {

    private static final Logger LOG = System.getLogger(Context.class.getName());

    private final Pipeline pipeline;
    private final int index;

    Context(Pipeline pipeline, int index) {
        this.pipeline = pipeline;
        this.index = index;
    }

    public Connection connection() {
        return pipeline.connection();
    }

    public void fireActive() throws Exception {
        Stage next = pipeline.stage(index + 1);
        if (next != null) {
            next.onActive(pipeline.context(index + 1));
        }
    }

    public void fireRead(Object msg) throws Exception {
        Stage next = pipeline.stage(index + 1);
        if (next != null) {
            next.onRead(pipeline.context(index + 1), msg);
        } else {
            LOG.log(Logger.Level.DEBUG, "message reached the end of the pipeline and was dropped: {0}", msg);
        }
    }

    public void fireWritabilityChanged() throws Exception {
        Stage next = pipeline.stage(index + 1);
        if (next != null) {
            next.onWritabilityChanged(pipeline.context(index + 1));
        }
    }

    public void fireDrain() throws Exception {
        Stage next = pipeline.stage(index + 1);
        if (next != null) {
            next.onDrain(pipeline.context(index + 1));
        }
    }

    public void fireClose(Throwable cause) {
        Stage next = pipeline.stage(index + 1);
        if (next != null) {
            next.onClose(pipeline.context(index + 1), cause);
        }
    }

    /** Sends {@code msg} towards the socket, through the stages before this one. */
    public void write(Object msg) throws Exception {
        if (index == 0) {
            if (msg instanceof ByteBuffer b) {
                connection().write(b);
            } else {
                throw new IllegalArgumentException("pipeline head only accepts ByteBuffer, got " + msg.getClass().getName()
                        + " (is an encoder stage missing?)");
            }
        } else {
            pipeline.stage(index - 1).onWrite(pipeline.context(index - 1), msg);
        }
    }

    /**
     * Closes the connection after all data written so far has gone out. Use this instead of
     * {@code connection().closeAfterFlush()}: that bypasses earlier stages, so a TLS stage that still holds
     * unsent data (or has not sent close_notify) would lose it.
     */
    public void closeAfterFlush() throws Exception {
        closeAfterFlush(null);
    }

    /**
     * Like {@link #closeAfterFlush()}, but lingering: after the last byte the output is half-closed and the peer's remaining
     * input is drained for up to {@code linger}, so a final message is not lost to a connection reset.
     */
    public void closeAfterFlush(java.time.Duration linger) throws Exception {
        if (index == 0) {
            connection().closeAfterFlush(linger);
        } else {
            pipeline.stage(index - 1).onCloseRequest(pipeline.context(index - 1), linger);
        }
    }

    /** Writes starting from the last stage, as application code outside any stage would. */
    public void writeFromTail(Object msg) throws Exception {
        pipeline.writeFromTail(msg);
    }
}
