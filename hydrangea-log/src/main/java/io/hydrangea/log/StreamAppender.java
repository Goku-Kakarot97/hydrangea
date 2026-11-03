package io.hydrangea.log;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/** Writes to an {@link OutputStream} as UTF-8; see {@link Appender#console()} and {@link Appender#stream(OutputStream)}. */
public final class StreamAppender extends Appender {

    private final OutputStream out;
    private final boolean closeStream;
    private byte[] buf = new byte[4096];
    private final int[] length = new int[1];

    /** @param closeStream whether {@link #close()} closes the stream (it always flushes it) */
    public StreamAppender(OutputStream out, LogFormat format, boolean closeStream) {
        super(format);
        this.out = Objects.requireNonNull(out, "out");
        this.closeStream = closeStream;
    }

    @Override
    protected void write(LogEvent event, CharSequence text) throws IOException {
        buf = Utf8.encode(text, buf, length);
        out.write(buf, 0, length[0]);
        buf = Utf8.shrink(buf);
    }

    @Override
    protected void flushOutput() throws IOException {
        out.flush();
    }

    @Override
    protected void closeOutput() throws IOException {
        if (closeStream) {
            out.close();
        } else {
            out.flush();
        }
    }

    @Override
    public String toString() {
        return "StreamAppender(" + out + ")";
    }
}
