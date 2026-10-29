package io.hydrangea.pipeline;

import io.hydrangea.codec.Accumulator;
import io.hydrangea.codec.FrameTooLargeException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Inbound: bytes → UTF-8 {@code String} lines (split on {@code \n}, trailing {@code \r} stripped).
 * Outbound: {@code CharSequence} → UTF-8 bytes terminated by {@code \n}.
 */
public final class LineStage implements Stage {

    private final int maxLineLength;
    private final Accumulator acc = new Accumulator();

    public LineStage(int maxLineLength) {
        this.maxLineLength = maxLineLength;
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        acc.append((ByteBuffer) msg);
        ByteBuffer in = acc.buffer();
        int scanned = in.position();
        while (true) {
            int nl = -1;
            for (int i = scanned; i < in.limit(); i++) {
                if (in.get(i) == '\n') {
                    nl = i;
                    break;
                }
            }
            if (nl < 0) {
                break;
            }
            int end = nl > in.position() && in.get(nl - 1) == '\r' ? nl - 1 : nl;
            String line = StandardCharsets.UTF_8.decode(in.slice(in.position(), end - in.position())).toString();
            in.position(nl + 1);
            scanned = in.position();
            ctx.fireRead(line);
        }
        if (in.remaining() > maxLineLength) {
            throw new FrameTooLargeException("line exceeds " + maxLineLength + " bytes");
        }
    }

    @Override
    public void onWrite(Context ctx, Object msg) throws Exception {
        ctx.write(StandardCharsets.UTF_8.encode(msg + "\n"));
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        acc.release();
        ctx.fireClose(cause);
    }
}
