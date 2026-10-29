package io.hydrangea.codec;

import io.hydrangea.Connection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/** Frames on {@code \n}; a preceding {@code \r} is stripped. Delivers each line as a UTF-8 string. */
public final class LineHandler extends FramingHandler {

    private final BiConsumer<Connection, String> onLine;

    public LineHandler(int maxLineLength, BiConsumer<Connection, String> onLine) {
        super(maxLineLength);
        this.onLine = onLine;
    }

    @Override
    protected ByteBuffer extract(ByteBuffer in) {
        for (int i = in.position(); i < in.limit(); i++) {
            if (in.get(i) == '\n') {
                int end = i > in.position() && in.get(i - 1) == '\r' ? i - 1 : i;
                ByteBuffer line = in.slice(in.position(), end - in.position());
                in.position(i + 1);
                return line;
            }
        }
        return null;
    }

    @Override
    protected void onFrame(Connection connection, ByteBuffer frame) {
        onLine.accept(connection, StandardCharsets.UTF_8.decode(frame).toString());
    }
}
