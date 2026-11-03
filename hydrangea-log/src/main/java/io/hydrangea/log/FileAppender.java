package io.hydrangea.log;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Appends to a file and rolls it by size: when a write would take the file past {@code maxBytes}, {@code app.log} becomes
 * {@code app.log.1}, the old {@code .1} becomes {@code .2} ... and the oldest beyond {@code maxFiles} is deleted. Opens the file
 * (creating directories) on construction and appends to what is there. Output goes through a 64 KB buffer flushed at the end of
 * every batch (async) or every event (sync).
 */
public final class FileAppender extends Appender {

    public static final long DEFAULT_MAX_BYTES = 100L << 20;
    public static final int DEFAULT_MAX_FILES = 10;

    private final Path file;
    private final long maxBytes;
    private final int maxFiles;
    private OutputStream out;
    private long size;
    private byte[] buf = new byte[4096];
    private final int[] length = new int[1];
    private boolean closed;

    /**
     * @param maxBytes roll when the file would grow past this; {@code 0} never rolls
     * @param maxFiles how many rolled files to keep ({@code 0}: the rolled file is deleted at once)
     */
    public FileAppender(Path file, LogFormat format, long maxBytes, int maxFiles) {
        super(format);
        this.file = Objects.requireNonNull(file, "file").toAbsolutePath();
        if (maxBytes < 0 || maxFiles < 0) {
            throw new IllegalArgumentException("maxBytes and maxFiles must be >= 0");
        }
        this.maxBytes = maxBytes;
        this.maxFiles = maxFiles;
        try {
            open();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot open log file " + this.file, e);
        }
    }

    public Path file() {
        return file;
    }

    private void open() throws IOException {
        Path dir = file.getParent();
        if (dir != null) {
            Files.createDirectories(dir);
        }
        out = new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND), 1 << 16);
        size = Files.exists(file) ? Files.size(file) : 0;
    }

    @Override
    protected void write(LogEvent event, CharSequence text) throws IOException {
        if (closed) {
            return;
        }
        buf = Utf8.encode(text, buf, length);
        int n = length[0];
        if (maxBytes > 0 && size > 0 && size + n > maxBytes) {
            roll();
        }
        out.write(buf, 0, n);
        size += n;
        buf = Utf8.shrink(buf);
    }

    private void roll() throws IOException {
        out.close();
        if (maxFiles == 0) {
            Files.deleteIfExists(file);
        } else {
            Files.deleteIfExists(rolled(maxFiles));
            for (int i = maxFiles - 1; i >= 1; i--) {
                Path from = rolled(i);
                if (Files.exists(from)) {
                    Files.move(from, rolled(i + 1), StandardCopyOption.REPLACE_EXISTING);
                }
            }
            Files.move(file, rolled(1), StandardCopyOption.REPLACE_EXISTING);
        }
        open();
    }

    private Path rolled(int i) {
        return file.resolveSibling(file.getFileName() + "." + i);
    }

    @Override
    protected void flushOutput() throws IOException {
        if (!closed) {
            out.flush();
        }
    }

    @Override
    protected void closeOutput() throws IOException {
        if (!closed) {
            closed = true;
            out.close();
        }
    }

    @Override
    public String toString() {
        return "FileAppender(" + file + ")";
    }
}
