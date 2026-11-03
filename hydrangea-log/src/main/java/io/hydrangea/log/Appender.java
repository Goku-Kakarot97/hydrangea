package io.hydrangea.log;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A stage that writes events somewhere, through a {@link LogFormat}, then passes them on. Formats into one reused builder under a
 * lock (uncontended in async mode, where only the log thread comes here), so a line is formatted once and allocates nothing.
 * A failing write is counted ({@link Log#stats}) and reported to {@code System.err} a few times, never thrown at the caller.
 *
 * <pre>{@code
 * Appender.console()                        // System.out, one write per batch (so redirections and test captures see the lines)
 * Appender.stderr()
 * Appender.stream(out)                      // any stream
 * Appender.file(Path.of("app.log"))         // rolls at 100 MB, keeps 10 old files
 * Appender.file(path, LogFormat.json(), 50_000_000, 5)
 * Appender.memory()                         // keeps the lines and events: for tests
 * Appender.to(line -> ...)                  // anything else
 * }</pre>
 */
public abstract class Appender implements LogStage {

    private final LogFormat format;
    private final ReentrantLock lock = new ReentrantLock();
    private StringBuilder line = new StringBuilder(256);

    protected Appender(LogFormat format) {
        this.format = Objects.requireNonNull(format, "format");
    }

    public final LogFormat format() {
        return format;
    }

    @Override
    public final void handle(LogEvent event, LogChain next) {
        lock.lock();
        try {
            StringBuilder sb = line;
            sb.setLength(0);
            format.format(event, sb);
            write(event, sb);
            if (sb.capacity() > 1 << 16) {
                line = new StringBuilder(256);
            }
        } catch (Exception e) {
            Log.failure(this + " write", e);
        } finally {
            lock.unlock();
        }
        next.next(event);
    }

    @Override
    public final void flush() {
        lock.lock();
        try {
            flushOutput();
        } catch (Exception e) {
            Log.failure(this + " flush", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void close() {
        lock.lock();
        try {
            closeOutput();
        } catch (Exception e) {
            Log.failure(this + " close", e);
        } finally {
            lock.unlock();
        }
    }

    /** Writes one formatted event (ends with {@code \n}); called under the lock. */
    protected abstract void write(LogEvent event, CharSequence text) throws IOException;

    /** Pushes buffered output out; called under the lock. */
    protected void flushOutput() throws IOException {
    }

    /** Releases what the appender holds; called under the lock. */
    protected void closeOutput() throws IOException {
    }

    public static Appender console() {
        return console(LogFormat.line());
    }

    /**
     * Standard output: lines are gathered in a buffer of this appender's own and handed to the current {@code System.out} once per
     * batch (one write and one flush, not one per line), so output stays fast and still goes wherever {@code System.out} points,
     * including a test runner's capture.
     */
    public static Appender console(LogFormat format) {
        return new StreamAppender(new BufferedOutputStream(new SystemStream(false), 1 << 15), format, false);
    }

    public static Appender stderr() {
        return stderr(LogFormat.line());
    }

    public static Appender stderr(LogFormat format) {
        return new StreamAppender(new BufferedOutputStream(new SystemStream(true), 1 << 15), format, false);
    }

    /** Any stream; it is flushed after every batch and not closed. */
    public static Appender stream(OutputStream out) {
        return stream(out, LogFormat.line());
    }

    public static Appender stream(OutputStream out, LogFormat format) {
        return new StreamAppender(out, format, false);
    }

    /** A file that rolls at 100 MB to {@code name.1} ... {@code name.10}. */
    public static FileAppender file(Path file) {
        return new FileAppender(file, LogFormat.line(), FileAppender.DEFAULT_MAX_BYTES, FileAppender.DEFAULT_MAX_FILES);
    }

    /** @see FileAppender#FileAppender(Path, LogFormat, long, int) */
    public static FileAppender file(Path file, LogFormat format, long maxBytes, int maxFiles) {
        return new FileAppender(file, format, maxBytes, maxFiles);
    }

    public static MemoryAppender memory() {
        return new MemoryAppender(LogFormat.line());
    }

    public static MemoryAppender memory(LogFormat format) {
        return new MemoryAppender(format);
    }

    /** Hands each formatted line (ending with {@code \n}) to the consumer. */
    public static Appender to(Consumer<String> consumer) {
        return to(consumer, LogFormat.line());
    }

    public static Appender to(Consumer<String> consumer, LogFormat format) {
        Objects.requireNonNull(consumer, "consumer");
        return new Appender(format) {
            @Override
            protected void write(LogEvent event, CharSequence text) {
                consumer.accept(text.toString());
            }

            @Override
            public String toString() {
                return "Appender.to(" + consumer + ")";
            }
        };
    }

    /** Forwards to whatever {@code System.out} / {@code System.err} is at the time of the write, so a later redirection is honoured. */
    private static final class SystemStream extends OutputStream {
        private final boolean err;

        SystemStream(boolean err) {
            this.err = err;
        }

        @Override
        public void write(int b) {
            (err ? System.err : System.out).write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            (err ? System.err : System.out).write(b, off, len);
        }

        @Override
        public void flush() {
            (err ? System.err : System.out).flush();
        }

        @Override
        public String toString() {
            return err ? "System.err" : "System.out";
        }
    }
}
