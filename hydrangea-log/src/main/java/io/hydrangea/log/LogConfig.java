package io.hydrangea.log;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the configuration and {@link #apply applies} it: levels, sync or async, and the main chain of stages.
 *
 * <pre>{@code
 * Log.configure()
 *    .level(Level.INFO)
 *    .level("io.hydrangea.http", Level.DEBUG)
 *    .format(LogFormat.json())                     // for the console() / file() that follow
 *    .stage(LogStage.map(Redact::passwords))        // stages run in the order given
 *    .console()
 *    .file(Path.of("/var/log/app.log"))
 *    .apply();
 * }</pre>
 *
 * Applying again replaces levels and the main chain; handlers registered with {@link Log#on} stay. Without any appender the
 * console is used. Nothing here can load a class or fetch anything: a configuration is data.
 */
public final class LogConfig {

    Level root = Level.INFO;
    final Map<String, Level> levels = new LinkedHashMap<>();
    boolean async = true;
    int buffer = 1 << 16;
    boolean dropWhenFull;
    LogFormat format = LogFormat.line();
    final List<Object> chain = new ArrayList<>();

    /** The level loggers get unless a more specific one is set: {@code INFO} by default. */
    public LogConfig level(Level root) {
        this.root = Objects.requireNonNull(root, "root");
        return this;
    }

    /** The level for loggers with this name or under it ({@code "io.hydrangea"} covers {@code io.hydrangea.http.HttpServerStage}). */
    public LogConfig level(String logger, Level level) {
        levels.put(Objects.requireNonNull(logger, "logger"), Objects.requireNonNull(level, "level"));
        return this;
    }

    /** Whether events go through the ring buffer and the log thread ({@code true}, the default) or are written by the logging thread. */
    public LogConfig async(boolean async) {
        this.async = async;
        return this;
    }

    /** The ring buffer's size in events (rounded up to a power of two; 65536 by default). */
    public LogConfig buffer(int events) {
        if (events < 2) {
            throw new IllegalArgumentException("buffer must hold at least 2 events");
        }
        this.buffer = events;
        return this;
    }

    /** When the buffer is full: drop the event (counted in {@link Log#stats}) instead of waiting for room, which is the default. */
    public LogConfig dropWhenFull(boolean drop) {
        this.dropWhenFull = drop;
        return this;
    }

    /** The format for the {@link #console()}, {@link #stderr()} and {@link #file(Path)} added after this call. */
    public LogConfig format(LogFormat format) {
        this.format = Objects.requireNonNull(format, "format");
        return this;
    }

    /** Adds a stage to the end of the main chain. */
    public LogConfig stage(LogStage stage) {
        chain.add(Objects.requireNonNull(stage, "stage"));
        return this;
    }

    /** Adds stages to the end of the main chain, in order. */
    public LogConfig chain(LogStage... stages) {
        for (LogStage s : stages) {
            stage(s);
        }
        return this;
    }

    public LogConfig console() {
        chain.add(new Spec(Spec.Kind.CONSOLE, format, null, 0, 0));
        return this;
    }

    public LogConfig stderr() {
        chain.add(new Spec(Spec.Kind.STDERR, format, null, 0, 0));
        return this;
    }

    public LogConfig file(Path file) {
        return file(file, FileAppender.DEFAULT_MAX_BYTES, FileAppender.DEFAULT_MAX_FILES);
    }

    /** @see FileAppender#FileAppender(Path, LogFormat, long, int) */
    public LogConfig file(Path file, long maxBytes, int maxFiles) {
        chain.add(new Spec(Spec.Kind.FILE, format, Objects.requireNonNull(file, "file"), maxBytes, maxFiles));
        return this;
    }

    /** Makes this the live configuration. */
    public void apply() {
        Log.install(this);
    }

    /** The stages, with the console/file requests turned into appenders; the second list is what this call created and so owns. */
    List<LogStage> build(List<LogStage> owned) {
        List<LogStage> stages = new ArrayList<>();
        for (Object o : chain) {
            if (o instanceof LogStage s) {
                stages.add(s);
            } else {
                LogStage s = ((Spec) o).build();
                owned.add(s);
                stages.add(s);
            }
        }
        if (stages.isEmpty()) {
            LogStage s = Appender.console(format);
            owned.add(s);
            stages.add(s);
        }
        return stages;
    }

    /** A console/file request, with the format that was current when it was made. */
    private record Spec(Kind kind, LogFormat format, Path file, long maxBytes, int maxFiles) {
        enum Kind { CONSOLE, STDERR, FILE }

        LogStage build() {
            return switch (kind) {
                case CONSOLE -> Appender.console(format);
                case STDERR -> Appender.stderr(format);
                case FILE -> Appender.file(file, format, maxBytes, maxFiles);
            };
        }
    }
}
