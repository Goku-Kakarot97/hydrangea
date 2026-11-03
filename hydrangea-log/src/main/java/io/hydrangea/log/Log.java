package io.hydrangea.log;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The logging system: loggers by name, the live configuration, the handlers registered per level, flush and shutdown.
 * <p>
 * At first use the configuration comes from {@code hydrangea-log.properties} on the classpath (or the file named by the system
 * property {@code hydrangea.log.config}) with {@code hydrangea.log.*} system properties on top; without any, INFO to the console,
 * asynchronously. {@link #configure()} changes it from code at any time. See {@link LogConfig} for the keys and the builder.
 * <p>
 * Registering by level:
 * <pre>{@code
 * Log.on(Level.ERROR, (e, next) -> pager.alert(e.logger(), e.message()));   // ERROR and above
 * Log.onExactly(Level.WARN, LogStage.each(e -> warnings.increment()));      // WARN only
 * Registration r = Log.on(Level.DEBUG, Appender.file(debugFile));            // a second output for DEBUG and above
 * r.cancel();
 * }</pre>
 * Registered handlers run after the main chain, on the log thread in async mode, and see every event of their levels whatever the
 * main chain did with it.
 */
public final class Log {

    private static final Object LOCK = new Object();
    private static final ConcurrentHashMap<String, Logger> LOGGERS = new ConcurrentHashMap<>();
    private static final List<Routes.Subscription> SUBSCRIPTIONS = new ArrayList<>();
    private static final AtomicLong ERRORS = new AtomicLong();
    private static final AtomicLong REPORTED = new AtomicLong();

    private static volatile Level root = Level.INFO;
    private static volatile Map<String, Level> levels = Map.of();
    private static volatile Routes routes = new Routes(LogChain.END, List.of());
    private static volatile Dispatcher dispatcher = new Dispatcher(false, 0, false);
    private static List<LogStage> owned = List.of();
    private static boolean shuttingDown;

    static {
        LogConfig config;
        try {
            config = LogProperties.load();
        } catch (RuntimeException e) {
            warn("configuration ignored: " + e);
            config = new LogConfig();
        }
        install(config);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread(Log::jvmShutdown, "hydrangea-log-shutdown"));
        } catch (IllegalStateException | SecurityException ignored) {
            // the JVM is already shutting down, or a security manager forbids it: shutdown() is still available to the application
        }
    }

    private Log() {
    }

    /** The logger with that name; the same object every time. */
    public static Logger logger(String name) {
        Objects.requireNonNull(name, "name");
        Logger l = LOGGERS.get(name);
        return l != null ? l : LOGGERS.computeIfAbsent(name, n -> new Logger(n, thresholdFor(n)));
    }

    /** A new configuration to fill and {@link LogConfig#apply apply}. */
    public static LogConfig configure() {
        return new LogConfig();
    }

    /** Sets the root level. */
    public static void level(Level level) {
        Objects.requireNonNull(level, "level");
        synchronized (LOCK) {
            root = level;
            relevel();
        }
    }

    /** Sets the level for a logger name and everything under it; {@code null} removes that setting. */
    public static void level(String logger, Level level) {
        Objects.requireNonNull(logger, "logger");
        synchronized (LOCK) {
            Map<String, Level> m = new HashMap<>(levels);
            if (level == null) {
                m.remove(logger);
            } else {
                m.put(logger, level);
            }
            levels = Map.copyOf(m);
            relevel();
        }
    }

    /** The effective level for a logger name. */
    public static Level levelOf(String logger) {
        return Level.VALUES[thresholdFor(logger)];
    }

    /** Registers a handler for events at {@code level} and above. */
    public static Registration on(Level level, LogStage handler) {
        return subscribe(level.ordinal(), Level.ERROR.ordinal(), handler);
    }

    /** Registers a handler for events at exactly {@code level}. */
    public static Registration onExactly(Level level, LogStage handler) {
        return subscribe(level.ordinal(), level.ordinal(), handler);
    }

    private static Registration subscribe(int from, int to, LogStage handler) {
        Objects.requireNonNull(handler, "handler");
        if (from > to || from >= Level.OFF.ordinal()) {
            throw new IllegalArgumentException("cannot register at " + Level.VALUES[from]);
        }
        Routes.Subscription s = new Routes.Subscription(from, to, LogChain.of(handler));
        synchronized (LOCK) {
            SUBSCRIPTIONS.add(s);
            routes = new Routes(routes.main, List.copyOf(SUBSCRIPTIONS));
        }
        return () -> {
            synchronized (LOCK) {
                if (SUBSCRIPTIONS.remove(s)) {
                    routes = new Routes(routes.main, List.copyOf(SUBSCRIPTIONS));
                }
            }
        };
    }

    /** Waits (up to 10 seconds) until everything logged so far has been written and the appenders flushed. {@code false} on timeout. */
    public static boolean flush() {
        return flush(Duration.ofSeconds(10));
    }

    public static boolean flush(Duration timeout) {
        return dispatcher.flush(timeout.toNanos());
    }

    /**
     * Drains the queue, flushes the appenders and stops the log thread; from then on events are written on the calling thread until
     * a configuration is applied again. Run by a JVM shutdown hook, so a normal exit loses nothing; call it yourself when you stop
     * the application another way.
     */
    public static void shutdown() {
        dispatcher.shutdown();
    }

    /** The shutdown hook: from here on no configuration starts a log thread, since the JVM is going away. */
    private static void jvmShutdown() {
        synchronized (LOCK) {
            shuttingDown = true;
        }
        shutdown();
    }

    /** Back to the built-in defaults (INFO, console, async), every registered handler removed. For tests. */
    public static void reset() {
        synchronized (LOCK) {
            SUBSCRIPTIONS.clear();
        }
        install(new LogConfig());
    }

    public static LogStats stats() {
        return dispatcher.stats(ERRORS.get());
    }

    static Routes routes() {
        return routes;
    }

    static void publish(LogEvent event) {
        dispatcher.publish(event);
    }

    static void install(LogConfig config) {
        List<LogStage> created = new ArrayList<>();
        List<LogStage> stages = config.build(created);
        Dispatcher old;
        List<LogStage> previous;
        Routes before;
        synchronized (LOCK) {
            root = config.root;
            levels = Map.copyOf(config.levels);
            relevel();
            before = routes;
            previous = owned;
            owned = created;
            routes = new Routes(LogChain.of(stages), List.copyOf(SUBSCRIPTIONS));
            Dispatcher current = dispatcher;
            boolean async = config.async && !shuttingDown;
            if (current.async != async || current.isStopped()
                    || (async && (current.stats(0).capacity() != roundUp(config.buffer) || current.dropWhenFull != config.dropWhenFull))) {
                dispatcher = new Dispatcher(async, config.buffer, config.dropWhenFull);
                old = current;
            } else {
                old = null;
            }
        }
        if (old != null) {
            old.shutdown();         // drains into the new routes
        }
        before.flushAll();
        for (LogStage s : previous) {
            if (!stages.contains(s)) {
                try {
                    s.close();
                } catch (RuntimeException e) {
                    failure(s + " close", e);
                }
            }
        }
    }

    private static int roundUp(int capacity) {
        int cap = 2;
        while (cap < capacity) {
            cap <<= 1;
        }
        return cap;
    }

    private static int thresholdFor(String name) {
        Map<String, Level> m = levels;
        if (!m.isEmpty()) {
            String n = name;
            for (;;) {
                Level l = m.get(n);
                if (l != null) {
                    return l.ordinal();
                }
                int dot = n.lastIndexOf('.');
                if (dot < 0) {
                    break;
                }
                n = n.substring(0, dot);
            }
        }
        return root.ordinal();
    }

    private static void relevel() {
        for (Logger l : LOGGERS.values()) {
            l.threshold(thresholdFor(l.name()));
        }
    }

    /** A stage or appender failed: counted; the first few and then one in a thousand go to {@code System.err}. */
    static void failure(String what, Throwable t) {
        ERRORS.incrementAndGet();
        long n = REPORTED.incrementAndGet();
        if (n <= 5 || n % 1000 == 0) {
            warn(what + " failed (" + n + " so far): " + t);
        }
    }

    static void warn(String message) {
        System.err.println("hydrangea-log: " + message);
    }
}
