package io.hydrangea.log;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * Per-thread key/value pairs copied into every event the thread logs (a request id, a user, a tenant ...): what other libraries call
 * MDC. Values are strings. Nothing is inherited by threads this thread starts; carry it over explicitly with {@link #copy} and
 * {@link #putAll}.
 *
 * <pre>{@code
 * try (var scope = LogContext.with("request", id)) {
 *     log.info("handling");             // ... request=42
 * }
 * }</pre>
 *
 * The snapshot taken when logging is cached until the next change, so a thread that logs many times between changes copies nothing.
 * <p>
 * Nothing crosses a thread by itself. When work continues elsewhere (a callback on another thread, a task on an executor, a
 * completion of a future), carry the context over explicitly:
 * <pre>{@code
 * executor.execute(LogContext.wrap(() -> log.info("still has the trace id")));
 * Executor traced = LogContext.wrap(executor);                               // every task submitted carries the submitter's context
 * LogContext.Snapshot snap = LogContext.capture();                           // ... later, on any thread:
 * try (var scope = snap.attach()) { log.info("same context"); }
 * }</pre>
 * In an event loop that serves many requests on one thread, open a scope per request (see {@code HttpRouter}, which does it for you).
 */
public final class LogContext {

    private static final ThreadLocal<Ctx> CTX = new ThreadLocal<>();

    private LogContext() {
    }

    /** Sets a value; {@code null} removes the key. */
    public static void put(String key, String value) {
        Objects.requireNonNull(key, "key");
        if (value == null) {
            remove(key);
            return;
        }
        Ctx c = CTX.get();
        if (c == null) {
            c = new Ctx();
            CTX.set(c);
        }
        c.put(key, value);
    }

    public static String get(String key) {
        Ctx c = CTX.get();
        return c == null ? null : c.get(key);
    }

    public static void remove(String key) {
        Ctx c = CTX.get();
        if (c != null) {
            c.remove(key);
        }
    }

    public static void clear() {
        Ctx c = CTX.get();
        if (c != null) {
            c.clear();
        }
    }

    /** The current entries, as a new map; empty when none. */
    public static Map<String, String> copy() {
        Ctx c = CTX.get();
        if (c == null || c.size == 0) {
            return Collections.emptyMap();
        }
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < c.size; i += 2) {
            m.put(c.kv[i], c.kv[i + 1]);
        }
        return m;
    }

    public static void putAll(Map<String, String> values) {
        values.forEach(LogContext::put);
    }

    /** Sets a value until the scope closes, when the previous value (or absence) comes back. */
    public static Scope with(String key, String value) {
        String previous = get(key);
        put(key, value);
        return () -> {
            if (previous == null) {
                remove(key);
            } else {
                put(key, previous);
            }
        };
    }

    /** The current context, to attach on another thread (or on this one later). */
    public static Snapshot capture() {
        String[] kv = snapshot();
        return kv == null ? Snapshot.EMPTY : new Snapshot(kv);
    }

    /** A runnable that runs with the context of the thread that called {@code wrap}, then restores what the running thread had. */
    public static Runnable wrap(Runnable task) {
        Objects.requireNonNull(task, "task");
        Snapshot snap = capture();
        return () -> {
            try (Scope ignored = snap.attach()) {
                task.run();
            }
        };
    }

    public static <T> Callable<T> wrap(Callable<T> task) {
        Objects.requireNonNull(task, "task");
        Snapshot snap = capture();
        return () -> {
            try (Scope ignored = snap.attach()) {
                return task.call();
            }
        };
    }

    /** An executor whose tasks run with the context of whoever submitted them. */
    public static Executor wrap(Executor executor) {
        Objects.requireNonNull(executor, "executor");
        return task -> executor.execute(wrap(task));
    }

    /** A frozen copy of a thread's context, taken with {@link #capture}. Immutable, safe to share. */
    public static final class Snapshot {
        static final Snapshot EMPTY = new Snapshot(new String[0]);
        private final String[] kv;

        private Snapshot(String[] kv) {
            this.kv = kv;
        }

        public boolean isEmpty() {
            return kv.length == 0;
        }

        public Map<String, String> asMap() {
            Map<String, String> m = new LinkedHashMap<>();
            for (int i = 0; i < kv.length; i += 2) {
                m.put(kv[i], kv[i + 1]);
            }
            return m;
        }

        /** Replaces the current thread's context with this one until the scope closes, when the previous context comes back. */
        public Scope attach() {
            Ctx c = CTX.get();
            String[] previous = c == null ? null : c.snapshot();
            if (c == null) {
                if (kv.length == 0) {
                    return () -> { };
                }
                c = new Ctx();
                CTX.set(c);
            }
            c.replace(kv);
            Ctx target = c;
            return () -> target.replace(previous == null ? new String[0] : previous);
        }
    }

    /** Alternating key, value, or {@code null} when empty. The array is shared and must not be modified. */
    static String[] snapshot() {
        Ctx c = CTX.get();
        return c == null ? null : c.snapshot();
    }

    public interface Scope extends AutoCloseable {
        @Override
        void close();
    }

    private static final class Ctx {
        String[] kv = new String[8];
        int size;
        String[] frozen;

        void put(String key, String value) {
            int i = indexOf(key);
            if (i >= 0) {
                if (value.equals(kv[i + 1])) {
                    return;
                }
                kv[i + 1] = value;
            } else {
                if (size == kv.length) {
                    kv = Arrays.copyOf(kv, size * 2);
                }
                kv[size++] = key;
                kv[size++] = value;
            }
            frozen = null;
        }

        String get(String key) {
            int i = indexOf(key);
            return i < 0 ? null : kv[i + 1];
        }

        void remove(String key) {
            int i = indexOf(key);
            if (i < 0) {
                return;
            }
            System.arraycopy(kv, i + 2, kv, i, size - i - 2);
            size -= 2;
            kv[size] = null;
            kv[size + 1] = null;
            frozen = null;
        }

        void clear() {
            Arrays.fill(kv, 0, size, null);
            size = 0;
            frozen = null;
        }

        /** Makes this context a copy of {@code pairs} (alternating key, value). */
        void replace(String[] pairs) {
            if (kv.length < pairs.length) {
                kv = new String[Math.max(8, pairs.length)];
            } else {
                Arrays.fill(kv, 0, size, null);
            }
            System.arraycopy(pairs, 0, kv, 0, pairs.length);
            size = pairs.length;
            frozen = null;
        }

        String[] snapshot() {
            if (size == 0) {
                return null;
            }
            String[] f = frozen;
            if (f == null) {
                f = Arrays.copyOf(kv, size);
                frozen = f;
            }
            return f;
        }

        private int indexOf(String key) {
            for (int i = 0; i < size; i += 2) {
                if (key.equals(kv[i])) {
                    return i;
                }
            }
            return -1;
        }
    }
}
