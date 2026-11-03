package io.hydrangea.log;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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
