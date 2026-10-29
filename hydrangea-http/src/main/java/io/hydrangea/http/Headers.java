package io.hydrangea.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/** Ordered, case-insensitive, multi-valued HTTP header collection. Small by design: lookups are linear. */
public final class Headers {

    private final List<Map.Entry<String, String>> entries = new ArrayList<>();

    public Headers add(String name, String value) {
        entries.add(Map.entry(name, value));
        return this;
    }

    /** Replaces all values of {@code name}. */
    public Headers set(String name, String value) {
        remove(name);
        return add(name, value);
    }

    public Headers remove(String name) {
        entries.removeIf(e -> e.getKey().equalsIgnoreCase(name));
        return this;
    }

    /** {@code s} in lower case; the same instance when it has no upper-case letters (HTTP/2 and most HTTP/1 header names), without a Locale lookup or a copy. */
    public static String lower(String s) {
        for (int i = 0, n = s.length(); i < n; i++) {
            char c = s.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                return s.toLowerCase(java.util.Locale.ROOT);
            }
        }
        return s;
    }

    /** First value, or null. */
    public String get(String name) {
        for (var e : entries) {
            if (e.getKey().equalsIgnoreCase(name)) {
                return e.getValue();
            }
        }
        return null;
    }

    public List<String> getAll(String name) {
        List<String> out = new ArrayList<>(2);
        for (var e : entries) {
            if (e.getKey().equalsIgnoreCase(name)) {
                out.add(e.getValue());
            }
        }
        return out;
    }

    public boolean contains(String name) {
        return get(name) != null;
    }

    /** True if some value of {@code name} contains {@code token} as a comma-separated element (case-insensitive). */
    public boolean hasToken(String name, String token) {
        for (String v : getAll(name)) {
            for (String part : v.split(",")) {
                if (part.trim().equalsIgnoreCase(token)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void forEach(BiConsumer<String, String> action) {
        entries.forEach(e -> action.accept(e.getKey(), e.getValue()));
    }

    public Headers copy() {
        Headers h = new Headers();
        h.entries.addAll(entries);
        return h;
    }

    public int size() {
        return entries.size();
    }

    public List<Map.Entry<String, String>> entries() {
        return Collections.unmodifiableList(entries);
    }

    @Override
    public String toString() {
        return entries.toString();
    }
}
