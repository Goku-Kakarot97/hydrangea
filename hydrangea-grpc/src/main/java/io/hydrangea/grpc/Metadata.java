package io.hydrangea.grpc;

import io.hydrangea.http.Headers;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The key/value pairs that travel with a call (HTTP/2 headers and trailers). Keys are lower case; values are ASCII text, or bytes when the
 * key ends in {@code -bin} (base64 on the wire). A key may have several values. Not thread-safe.
 */
public final class Metadata {

    private record Entry(String key, Object value) {   // value: String, or byte[] for -bin keys
    }

    private final List<Entry> entries = new ArrayList<>();

    /** Adds an ASCII value. The key must not end in {@code -bin} and must not be one the protocol reserves. */
    public Metadata put(String key, String value) {
        String k = checkKey(key);
        if (k.endsWith("-bin")) {
            throw new IllegalArgumentException("binary keys (-bin) take putBinary: " + key);
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < 0x20 || c > 0x7e) {
                throw new IllegalArgumentException("metadata values are printable ASCII (use a -bin key for anything else): " + key);
            }
        }
        entries.add(new Entry(k, value));
        return this;
    }

    /** Adds a binary value under a key that ends in {@code -bin}. */
    public Metadata putBinary(String key, byte[] value) {
        String k = checkKey(key);
        if (!k.endsWith("-bin")) {
            throw new IllegalArgumentException("binary metadata keys must end in -bin: " + key);
        }
        entries.add(new Entry(k, value.clone()));
        return this;
    }

    /** The first value of {@code key}, or null. */
    public String get(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        for (Entry e : entries) {
            if (e.key().equals(k) && e.value() instanceof String s) {
                return s;
            }
        }
        return null;
    }

    public List<String> getAll(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        List<String> out = new ArrayList<>();
        for (Entry e : entries) {
            if (e.key().equals(k) && e.value() instanceof String s) {
                out.add(s);
            }
        }
        return out;
    }

    public byte[] getBinary(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        for (Entry e : entries) {
            if (e.key().equals(k) && e.value() instanceof byte[] b) {
                return b.clone();
            }
        }
        return null;
    }

    public List<byte[]> getAllBinary(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        List<byte[]> out = new ArrayList<>();
        for (Entry e : entries) {
            if (e.key().equals(k) && e.value() instanceof byte[] b) {
                out.add(b.clone());
            }
        }
        return out;
    }

    public boolean containsKey(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        return entries.stream().anyMatch(e -> e.key().equals(k));
    }

    public Set<String> keys() {
        Set<String> out = new LinkedHashSet<>();
        entries.forEach(e -> out.add(e.key()));
        return out;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public Metadata copy() {
        Metadata m = new Metadata();
        m.entries.addAll(entries);
        return m;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Metadata{");
        for (Entry e : entries) {
            sb.append(e.key()).append('=').append(e.value() instanceof byte[] b ? "<" + b.length + " bytes>" : e.value()).append(',');
        }
        return sb.append('}').toString();
    }

    // ---- wire format ----

    /**
     * The metadata in a header block: everything except pseudo-headers and the headers the protocol itself uses ({@code content-type},
     * {@code te}, {@code user-agent}, {@code grpc-*}, ...). Binary values are decoded; ones that are not valid base64 are dropped.
     */
    public static Metadata fromHeaders(Headers h) {
        Metadata m = new Metadata();
        h.forEach((name, value) -> {
            String k = io.hydrangea.http.Headers.lower(name);
            if (k.startsWith(":") || isReserved(k)) {
                return;
            }
            if (k.endsWith("-bin")) {
                for (String part : value.split(",")) {    // intermediaries may join repeated values with commas
                    try {
                        m.entries.add(new Entry(k, Base64.getDecoder().decode(part.trim().getBytes(StandardCharsets.US_ASCII))));
                    } catch (IllegalArgumentException invalid) {
                        // a malformed binary value is ignored rather than failing the call
                    }
                }
            } else {
                m.entries.add(new Entry(k, value));
            }
        });
        return m;
    }

    /** Adds a pair from {@link #forEachRaw}: bytes under a -bin key, ASCII text otherwise. */
    void putRaw(String key, byte[] value) {
        entries.add(new Entry(key, key.endsWith("-bin") ? value : new String(value, StandardCharsets.US_ASCII)));
    }

    /** Every pair with its value as bytes (ASCII for text values, the raw bytes for -bin ones). */
    void forEachRaw(java.util.function.BiConsumer<String, byte[]> out) {
        for (Entry e : entries) {
            out.accept(e.key(), e.value() instanceof byte[] b ? b : ((String) e.value()).getBytes(StandardCharsets.US_ASCII));
        }
    }

    /** Adds these pairs to a header block. */
    public void toHeaders(Headers out) {
        for (Entry e : entries) {
            if (e.value() instanceof byte[] b) {
                out.add(e.key(), Base64.getEncoder().withoutPadding().encodeToString(b));
            } else {
                out.add(e.key(), (String) e.value());
            }
        }
    }

    static boolean isReserved(String k) {
        return k.startsWith("grpc-") || k.equals("content-type") || k.equals("te") || k.equals("user-agent") || k.equals("host")
                || k.equals("content-length") || k.equals("content-encoding") || k.equals("accept-encoding") || k.equals("connection")
                || k.equals("transfer-encoding") || k.equals("upgrade") || k.equals("keep-alive") || k.equals("date");
    }

    private static String checkKey(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        if (k.isEmpty()) {
            throw new IllegalArgumentException("empty metadata key");
        }
        for (int i = 0; i < k.length(); i++) {
            char c = k.charAt(i);
            if (!(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.')) {
                throw new IllegalArgumentException("metadata keys use a-z 0-9 - _ . only: " + key);
            }
        }
        if (isReserved(k)) {
            throw new IllegalArgumentException("reserved by the protocol: " + key);
        }
        return k;
    }
}
