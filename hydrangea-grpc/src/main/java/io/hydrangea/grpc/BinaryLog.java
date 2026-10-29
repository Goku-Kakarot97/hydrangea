package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoWriter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binary logging (the {@code grpc.binarylog.v1} format): a record of every call's headers, messages and status, to replay or audit later with
 * the tools of the gRPC ecosystem. Entries are {@code GrpcLogEntry} protobuf messages, each preceded by its length as a varint, appended to a file.
 * <pre>{@code
 * BinaryLog log = BinaryLog.toFile(Path.of("calls.binlog"), "*{h:256;m:256}");   // everything, headers and messages cut at 256 bytes
 * GrpcServer.builder().binaryLog(log)...
 * GrpcChannel.forTarget(url).binaryLog(log)...
 * }</pre>
 * Messages can hold personal data: log only the methods you need, and keep the file private.
 *
 * <h2>Configuration</h2>
 * Comma-separated rules, as in {@code GRPC_BINARY_LOG_CONFIG}: {@code *} (all methods), {@code pkg.Service/*}, {@code pkg.Service/Method},
 * and {@code -pkg.Service/Method} to exclude; the most specific rule applies. A rule may end in {@code {h;m}}: {@code h} logs headers and
 * {@code m} logs messages, each optionally with a byte limit ({@code h:256}); a rule without braces, or with empty ones, logs both in full.
 * A method that no rule matches is not logged.
 *
 * <p>Writing happens on a thread of its own; if it falls behind (more than 10 000 entries queued) entries are dropped and counted
 * ({@link #dropped()}) instead of slowing the calls. A client logs each retry attempt as a call of its own.
 */
public final class BinaryLog implements AutoCloseable {

    /** Where finished entries go: the entry already prefixed with its varint length. */
    @FunctionalInterface
    public interface Sink {
        void write(byte[] lengthDelimitedEntry) throws IOException;
    }

    private record Rule(String service, String method, boolean exclude, boolean headers, int headerLimit, boolean messages, int messageLimit) {
    }

    private final List<Rule> rules;
    private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(10_000);
    private final Thread writer;
    private final AtomicLong callIds = new AtomicLong(1);
    private final AtomicInteger dropped = new AtomicInteger();
    private volatile boolean closed;
    private final Sink sink;
    private final OutputStream owned;

    private BinaryLog(List<Rule> rules, Sink sink, OutputStream owned) {
        this.rules = rules;
        this.sink = sink;
        this.owned = owned;
        this.writer = new Thread(this::drain, "hydrangea-binarylog");
        this.writer.setDaemon(true);
        this.writer.start();
    }

    /** Appends to {@code file} (created if need be). @throws IllegalArgumentException for a bad configuration */
    public static BinaryLog toFile(Path file, String config) throws IOException {
        List<Rule> rules = parse(config);
        OutputStream out = new BufferedOutputStream(Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND), 1 << 16);
        return new BinaryLog(rules, out::write, out);
    }

    /** Sends entries to your own {@link Sink}. */
    public static BinaryLog to(Sink sink, String config) {
        return new BinaryLog(parse(config), sink, null);
    }

    /** Entries lost because the writer could not keep up. */
    public int dropped() {
        return dropped.get();
    }

    /** Writes what is queued, then closes the file. */
    @Override
    public void close() throws IOException {
        closed = true;
        writer.interrupt();
        try {
            writer.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (owned != null) {
            owned.close();
        }
    }

    private void drain() {
        for (;;) {
            byte[] e;
            try {
                e = closed ? queue.poll() : queue.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                e = queue.poll();   // closing: finish what is left
                if (e == null) {
                    return;
                }
            }
            if (e == null) {
                if (closed) {
                    return;
                }
                try {
                    if (owned != null) {
                        owned.flush();
                    }
                } catch (IOException ignored) {
                    // the file is gone; nothing to be done
                }
                continue;
            }
            try {
                sink.write(e);
            } catch (IOException ioe) {
                dropped.incrementAndGet();
            }
        }
    }

    // ---- configuration ----

    static List<Rule> parse(String config) {
        List<Rule> out = new ArrayList<>();
        for (String raw : splitTop(config)) {
            String item = raw.trim();
            if (item.isEmpty()) {
                continue;
            }
            boolean exclude = item.startsWith("-");
            if (exclude) {
                item = item.substring(1);
            }
            boolean h = true, m = true;
            int hl = Integer.MAX_VALUE, ml = Integer.MAX_VALUE;
            int brace = item.indexOf('{');
            if (brace >= 0) {
                if (!item.endsWith("}")) {
                    throw new IllegalArgumentException("unterminated { in binary log config: " + raw);
                }
                String opts = item.substring(brace + 1, item.length() - 1).trim();
                item = item.substring(0, brace);
                if (!opts.isEmpty()) {
                    h = false;
                    m = false;
                    for (String o : opts.split(";")) {
                        String[] kv = o.trim().split(":", 2);
                        int limit = kv.length == 2 ? limit(kv[1], raw) : Integer.MAX_VALUE;
                        if (kv[0].equals("h")) { h = true; hl = limit; }
                        else if (kv[0].equals("m")) { m = true; ml = limit; }
                        else throw new IllegalArgumentException("unknown option '" + kv[0] + "' in binary log config: " + raw);
                    }
                }
            }
            String service, method;
            if (item.equals("*")) {
                service = null;
                method = null;
            } else {
                int slash = item.indexOf('/');
                if (slash <= 0 || slash == item.length() - 1 || item.contains("*") && !item.endsWith("/*")) {
                    throw new IllegalArgumentException("a binary log rule is *, service/* or service/method: " + raw);
                }
                service = item.substring(0, slash);
                method = item.substring(slash + 1).equals("*") ? null : item.substring(slash + 1);
            }
            out.add(new Rule(service, method, exclude, h, hl, m, ml));
        }
        return List.copyOf(out);
    }

    private static int limit(String s, String raw) {
        try {
            int v = Integer.parseInt(s.trim());
            if (v < 0) throw new NumberFormatException();
            return v;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad byte limit in binary log config: " + raw);
        }
    }

    /** Splits on commas that are not inside braces. */
    private static List<String> splitTop(String s) {
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') depth--;
            else if (c == ',' && depth == 0) {
                parts.add(s.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(s.substring(start));
        return parts;
    }

    private Rule ruleFor(String methodPath) {
        int slash = methodPath.indexOf('/', 1);
        String service = methodPath.substring(1, slash), method = methodPath.substring(slash + 1);
        Rule exact = null, serviceWide = null, all = null;
        for (Rule r : rules) {
            if (r.service == null) all = r;
            else if (r.service.equals(service) && r.method == null) serviceWide = r;
            else if (r.service.equals(service) && r.method.equals(method)) exact = r;
        }
        Rule best = exact != null ? exact : serviceWide != null ? serviceWide : all;
        return best == null || best.exclude ? null : best;
    }

    // ---- logging one call ----

    enum Side { CLIENT(1), SERVER(2); final int value; Side(int v) { value = v; } }

    /** The log of one call, or null if the configuration does not cover this method. */
    CallLog start(Side side, String methodPath) {
        if (closed) {
            return null;
        }
        Rule r = ruleFor(methodPath);
        return r == null ? null : new CallLog(side, r);
    }

    private void emit(byte[] entry) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(entry.length + 5);
        int n = entry.length;
        while (n > 127) {
            out.write((n & 127) | 128);
            n >>>= 7;
        }
        out.write(n);
        out.writeBytes(entry);
        if (!queue.offer(out.toByteArray())) {
            dropped.incrementAndGet();
        }
    }

    final class CallLog {
        private final Side side;
        private final Rule rule;
        private final long callId = callIds.getAndIncrement();
        private long seq;

        CallLog(Side side, Rule rule) {
            this.side = side;
            this.rule = rule;
        }

        private synchronized void log(int type, int payloadField, byte[] payload, boolean truncated, SocketAddress peer) {
            ProtoWriter w = new ProtoWriter();
            long now = System.currentTimeMillis();
            w.message(1, new ProtoWriter().varint(1, now / 1000).varint(2, (now % 1000) * 1_000_000L).toByteArray());
            w.varint(2, callId).varint(3, ++seq).varint(4, type).varint(5, side.value);
            if (payload != null) {
                w.message(payloadField, payload);
            }
            if (truncated) {
                w.bool(10, true);
            }
            if (peer != null) {
                byte[] a = address(peer);
                if (a != null) {
                    w.message(11, a);
                }
            }
            emit(w.toByteArray());
        }

        private byte[] metadata(Metadata m) {
            ProtoWriter w = new ProtoWriter();
            if (m != null && rule.headers) {
                int[] budget = {rule.headerLimit};
                m.forEachRaw((k, v) -> {
                    int cost = k.length() + v.length;
                    if (cost > budget[0]) {
                        return;   // over the limit: this entry is left out, and the entry is marked truncated
                    }
                    budget[0] -= cost;
                    w.message(1, new ProtoWriter().string(1, k).bytes(2, v).toByteArray());
                });
            }
            return w.toByteArray();
        }

        private boolean headersTruncated(Metadata m) {
            if (m == null || !rule.headers || rule.headerLimit == Integer.MAX_VALUE) {
                return false;
            }
            int[] total = {0};
            m.forEachRaw((k, v) -> total[0] += k.length() + v.length);
            return total[0] > rule.headerLimit;
        }

        void clientHeader(Metadata md, String methodPath, String authority, Duration timeout, SocketAddress peer) {
            ProtoWriter h = new ProtoWriter().message(1, metadata(md)).string(2, methodPath);
            if (authority != null) {
                h.string(3, authority);
            }
            if (timeout != null) {
                h.message(4, new ProtoWriter().varint(1, timeout.getSeconds()).varint(2, timeout.getNano()).toByteArray());
            }
            log(1, 6, rule.headers ? h.toByteArray() : new ProtoWriter().string(2, methodPath).toByteArray(), headersTruncated(md), peer);
        }

        void serverHeader(Metadata md, SocketAddress peer) {
            log(2, 7, new ProtoWriter().message(1, metadata(md)).toByteArray(), headersTruncated(md), peer);
        }

        void clientMessage(byte[] payload) {
            message(3, payload);
        }

        void serverMessage(byte[] payload) {
            message(4, payload);
        }

        private void message(int type, byte[] payload) {
            if (!rule.messages) {
                log(type, 8, null, false, null);
                return;
            }
            boolean cut = payload.length > rule.messageLimit;
            byte[] data = cut ? java.util.Arrays.copyOf(payload, rule.messageLimit) : payload;
            log(type, 8, new ProtoWriter().varint(1, payload.length).bytes(2, data).toByteArray(), cut, null);
        }

        void clientHalfClose() {
            log(5, 0, null, false, null);
        }

        void serverTrailer(Status status, Metadata md, SocketAddress peer) {
            ProtoWriter t = new ProtoWriter().message(1, metadata(md)).varint(2, status.code().value());
            if (status.description() != null && !status.description().isEmpty()) {
                t.string(3, status.description());
            }
            log(6, 9, t.toByteArray(), headersTruncated(md), peer);
        }

        void cancel() {
            log(7, 0, null, false, null);
        }
    }

    private static byte[] address(SocketAddress a) {
        if (a instanceof InetSocketAddress i && i.getAddress() != null) {
            byte[] raw = i.getAddress().getAddress();
            return new ProtoWriter().varint(1, raw.length == 4 ? 1 : 2).string(2, i.getAddress().getHostAddress()).varint(3, i.getPort()).toByteArray();
        }
        if (a instanceof UnixDomainSocketAddress u) {
            return new ProtoWriter().varint(1, 3).string(2, u.getPath().toString()).toByteArray();
        }
        return null;
    }
}
