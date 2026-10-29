package bench;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Framework-neutral load generator for HTTP/1.1 keep-alive, WebSocket echo and SSE fan-out, written with plain java.nio so no
 * contender benefits from its own client. Every response / echo / event is checked; mismatches count as errors.
 * <pre>
 * HttpLoad mode port conns threads warmupSec durationSec [key=value ...]
 *   mode http : closed loop, one request in flight per connection.   path=/plain|/json|/users/%d?q=hello|/echo|/download/N
 *               method=GET|POST  body=bytes (POST)  verify=true|false  host=127.0.0.1
 *   mode ws   : closed loop text echo on /ws.                         size=64 (payload bytes)
 *   mode sse  : subscribe to /sse and measure delivery latency of the server's timestamp events.
 *   common    : think=ms   wait this long after each response (default 0: closed loop at full speed)
 *               reconnect=true  open a new connection when the server closes one (graceful shutdown, rolling update); an idle close is not an error
 *               close=true      http: send "Connection: close" and reconnect after every response (connection-per-request)
 * </pre>
 * Prints one line: {@code RESULT ops=.. mbps=.. p50=.. p90=.. p99=.. p999=.. max=.. errors=.. connected=..} (latency in microseconds).
 */
public final class HttpLoad {

    static volatile int phase; // 0 warm-up, 1 measuring, 2 stop
    static final int HIST = 200_000;
    static final Map<String, AtomicInteger> ERRORS = new ConcurrentHashMap<>();
    static final byte[] JSON = ("{\"id\":12345,\"name\":\"hydrangea\",\"active\":true,\"tags\":[\"alpha\",\"beta\",\"gamma\"],"
            + "\"scores\":[1.5,2.25,3.125],\"address\":{\"city\":\"Ho Chi Minh\",\"zip\":\"700000\"}}").getBytes(StandardCharsets.UTF_8);

    record Args(String mode, String host, int port, int conns, int threads, int warmup, int duration, Map<String, String> opts) {
        String opt(String k, String d) {
            return opts.getOrDefault(k, d);
        }
    }

    public static void main(String[] a) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 6; i < a.length; i++) {
            int eq = a[i].indexOf('=');
            opts.put(a[i].substring(0, eq), a[i].substring(eq + 1));
        }
        Args args = new Args(a[0], opts.getOrDefault("host", "127.0.0.1"), Integer.parseInt(a[1]), Integer.parseInt(a[2]),
                Math.min(Integer.parseInt(a[3]), Integer.parseInt(a[2])), Integer.parseInt(a[4]), Integer.parseInt(a[5]), opts);
        List<Worker> workers = new ArrayList<>();
        for (int t = 0; t < args.threads(); t++) {
            int share = args.conns() / args.threads() + (t < args.conns() % args.threads() ? 1 : 0);
            workers.add(new Worker(args, share, t));
        }
        workers.forEach(Thread::start);
        long deadline = System.nanoTime() + 30_000_000_000L;
        while (workers.stream().anyMatch(w -> !w.ready) && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        int connected = workers.stream().mapToInt(w -> w.connectedCount).sum();
        Thread.sleep(args.warmup() * 1000L);
        long t0 = System.nanoTime();
        phase = 1;
        Thread.sleep(args.duration() * 1000L);
        phase = 2;
        double secs = (System.nanoTime() - t0) / 1e9;
        for (Worker w : workers) w.join();

        long ops = 0, bytes = 0, errors = 0, max = 0;
        long[] hist = new long[HIST];
        long over = 0;
        for (Worker w : workers) {
            ops += w.ops;
            bytes += w.bytes;
            errors += w.errors;
            over += w.overflow;
            max = Math.max(max, w.maxUs);
            for (int i = 0; i < HIST; i++) hist[i] += w.hist[i];
        }
        long rc = workers.stream().mapToLong(w -> w.reconnects).sum();
        if (rc > 0) System.err.println("RECONNECTS " + rc + "  retried-GETs " + workers.stream().mapToLong(w -> w.retries).sum()
                + "  ws-goodbyes " + workers.stream().mapToLong(w -> w.goodbyes).sum());
        ERRORS.forEach((k, v) -> System.err.println("ERROR-KIND " + v + "x " + k));
        System.out.printf("RESULT ops=%.0f mbps=%.1f p50=%d p90=%d p99=%d p999=%d max=%d errors=%d connected=%d%n", ops / secs, bytes / secs / 1e6,
                pct(hist, ops, max, 0.50), pct(hist, ops, max, 0.90), pct(hist, ops, max, 0.99), pct(hist, ops, max, 0.999), max, errors, connected);
    }

    static long pct(long[] hist, long total, long max, double p) {
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * p), seen = 0;
        for (int i = 0; i < HIST; i++) {
            seen += hist[i];
            if (seen >= target) return i;
        }
        return max;
    }

    static void noteError(String kind) {
        ERRORS.computeIfAbsent(kind, k -> new AtomicInteger()).incrementAndGet();
    }

    // ---- per-connection state ----

    static final class Conn {
        SocketChannel ch;
        SelectionKey key;
        ByteBuffer out;
        final ByteBuffer rd = ByteBuffer.allocateDirect(64 * 1024);
        byte[] acc = new byte[4096];
        int accLen;
        long start;
        long dueAt;             // think time: when to send the next request
        boolean inFlight;       // a request was sent and its response is not complete
        boolean closeAfter;     // the server announced Connection: close
        int served;             // exchanges completed on the current connection
        boolean reopen;         // sleeping connection that must be re-established when it wakes
        int seq;
        // http parsing
        int headEnd = -1;
        long bodyLen;
        int status;
        boolean upgraded;       // ws: handshake complete
        byte[] expect;          // ws / http: payload we expect back
        // sse
        int sseScan;
        long bodyReceived;
    }

    static final class Worker extends Thread {
        final Args args;
        final int count;
        final String mode;
        final String path;
        final String method;
        final byte[] postBody;
        final boolean verify;
        final int wsSize;
        final long thinkNanos;
        final boolean reconnect;
        final boolean closeEach;
        final List<Conn> sleeping = new ArrayList<>();   // connections waiting out their think time
        long reconnects, retries, goodbyes;
        volatile boolean ready;
        int connectedCount;
        long ops, bytes, errors, overflow, maxUs;
        final long[] hist = new long[HIST];
        final Random rnd = new Random(42);
        final Selector sel;

        Worker(Args args, int count, int index) {
            super("load-" + index);
            this.args = args;
            this.count = count;
            this.mode = args.mode();
            this.path = args.opt("path", mode.equals("sse") ? "/sse?interval=100" : "/plain");
            this.method = args.opt("method", "GET");
            int bodySize = Integer.parseInt(args.opt("body", "0"));
            this.postBody = new byte[bodySize];
            new Random(7).nextBytes(postBody);
            this.verify = Boolean.parseBoolean(args.opt("verify", "true"));
            this.wsSize = Integer.parseInt(args.opt("size", "64"));
            this.thinkNanos = Long.parseLong(args.opt("think", "0")) * 1_000_000L;
            this.reconnect = Boolean.parseBoolean(args.opt("reconnect", "false"));
            this.closeEach = Boolean.parseBoolean(args.opt("close", "false"));
            try {
                this.sel = Selector.open();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void run() {
            try (sel) {
                List<Conn> all = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    Conn c = new Conn();
                    all.add(c);
                    open(c);
                }
                int pendingConnects = count;
                while (phase != 2) {
                    sel.select(sleeping.isEmpty() ? 50 : 3);
                    if (!sleeping.isEmpty()) wakeSleepers();
                    Iterator<SelectionKey> it = sel.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey k = it.next();
                        it.remove();
                        Conn c = (Conn) k.attachment();
                        try {
                            if (!k.isValid()) continue;
                            if (k.isConnectable() && c.ch.finishConnect()) {
                                c.ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                                connectedCount++;
                                if (connectedCount == count) ready = true;
                                begin(c);
                                continue;
                            }
                            if (k.isReadable()) onRead(c);
                            if (k.isValid() && k.isWritable()) onWrite(c);
                        } catch (IOException | RuntimeException e) {
                            fail(c, e);
                        }
                    }
                }
                for (Conn c : all) {
                    try { c.ch.close(); } catch (IOException ignored) { }
                }
            } catch (IOException e) {
                errors++;
            } finally {
                ready = true;
            }
        }

        /** Handles a failed connection: classify it, close it and (reconnect=true) schedule a new one. */
        void fail(Conn c, Exception e) {
            // With reconnect=true the load behaves like a well-written client facing a server that goes away:
            //  - a connection that was idle when the server closed it is not a failure;
            //  - a GET written to a connection the server had already closed (nothing came back) is retried on a new
            //    connection, as real HTTP clients do for idempotent requests (the server never saw it);
            //  - a WebSocket close frame (1001 going away) while waiting for an echo is the server saying goodbye.
            boolean idleClose = reconnect && !c.inFlight;
            boolean idempotentRetry = reconnect && c.inFlight && mode.equals("http") && method.equals("GET") && c.served > 0 && c.accLen == 0 && c.headEnd < 0;
            boolean goodbye = reconnect && mode.equals("ws") && String.valueOf(e.getMessage()).contains("opcode 8");
            if (idempotentRetry) {
                retries++;
            } else if (goodbye) {
                goodbyes++;
            } else if (!idleClose) {
                if (phase == 1) {
                    errors++;
                }
                noteError((c.served == 0 ? "[fresh connection] " : "[established connection, request lost] ")
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            try {
                c.ch.close();
            } catch (IOException ignored) { }
            sleeping.remove(c);
            if (reconnect && phase != 2) {
                reconnects++;
                c.reopen = true; // back off a little: a server that is shutting down refuses connections, do not spin on it
                c.dueAt = System.nanoTime() + 20_000_000L;
                sleeping.add(c);
            }
        }

        void wakeSleepers() {
            long now = System.nanoTime();
            for (int i = 0; i < sleeping.size(); ) {
                Conn c = sleeping.get(i);
                if (c.dueAt - now <= 0) {
                    sleeping.remove(i);
                    try {
                        if (c.reopen) {
                            c.reopen = false;
                            open(c);
                        } else if (mode.equals("ws")) {
                            sendWs(c);
                        } else {
                            sendHttp(c);
                        }
                    } catch (IOException | RuntimeException e) {
                        fail(c, e);
                    }
                } else {
                    i++;
                }
            }
        }

        /** After a completed exchange: reconnect if the server said Connection: close, wait out the think time, or go straight on. */
        void next(Conn c) throws IOException {
            c.inFlight = false;
            c.served++;
            if (c.closeAfter || (closeEach && mode.equals("http"))) {
                c.ch.close();
                reconnects++;
                if (thinkNanos > 0) {
                    c.reopen = true;
                    c.dueAt = System.nanoTime() + thinkNanos;
                    sleeping.add(c);
                } else {
                    open(c);
                }
                return;
            }
            if (thinkNanos > 0) {
                c.dueAt = System.nanoTime() + thinkNanos;
                sleeping.add(c);
            } else if (mode.equals("ws")) {
                sendWs(c);
            } else {
                sendHttp(c);
            }
        }

        void open(Conn c) throws IOException {
            c.closeAfter = false;
            c.inFlight = false;
            c.served = 0;
            c.ch = SocketChannel.open();
            c.ch.configureBlocking(false);
            c.key = c.ch.register(sel, SelectionKey.OP_CONNECT, c);
            c.ch.connect(new InetSocketAddress(args.host(), args.port()));
        }

        // ---------- starting a request / handshake ----------

        void begin(Conn c) throws IOException {
            c.accLen = 0;
            c.headEnd = -1;
            c.upgraded = false;
            switch (mode) {
                case "http" -> sendHttp(c);
                case "ws" -> {
                    c.out = ByteBuffer.wrap(("GET /ws HTTP/1.1\r\nHost: " + args.host() + "\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                            + "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
                    write(c);
                }
                case "sse" -> {
                    c.out = ByteBuffer.wrap(("GET " + path + " HTTP/1.1\r\nHost: " + args.host() + "\r\nAccept: text/event-stream\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
                    write(c);
                }
                default -> throw new IllegalArgumentException(mode);
            }
        }

        void sendHttp(Conn c) throws IOException {
            c.seq++;
            String p = path.contains("%d") ? String.format(path, c.seq) : path;
            StringBuilder sb = new StringBuilder(160);
            sb.append(method).append(' ').append(p).append(" HTTP/1.1\r\nHost: ").append(args.host()).append("\r\n");
            if (closeEach) sb.append("Connection: close\r\n");
            if (method.equals("POST")) {
                sb.append("Content-Type: application/octet-stream\r\nContent-Length: ").append(postBody.length).append("\r\n");
            }
            sb.append("\r\n");
            byte[] head = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
            byte[] req = new byte[head.length + (method.equals("POST") ? postBody.length : 0)];
            System.arraycopy(head, 0, req, 0, head.length);
            if (method.equals("POST")) System.arraycopy(postBody, 0, req, head.length, postBody.length);
            c.out = ByteBuffer.wrap(req);
            c.expect = verify ? expectedBody(p, c.seq) : null;
            c.accLen = 0;
            c.headEnd = -1;
            c.start = System.nanoTime();
            c.inFlight = true;
            write(c);
        }

        byte[] expectedBody(String p, int seq) {
            if (p.startsWith("/plain")) return "Hello, World!".getBytes(StandardCharsets.UTF_8);
            if (p.startsWith("/json")) return JSON;
            if (p.startsWith("/users/")) {
                int q = p.indexOf('?');
                String id = p.substring(7, q < 0 ? p.length() : q);
                String qv = q < 0 ? "-" : p.substring(q + 1).replaceFirst("^q=", "");
                return ("user " + id + " q=" + qv).getBytes(StandardCharsets.UTF_8);
            }
            if (p.startsWith("/echo")) return postBody;
            return null; // downloads: only the length is checked
        }

        void write(Conn c) throws IOException {
            while (c.out.hasRemaining()) {
                if (c.ch.write(c.out) == 0) {
                    c.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    return;
                }
            }
            c.key.interestOps(SelectionKey.OP_READ);
        }

        void onWrite(Conn c) throws IOException {
            write(c);
        }

        // ---------- reading ----------

        void onRead(Conn c) throws IOException {
            for (;;) {
                c.rd.clear();
                int n = c.ch.read(c.rd);
                if (n < 0) throw new IOException("closed by server");
                if (n == 0) return;
                c.rd.flip();
                handle(c, n);
                if (n < c.rd.capacity()) return;
            }
        }

        void ensure(Conn c, int extra) {
            if (c.accLen + extra > c.acc.length) c.acc = Arrays.copyOf(c.acc, Math.max(c.acc.length * 2, c.accLen + extra));
        }

        void handle(Conn c, int n) throws IOException {
            switch (mode) {
                case "http" -> handleHttp(c, n);
                case "ws" -> handleWs(c, n);
                default -> handleSse(c, n);
            }
        }

        // -- http: head, then Content-Length bytes (the servers under test always send a length) --

        void handleHttp(Conn c, int n) throws IOException {
            boolean keepBody = c.expect != null;
            if (c.headEnd < 0) {
                ensure(c, n);
                c.rd.get(c.acc, c.accLen, n);
                c.accLen += n;
                int he = indexOf(c.acc, c.accLen, "\r\n\r\n");
                if (he < 0) return;
                c.headEnd = he + 4;
                String head = new String(c.acc, 0, he, StandardCharsets.ISO_8859_1);
                c.status = Integer.parseInt(head.substring(9, 12));
                int i = head.toLowerCase().indexOf("content-length:");
                if (i < 0) throw new IOException("response without Content-Length: " + head.split("\r\n")[0]);
                int eol = head.indexOf('\r', i);
                c.bodyLen = Long.parseLong(head.substring(i + 15, eol < 0 ? head.length() : eol).trim());
                c.closeAfter = head.toLowerCase().contains("connection: close");
                c.bodyReceived = c.accLen - c.headEnd;
                if (!keepBody) c.accLen = 0; // download: do not hold the body
            } else {
                c.bodyReceived += n;
                if (keepBody) {
                    ensure(c, n);
                    c.rd.get(c.acc, c.accLen, n);
                    c.accLen += n;
                } else {
                    c.rd.position(c.rd.limit());
                }
            }
            if (c.bodyReceived < c.bodyLen) return;
            if (c.bodyReceived > c.bodyLen) throw new IOException("more bytes than Content-Length");
            boolean ok = (c.status == 200 || c.status == 201);
            if (ok && keepBody) {
                ok = c.accLen - c.headEnd == c.expect.length && Arrays.equals(c.acc, c.headEnd, c.accLen, c.expect, 0, c.expect.length);
            }
            if (!ok) {
                if (phase == 1) errors++;
                noteError("bad response status=" + c.status);
            }
            long us = (System.nanoTime() - c.start) / 1000;
            if (phase == 1) {
                ops++;
                bytes += c.bodyLen;
                record(us);
            }
            next(c);
        }

        // -- websocket: handshake, then closed-loop masked text frames --

        void handleWs(Conn c, int n) throws IOException {
            ensure(c, n);
            c.rd.get(c.acc, c.accLen, n);
            c.accLen += n;
            if (!c.upgraded) {
                int he = indexOf(c.acc, c.accLen, "\r\n\r\n");
                if (he < 0) return;
                String head = new String(c.acc, 0, he, StandardCharsets.ISO_8859_1);
                if (!head.startsWith("HTTP/1.1 101")) throw new IOException("handshake failed: " + head.split("\r\n")[0]);
                c.upgraded = true;
                System.arraycopy(c.acc, he + 4, c.acc, 0, c.accLen - he - 4);
                c.accLen -= he + 4;
                sendWs(c);
                if (c.accLen == 0) return;
            }
            // one frame at a time
            while (c.accLen >= 2) {
                int b1 = c.acc[1] & 0x7F;
                int hdr = 2;
                long len = b1;
                if (b1 == 126) { if (c.accLen < 4) return; len = ((c.acc[2] & 0xFF) << 8) | (c.acc[3] & 0xFF); hdr = 4; }
                else if (b1 == 127) throw new IOException("unexpected 64-bit frame length");
                if (c.accLen < hdr + len) return;
                if ((c.acc[0] & 0x0F) != 1) throw new IOException("unexpected opcode " + (c.acc[0] & 0x0F));
                boolean same = len == c.expect.length && Arrays.equals(c.acc, hdr, hdr + (int) len, c.expect, 0, c.expect.length);
                if (!same) {
                    if (phase == 1) errors++;
                    noteError("ws echo mismatch");
                }
                long us = (System.nanoTime() - c.start) / 1000;
                if (phase == 1) {
                    ops++;
                    bytes += len;
                    record(us);
                }
                int consumed = hdr + (int) len;
                System.arraycopy(c.acc, consumed, c.acc, 0, c.accLen - consumed);
                c.accLen -= consumed;
                next(c);
            }
        }

        void sendWs(Conn c) throws IOException {
            c.seq++;
            byte[] payload = new byte[wsSize];
            byte[] text = ("msg-" + c.seq + "-").getBytes(StandardCharsets.US_ASCII);
            for (int i = 0; i < payload.length; i++) payload[i] = (byte) ('a' + (i + c.seq) % 26);
            System.arraycopy(text, 0, payload, 0, Math.min(text.length, payload.length));
            c.expect = payload;
            byte[] key = new byte[4];
            rnd.nextBytes(key);
            int hdr = wsSize < 126 ? 6 : 8;
            byte[] frame = new byte[hdr + wsSize];
            frame[0] = (byte) 0x81;
            if (wsSize < 126) { frame[1] = (byte) (0x80 | wsSize); } else { frame[1] = (byte) (0x80 | 126); frame[2] = (byte) (wsSize >> 8); frame[3] = (byte) wsSize; }
            System.arraycopy(key, 0, frame, hdr - 4, 4);
            for (int i = 0; i < wsSize; i++) frame[hdr + i] = (byte) (payload[i] ^ key[i & 3]);
            c.out = ByteBuffer.wrap(frame);
            c.start = System.nanoTime();
            c.inFlight = true;
            write(c);
        }

        // -- sse: look for "data: <epoch millis>" lines anywhere in the (chunked) byte stream --

        void handleSse(Conn c, int n) throws IOException {
            ensure(c, n);
            c.rd.get(c.acc, c.accLen, n);
            c.accLen += n;
            if (!c.upgraded) { // response head
                int he = indexOf(c.acc, c.accLen, "\r\n\r\n");
                if (he < 0) return;
                String head = new String(c.acc, 0, he, StandardCharsets.ISO_8859_1);
                if (!head.startsWith("HTTP/1.1 200")) throw new IOException("sse: " + head.split("\r\n")[0]);
                c.upgraded = true;
                System.arraycopy(c.acc, he + 4, c.acc, 0, c.accLen - he - 4);
                c.accLen -= he + 4;
            }
            int pos = 0;
            while (true) {
                int d = indexOf(c.acc, pos, c.accLen, "data: ");
                if (d < 0) break;
                int eol = indexOf(c.acc, d + 6, c.accLen, "\n");
                if (eol < 0) break; // incomplete line: wait for more
                long sent;
                try {
                    sent = Long.parseLong(new String(c.acc, d + 6, eol - d - 6, StandardCharsets.US_ASCII).trim());
                } catch (NumberFormatException e) {
                    throw new IOException("bad sse data line");
                }
                long ms = Math.max(0, System.currentTimeMillis() - sent);
                if (phase == 1) {
                    ops++;
                    record(ms * 1000);
                }
                pos = eol + 1;
            }
            // keep only the unparsed tail
            int keep = Math.min(c.accLen - pos, 64);
            int from = c.accLen - keep;
            from = Math.max(from, pos > 0 ? pos : 0);
            System.arraycopy(c.acc, from, c.acc, 0, c.accLen - from);
            c.accLen -= from;
        }

        void record(long us) {
            if (us >= HIST) overflow++;
            else hist[(int) us]++;
            if (us > maxUs) maxUs = us;
        }
    }

    static int indexOf(byte[] a, int len, String needle) {
        return indexOf(a, 0, len, needle);
    }

    static int indexOf(byte[] a, int from, int len, String needle) {
        byte[] nb = needle.getBytes(StandardCharsets.ISO_8859_1);
        outer:
        for (int i = from; i <= len - nb.length; i++) {
            for (int j = 0; j < nb.length; j++) if (a[i + j] != nb[j]) continue outer;
            return i;
        }
        return -1;
    }
}
