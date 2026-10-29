package bench;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Framework-neutral load generator (plain java.nio) so neither contender benefits from its own client.
 * Prints one line: {@code RESULT key=value ...}.
 * <pre>
 * LoadGenerator mode port conns size threads warmupSec durationSec
 *   mode: pingpong  each connection sends `size` bytes, waits for the full echo, repeats (latency + rate)
 *         stream    each connection keeps up to 512 KiB in flight and checks every echoed byte (throughput)
 *         churn     connect, send `size` bytes, read echo, close, repeat (accept/close path)
 * </pre>
 * Every echoed byte is verified; mismatches count as errors.
 */
public final class LoadGenerator {

    static volatile int phase; // 0 warmup, 1 measure, 2 stop
    static final int HIST = 200_000; // 1 us buckets up to 200 ms; slower samples go to the overflow counter
    static final int WINDOW = 512 * 1024;
    static final java.util.concurrent.ConcurrentHashMap<String, java.util.concurrent.atomic.AtomicInteger> ERROR_KINDS =
            new java.util.concurrent.ConcurrentHashMap<>();

    static void noteError(String kind) {
        ERROR_KINDS.computeIfAbsent(kind, k -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
    }

    static final ByteBuffer BLOCK = ByteBuffer.allocateDirect(65536);

    static {
        for (int i = 0; i < BLOCK.capacity(); i++) {
            BLOCK.put(i, (byte) (i * 31 + (i >>> 8)));
        }
    }

    public static void main(String[] a) throws Exception {
        String mode = a[0];
        int port = Integer.parseInt(a[1]);
        int conns = Integer.parseInt(a[2]);
        int size = Integer.parseInt(a[3]);
        int threads = Math.min(Integer.parseInt(a[4]), conns);
        int warmup = Integer.parseInt(a[5]);
        int duration = Integer.parseInt(a[6]);

        List<Worker> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            int share = conns / threads + (t < conns % threads ? 1 : 0);
            workers.add(new Worker(mode, port, share, size));
        }
        workers.forEach(Thread::start);
        long deadline = System.nanoTime() + 20_000_000_000L;
        while (workers.stream().anyMatch(w -> !w.ready) && System.nanoTime() < deadline && !mode.equals("churn")) {
            Thread.sleep(20);
        }
        Thread.sleep(warmup * 1000L);
        long t0 = System.nanoTime();
        phase = 1;
        Thread.sleep(duration * 1000L);
        phase = 2;
        double secs = (System.nanoTime() - t0) / 1e9;
        for (Worker w : workers) w.join();

        long ops = 0, bytes = 0, errors = 0, over = 0;
        long[] hist = new long[HIST];
        long max = 0;
        for (Worker w : workers) {
            ops += w.ops;
            bytes += w.bytes;
            errors += w.errors;
            over += w.overflow;
            max = Math.max(max, w.maxUs);
            for (int i = 0; i < HIST; i++) hist[i] += w.hist[i];
        }
        ERROR_KINDS.forEach((k, v) -> System.err.println("ERROR-KIND " + v + "x " + k));
        System.out.printf("RESULT ops=%.0f mbps=%.1f p50=%d p90=%d p99=%d p999=%d max=%d errors=%d%n",
                ops / secs, bytes / secs / 1e6,
                pct(hist, ops, over, max, 0.50), pct(hist, ops, over, max, 0.90),
                pct(hist, ops, over, max, 0.99), pct(hist, ops, over, max, 0.999), max, errors);
    }

    static long pct(long[] hist, long total, long over, long max, double p) {
        if (total == 0) return 0;
        long target = (long) Math.ceil(total * p);
        long seen = 0;
        for (int i = 0; i < HIST; i++) {
            seen += hist[i];
            if (seen >= target) return i;
        }
        return max; // in the overflow region
    }

    static final class Conn {
        SocketChannel ch;
        SelectionKey key;
        ByteBuffer out;
        final ByteBuffer in;
        long start;
        long sent;
        long received;

        Conn(int inSize) {
            in = ByteBuffer.allocateDirect(inSize);
        }
    }

    static final class Worker extends Thread {
        final String mode;
        final int port;
        final int count;
        final int size;
        volatile boolean ready;
        long ops, bytes, errors, overflow, maxUs;
        final long[] hist = new long[HIST];

        Worker(String mode, int port, int count, int size) {
            this.mode = mode;
            this.port = port;
            this.count = count;
            this.size = size;
        }

        @Override
        public void run() {
            try (Selector sel = Selector.open()) {
                List<Conn> all = new ArrayList<>();
                int inSize = mode.equals("stream") ? 65536 : size;
                for (int i = 0; i < count; i++) {
                    Conn c = new Conn(inSize);
                    all.add(c);
                    open(sel, c);
                }
                int connected = 0;
                while (phase != 2) {
                    sel.select(50);
                    Iterator<SelectionKey> it = sel.selectedKeys().iterator();
                    while (it.hasNext()) {
                        SelectionKey k = it.next();
                        it.remove();
                        Conn c = (Conn) k.attachment();
                        try {
                            if (!k.isValid()) continue;
                            if (k.isConnectable()) {
                                if (c.ch.finishConnect()) {
                                    if (!mode.equals("churn")) c.ch.setOption(StandardSocketOptions.TCP_NODELAY, true);
                                    if (++connected == count) ready = true;
                                    begin(c);
                                }
                                continue;
                            }
                            if (k.isReadable()) onRead(sel, c);
                            if (k.isValid() && k.isWritable()) onWrite(c);
                        } catch (IOException | RuntimeException e) {
                            if (phase == 1) {
                                errors++;
                                noteError(e.getClass().getSimpleName() + ": " + e.getMessage());
                            }
                            fail(sel, c);
                        }
                    }
                }
                for (Conn c : all) {
                    try { if (c.ch != null) c.ch.close(); } catch (IOException ignored) { }
                }
            } catch (IOException e) {
                errors++;
            }
        }

        void open(Selector sel, Conn c) throws IOException {
            c.ch = SocketChannel.open();
            c.ch.configureBlocking(false);
            if (mode.equals("churn")) {
                c.ch.setOption(StandardSocketOptions.SO_LINGER, 0); // RST on close: avoids exhausting ephemeral ports in TIME_WAIT
            }
            c.start = System.nanoTime();
            c.sent = 0;
            c.received = 0;
            c.in.clear();
            c.key = c.ch.register(sel, SelectionKey.OP_CONNECT, c);
            c.ch.connect(new InetSocketAddress("127.0.0.1", port));
        }

        void fail(Selector sel, Conn c) {
            try { c.ch.close(); } catch (IOException ignored) { }
            if (mode.equals("churn") && phase != 2) {
                try { open(sel, c); } catch (IOException e) { errors++; }
            }
        }

        void begin(Conn c) throws IOException {
            c.in.clear();
            c.received = 0;
            c.sent = 0;
            c.start = System.nanoTime();
            if (mode.equals("stream")) {
                c.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            } else {
                c.out = BLOCK.duplicate().position(0).limit(size);
                c.in.limit(size);
                onWrite(c);
            }
        }

        void onWrite(Conn c) throws IOException {
            if (mode.equals("stream")) {
                while (c.sent - c.received < WINDOW) {
                    int off = (int) (c.sent & 0xFFFF);
                    ByteBuffer src = BLOCK.duplicate().position(off).limit(65536);
                    int n = c.ch.write(src);
                    if (n == 0) return;
                    c.sent += n;
                }
                c.key.interestOps(SelectionKey.OP_READ); // window full: wait for echoes
                return;
            }
            while (c.out.hasRemaining()) {
                if (c.ch.write(c.out) == 0) {
                    c.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    return;
                }
            }
            c.key.interestOps(SelectionKey.OP_READ);
        }

        void onRead(Selector sel, Conn c) throws IOException {
            if (mode.equals("stream")) {
                for (;;) {
                    c.in.clear();
                    int n = c.ch.read(c.in);
                    if (n < 0) throw new IOException("closed");
                    if (n == 0) break;
                    c.in.flip();
                    check(c, n);
                    c.received += n;
                    if (phase == 1) bytes += n;
                }
                if (c.sent - c.received < WINDOW) c.key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                return;
            }
            int n = c.ch.read(c.in);
            if (n < 0) throw new IOException("closed");
            if (c.in.hasRemaining()) return;
            c.in.flip();
            if (BLOCK.slice(0, size).mismatch(c.in) != -1) {
                if (phase == 1) {
                    errors++;
                    noteError("echo content mismatch");
                }
            }
            long us = (System.nanoTime() - c.start) / 1000;
            if (phase == 1) {
                ops++;
                bytes += size;
                if (us >= HIST) overflow++; else hist[(int) us]++;
                if (us > maxUs) maxUs = us;
            }
            if (mode.equals("churn")) {
                c.ch.close();
                if (phase != 2) open(sel, c);
            } else {
                begin(c);
            }
        }

        /** Verifies echoed stream bytes against the position-dependent pattern. */
        void check(Conn c, int n) {
            int off = (int) (c.received & 0xFFFF);
            int first = Math.min(n, 65536 - off);
            if (BLOCK.slice(off, first).mismatch(c.in.slice(0, first)) != -1
                    || (n > first && BLOCK.slice(0, n - first).mismatch(c.in.slice(first, n - first)) != -1)) {
                errors++;
            }
        }
    }
}
