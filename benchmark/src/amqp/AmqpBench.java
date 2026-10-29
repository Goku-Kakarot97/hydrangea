package amqp;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.IntConsumer;

/**
 * Load test of two AMQP 0-9-1 clients against the same RabbitMQ: Hydrangea's {@code io.hydrangea.amqp} and the official
 * {@code com.rabbitmq:amqp-client}. One scenario, one library, one JVM per run (see {@code benchmark/amqp-bench.sh}); the last line
 * printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... amqp.AmqpBench hydrangea|official scenario [key=value ...]     (host=, port=, seconds=, size=, producers=, window=, ...)
 * scenarios: publish-noconfirm, publish-confirm, consume, latency, connections
 * </pre>
 */
public final class AmqpBench {

    // ---- the two libraries behind one small interface (everything else is shared, so both do identical work) ----

    interface Lib extends AutoCloseable {
        Conn connect() throws Exception;

        @Override
        void close() throws Exception;
    }

    interface Conn extends AutoCloseable {
        Chan channel() throws Exception;

        /** False when the library's socket buffers are full and a producer should wait. */
        boolean writable();

        /** Returns when more data may be sent (or {@code stop} is set): Hydrangea's {@code whenWritable()}; nothing to do for a blocking client. */
        default void awaitWritable(AtomicBoolean stop) throws Exception {
        }

        @Override
        void close() throws Exception;
    }

    interface Chan {
        void queueDeclare(String q, boolean bounded) throws Exception;

        void queueDelete(String q) throws Exception;

        long messageCount(String q) throws Exception;

        /** Fire and forget. */
        void publish(String q, byte[] body) throws Exception;

        /** Confirm mode; {@code onConfirmed} is told how many messages the broker has confirmed (any thread). */
        void confirmSelect(IntConsumer onConfirmed) throws Exception;

        void publishConfirmed(String q, byte[] body) throws Exception;

        /** Consume; with {@code ackEach} the library acknowledges every message after the handler returns. */
        void consume(String q, int prefetch, boolean ackEach, Consumer<byte[]> handler) throws Exception;
    }

    // ---- Hydrangea ----

    static final class HydLib implements Lib {
        private final String host;
        private final int port;
        private final io.hydrangea.EventLoopGroup group = new io.hydrangea.EventLoopGroup(2);

        HydLib(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        public Conn connect() throws Exception {
            var c = io.hydrangea.amqp.AmqpConnection.connect(group, io.hydrangea.amqp.AmqpConfig.of(host, port)).get(30, TimeUnit.SECONDS);
            return new Conn() {
                @Override
                public Chan channel() throws Exception {
                    return new HydChan(c.openChannel().get(30, TimeUnit.SECONDS));
                }

                @Override
                public boolean writable() {
                    return c.isWritable();
                }

                @Override
                public void awaitWritable(AtomicBoolean stop) throws Exception {
                    if (c.isWritable()) {
                        return;
                    }
                    var f = c.whenWritable();
                    while (!stop.get()) {
                        try {
                            f.get(50, TimeUnit.MILLISECONDS);
                            return;
                        } catch (java.util.concurrent.TimeoutException stillWaiting) {
                            // look at stop, then wait again
                        }
                    }
                }

                @Override
                public void close() throws Exception {
                    c.closeAsync().get(30, TimeUnit.SECONDS);
                }
            };
        }

        @Override
        public void close() {
            group.close();
        }
    }

    static final class HydChan implements Chan {
        private final io.hydrangea.amqp.AmqpChannel ch;
        private volatile IntConsumer onConfirmed = n -> { };
        final LongAdder failed = new LongAdder();

        HydChan(io.hydrangea.amqp.AmqpChannel ch) {
            this.ch = ch;
        }

        @Override
        public void queueDeclare(String q, boolean bounded) throws Exception {
            ch.queueDeclare(q, false, false, false, bounded ? Map.of("x-max-length", 10000, "x-overflow", "drop-head") : null).get(30, TimeUnit.SECONDS);
        }

        @Override
        public void queueDelete(String q) throws Exception {
            ch.queueDelete(q, false, false).get(30, TimeUnit.SECONDS);
        }

        @Override
        public long messageCount(String q) throws Exception {
            return ch.queueDeclarePassive(q).get(30, TimeUnit.SECONDS).messageCount();
        }

        @Override
        public void publish(String q, byte[] body) {
            ch.basicPublish("", q, false, null, body);
        }

        @Override
        public void confirmSelect(IntConsumer onConfirmed) throws Exception {
            this.onConfirmed = onConfirmed;
            ch.confirmSelect().get(30, TimeUnit.SECONDS);
        }

        @Override
        public void publishConfirmed(String q, byte[] body) {
            ch.basicPublish("", q, false, null, body).whenComplete((v, e) -> {
                if (e == null) {
                    onConfirmed.accept(1);
                } else {
                    failed.increment();
                }
            });
        }

        @Override
        public void consume(String q, int prefetch, boolean ackEach, Consumer<byte[]> handler) throws Exception {
            if (ackEach) {
                ch.basicQos(prefetch).get(30, TimeUnit.SECONDS);
            }
            ch.basicConsume(q, !ackEach, d -> {
                handler.accept(d.body());
                if (ackEach) {
                    ch.basicAck(d.deliveryTag(), false);
                }
            }).get(30, TimeUnit.SECONDS);
        }
    }

    // ---- the official client ----

    static final class OffLib implements Lib {
        private final com.rabbitmq.client.ConnectionFactory f = new com.rabbitmq.client.ConnectionFactory();

        OffLib(String host, int port) {
            f.setHost(host);
            f.setPort(port);
            f.setAutomaticRecoveryEnabled(false); // Hydrangea's AmqpConnection does not reconnect either: compare the plain paths
            f.setTopologyRecoveryEnabled(false);
        }

        @Override
        public Conn connect() throws Exception {
            com.rabbitmq.client.Connection c = f.newConnection("amqp-bench");
            return new Conn() {
                @Override
                public Chan channel() throws Exception {
                    return new OffChan(c.createChannel());
                }

                @Override
                public boolean writable() {
                    return true; // blocking writes: backpressure is the call not returning
                }

                @Override
                public void close() throws Exception {
                    c.close(30_000);
                }
            };
        }

        @Override
        public void close() {
        }
    }

    static final class OffChan implements Chan {
        private final com.rabbitmq.client.Channel ch;
        private final ConcurrentSkipListSet<Long> unconfirmed = new ConcurrentSkipListSet<>();
        private volatile IntConsumer onConfirmed = n -> { };
        final LongAdder failed = new LongAdder();

        OffChan(com.rabbitmq.client.Channel ch) {
            this.ch = ch;
        }

        @Override
        public void queueDeclare(String q, boolean bounded) throws Exception {
            ch.queueDeclare(q, false, false, false, bounded ? Map.of("x-max-length", 10000, "x-overflow", "drop-head") : null);
        }

        @Override
        public void queueDelete(String q) throws Exception {
            ch.queueDelete(q);
        }

        @Override
        public long messageCount(String q) throws Exception {
            return ch.queueDeclarePassive(q).getMessageCount();
        }

        @Override
        public void publish(String q, byte[] body) throws Exception {
            synchronized (ch) {
                ch.basicPublish("", q, null, body);
            }
        }

        @Override
        public void confirmSelect(IntConsumer onConfirmed) throws Exception {
            this.onConfirmed = onConfirmed;
            ch.confirmSelect();
            ch.addConfirmListener((tag, multiple) -> onConfirmed.accept(remove(tag, multiple)), (tag, multiple) -> failed.add(remove(tag, multiple)));
        }

        private int remove(long tag, boolean multiple) {
            if (!multiple) {
                return unconfirmed.remove(tag) ? 1 : 0;
            }
            var head = unconfirmed.headSet(tag, true);
            int n = head.size();
            head.clear();
            return n;
        }

        @Override
        public void publishConfirmed(String q, byte[] body) throws Exception {
            synchronized (ch) {
                unconfirmed.add(ch.getNextPublishSeqNo());
                ch.basicPublish("", q, null, body);
            }
        }

        @Override
        public void consume(String q, int prefetch, boolean ackEach, Consumer<byte[]> handler) throws Exception {
            if (ackEach) {
                ch.basicQos(prefetch);
            }
            ch.basicConsume(q, !ackEach, (tag, d) -> {
                handler.accept(d.getBody());
                if (ackEach) {
                    ch.basicAck(d.getEnvelope().getDeliveryTag(), false);
                }
            }, (String tag) -> { });
        }
    }

    // ---- harness ----

    static Map<String, String> args = Map.of();

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    private static final OperatingSystemMXBean OS = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    static double cpuSeconds() {
        return OS.getProcessCpuTime() / 1e9;
    }

    static long rssMb() {
        try {
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid())).start();
            String s = new String(p.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim();
            return Long.parseLong(s) / 1024;
        } catch (Exception e) {
            return -1;
        }
    }

    static byte[] payload(int size) {
        byte[] b = new byte[size];
        Arrays.fill(b, (byte) 'x');
        return b;
    }

    /**
     * How a producer waits while the connection is not writable: {@code callback} (default; Hydrangea's {@code whenWritable()}, a no-op for the
     * blocking official client), {@code park} / {@code spin} (poll {@code isWritable()}), or {@code none} (queue everything in memory).
     */
    static void waitWritable(Conn c, AtomicBoolean stop) throws Exception {
        String mode = arg("backoff", "callback");
        if (mode.equals("none")) {
            return;
        }
        if (mode.equals("callback")) {
            c.awaitWritable(stop);
            return;
        }
        while (!c.writable() && !stop.get()) {
            if (mode.equals("spin")) {
                Thread.onSpinWait();
            } else {
                java.util.concurrent.locks.LockSupport.parkNanos(20_000);
            }
        }
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static String uniq(String p) {
        return "bench-" + p + "-" + Long.toString(System.nanoTime() & 0xffffffffL, 36);
    }

    static void result(String scenario, String lib, String... kv) {
        StringBuilder sb = new StringBuilder("RESULT scenario=").append(scenario).append(" lib=").append(lib);
        for (int i = 0; i < kv.length; i += 2) {
            sb.append(' ').append(kv[i]).append('=').append(kv[i + 1]);
        }
        System.out.println(sb);
    }

    static String f1(double v) {
        return String.format(java.util.Locale.ROOT, "%.1f", v);
    }

    public static void main(String[] a) throws Exception {
        String lib = a[0];
        String scenario = a[1];
        Map<String, String> m = new java.util.HashMap<>();
        for (int i = 2; i < a.length; i++) {
            String[] kv = a[i].split("=", 2);
            m.put(kv[0], kv[1]);
        }
        args = m;
        String host = arg("host", "127.0.0.1");
        int port = iarg("port", 25672);
        try (Lib l = lib.equals("hydrangea") ? new HydLib(host, port) : new OffLib(host, port)) {
            switch (scenario) {
                case "publish-noconfirm" -> publishNoConfirm(l, lib);
                case "publish-confirm" -> publishConfirm(l, lib);
                case "consume" -> consume(l, lib);
                case "latency" -> latency(l, lib);
                case "connections" -> connections(l, lib);
                default -> throw new IllegalArgumentException(scenario);
            }
        }
        System.exit(0);
    }

    // ---- scenarios ----

    /** Producers publish as fast as the library lets them (backpressure: a blocking write, or Hydrangea's isWritable). Verified against the broker's count. */
    static void publishNoConfirm(Lib l, String lib) throws Exception {
        int producers = iarg("producers", 2);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        byte[] body = payload(iarg("size", 128));
        List<Conn> conns = new ArrayList<>();
        List<Chan> chans = new ArrayList<>();
        List<String> queues = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            Conn c = l.connect();
            Chan ch = c.channel();
            String q = uniq("pub" + i);
            ch.queueDeclare(q, false);
            conns.add(c);
            chans.add(ch);
            queues.add(q);
        }
        LongAdder sent = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            Conn c = conns.get(i);
            Chan ch = chans.get(i);
            String q = queues.get(i);
            Thread t = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        waitWritable(c, stop);
                        ch.publish(q, body);
                        sent.increment();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
            ts.add(t);
        }
        sleep(warm * 1000L);
        long s0 = sent.sum();
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        sleep(seconds * 1000L);
        long s1 = sent.sum();
        double cpu1 = cpuSeconds();
        double secs = (System.nanoTime() - t0) / 1e9;
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        long total = sent.sum();
        long inBroker = settle(chans, queues);
        long threads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        for (int i = 0; i < producers; i++) {
            chans.get(i).queueDelete(queues.get(i));
            conns.get(i).close();
        }
        result("publish-noconfirm", lib, "msgs_per_s", f1((s1 - s0) / secs), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / ((s1 - s0) / 1e5)),
                "sent_total", Long.toString(total), "in_broker", Long.toString(inBroker), "lost", Long.toString(total - inBroker),
                "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }

    /** Waits until the broker's message count stops changing and returns it. */
    static long settle(List<Chan> chans, List<String> queues) throws Exception {
        long last = -1;
        int stable = 0;
        for (int i = 0; i < 200 && stable < 4; i++) {
            long n = 0;
            for (int k = 0; k < chans.size(); k++) {
                n += chans.get(k).messageCount(queues.get(k));
            }
            stable = n == last ? stable + 1 : 0;
            last = n;
            sleep(250);
        }
        return last;
    }

    /** Publisher confirms with a window of outstanding messages per producer; the throughput is what the broker has confirmed. */
    static void publishConfirm(Lib l, String lib) throws Exception {
        int producers = iarg("producers", 2);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        int window = iarg("window", 1000);
        int size = iarg("size", 128);
        boolean bounded = size >= 16 * 1024;
        byte[] body = payload(size);
        List<Conn> conns = new ArrayList<>();
        List<Chan> chans = new ArrayList<>();
        List<String> queues = new ArrayList<>();
        Semaphore[] permits = new Semaphore[producers];
        LongAdder confirmed = new LongAdder();
        for (int i = 0; i < producers; i++) {
            Conn c = l.connect();
            Chan ch = c.channel();
            String q = uniq("conf" + i);
            ch.queueDeclare(q, bounded);
            Semaphore p = new Semaphore(window);
            permits[i] = p;
            ch.confirmSelect(n -> {
                confirmed.add(n);
                p.release(n);
            });
            conns.add(c);
            chans.add(ch);
            queues.add(q);
        }
        AtomicBoolean stop = new AtomicBoolean();
        LongAdder issued = new LongAdder();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            Conn c = conns.get(i);
            Chan ch = chans.get(i);
            String q = queues.get(i);
            Semaphore p = permits[i];
            Thread t = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        if (!p.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                            continue;
                        }
                        waitWritable(c, stop);
                        ch.publishConfirmed(q, body);
                        issued.increment();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
            ts.add(t);
        }
        sleep(warm * 1000L);
        long c0 = confirmed.sum();
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        sleep(seconds * 1000L);
        long c1 = confirmed.sum();
        double cpu1 = cpuSeconds();
        double secs = (System.nanoTime() - t0) / 1e9;
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (confirmed.sum() < issued.sum() && System.nanoTime() < deadline) {
            sleep(20);
        }
        long unconfirmedAtEnd = issued.sum() - confirmed.sum();
        long inBroker = bounded ? -1 : settle(chans, queues);
        long threads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        for (int i = 0; i < producers; i++) {
            chans.get(i).queueDelete(queues.get(i));
            conns.get(i).close();
        }
        double rate = (c1 - c0) / secs;
        result("publish-confirm", lib, "msgs_per_s", f1(rate), "mb_per_s", f1(rate * size / 1e6), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / ((c1 - c0) / 1e5)),
                "issued", Long.toString(issued.sum()), "confirmed", Long.toString(confirmed.sum()), "unconfirmed_at_end", Long.toString(unconfirmedAtEnd),
                "in_broker", Long.toString(inBroker), "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }

    /** Fills a queue (not timed), then drains it with {@code consumers} competing consumers that acknowledge every message. */
    static void consume(Lib l, String lib) throws Exception {
        int total = iarg("messages", 200_000);
        int consumers = iarg("consumers", 2);
        int prefetch = iarg("prefetch", 250);
        byte[] body = payload(iarg("size", 128));
        Conn pc = l.connect();
        Chan pch = pc.channel();
        String q = uniq("cons");
        pch.queueDeclare(q, false);
        Semaphore win = new Semaphore(2000);
        LongAdder done = new LongAdder();
        pch.confirmSelect(n -> {
            done.add(n);
            win.release(n);
        });
        for (int i = 0; i < total; i++) {
            win.acquire();
            waitWritable(pc, new AtomicBoolean());
            pch.publishConfirmed(q, body);
        }
        while (done.sum() < total) {
            sleep(10);
        }
        long queued = pch.messageCount(q);

        LongAdder got = new LongAdder();
        AtomicLong first = new AtomicLong();
        AtomicLong last = new AtomicLong();
        CountDownLatch all = new CountDownLatch(1);
        List<Conn> conns = new ArrayList<>();
        double cpu0 = cpuSeconds();
        for (int i = 0; i < consumers; i++) {
            Conn c = l.connect();
            Chan ch = c.channel();
            conns.add(c);
            ch.consume(q, prefetch, true, b -> {
                long now = System.nanoTime();
                first.compareAndSet(0, now);
                got.increment();
                if (got.sum() == total) {
                    last.set(now);
                    all.countDown();
                }
            });
        }
        boolean ok = all.await(120, TimeUnit.SECONDS);
        double cpu1 = cpuSeconds();
        double secs = ok ? (last.get() - first.get()) / 1e9 : Double.NaN;
        sleep(500);
        long left = pch.messageCount(q);
        long threads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        pch.queueDelete(q);
        for (Conn c : conns) {
            c.close();
        }
        pc.close();
        result("consume", lib, "msgs_per_s", f1(total / secs), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / (total / 1e5)), "queued", Long.toString(queued),
                "received", Long.toString(got.sum()), "left_in_queue", Long.toString(left), "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }

    /** One message at a time through the broker (publish, deliver to a consumer on the same connection): the latency of the whole path. */
    static void latency(Lib l, String lib) throws Exception {
        int samples = iarg("samples", 20_000);
        int warm = iarg("warmup", 3_000);
        byte[] body = payload(iarg("size", 128));
        Conn c = l.connect();
        Chan pub = c.channel();
        Chan sub = c.channel();
        String q = uniq("lat");
        pub.queueDeclare(q, false);
        LinkedTransferQueue<Long> arrived = new LinkedTransferQueue<>();
        sub.consume(q, 1, false, b -> arrived.add(System.nanoTime()));
        long[] us = new long[samples];
        for (int i = 0; i < warm + samples; i++) {
            long t0 = System.nanoTime();
            pub.publish(q, body);
            Long t1 = arrived.poll(10, TimeUnit.SECONDS);
            if (t1 == null) {
                throw new IllegalStateException("no delivery");
            }
            if (i >= warm) {
                us[i - warm] = (t1 - t0) / 1000;
            }
        }
        Arrays.sort(us);
        pub.queueDelete(q);
        c.close();
        result("latency", lib, "p50_us", Long.toString(us[samples / 2]), "p90_us", Long.toString(us[samples * 9 / 10]), "p99_us", Long.toString(us[samples * 99 / 100]),
                "p999_us", Long.toString(us[samples * 999 / 1000]), "max_us", Long.toString(us[samples - 1]), "samples", Long.toString(samples));
    }

    /** Many connections at once, each with a confirmed publisher and an acknowledging consumer on its own queue; a few driver threads serve them all. */
    static void connections(Lib l, String lib) throws Exception {
        int n = iarg("connections", 100);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        int window = iarg("window", 10);
        int drivers = iarg("drivers", 4);
        byte[] body = payload(iarg("size", 128));
        Conn[] conns = new Conn[n];
        Chan[] pubs = new Chan[n];
        Chan[] subs = new Chan[n];
        String[] qs = new String[n];
        Semaphore[] permits = new Semaphore[n];
        LongAdder confirmed = new LongAdder();
        LongAdder delivered = new LongAdder();
        double cpuSetup0 = cpuSeconds();
        long ts0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            conns[i] = l.connect();
            pubs[i] = conns[i].channel();
            subs[i] = conns[i].channel();
            qs[i] = uniq("c" + i);
            pubs[i].queueDeclare(qs[i], false);
            Semaphore p = new Semaphore(window);
            permits[i] = p;
            pubs[i].confirmSelect(k -> {
                confirmed.add(k);
                p.release(k);
            });
            subs[i].consume(qs[i], 50, true, b -> delivered.increment());
        }
        double setupSecs = (System.nanoTime() - ts0) / 1e9;
        double setupCpu = cpuSeconds() - cpuSetup0;
        long threadsAfterSetup = Thread.getAllStackTraces().keySet().size();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> ts = new ArrayList<>();
        for (int d = 0; d < drivers; d++) {
            int dd = d;
            Thread t = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        boolean any = false;
                        for (int i = dd; i < n; i += drivers) {
                            if (permits[i].tryAcquire()) {
                                if (conns[i].writable()) {
                                    pubs[i].publishConfirmed(qs[i], body);
                                    any = true;
                                } else {
                                    permits[i].release();
                                }
                            }
                        }
                        if (!any) {
                            Thread.onSpinWait();
                            Thread.yield();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            t.start();
            ts.add(t);
        }
        sleep(warm * 1000L);
        long c0 = confirmed.sum();
        long d0 = delivered.sum();
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        sleep(seconds * 1000L);
        long c1 = confirmed.sum();
        long d1 = delivered.sum();
        double cpu1 = cpuSeconds();
        double secs = (System.nanoTime() - t0) / 1e9;
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        long threads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        sleep(500);
        for (int i = 0; i < n; i++) {
            try {
                pubs[i].queueDelete(qs[i]);
            } catch (Exception ignored) {
                // cleanup only
            }
            conns[i].close();
        }
        result("connections", lib, "connections", Integer.toString(n), "confirmed_per_s", f1((c1 - c0) / secs), "delivered_per_s", f1((d1 - d0) / secs),
                "client_cpu_s_per_100k", f1((cpu1 - cpu0) / ((d1 - d0) / 1e5)), "setup_s", f1(setupSecs), "setup_cpu_s", f1(setupCpu),
                "threads_after_setup", Long.toString(threadsAfterSetup), "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }
}
