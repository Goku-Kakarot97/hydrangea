package redis;

import com.sun.management.OperatingSystemMXBean;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.redis.RedisClient;
import io.hydrangea.redis.RedisConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Load test of two Redis clients against the same Redis: Hydrangea's {@code io.hydrangea.redis} and Jedis. One scenario, one client, one JVM per run (see
 * {@code benchmark/redis-bench.sh}); the last line printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... redis.RedisBench hydrangea|hydrangea-conns|jedis scenario [key=value ...]     (host=, port=, seconds=, warmup=, threads=, window=, size=)
 * scenarios: setup, connect, get, pipeline, large, bulk-reply, pubsub, tx
 * </pre>
 * {@code hydrangea} shares ONE connection among all threads (that is how the client is meant to be used); {@code hydrangea-conns} gives each thread a connection
 * of its own, as a pool does for Jedis, to separate the effect of sharing from the rest. Jedis uses a {@code JedisPool} with one connection per thread.
 * A blocking call of Hydrangea is {@code client.get(k).join()}.
 */
public final class RedisBench {

    static Map<String, String> args = Map.of();
    static String lib;
    static String host;
    static int port;

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    // ---- histogram: 16 sub-buckets per power of two ----

    static final class Histogram {
        final long[] counts = new long[64 * 16];

        void record(long nanos) {
            int msb = 63 - Long.numberOfLeadingZeros(Math.max(nanos, 1));
            int sub = msb >= 4 ? (int) ((nanos >>> (msb - 4)) & 15) : (int) (nanos & 15);
            counts[msb * 16 + sub]++;
        }

        void add(Histogram o) {
            for (int i = 0; i < counts.length; i++) counts[i] += o.counts[i];
        }

        long percentile(double p) {
            long total = 0;
            for (long c : counts) total += c;
            long target = (long) Math.ceil(total * p), seen = 0;
            for (int i = 0; i < counts.length; i++) {
                seen += counts[i];
                if (seen >= target && counts[i] > 0) {
                    int msb = i / 16, sub = i % 16;
                    return msb >= 4 ? (16L + sub) << (msb - 4) : sub;
                }
            }
            return 0;
        }
    }

    static double cpuSeconds() {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1e9;
    }

    static long rssMb() {
        try {
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid())).start();
            return Long.parseLong(new String(p.getInputStream().readAllBytes(), StandardCharsets.US_ASCII).trim()) / 1024;
        } catch (Exception e) {
            return -1;
        }
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static String f1(double v) {
        return String.format(Locale.ROOT, "%.1f", v);
    }

    static void result(String scenario, String... kv) {
        StringBuilder sb = new StringBuilder("RESULT scenario=").append(scenario).append(" lib=").append(lib);
        for (int i = 0; i < kv.length; i += 2) sb.append(' ').append(kv[i]).append('=').append(kv[i + 1]);
        System.out.println(sb);
    }

    static EventLoopGroup group;
    static RedisClient shared;
    static JedisPool pool;

    /** One handle per worker: for Jedis a pooled connection, for Hydrangea the shared client or one of its own. */
    interface Handle extends AutoCloseable {
        String get(String key);

        void set(String key, String value);

        byte[] getBytes(String key);

        void setBytes(String key, byte[] value);

        @Override
        void close();
    }

    static Handle handle() throws Exception {
        if (lib.equals("jedis") && arg("borrow", "false").equals("true")) {
            return new Handle() {
                public String get(String key) {
                    try (Jedis j = pool.getResource()) {
                        return j.get(key);
                    }
                }

                public void set(String key, String value) {
                    try (Jedis j = pool.getResource()) {
                        j.set(key, value);
                    }
                }

                public byte[] getBytes(String key) {
                    try (Jedis j = pool.getResource()) {
                        return j.get(key.getBytes(StandardCharsets.UTF_8));
                    }
                }

                public void setBytes(String key, byte[] value) {
                    try (Jedis j = pool.getResource()) {
                        j.set(key.getBytes(StandardCharsets.UTF_8), value);
                    }
                }

                public void close() {
                }
            };
        }
        if (lib.equals("jedis")) {
            Jedis j = pool.getResource();
            return new Handle() {
                public String get(String key) {
                    return j.get(key);
                }

                public void set(String key, String value) {
                    j.set(key, value);
                }

                public byte[] getBytes(String key) {
                    return j.get(key.getBytes(StandardCharsets.UTF_8));
                }

                public void setBytes(String key, byte[] value) {
                    j.set(key.getBytes(StandardCharsets.UTF_8), value);
                }

                public void close() {
                    j.close();
                }
            };
        }
        RedisClient c = lib.equals("hydrangea") ? shared : RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
        boolean own = c != shared;
        return new Handle() {
            public String get(String key) {
                return c.get(key).join();
            }

            public void set(String key, String value) {
                c.set(key, value).join();
            }

            public byte[] getBytes(String key) {
                return c.getBytes(key).join();
            }

            public void setBytes(String key, byte[] value) {
                c.setBytes(key, value).join();
            }

            public void close() {
                if (own) c.close();
            }
        };
    }

    static void init(int threads) throws Exception {
        if (lib.equals("jedis")) {
            JedisPoolConfig cfg = new JedisPoolConfig();
            // each worker holds one connection (its Handle) and the bulk-reply / tx scenarios borrow a second one per call: room for both, or the pool deadlocks
            int size = args.containsKey("pool") ? iarg("pool", 32) : threads * 2 + 2;
            cfg.setMaxTotal(size);
            cfg.setMaxIdle(size);
            cfg.setMinIdle(threads);
            cfg.setBlockWhenExhausted(true);
            pool = new JedisPool(cfg, host, port);
        } else {
            group = new EventLoopGroup(2);
            if (arg("metrics", "off").equals("on")) {
                group.metrics().bindTo(io.hydrangea.MetricsSink.NOOP);   // switches latency recording on (what a real sink does), to measure its cost
            }
            if (lib.equals("hydrangea")) {
                shared = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            }
        }
    }

    public static void main(String[] a) throws Exception {
        lib = a[0];
        String scenario = a[1];
        Map<String, String> m = new HashMap<>();
        for (int i = 2; i < a.length; i++) {
            String[] kv = a[i].split("=", 2);
            m.put(kv[0], kv[1]);
        }
        args = m;
        host = arg("host", "127.0.0.1");
        port = iarg("port", 6379);
        try {
            switch (scenario) {
                case "setup" -> setup();
                case "connect" -> connect();
                case "get" -> get();
                case "pipeline" -> pipeline();
                case "large" -> large();
                case "bulk-reply" -> bulkReply();
                case "pubsub" -> pubsub();
                case "tx" -> tx();
                default -> throw new IllegalArgumentException(scenario);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    static final int KEYS = 100_000;

    static String value(int size) {
        return "x".repeat(size);
    }

    // ---- scenarios ----

    /** Fills the data the other scenarios read (always with Hydrangea's pipelining: the quickest way). */
    static void setup() throws Exception {
        try (EventLoopGroup g = new EventLoopGroup(1)) {
            RedisClient c = RedisClient.connect(g, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            c.flushDb().get();
            String v = value(128);
            List<CompletableFuture<Void>> fs = new ArrayList<>();
            for (int i = 0; i < KEYS; i++) {
                fs.add(c.set("bench:k" + i, v));
                if (fs.size() == 5000) {
                    for (var f : fs) f.get();
                    fs.clear();
                }
            }
            for (var f : fs) f.get();
            Map<String, String> h = new LinkedHashMap<>();
            for (int i = 0; i < 1000; i++) h.put("field" + i, "value-" + i);
            c.hset("bench:hash", h).get();
            String[] items = new String[10_000];
            for (int i = 0; i < items.length; i++) items[i] = "item-number-" + i;
            c.rpush("bench:list", items).get();
            c.sadd("bench:set", items).get();
            System.out.println("setup done: " + c.dbSize().get() + " keys");
        }
    }

    static void connect() throws Exception {
        int n = iarg("connections", 500);
        init(1);
        for (int i = 0; i < 30; i++) {
            oneConnection();
        }
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        Histogram hist = new Histogram();
        for (int i = 0; i < n; i++) {
            long s0 = System.nanoTime();
            oneConnection();
            hist.record(System.nanoTime() - s0);
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        result("connect", "connections", Integer.toString(n), "per_s", f1(n / secs), "p50_us", Long.toString(hist.percentile(0.5) / 1000), "p99_us", Long.toString(hist.percentile(0.99) / 1000),
                "client_cpu_ms_per_conn", f1((cpuSeconds() - cpu0) * 1000 / n), "threads_peak", Long.toString(ManagementFactory.getThreadMXBean().getPeakThreadCount()), "rss_mb", Long.toString(rssMb()));
    }

    /** Opens a connection (no pool), runs one command, closes it. */
    static void oneConnection() throws Exception {
        if (lib.equals("jedis")) {
            try (Jedis j = new Jedis(host, port)) {
                j.ping();
            }
        } else {
            RedisClient c = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            c.ping().get();
            c.disconnect().get();
        }
    }

    interface Work {
        void run(Handle h, Random rnd) throws Exception;
    }

    /** {@code threads} workers loop over {@code work} for {@code seconds} after {@code warmup}; reports throughput, latency and CPU. */
    static void closedLoop(String scenario, int threads, boolean virtual, int seconds, int warm, Work work, String... extra) throws Exception {
        init(threads);
        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean measuring = new AtomicBoolean();
        LongAdder done = new LongAdder();
        Histogram[] hists = new Histogram[threads];
        List<Thread> ts = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(threads);
        List<Throwable> errors = new CopyOnWriteArrayList<>();
        long setupStart = System.nanoTime();
        for (int i = 0; i < threads; i++) {
            int id = i;
            hists[i] = new Histogram();
            Runnable r = () -> {
                try (Handle h = handle()) {
                    Random rnd = new Random(id * 7919L + 1);
                    work.run(h, rnd);
                    ready.countDown();
                    while (!stop.get()) {
                        long t0 = System.nanoTime();
                        work.run(h, rnd);
                        if (measuring.get()) {
                            hists[id].record(System.nanoTime() - t0);
                            done.increment();
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                    ready.countDown();
                }
            };
            ts.add(virtual ? Thread.ofVirtual().unstarted(r) : new Thread(r));
        }
        ts.forEach(Thread::start);
        ready.await();
        double setupSecs = (System.nanoTime() - setupStart) / 1e9;
        sleep(warm * 1000L);
        double cpu0 = cpuSeconds();
        long d0 = done.sum();
        long t0 = System.nanoTime();
        measuring.set(true);
        sleep(seconds * 1000L);
        measuring.set(false);
        long d1 = done.sum();
        double secs = (System.nanoTime() - t0) / 1e9;
        double cpu1 = cpuSeconds();
        long threadsPeak = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        stop.set(true);
        for (Thread t : ts) t.join();
        if (!errors.isEmpty()) {
            errors.get(0).printStackTrace();
            throw new IllegalStateException(errors.size() + " workers failed");
        }
        Histogram all = new Histogram();
        for (Histogram hh : hists) all.add(hh);
        List<String> kv = new ArrayList<>(List.of("threads", Integer.toString(threads), "per_s", f1((d1 - d0) / secs), "p50_us", Long.toString(all.percentile(0.5) / 1000),
                "p99_us", Long.toString(all.percentile(0.99) / 1000), "p999_us", Long.toString(all.percentile(0.999) / 1000), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / Math.max(1, d1 - d0) * 1e5),
                "setup_s", f1(setupSecs), "threads_peak", Long.toString(threadsPeak), "rss_mb", Long.toString(rss)));
        kv.addAll(Arrays.asList(extra));
        result(scenario, kv.toArray(new String[0]));
    }

    /** Reads and writes 128-byte values at random keys (80 % reads), one request at a time per thread. */
    static void get() throws Exception {
        String v = value(128);
        closedLoop("get", iarg("threads", 1), arg("virtual", "false").equals("true"), iarg("seconds", 10), iarg("warmup", 3), (h, rnd) -> {
            String key = "bench:k" + rnd.nextInt(KEYS);
            if (rnd.nextInt(10) < 8) {
                if (h.get(key) == null) throw new IllegalStateException("missing " + key);
            } else {
                h.set(key, v);
            }
        });
    }

    /**
     * One connection, many requests at a time. Hydrangea keeps {@code window} GETs in flight and issues a new one as each reply arrives; Jedis sends a batch of
     * {@code window} through a Pipeline and waits for all of them (its way of pipelining). SET of 128-byte values with {@code op=set}, GET otherwise.
     */
    static void pipeline() throws Exception {
        int window = iarg("window", 100);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        boolean set = arg("op", "get").equals("set");
        String v = value(128);
        Random rnd = new Random(1);
        long done = 0;
        long measuredFrom = 0;
        double cpu0 = 0;
        long t0 = 0;
        long deadline = System.nanoTime() + (warm + seconds) * 1_000_000_000L;
        long measureStart = System.nanoTime() + warm * 1_000_000_000L;
        boolean started = false;
        if (lib.equals("jedis")) {
            try (Jedis j = new Jedis(host, port)) {
                while (System.nanoTime() < deadline) {
                    Pipeline p = j.pipelined();
                    List<Response<String>> rs = new ArrayList<>(window);
                    for (int i = 0; i < window; i++) {
                        String key = "bench:k" + rnd.nextInt(KEYS);
                        if (set) p.set(key, v);
                        else rs.add(p.get(key));
                    }
                    p.sync();
                    done += window;
                    if (!started && System.nanoTime() >= measureStart) {
                        started = true;
                        cpu0 = cpuSeconds();
                        t0 = System.nanoTime();
                        measuredFrom = done;
                    }
                }
            }
        } else {
            RedisClient c = RedisClient.connect(group = new EventLoopGroup(2), RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            ArrayDeque<CompletableFuture<?>> q = new ArrayDeque<>();
            for (int i = 0; i < window; i++) {
                q.add(set ? c.set("bench:k" + rnd.nextInt(KEYS), v) : c.get("bench:k" + rnd.nextInt(KEYS)));
            }
            while (System.nanoTime() < deadline) {
                q.poll().join();
                q.add(set ? c.set("bench:k" + rnd.nextInt(KEYS), v) : c.get("bench:k" + rnd.nextInt(KEYS)));
                done++;
                if (!started && System.nanoTime() >= measureStart) {
                    started = true;
                    cpu0 = cpuSeconds();
                    t0 = System.nanoTime();
                    measuredFrom = done;
                }
            }
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        long n = done - measuredFrom;
        result("pipeline", "window", Integer.toString(window), "per_s", f1(n / secs), "client_cpu_s_per_100k", f1((cpuSeconds() - cpu0) / Math.max(1, n) * 1e5),
                "threads_peak", Long.toString(ManagementFactory.getThreadMXBean().getPeakThreadCount()), "rss_mb", Long.toString(rssMb()));
    }

    /** Writes and reads back values of a given size: megabytes per second each way. */
    static void large() throws Exception {
        int size = iarg("size", 1024 * 1024);
        int rounds = iarg("rounds", 200);
        init(1);
        byte[] data = new byte[size];
        new Random(1).nextBytes(data);
        try (Handle h = handle()) {
            double w = 0, r = 0, cpu0 = 0;
            for (int i = -10; i < rounds; i++) {
                if (i == 0) cpu0 = cpuSeconds();
                String key = "bench:large:" + (i & 3);
                long t0 = System.nanoTime();
                h.setBytes(key, data);
                long t1 = System.nanoTime();
                byte[] back = h.getBytes(key);
                long t2 = System.nanoTime();
                if (back.length != size) throw new IllegalStateException("short");
                if (i >= 0) {
                    w += (t1 - t0) / 1e9;
                    r += (t2 - t1) / 1e9;
                }
            }
            double mb = size / 1048576.0 * rounds;
            result("large", "size_kb", Integer.toString(size / 1024), "write_mb_per_s", f1(mb / w), "read_mb_per_s", f1(mb / r), "client_cpu_s_per_gb", f1((cpuSeconds() - cpu0) / (mb * 2 / 1024)),
                    "rss_mb", Long.toString(rssMb()));
        }
    }

    /** Replies with many elements: the cost of reading and building them. {@code what}: mget (100 keys), hgetall (1000 fields), lrange (10 000), smembers (10 000). */
    static void bulkReply() throws Exception {
        String what = arg("what", "lrange");
        String[] mgetKeys = new String[100];
        for (int i = 0; i < 100; i++) mgetKeys[i] = "bench:k" + i;
        closedLoop("bulk-reply-" + what, iarg("threads", 1), false, iarg("seconds", 10), iarg("warmup", 3), (h, rnd) -> {
            int n;
            if (lib.equals("jedis")) {
                try (Jedis j = pool.getResource()) {
                    n = switch (what) {
                        case "mget" -> j.mget(mgetKeys).size();
                        case "hgetall" -> j.hgetAll("bench:hash").size();
                        case "lrange" -> j.lrange("bench:list", 0, -1).size();
                        default -> j.smembers("bench:set").size();
                    };
                }
            } else {
                RedisClient c = bulkClient(h);
                n = switch (what) {
                    case "mget" -> c.mget(mgetKeys).join().size();
                    case "hgetall" -> c.hgetAll("bench:hash").join().size();
                    case "lrange" -> c.lrange("bench:list", 0, -1).join().size();
                    default -> c.smembers("bench:set").join().size();
                };
            }
            if (n < 100) throw new IllegalStateException("short reply " + n);
        });
    }

    static RedisClient bulkClient(Handle h) {
        return shared != null ? shared : bulkOwn();
    }

    static final ThreadLocal<RedisClient> OWN = new ThreadLocal<>();

    static RedisClient bulkOwn() {
        RedisClient c = OWN.get();
        if (c == null) {
            try {
                c = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            OWN.set(c);
        }
        return c;
    }

    /** A publisher on one connection, a subscriber on another: messages per second that reach the subscriber, 128-byte payloads. */
    static void pubsub() throws Exception {
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        int window = iarg("window", 100);
        String channel = "bench:chan";
        String payload = value(128);
        LongAdder received = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean();
        long published = 0, measuredFrom = 0;
        double cpu0 = 0;
        long t0 = 0;
        long r0 = 0;
        long measureStart = System.nanoTime() + warm * 1_000_000_000L;
        long deadline = measureStart + seconds * 1_000_000_000L;
        boolean started = false;
        if (lib.equals("jedis")) {
            CountDownLatch subscribed = new CountDownLatch(1);
            JedisPubSub ps = new JedisPubSub() {
                @Override
                public void onMessage(String c, String m) {
                    received.increment();
                }

                @Override
                public void onSubscribe(String c, int n) {
                    subscribed.countDown();
                }
            };
            Jedis subJ = new Jedis(host, port);
            Thread sub = new Thread(() -> subJ.subscribe(ps, channel));
            sub.setDaemon(true);
            sub.start();
            subscribed.await();
            try (Jedis pub = new Jedis(host, port)) {
                while (System.nanoTime() < deadline) {
                    Pipeline p = pub.pipelined();
                    for (int i = 0; i < window; i++) p.publish(channel, payload);
                    p.sync();
                    published += window;
                    if (!started && System.nanoTime() >= measureStart) {
                        started = true;
                        cpu0 = cpuSeconds();
                        t0 = System.nanoTime();
                        measuredFrom = published;
                        r0 = received.sum();
                    }
                }
            }
        } else {
            group = new EventLoopGroup(2);
            RedisClient pub = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            RedisClient sub = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            sub.subscribe(channel, (ch, pat, m) -> received.increment()).get();
            ArrayDeque<CompletableFuture<?>> q = new ArrayDeque<>();
            for (int i = 0; i < window; i++) q.add(pub.publish(channel, payload));
            while (System.nanoTime() < deadline) {
                q.poll().join();
                q.add(pub.publish(channel, payload));
                published++;
                if (!started && System.nanoTime() >= measureStart) {
                    started = true;
                    cpu0 = cpuSeconds();
                    t0 = System.nanoTime();
                    measuredFrom = published;
                    r0 = received.sum();
                }
            }
        }
        double secs = (System.nanoTime() - t0) / 1e9;
        double cpu1 = cpuSeconds();
        sleep(500);
        long n = received.sum() - r0;
        result("pubsub", "published_per_s", f1((published - measuredFrom) / secs), "received_per_s", f1(n / secs), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / Math.max(1, n) * 1e5),
                "threads_peak", Long.toString(ManagementFactory.getThreadMXBean().getPeakThreadCount()), "rss_mb", Long.toString(rssMb()));
    }

    /** MULTI, three commands, EXEC: transactions per second on one connection per thread (Hydrangea writes the five commands together). */
    static void tx() throws Exception {
        closedLoop("tx", iarg("threads", 8), false, iarg("seconds", 10), iarg("warmup", 3), (h, rnd) -> {
            String key = "bench:tx:" + rnd.nextInt(1000);
            if (lib.equals("jedis")) {
                try (Jedis j = pool.getResource()) {
                    var t = j.multi();
                    t.incr(key);
                    t.expire(key, 60);
                    t.get(key);
                    t.exec();
                }
            } else {
                RedisClient c = bulkClient(h);
                c.transaction(new Object[] {"INCR", key}, new Object[] {"EXPIRE", key, 60}, new Object[] {"GET", key}).join();
            }
        });
    }
}
