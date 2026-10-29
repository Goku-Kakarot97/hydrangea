package kafka;

import com.sun.management.OperatingSystemMXBean;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.kafka.ConsumerConfig;
import io.hydrangea.kafka.ConsumerRecord;
import io.hydrangea.kafka.KafkaClient;
import io.hydrangea.kafka.KafkaConfig;
import io.hydrangea.kafka.KafkaConsumer;
import io.hydrangea.kafka.KafkaProducer;
import io.hydrangea.kafka.ProducerConfig;
import io.hydrangea.kafka.TopicPartition;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Hydrangea's Kafka client against the Apache Kafka Java client ({@code kafka-clients} 3.9.0), on the same broker. One scenario, one client, one JVM per run;
 * the last line printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... kafka.KafkaBench hydrangea|kafka-clients scenario [key=value ...]
 * keys: bootstrap= seconds= warmup= size= threads= partitions= acks= linger= batch= window= compression=
 * scenarios: produce (callers loop with a window of records in flight), produce-threads (virtual threads, one record each, wait for the ack),
 *            consume (read a pre-filled topic), consume-group (same with a group), e2e (latency from send to receive), connect
 * </pre>
 */
public final class KafkaBench {

    static Map<String, String> args = Map.of();
    static String lib;

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    static String f1(double v) {
        return String.format(Locale.ROOT, "%.1f", v);
    }

    static void result(String scenario, String... kv) {
        StringBuilder sb = new StringBuilder("RESULT scenario=").append(scenario).append(" lib=").append(lib);
        for (int i = 0; i < kv.length; i += 2) sb.append(' ').append(kv[i]).append('=').append(kv[i + 1]);
        System.out.println(sb);
    }

    /** CPU seconds used so far by the caller ("main") and by the I/O thread (Hydrangea's event loop, or the Java client's sender thread). */
    static double[] threadCpu() {
        java.lang.management.ThreadMXBean mx = ManagementFactory.getThreadMXBean();
        double main = 0, io = 0;
        for (long id : mx.getAllThreadIds()) {
            java.lang.management.ThreadInfo ti = mx.getThreadInfo(id);
            if (ti == null) continue;
            long cpu = mx.getThreadCpuTime(id);
            if (cpu < 0) continue;
            String n = ti.getThreadName();
            if (n.equals("main")) main += cpu / 1e9;
            else if (n.contains("-loop-") || n.contains("kafka-producer-network-thread")) io += cpu / 1e9;
        }
        return new double[] {main, io};
    }

    static String f3(double v) {
        return String.format(Locale.ROOT, "%.3f", v);
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

    static final class Histogram {
        final long[] counts = new long[64 * 16];

        synchronized void record(long nanos) {
            int msb = 63 - Long.numberOfLeadingZeros(Math.max(nanos, 1));
            int sub = msb >= 4 ? (int) ((nanos >>> (msb - 4)) & 15) : (int) (nanos & 15);
            counts[msb * 16 + sub]++;
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

    static String bootstrap;
    static EventLoopGroup group;
    static KafkaClient hk;

    static Properties common() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        return p;
    }

    static final List<String> created = new ArrayList<>();

    /** Drops the topics this run made (a produce run writes gigabytes). */
    static void cleanup() {
        if (created.isEmpty()) return;
        try (Admin admin = Admin.create(common())) {
            admin.deleteTopics(created).all().get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            System.err.println("cleanup failed: " + e);
        }
    }

    static void createTopic(String topic, int partitions) throws Exception {
        created.add(topic);
        try (Admin admin = Admin.create(common())) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(30, TimeUnit.SECONDS);
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
        bootstrap = arg("bootstrap", "127.0.0.1:29092");
        try {
            if (lib.equals("hydrangea")) {
                group = new EventLoopGroup(2);
                if (arg("metrics", "off").equals("on")) {
                    group.metrics().bindTo(io.hydrangea.MetricsSink.NOOP);   // what a real sink does: switches latency recording on
                }
                hk = KafkaClient.create(group, KafkaConfig.of(bootstrap));
            }
            switch (scenario) {
                case "produce" -> produce();
                case "produce-threads" -> produceThreads();
                case "fill" -> fillOnly();
                case "consume" -> consume(false);
                case "consume-group" -> consume(true);
                case "e2e" -> e2e();
                case "connect" -> connect();
                default -> throw new IllegalArgumentException(scenario);
            }
        } catch (Throwable t) {
            t.printStackTrace();
            cleanup();
            System.exit(1);
        }
        cleanup();
        System.exit(0);
    }

    // ---- producing ----

    /** Either producer behind one send(). */
    interface Sender extends AutoCloseable {
        /** Sends one record; returns a future completed when acknowledged. */
        CompletableFuture<?> send(String topic, byte[] key, byte[] value);

        void flush();

        @Override
        void close();
    }

    static Sender sender() {
        int acks = iarg("acks", 1);
        int linger = iarg("linger", 5);
        int batch = iarg("batch", 64 * 1024);
        int compression = iarg("compression", 0);
        boolean idempotent = arg("idempotent", "false").equals("true");
        int inflight = iarg("inflight", 1);   // batches in flight per partition (Hydrangea) / requests per connection (kafka-clients)
        if (lib.equals("hydrangea")) {
            ProducerConfig pc = ProducerConfig.defaults().withoutIdempotence().withAcks(acks).withLinger(Duration.ofMillis(linger)).withBatchBytes(batch).withCompression(compression)
                    .withBufferBytes(256L * 1024 * 1024).withMaxInFlight(inflight);
            if (idempotent) pc = pc.withIdempotence().withMaxInFlight(inflight);
            KafkaProducer p = hk.producer(pc);
            return new Sender() {
                public CompletableFuture<?> send(String topic, byte[] key, byte[] value) {
                    return p.send(topic, key, value);
                }

                public void flush() {
                    p.flush().join();
                }

                public void close() {
                    p.close();
                }
            };
        }
        Properties pr = common();
        pr.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        pr.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        pr.put("acks", Integer.toString(acks));
        pr.put("linger.ms", Integer.toString(linger));
        pr.put("batch.size", Integer.toString(batch));
        pr.put("compression.type", compression == 1 ? "gzip" : "none");
        pr.put("buffer.memory", Long.toString(256L * 1024 * 1024));
        pr.put("enable.idempotence", Boolean.toString(idempotent));
        pr.put("max.in.flight.requests.per.connection", Integer.toString(inflight));
        org.apache.kafka.clients.producer.KafkaProducer<byte[], byte[]> p = new org.apache.kafka.clients.producer.KafkaProducer<>(pr);
        return new Sender() {
            public CompletableFuture<?> send(String topic, byte[] key, byte[] value) {
                CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
                p.send(new ProducerRecord<>(topic, key, value), (md, e) -> {
                    if (e != null) f.completeExceptionally(e); else f.complete(md);
                });
                return f;
            }

            public void flush() {
                p.flush();
            }

            public void close() {
                p.close();
            }
        };
    }

    static byte[] payload(int size) {
        byte[] b = new byte[size];
        new Random(7).nextBytes(b);
        return b;
    }

    /**
     * One thread produces as fast as it can, keeping {@code window} records unacknowledged. The reported rate is acknowledged records per second.
     */
    static void produce() throws Exception {
        int seconds = iarg("seconds", 10), warm = iarg("warmup", 3), size = iarg("size", 100), partitions = iarg("partitions", 3), window = iarg("window", 50_000);
        String topic = "bench-produce-" + System.nanoTime();
        createTopic(topic, partitions);
        byte[] value = payload(size);
        try (Sender s = sender()) {
            Semaphore inFlight = new Semaphore(window);
            LongAdder acked = new LongAdder();
            AtomicBoolean measuring = new AtomicBoolean();
            Histogram hist = new Histogram();
            long deadline = System.nanoTime() + (warm + seconds) * 1_000_000_000L;
            long measureStart = System.nanoTime() + warm * 1_000_000_000L;
            boolean started = false;
            double cpu0 = 0;
            double[] tc0 = null;
            long t0 = 0, a0 = 0;
            long n = 0;
            while (System.nanoTime() < deadline) {
                inFlight.acquire();
                long sent = System.nanoTime();
                boolean sample = (n++ & 63) == 0;
                s.send(topic, null, value).whenComplete((r, e) -> {
                    inFlight.release();
                    if (e == null) {
                        acked.increment();
                        if (sample && measuring.get()) hist.record(System.nanoTime() - sent);
                    }
                });
                if (!started && System.nanoTime() >= measureStart) {
                    started = true;
                    measuring.set(true);
                    cpu0 = cpuSeconds();
                    tc0 = threadCpu();
                    t0 = System.nanoTime();
                    a0 = acked.sum();
                }
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            long done = acked.sum() - a0;
            double cpu1 = cpuSeconds();
            double[] tc1 = threadCpu();
            long threadsPeak = ManagementFactory.getThreadMXBean().getPeakThreadCount();
            s.flush();
            result("produce", "size", Integer.toString(size), "acks", arg("acks", "1"), "idem", arg("idempotent", "false"), "inflight", arg("inflight", "1"), "partitions", Integer.toString(partitions), "per_s", f1(done / secs), "mb_per_s", f1(done / secs * size / 1048576.0),
                    "p50_ms", f1(hist.percentile(0.5) / 1e6), "p99_ms", f1(hist.percentile(0.99) / 1e6), "client_cpu_s_per_100k", f3((cpu1 - cpu0) / Math.max(1, done) * 1e5),
                    "caller_cpu_s_per_100k", f3((tc1[0] - tc0[0]) / Math.max(1, done) * 1e5), "io_cpu_s_per_100k", f3((tc1[1] - tc0[1]) / Math.max(1, done) * 1e5),
                    "threads_peak", Long.toString(threadsPeak), "rss_mb", Long.toString(rssMb()));
        }
    }

    /** Many virtual threads share one producer; each sends a record and waits for its acknowledgement. */
    static void produceThreads() throws Exception {
        int seconds = iarg("seconds", 10), warm = iarg("warmup", 3), size = iarg("size", 100), partitions = iarg("partitions", 3), threads = iarg("threads", 1000);
        String topic = "bench-threads-" + System.nanoTime();
        createTopic(topic, partitions);
        byte[] value = payload(size);
        try (Sender s = sender()) {
            AtomicBoolean stop = new AtomicBoolean(), measuring = new AtomicBoolean();
            LongAdder done = new LongAdder();
            Histogram hist = new Histogram();
            List<Thread> ts = new ArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            for (int i = 0; i < threads; i++) {
                ts.add(Thread.ofVirtual().start(() -> {
                    try {
                        while (!stop.get()) {
                            long t = System.nanoTime();
                            s.send(topic, null, value).get();
                            if (measuring.get()) {
                                hist.record(System.nanoTime() - t);
                                done.increment();
                            }
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }));
            }
            sleep(warm * 1000L);
            double cpu0 = cpuSeconds();
            long d0 = done.sum(), t0 = System.nanoTime();
            measuring.set(true);
            sleep(seconds * 1000L);
            measuring.set(false);
            long d1 = done.sum();
            double secs = (System.nanoTime() - t0) / 1e9, cpu1 = cpuSeconds();
            long threadsPeak = ManagementFactory.getThreadMXBean().getPeakThreadCount();
            stop.set(true);
            for (Thread t : ts) t.join();
            if (!errors.isEmpty()) {
                errors.get(0).printStackTrace();
                throw new IllegalStateException(errors.size() + " threads failed");
            }
            result("produce-threads", "threads", Integer.toString(threads), "acks", arg("acks", "1"), "per_s", f1((d1 - d0) / secs), "p50_ms", f1(hist.percentile(0.5) / 1e6),
                    "p99_ms", f1(hist.percentile(0.99) / 1e6), "client_cpu_s_per_100k", f1((cpu1 - cpu0) / Math.max(1, d1 - d0) * 1e5), "threads_peak", Long.toString(threadsPeak),
                    "rss_mb", Long.toString(rssMb()));
        }
    }

    // ---- consuming ----

    static String fill(int size, int partitions, long records) throws Exception {
        String topic = "bench-consume-" + System.nanoTime();
        createTopic(topic, partitions);
        byte[] value = payload(size);
        // always filled with Hydrangea's producer (the quickest), so that both consumers read identical data
        try (EventLoopGroup g = new EventLoopGroup(1)) {
            KafkaClient c = KafkaClient.create(g, KafkaConfig.of(bootstrap));
            KafkaProducer p = c.producer(ProducerConfig.defaults().withBatchBytes(256 * 1024).withBufferBytes(512L * 1024 * 1024));
            Semaphore w = new Semaphore(100_000);
            for (long i = 0; i < records; i++) {
                w.acquire();
                p.send(topic, null, value).whenComplete((r, e) -> w.release());
            }
            p.flush().get(120, TimeUnit.SECONDS);
            c.close();
        }
        return topic;
    }

    /** Creates and fills a topic and leaves it there: {@code fill topic=NAME_OUT}; prints TOPIC=name. Read it with {@code consume topic=name passes=N}. */
    static void fillOnly() throws Exception {
        String topic = fill(iarg("size", 100), iarg("partitions", 3), iarg("records", 2_000_000));
        created.remove(topic);
        System.out.println("TOPIC=" + topic);
    }

    /**
     * Reads a topic from the start as fast as possible; the rate is records per second over the whole read. The topic is filled first (by Hydrangea's producer, so that
     * both consumers read identical data) unless {@code topic=} names one that {@code fill} left; {@code passes} reads it that many times, one RESULT per pass.
     */
    static void consume(boolean grouped) throws Exception {
        int size = iarg("size", 100), partitions = iarg("partitions", 3), records = iarg("records", 2_000_000), passes = iarg("passes", 1);
        String topic = args.containsKey("topic") ? arg("topic", null) : fill(size, partitions, records);
        for (int pass = 0; pass < passes; pass++) {
            consumeOnce(grouped, topic, size, partitions, records);
        }
    }

    static void consumeOnce(boolean grouped, String topic, int size, int partitions, int records) throws Exception {
        long total = 0;
        double cpu0;
        long t0;
        String groupId = "bench-" + topic + "-" + System.nanoTime();
        if (lib.equals("hydrangea")) {
            ConsumerConfig cfg = ConsumerConfig.defaults().withBuffer(200_000, 20_000);
            if (grouped) cfg = cfg.withGroup(groupId);
            KafkaConsumer c = hk.consumer(cfg);
            if (grouped) c.subscribe(List.of(topic)).get(30, TimeUnit.SECONDS);
            else {
                List<TopicPartition> tps = new ArrayList<>();
                for (int i = 0; i < partitions; i++) tps.add(new TopicPartition(topic, i));
                c.assign(tps).get(30, TimeUnit.SECONDS);
            }
            t0 = System.nanoTime();
            cpu0 = cpuSeconds();
            while (total < records) {
                total += c.poll(Duration.ofSeconds(5)).get().size();
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            report("consume", grouped, total, secs, size, cpu0);
            c.close();
        } else {
            Properties cp = common();
            cp.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            cp.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            cp.put("auto.offset.reset", "earliest");
            cp.put("max.poll.records", "20000");
            cp.put("fetch.max.bytes", Integer.toString(16 * 1024 * 1024));
            cp.put("max.partition.fetch.bytes", Integer.toString(4 * 1024 * 1024));
            cp.put("enable.auto.commit", "false");
            cp.put("group.id", groupId);
            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> c = new org.apache.kafka.clients.consumer.KafkaConsumer<>(cp);
            if (grouped) {
                c.subscribe(List.of(topic));
                while (c.assignment().isEmpty()) c.poll(Duration.ofMillis(100));   // joined (what the Hydrangea side waits for before it starts the clock)
                c.seekToBeginning(c.assignment());
            } else {
                List<org.apache.kafka.common.TopicPartition> tps = new ArrayList<>();
                for (int i = 0; i < partitions; i++) tps.add(new org.apache.kafka.common.TopicPartition(topic, i));
                c.assign(tps);
                c.seekToBeginning(tps);
            }
            t0 = System.nanoTime();
            cpu0 = cpuSeconds();
            while (total < records) {
                total += c.poll(Duration.ofSeconds(5)).count();
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            report("consume", grouped, total, secs, size, cpu0);
            c.close();
        }
    }

    static void report(String name, boolean grouped, long total, double secs, int size, double cpu0) {
        result(grouped ? "consume-group" : name, "size", Integer.toString(size), "records", Long.toString(total), "per_s", f1(total / secs), "mb_per_s", f1(total / secs * size / 1048576.0),
                "client_cpu_s_per_100k", f3((cpuSeconds() - cpu0) / total * 1e5), "threads_peak", Long.toString(ManagementFactory.getThreadMXBean().getPeakThreadCount()),
                "rss_mb", Long.toString(rssMb()));
    }

    /** A producer and a consumer in one JVM: how long from send() to the record arriving, one record at a time, 5000 samples. */
    static void e2e() throws Exception {
        int partitions = 1, n = iarg("samples", 5000);
        args = new HashMap<>(args);
        args.put("linger", "0");   // one record at a time: no waiting to fill a batch, for either producer
        String topic = "bench-e2e-" + System.nanoTime();
        createTopic(topic, partitions);
        Histogram hist = new Histogram();
        if (lib.equals("hydrangea")) {
            KafkaProducer p = hk.producer(ProducerConfig.defaults().withLinger(Duration.ZERO));
            KafkaConsumer c = hk.consumer(ConsumerConfig.defaults().withEarliest(false).withFetch(Duration.ofMillis(500), 1, 16 * 1024 * 1024, 4 * 1024 * 1024));
            c.assign(List.of(new TopicPartition(topic, 0))).get(30, TimeUnit.SECONDS);
            for (int i = -200; i < n; i++) {
                long t = System.nanoTime();
                p.send(topic, null, Long.toString(t).getBytes());
                int got = 0;
                while (got == 0) {
                    got = c.poll(Duration.ofSeconds(5)).get().size();
                }
                if (i >= 0) hist.record(System.nanoTime() - t);
            }
        } else {
            Sender s = sender();
            Properties cp = common();
            cp.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            cp.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            cp.put("auto.offset.reset", "latest");
            cp.put("fetch.max.wait.ms", "500");
            org.apache.kafka.clients.consumer.KafkaConsumer<byte[], byte[]> c = new org.apache.kafka.clients.consumer.KafkaConsumer<>(cp);
            org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToEnd(List.of(tp));
            c.position(tp);
            for (int i = -200; i < n; i++) {
                long t = System.nanoTime();
                s.send(topic, null, Long.toString(t).getBytes());
                int got = 0;
                while (got == 0) {
                    got = c.poll(Duration.ofSeconds(5)).count();
                }
                if (i >= 0) hist.record(System.nanoTime() - t);
            }
        }
        result("e2e", "samples", Integer.toString(n), "p50_ms", f1(hist.percentile(0.5) / 1e6), "p99_ms", f1(hist.percentile(0.99) / 1e6), "p999_ms", f1(hist.percentile(0.999) / 1e6),
                "rss_mb", Long.toString(rssMb()));
    }

    /** Time to create a producer and send the first record (connect, metadata, produce). */
    static void connect() throws Exception {
        int n = iarg("connections", 30);
        String topic = "bench-connect-" + System.nanoTime();
        createTopic(topic, 1);
        Histogram hist = new Histogram();
        for (int i = -5; i < n; i++) {
            long t = System.nanoTime();
            try (Sender s = sender()) {
                s.send(topic, null, new byte[10]).get(30, TimeUnit.SECONDS);
            }
            if (i >= 0) hist.record(System.nanoTime() - t);
        }
        result("connect", "samples", Integer.toString(n), "p50_ms", f1(hist.percentile(0.5) / 1e6), "p99_ms", f1(hist.percentile(0.99) / 1e6), "rss_mb", Long.toString(rssMb()));
    }
}
