package mqtt;

import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Load test of MQTT clients against the same Mosquitto: Hydrangea's {@code io.hydrangea.mqtt} and Eclipse Paho (its asynchronous
 * {@code MqttAsyncClient}, the fast one): {@code org.eclipse.paho.client.mqttv3} for MQTT 3.1.1, {@code org.eclipse.paho.mqttv5.client} for MQTT 5.
 * One scenario, one library, one JVM per run (see {@code benchmark/mqtt-bench.sh}); the last line printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... mqtt.MqttBench LIB scenario [key=value ...]     (host=, port=, wsport=, seconds=, size=, producers=, window=, qos=, ...)
 * LIB: hydrangea | paho, with "5" for MQTT 5, "-ws" for MQTT over WebSocket, and for Paho "-nodelay" for TCP_NODELAY:
 *      hydrangea, hydrangea5, hydrangea5-ws, paho, paho-nodelay, paho5, paho5-nodelay, paho5-ws, paho5-ws-nodelay
 * scenarios: publish, latency, connections
 * </pre>
 * Both libraries are driven through the same small interface, so they are asked for identical work. Every publisher is limited to {@code window}
 * unfinished publishes (Paho throws beyond its own in-flight limit, which is set to the same number, and queues QoS 0 without bound otherwise).
 */
public final class MqttBench {

    interface Lib extends AutoCloseable {
        Conn connect() throws Exception;

        @Override
        void close() throws Exception;
    }

    interface Conn extends AutoCloseable {
        /** {@code done} runs when the publish is finished: written (QoS 0), PUBACK (1) or PUBCOMP (2); any thread. */
        void publish(String topic, byte[] payload, int qos, Runnable done) throws Exception;

        void subscribe(String filter, int qos, Consumer<byte[]> handler) throws Exception;

        boolean writable();

        /** Returns when more data may be sent (or {@code stop}): Hydrangea's {@code whenWritable()}; Paho has no such thing. */
        default void awaitWritable(AtomicBoolean stop) throws Exception {
        }

        @Override
        void close() throws Exception;
    }

    static Map<String, String> args = Map.of();

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    // ---- Hydrangea ----

    static final class HydLib implements Lib {
        private final String host;
        private final int port;
        private final int window;
        private final boolean v5;
        private final String wsUri;   // null = TCP
        private final io.hydrangea.EventLoopGroup group = new io.hydrangea.EventLoopGroup(2);

        HydLib(String host, int port, int window, boolean v5, String wsUri) {
            this.host = host;
            this.port = port;
            this.window = window;
            this.v5 = v5;
            this.wsUri = wsUri;
        }

        @Override
        public Conn connect() throws Exception {
            var cfg = io.hydrangea.mqtt.MqttConfig.of(host, port).withClientId(uniq("h")).withReconnect(null)
                    .withLimits(Math.max(window, 1), 1_000_000);
            if (v5) {
                cfg = cfg.withVersion(5);
            }
            if (wsUri != null) {
                cfg = cfg.withTransport(io.hydrangea.mqtt.MqttWebSocket.of(wsUri));
            }
            var c = io.hydrangea.mqtt.MqttClient.connect(group, cfg).get(30, TimeUnit.SECONDS);
            return new Conn() {
                @Override
                public void publish(String topic, byte[] payload, int qos, Runnable done) {
                    c.publish(topic, payload, io.hydrangea.mqtt.QoS.of(qos)).whenComplete((v, t) -> done.run());
                }

                @Override
                public void subscribe(String filter, int qos, Consumer<byte[]> handler) throws Exception {
                    c.subscribe(filter, io.hydrangea.mqtt.QoS.of(qos), m -> handler.accept(m.payload())).get(30, TimeUnit.SECONDS);
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
                    c.disconnect().get(30, TimeUnit.SECONDS);
                }
            };
        }

        @Override
        public void close() {
            group.close();
        }
    }

    // ---- Eclipse Paho ----

    static final class PahoLib implements Lib {
        private final String uri;
        private final int window;
        private final boolean noDelay;

        /** {@code noDelay}: a socket factory that sets TCP_NODELAY (Paho has no option for it and leaves Nagle's algorithm on). */
        PahoLib(String host, int port, int window, boolean noDelay) {
            this(host, port, window, noDelay, false);
        }

        PahoLib(String host, int port, int window, boolean noDelay, boolean ws) {
            this.uri = (ws ? "ws://" : "tcp://") + host + ":" + port + (ws ? "/mqtt" : "");
            this.window = window;
            this.noDelay = noDelay;
        }

        static final class NoDelayFactory extends javax.net.SocketFactory {
            private final javax.net.SocketFactory d = javax.net.SocketFactory.getDefault();

            private static java.net.Socket nd(java.net.Socket s) throws java.io.IOException {
                s.setTcpNoDelay(true);
                return s;
            }

            @Override
            public java.net.Socket createSocket() throws java.io.IOException {
                return nd(d.createSocket());
            }

            @Override
            public java.net.Socket createSocket(String h, int p) throws java.io.IOException {
                return nd(d.createSocket(h, p));
            }

            @Override
            public java.net.Socket createSocket(String h, int p, java.net.InetAddress la, int lp) throws java.io.IOException {
                return nd(d.createSocket(h, p, la, lp));
            }

            @Override
            public java.net.Socket createSocket(java.net.InetAddress h, int p) throws java.io.IOException {
                return nd(d.createSocket(h, p));
            }

            @Override
            public java.net.Socket createSocket(java.net.InetAddress h, int p, java.net.InetAddress la, int lp) throws java.io.IOException {
                return nd(d.createSocket(h, p, la, lp));
            }
        }

        @Override
        public Conn connect() throws Exception {
            var c = new org.eclipse.paho.client.mqttv3.MqttAsyncClient(uri, uniq("p"), new org.eclipse.paho.client.mqttv3.persist.MemoryPersistence());
            var o = new org.eclipse.paho.client.mqttv3.MqttConnectOptions();
            o.setCleanSession(true);
            o.setAutomaticReconnect(false);
            o.setMaxInflight(Math.max(window, 10));
            o.setKeepAliveInterval(60);
            if (noDelay) {
                o.setSocketFactory(new NoDelayFactory());
            }
            c.connect(o).waitForCompletion(30_000);
            return new Conn() {
                @Override
                public void publish(String topic, byte[] payload, int qos, Runnable done) throws Exception {
                    var msg = new org.eclipse.paho.client.mqttv3.MqttMessage(payload);
                    msg.setQos(qos);
                    c.publish(topic, msg, null, new org.eclipse.paho.client.mqttv3.IMqttActionListener() {
                        @Override
                        public void onSuccess(org.eclipse.paho.client.mqttv3.IMqttToken t) {
                            done.run();
                        }

                        @Override
                        public void onFailure(org.eclipse.paho.client.mqttv3.IMqttToken t, Throwable e) {
                            done.run();
                        }
                    });
                }

                @Override
                public void subscribe(String filter, int qos, Consumer<byte[]> handler) throws Exception {
                    c.subscribe(filter, qos, null, null, (t, m) -> handler.accept(m.getPayload())).waitForCompletion(30_000);
                }

                @Override
                public boolean writable() {
                    return true;
                }

                @Override
                public void close() throws Exception {
                    c.disconnect().waitForCompletion(30_000);
                    c.close();
                }
            };
        }

        @Override
        public void close() {
        }
    }

    // ---- Eclipse Paho, MQTT 5 ----

    static final class Paho5Lib implements Lib {
        private final String uri;
        private final boolean noDelay;

        Paho5Lib(String host, int port, boolean noDelay, boolean ws) {
            this.uri = (ws ? "ws://" : "tcp://") + host + ":" + port + (ws ? "/mqtt" : "");
            this.noDelay = noDelay;
        }

        @Override
        public Conn connect() throws Exception {
            var c = new org.eclipse.paho.mqttv5.client.MqttAsyncClient(uri, uniq("p5"), new org.eclipse.paho.mqttv5.client.persist.MemoryPersistence());
            var o = new org.eclipse.paho.mqttv5.client.MqttConnectionOptions();
            o.setCleanStart(true);
            o.setAutomaticReconnect(false);
            o.setKeepAliveInterval(60);
            if (noDelay) {
                o.setSocketFactory(new PahoLib.NoDelayFactory());
            }
            var sink = new java.util.concurrent.atomic.AtomicReference<Consumer<byte[]>>();
            c.setCallback(new org.eclipse.paho.mqttv5.client.MqttCallback() {   // one callback for the connection: the benchmark subscribes once per connection
                @Override
                public void disconnected(org.eclipse.paho.mqttv5.client.MqttDisconnectResponse r) {
                }

                @Override
                public void mqttErrorOccurred(org.eclipse.paho.mqttv5.common.MqttException e) {
                }

                @Override
                public void messageArrived(String topic, org.eclipse.paho.mqttv5.common.MqttMessage m) {
                    sink.get().accept(m.getPayload());
                }

                @Override
                public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken t) {
                }

                @Override
                public void connectComplete(boolean reconnect, String uri) {
                }

                @Override
                public void authPacketArrived(int reason, org.eclipse.paho.mqttv5.common.packet.MqttProperties p) {
                }
            });
            c.connect(o).waitForCompletion(30_000);
            return new Conn() {
                @Override
                public void publish(String topic, byte[] payload, int qos, Runnable done) throws Exception {
                    var msg = new org.eclipse.paho.mqttv5.common.MqttMessage(payload);
                    msg.setQos(qos);
                    c.publish(topic, msg, null, new org.eclipse.paho.mqttv5.client.MqttActionListener() {
                        @Override
                        public void onSuccess(org.eclipse.paho.mqttv5.client.IMqttToken t) {
                            done.run();
                        }

                        @Override
                        public void onFailure(org.eclipse.paho.mqttv5.client.IMqttToken t, Throwable e) {
                            done.run();
                        }
                    });
                }

                @Override
                public void subscribe(String filter, int qos, Consumer<byte[]> handler) throws Exception {
                    sink.set(handler);
                    c.subscribe(filter, qos).waitForCompletion(30_000);
                }

                @Override
                public boolean writable() {
                    return true;
                }

                @Override
                public void close() throws Exception {
                    c.disconnect().waitForCompletion(30_000);
                    c.close();
                }
            };
        }

        @Override
        public void close() {
        }
    }

    // ---- helpers ----

    static double cpuSeconds() {
        return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime() / 1e9;
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

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static String uniq(String p) {
        return p + "-" + Long.toString(System.nanoTime() & 0xffffffffL, 36);
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
        Map<String, String> m = new HashMap<>();
        for (int i = 2; i < a.length; i++) {
            String[] kv = a[i].split("=", 2);
            m.put(kv[0], kv[1]);
        }
        args = m;
        String host = arg("host", "127.0.0.1");
        int port = iarg("port", 21883);
        int window = iarg("window", scenario.equals("connections") ? 10 : 1000);
        boolean v5 = lib.contains("5");
        boolean ws = lib.contains("-ws");
        boolean noDelay = lib.endsWith("-nodelay");
        int wsPort = iarg("wsport", 9001);
        try (Lib l = lib.startsWith("hydrangea") ? new HydLib(host, ws ? wsPort : port, window, v5, ws ? "ws://" + host + ":" + wsPort + "/mqtt" : null)
                : v5 ? new Paho5Lib(host, ws ? wsPort : port, noDelay, ws) : new PahoLib(host, ws ? wsPort : port, window, noDelay, ws)) {
            switch (scenario) {
                case "publish" -> publish(l, lib, window);
                case "latency" -> latency(l, lib);
                case "connections" -> connections(l, lib, window);
                default -> throw new IllegalArgumentException(scenario);
            }
        } catch (Throwable t) {
            t.printStackTrace();   // a library's non-daemon threads would otherwise keep the JVM, and the whole benchmark run, waiting
            System.exit(1);
        }
        System.exit(0);
    }

    // ---- scenarios ----

    /**
     * Producers publish to their own topics; one subscriber (same library) receives everything. Two limits apply to a producer: {@code window}
     * publishes not yet finished (written at QoS 0, PUBACK at 1, PUBCOMP at 2: what each library's own in-flight limit counts), and, at QoS 1 and 2,
     * {@code system} messages published but not yet <em>received</em> by the subscriber, all producers together (a closed loop: the broker's queue
     * cannot grow without bound and nothing is dropped). At QoS 0 there is no second limit, so QoS 0 may be dropped; {@code lost} says how many were.
     * Reports what the senders finished and what the subscriber got over the same measured window.
     */
    static void publish(Lib l, String lib, int window) throws Exception {
        int qos = iarg("qos", 1);
        int producers = iarg("producers", 2);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        int size = iarg("size", 128);
        byte[] body = payload(size);
        String base = uniq("bench");
        Conn sub = l.connect();
        LongAdder received = new LongAdder();
        Semaphore[] permits = new Semaphore[producers];
        Semaphore inSystem = new Semaphore(iarg("system", 2000));
        sub.subscribe(base + "/#", qos, b -> {
            received.increment();
            if (qos > 0) {
                inSystem.release();
            }
        });
        List<Conn> conns = new ArrayList<>();
        LongAdder completed = new LongAdder();
        LongAdder issued = new LongAdder();
        for (int i = 0; i < producers; i++) {
            conns.add(l.connect());
            permits[i] = new Semaphore(window);
        }
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> ts = new ArrayList<>();
        for (int i = 0; i < producers; i++) {
            Conn c = conns.get(i);
            Semaphore p = permits[i];
            String topic = base + "/p" + i;
            byte[] mine = body;
            Runnable done = () -> {
                completed.increment();
                p.release();
            };
            Thread t = new Thread(() -> {
                try {
                    while (!stop.get()) {
                        if (!p.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                            continue;
                        }
                        if (qos > 0 && !inSystem.tryAcquire(50, TimeUnit.MILLISECONDS)) {
                            p.release();
                            continue;
                        }
                        c.awaitWritable(stop);
                        c.publish(topic, mine, qos, done);
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
        long c0 = completed.sum();
        long r0 = received.sum();
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        sleep(seconds * 1000L);
        long c1 = completed.sum();
        long r1 = received.sum();
        double cpu1 = cpuSeconds();
        double secs = (System.nanoTime() - t0) / 1e9;
        stop.set(true);
        for (Thread t : ts) {
            t.join();
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (completed.sum() < issued.sum() && System.nanoTime() < deadline) {
            sleep(20);
        }
        long last = -1;
        for (int i = 0; i < 100 && received.sum() != last; i++) {   // until the subscriber has received nothing for 2 s
            last = received.sum();
            sleep(2000);
        }
        long unfinished = issued.sum() - completed.sum();
        long threads = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssMb();
        for (Conn c : conns) {
            c.close();
        }
        sub.close();
        double rate = (c1 - c0) / secs;
        result("publish", lib, "qos", Integer.toString(qos), "completed_per_s", f1(rate), "received_per_s", f1((r1 - r0) / secs), "mb_per_s", f1(rate * size / 1e6),
                "client_cpu_s_per_100k", f1((cpu1 - cpu0) / (Math.max(1, (c1 - c0) + (r1 - r0)) / 2e5)),
                "issued", Long.toString(issued.sum()), "received_total", Long.toString(received.sum()), "unfinished_at_end", Long.toString(unfinished),
                "lost", Long.toString(issued.sum() - received.sum()), "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }

    /** One message at a time (publish, delivery to a subscription of the same connection): the latency of the whole path through the broker. */
    static void latency(Lib l, String lib) throws Exception {
        int qos = iarg("qos", 1);
        int samples = iarg("samples", 20_000);
        int warm = iarg("warmup", 3_000);
        byte[] body = payload(iarg("size", 128));
        Conn c = l.connect();
        String topic = uniq("lat");
        LinkedTransferQueue<Long> arrived = new LinkedTransferQueue<>();
        c.subscribe(topic, qos, b -> arrived.add(System.nanoTime()));
        long[] us = new long[samples];
        for (int i = 0; i < warm + samples; i++) {
            long t0 = System.nanoTime();
            c.publish(topic, body, qos, () -> { });
            Long t1 = arrived.poll(10, TimeUnit.SECONDS);
            if (t1 == null) {
                throw new IllegalStateException("no delivery");
            }
            if (i >= warm) {
                us[i - warm] = (t1 - t0) / 1000;
            }
        }
        Arrays.sort(us);
        c.close();
        result("latency", lib, "qos", Integer.toString(qos), "p50_us", Long.toString(us[samples / 2]), "p90_us", Long.toString(us[samples * 9 / 10]),
                "p99_us", Long.toString(us[samples * 99 / 100]), "p999_us", Long.toString(us[samples * 999 / 1000]), "max_us", Long.toString(us[samples - 1]),
                "samples", Long.toString(samples));
    }

    /** Many connections at once, each subscribed to its own topic and publishing QoS 1 to it; a few driver threads serve them all. */
    static void connections(Lib l, String lib, int window) throws Exception {
        int n = iarg("connections", 100);
        int seconds = iarg("seconds", 10);
        int warm = iarg("warmup", 3);
        int drivers = iarg("drivers", 4);
        byte[] body = payload(iarg("size", 128));
        Conn[] conns = new Conn[n];
        String[] topics = new String[n];
        Semaphore[] permits = new Semaphore[n];
        LongAdder completed = new LongAdder();
        LongAdder delivered = new LongAdder();
        double cpuSetup0 = cpuSeconds();
        long ts0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            conns[i] = l.connect();
            topics[i] = uniq("c" + i);
            permits[i] = new Semaphore(window);
            conns[i].subscribe(topics[i], 1, b -> delivered.increment());
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
                                    Semaphore p = permits[i];
                                    conns[i].publish(topics[i], body, 1, () -> {
                                        completed.increment();
                                        p.release();
                                    });
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
        long c0 = completed.sum();
        long d0 = delivered.sum();
        double cpu0 = cpuSeconds();
        long t0 = System.nanoTime();
        sleep(seconds * 1000L);
        long c1 = completed.sum();
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
        for (Conn c : conns) {
            c.close();
        }
        result("connections", lib, "connections", Integer.toString(n), "completed_per_s", f1((c1 - c0) / secs), "delivered_per_s", f1((d1 - d0) / secs),
                "client_cpu_s_per_100k", f1((cpu1 - cpu0) / (Math.max(1, d1 - d0) / 1e5)), "setup_s", f1(setupSecs), "setup_cpu_s", f1(setupCpu),
                "threads_after_setup", Long.toString(threadsAfterSetup), "threads_peak", Long.toString(threads), "rss_mb", Long.toString(rss));
    }
}
