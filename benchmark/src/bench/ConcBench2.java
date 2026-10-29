import io.hydrangea.*;
import io.hydrangea.http.*;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** h2c variant: ConcBench2 mode port callers cap streams [secs] [warm] */
public class ConcBench2 {
    static final AtomicBoolean stop = new AtomicBoolean(), measuring = new AtomicBoolean();
    static final LongAdder ok = new LongAdder(), err = new LongAdder();
    static final ConcurrentLinkedQueue<Long> lat = new ConcurrentLinkedQueue<>();

    static final ConcurrentHashMap<String, LongAdder> errKinds = new ConcurrentHashMap<>();

    static void record(long t0, boolean good, Throwable t) {
        if (!good && t != null) {
            Throwable c = t;
            while (c.getCause() != null && (c instanceof CompletionException || c instanceof ExecutionException)) c = c.getCause();
            errKinds.computeIfAbsent(c.getClass().getSimpleName() + ": " + c.getMessage(), k -> new LongAdder()).increment();
        }
        record(t0, good);
    }

    static void record(long t0, boolean good) {
        if (!measuring.get()) return;
        if (good) { ok.increment(); lat.add(System.nanoTime() - t0); } else err.increment();
    }

    public static void main(String[] a) throws Exception {
        String mode = a[0];
        int port = Integer.parseInt(a[1]), n = Integer.parseInt(a[2]), cap = Integer.parseInt(a[3]);
        int streams = Integer.parseInt(a[4]);
        int secs = a.length > 5 ? Integer.parseInt(a[5]) : 10, warm = a.length > 6 ? Integer.parseInt(a[6]) : 3;
        String url = "http://127.0.0.1:" + port + "/";
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        List<Thread> ts = new ArrayList<>();
        String failed = "";
        try {
            switch (mode) {
                case "hy-block", "hy-async" -> {
                    EventLoopGroup g = new EventLoopGroup(2);
                    HttpClientConfig cfg = new HttpClientConfig(Duration.ofSeconds(10), Duration.ofSeconds(300), Duration.ofSeconds(30),
                            cap, cap, 1 << 20, 16 * 1024, 5, null, "bench",
                            new Http2ClientConfig(streams, 1 << 20, 1 << 24, 4096, false, true));
                    var c = new io.hydrangea.http.HttpClient(g, cfg);
                    c.get(url).join(); // upgrade to h2c before the load starts
                    if (mode.equals("hy-block")) {
                        for (int i = 0; i < n; i++) ts.add(Thread.ofPlatform().start(() -> {
                            while (!stop.get()) {
                                long t0 = System.nanoTime();
                                try { c.get(url).join(); record(t0, true); } catch (Exception e) { record(t0, false, e); }
                            }
                        }));
                    } else {
                        for (int i = 0; i < n; i++) {
                            Runnable[] next = new Runnable[1];
                            next[0] = () -> {
                                if (stop.get()) return;
                                long t0 = System.nanoTime();
                                c.get(url).whenComplete((r, e) -> { record(t0, e == null, e); next[0].run(); });
                            };
                            next[0].run();
                        }
                    }
                }
                case "jdk-plat", "jdk-virt" -> {
                    var c = java.net.http.HttpClient.newBuilder().version(java.net.http.HttpClient.Version.HTTP_2)
                            .connectTimeout(Duration.ofSeconds(10)).build();
                    var req = java.net.http.HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(300)).build();
                    Semaphore permits = new Semaphore(Integer.MAX_VALUE, true); // no cap: h2 multiplexes all callers on the connection
                    var warmRes = c.send(req, java.net.http.HttpResponse.BodyHandlers.discarding());
                    System.out.println("JDK protocol " + warmRes.version());
                    Runnable body = () -> {
                        while (!stop.get()) {
                            long t0 = System.nanoTime();
                            try {
                                permits.acquire();
                                try { c.send(req, java.net.http.HttpResponse.BodyHandlers.discarding()); } finally { permits.release(); }
                                record(t0, true);
                            } catch (Exception e) { record(t0, false, e); }
                        }
                    };
                    for (int i = 0; i < n; i++) ts.add(mode.equals("jdk-plat") ? Thread.ofPlatform().start(body) : Thread.ofVirtual().start(body));
                }
                default -> throw new IllegalArgumentException(mode);
            }
        } catch (Throwable t) {
            failed = t.getClass().getSimpleName() + " after " + ts.size() + " threads";
        }
        if (!failed.isEmpty()) {
            stop.set(true);
            System.out.printf("RESULT mode=%s callers=%d cap=%d FAILED=%s%n", mode, n, cap, failed);
            System.exit(0);
        }
        Thread.sleep(warm * 1000L);
        long cpu0 = os.getProcessCpuTime();
        measuring.set(true);
        Thread.sleep(secs * 1000L);
        measuring.set(false);
        long cpu = os.getProcessCpuTime() - cpu0;
        long done = ok.sum();
        int peak = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssKb();
        System.gc();
        long heap = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20;
        long[] l = lat.stream().mapToLong(Long::longValue).sorted().toArray();
        System.out.printf("RESULT mode=%s callers=%d cap=%d rps=%d err=%d p50_ms=%.1f p99_ms=%.1f cpu_ms_per_req=%.3f threads_peak=%d heap_mb=%d rss_mb=%d%n",
                mode, n, cap, done / secs, err.sum(), l.length == 0 ? 0 : l[l.length / 2] / 1e6, l.length == 0 ? 0 : l[(int) (l.length * 0.99)] / 1e6,
                cpu / 1e6 / Math.max(1, done), peak, heap, rss / 1024);
        errKinds.forEach((k, v) -> System.out.println("ERRKIND " + v.sum() + " x " + k));
        stop.set(true);
        System.exit(0);
    }

    static long rssKb() throws Exception {
        Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(ProcessHandle.current().pid())).start();
        return Long.parseLong(new String(p.getInputStream().readAllBytes()).trim());
    }
}
