import io.hydrangea.*;
import io.hydrangea.http.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** usage: HttpMetricsAb server|client on|off [seconds] [warmup] [concurrency]  -- the side being measured has its metrics sink bound when "on". */
public class HttpMetricsAb {
    static final com.sun.management.OperatingSystemMXBean OS = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static void main(String[] a) throws Exception {
        String scenario = a[0];
        boolean on = a[1].equals("on");
        int secs = a.length > 2 ? Integer.parseInt(a[2]) : 6;
        int warm = a.length > 3 ? Integer.parseInt(a[3]) : 2;
        int conc = a.length > 4 ? Integer.parseInt(a[4]) : 32;
        EventLoopGroup serverGroup = new EventLoopGroup(2);
        EventLoopGroup clientGroup = new EventLoopGroup(2);
        if (on) (scenario.equals("server") ? serverGroup : clientGroup).metrics().bindTo(MetricsSink.NOOP);
        HttpRouter router = new HttpRouter().get("/", r -> HttpResponse.ok("hello"));
        Server server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), router);
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        long[] result = scenario.equals("server") ? serverLoad(port, conc, secs, warm) : clientLoad(clientGroup, port, conc, secs, warm);
        System.out.printf("RESULT scenario=%s metrics=%s rps=%d cpu_us_per_req=%.2f p99_us=%d%n", scenario, a[1], result[0] / secs, result[1] / 1000.0 / result[0], result[2] / 1000);
        System.exit(0);
    }

    static long[] serverLoad(int port, int conc, int secs, int warm) throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean measuring = new AtomicBoolean();
        LongAdder count = new LongAdder();
        List<long[]> lat = Collections.synchronizedList(new ArrayList<>());
        Thread[] ts = new Thread[conc];
        byte[] req = "GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < conc; i++) {
            ts[i] = Thread.ofPlatform().start(() -> {
                long[] mine = new long[200_000];
                int n = 0;
                try (Socket s = new Socket("127.0.0.1", port)) {
                    s.setTcpNoDelay(true);
                    InputStream in = new BufferedInputStream(s.getInputStream());
                    OutputStream out = s.getOutputStream();
                    while (!stop.get()) {
                        long t0 = System.nanoTime();
                        out.write(req);
                        int len = 0;
                        StringBuilder line = new StringBuilder();
                        int c, prev = 0;
                        // headers until blank line
                        while (true) {
                            line.setLength(0);
                            while ((c = in.read()) >= 0 && c != '\n') if (c != '\r') line.append((char) c);
                            if (c < 0) return;
                            if (line.isEmpty()) break;
                            if (line.toString().regionMatches(true, 0, "content-length:", 0, 15)) len = Integer.parseInt(line.substring(15).trim());
                        }
                        in.readNBytes(len);
                        if (measuring.get()) {
                            count.increment();
                            if (n < mine.length) mine[n++] = System.nanoTime() - t0;
                        }
                    }
                } catch (IOException e) {
                    // ends with the run
                }
                lat.add(Arrays.copyOf(mine, n));
            });
        }
        return finish(stop, measuring, count, lat, ts, secs, warm);
    }

    static long[] clientLoad(EventLoopGroup group, int port, int conc, int secs, int warm) throws Exception {
        io.hydrangea.http.HttpClient client = new io.hydrangea.http.HttpClient(group, HttpClientConfig.defaults());
        AtomicBoolean stop = new AtomicBoolean();
        AtomicBoolean measuring = new AtomicBoolean();
        LongAdder count = new LongAdder();
        List<long[]> lat = Collections.synchronizedList(new ArrayList<>());
        String url = "http://127.0.0.1:" + port + "/";
        ThreadLocal<long[]> buf = ThreadLocal.withInitial(() -> new long[400_000]);
        ThreadLocal<int[]> cnt = ThreadLocal.withInitial(() -> new int[1]);
        Set<long[]> bufs = ConcurrentHashMap.newKeySet();
        Map<long[], int[]> counts = new ConcurrentHashMap<>();
        for (int i = 0; i < conc; i++) {
            Runnable[] next = new Runnable[1];
            next[0] = () -> {
                if (stop.get()) return;
                long t0 = System.nanoTime();
                client.get(url).whenComplete((r, e) -> {
                    if (measuring.get()) {
                        count.increment();
                        long[] b = buf.get();
                        int[] c = cnt.get();
                        if (bufs.add(b)) counts.put(b, c);
                        if (c[0] < b.length) b[c[0]++] = System.nanoTime() - t0;
                    }
                    next[0].run();
                });
            };
            next[0].run();
        }
        long[] res = finish(stop, measuring, count, lat, new Thread[0], secs, warm);
        // merge the per-thread latency buffers
        List<Long> all = new ArrayList<>();
        for (long[] b : bufs) for (int i = 0; i < counts.get(b)[0]; i++) all.add(b[i]);
        if (!all.isEmpty()) {
            Collections.sort(all);
            res[2] = all.get((int) (all.size() * 0.99));
        }
        return res;
    }

    static long[] finish(AtomicBoolean stop, AtomicBoolean measuring, LongAdder count, List<long[]> lat, Thread[] ts, int secs, int warm) throws Exception {
        Thread.sleep(warm * 1000L);
        long cpu0 = OS.getProcessCpuTime();
        measuring.set(true);
        Thread.sleep(secs * 1000L);
        measuring.set(false);
        long cpu = OS.getProcessCpuTime() - cpu0;
        long n = count.sum();
        stop.set(true);
        for (Thread t : ts) t.join(2000);
        long p99 = 0;
        List<Long> all = new ArrayList<>();
        synchronized (lat) { for (long[] l : lat) for (long v : l) all.add(v); }
        if (!all.isEmpty()) { Collections.sort(all); p99 = all.get((int) (all.size() * 0.99)); }
        return new long[]{n, cpu, p99};
    }
}
