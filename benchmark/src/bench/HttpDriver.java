package bench;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP / WebSocket / SSE benchmark: each scenario against each applicable server (own JVM, same flags), several rounds with the
 * order rotated, median round reported. Usage: HttpDriver rounds warmupSec durationSec ioThreads loadThreads outFile [scenarioFilter]
 * Classpath comes from env vars BENCH_CP (compiled benchmark classes), HYD_CP and NETTY_CP.
 */
public final class HttpDriver {

    record Impl(String name, String mainClass, boolean netty) {
    }

    record Scenario(String name, String mode, int conns, List<String> loadArgs, List<String> impls, String unit) {
    }

    record Result(double ops, double mbps, long p50, long p90, long p99, long p999, long max, long errors, long connected,
                  double cpuUsPerOp, long rssMb) {
    }

    static final List<Impl> IMPLS = List.of(
            new Impl("hydrangea", "servers.HydrangeaHttpServer", false),
            new Impl("netty", "servers.NettyHttpServer", true),
            new Impl("jdk-httpserver", "servers.JdkHttpServer", false));

    static final List<String> ALL = List.of("hydrangea", "netty", "jdk-httpserver");
    static final List<String> WS = List.of("hydrangea", "netty");

    static final List<Scenario> SCENARIOS = List.of(
            new Scenario("GET /plain (13 B), 64 connections", "http", 64, List.of("path=/plain"), ALL, "req/s"),
            new Scenario("GET /plain (13 B), 1000 connections", "http", 1000, List.of("path=/plain"), ALL, "req/s"),
            new Scenario("GET /json (180 B), 64 connections", "http", 64, List.of("path=/json"), ALL, "req/s"),
            new Scenario("GET /users/{id}?q= (path variable + query), 64 connections", "http", 64, List.of("path=/users/%d?q=hello"), ALL, "req/s"),
            new Scenario("POST /echo 1 KiB, 64 connections", "http", 64, List.of("method=POST", "path=/echo", "body=1024"), ALL, "req/s"),
            new Scenario("GET /download/1 MiB, 8 connections", "http", 8, List.of("path=/download/1048576", "verify=false"), ALL, "req/s"),
            new Scenario("WebSocket echo 64 B, 64 connections", "ws", 64, List.of("size=64"), WS, "msg/s"),
            new Scenario("WebSocket echo 64 B, 1000 connections", "ws", 1000, List.of("size=64"), WS, "msg/s"),
            new Scenario("SSE: 1000 subscribers, 10 events/s each", "sse", 1000, List.of("path=/sse"), List.of("hydrangea"), "events/s"));

    public static void main(String[] a) throws Exception {
        int rounds = Integer.parseInt(a[0]);
        int warmup = Integer.parseInt(a[1]);
        int duration = Integer.parseInt(a[2]);
        int ioThreads = Integer.parseInt(a[3]);
        int loadThreads = Integer.parseInt(a[4]);
        Path outFile = Path.of(a[5]);
        String filter = a.length > 6 ? a[6] : "";
        int port = 21000 + (int) (ProcessHandle.current().pid() % 15000);

        Map<String, Map<String, List<Result>>> results = new LinkedHashMap<>();
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < IMPLS.size(); i++) {
                Impl impl = IMPLS.get((i + round) % IMPLS.size());
                List<Scenario> mine = SCENARIOS.stream().filter(s -> s.impls().contains(impl.name()) && s.name().contains(filter)).toList();
                if (mine.isEmpty()) continue;
                System.err.printf("== round %d/%d: %s%n", round + 1, rounds, impl.name());
                Process server = startServer(impl, ++port, ioThreads);
                try {
                    run("http", port, 64, loadThreads, 3, 2, List.of("path=/plain")); // throw-away: warm the server's JIT
                    for (Scenario sc : mine) {
                        Result r = measure(server, sc, port, loadThreads, warmup, duration);
                        System.err.printf("   %-62s %s%n", sc.name(), r);
                        results.computeIfAbsent(sc.name(), k -> new LinkedHashMap<>()).computeIfAbsent(impl.name(), k -> new ArrayList<>()).add(r);
                    }
                } finally {
                    server.destroy();
                    server.waitFor();
                }
            }
        }
        String report = report(results, rounds, warmup, duration, ioThreads, loadThreads);
        Files.writeString(outFile, report);
        System.out.println(report);
    }

    static Process startServer(Impl impl, int port, int threads) throws Exception {
        String cp = System.getenv("BENCH_CP") + File.pathSeparator + System.getenv("HYD_CP")
                + (impl.netty() ? File.pathSeparator + System.getenv("NETTY_CP") : "");
        Process p = new ProcessBuilder(javaBin(), "-Xms512m", "-Xmx512m", "-XX:+UseG1GC", "-Dsse.tick.ms=100", "-cp", cp, impl.mainClass(),
                String.valueOf(port), String.valueOf(threads)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
        var ready = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                for (String line; (line = r.readLine()) != null; ) if (line.equals("READY")) return true;
            } catch (IOException ignored) { }
            return false;
        });
        if (ready.get(30, java.util.concurrent.TimeUnit.SECONDS)) {
            Thread drain = new Thread(() -> { try { while (r.readLine() != null) { } } catch (IOException ignored) { } });
            drain.setDaemon(true);
            drain.start();
            return p;
        }
        p.destroyForcibly();
        throw new IllegalStateException("server " + impl.name() + " did not start");
    }

    static Result measure(Process server, Scenario sc, int port, int loadThreads, int warmup, int duration) throws Exception {
        long pid = server.pid();
        double cpu0 = cpuSeconds(pid);
        AtomicLong peak = new AtomicLong();
        var stop = new java.util.concurrent.atomic.AtomicBoolean();
        Thread sampler = new Thread(() -> {
            while (!stop.get()) {
                peak.accumulateAndGet(rssKb(pid), Math::max);
                try { Thread.sleep(250); } catch (InterruptedException e) { return; }
            }
        });
        sampler.start();
        Map<String, String> kv = run(sc.mode(), port, sc.conns(), loadThreads, warmup, duration, sc.loadArgs());
        stop.set(true);
        sampler.join();
        double cpu = (cpuSeconds(pid) - cpu0) * duration / (warmup + duration);
        double ops = Double.parseDouble(kv.get("ops"));
        return new Result(ops, Double.parseDouble(kv.get("mbps")), Long.parseLong(kv.get("p50")), Long.parseLong(kv.get("p90")),
                Long.parseLong(kv.get("p99")), Long.parseLong(kv.get("p999")), Long.parseLong(kv.get("max")), Long.parseLong(kv.get("errors")),
                Long.parseLong(kv.get("connected")), ops > 0 ? cpu * 1e6 / (ops * duration) : 0, peak.get() / 1024);
    }

    static Map<String, String> run(String mode, int port, int conns, int threads, int warmup, int duration, List<String> extra) throws Exception {
        List<String> cmd = new ArrayList<>(List.of(javaBin(), "-Xmx512m", "-cp", System.getenv("BENCH_CP"), "bench.HttpLoad", mode,
                String.valueOf(port), String.valueOf(conns), String.valueOf(threads), String.valueOf(warmup), String.valueOf(duration)));
        cmd.addAll(extra);
        Process p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        String out = new String(p.getInputStream().readAllBytes());
        p.waitFor();
        for (String line : out.split("\n")) {
            if (line.startsWith("RESULT ")) {
                Map<String, String> kv = new LinkedHashMap<>();
                for (String t : line.substring(7).trim().split(" ")) {
                    String[] x = t.split("=");
                    kv.put(x[0], x[1]);
                }
                return kv;
            }
        }
        throw new IllegalStateException("load generator failed: " + out);
    }

    static String javaBin() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }

    static String ps(long pid, String field) throws IOException, InterruptedException {
        Process p = new ProcessBuilder("ps", "-o", field + "=", "-p", String.valueOf(pid)).start();
        String s = new String(p.getInputStream().readAllBytes()).trim();
        p.waitFor();
        return s;
    }

    static long rssKb(long pid) {
        try {
            return Long.parseLong(ps(pid, "rss"));
        } catch (Exception e) {
            return 0;
        }
    }

    static double cpuSeconds(long pid) throws Exception {
        String t = ps(pid, "time");
        double total = 0;
        if (t.contains("-")) {
            total += Integer.parseInt(t.substring(0, t.indexOf('-'))) * 86400.0;
            t = t.substring(t.indexOf('-') + 1);
        }
        for (String part : t.split(":")) total = total * 60 + Double.parseDouble(part);
        return total;
    }

    static String report(Map<String, Map<String, List<Result>>> results, int rounds, int warmup, int duration, int io, int load) {
        StringBuilder sb = new StringBuilder("# Benchmark: HTTP, WebSocket and SSE\n\n");
        sb.append(String.format("%s, %s, %s cores; JDK %s; server %d I/O threads, load generator %d threads (same machine).%n",
                System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(), System.getProperty("java.version"), io, load));
        sb.append(String.format("%d rounds, %ds warm-up + %ds measured per scenario; each cell is the median round by throughput. Every response, echo and event is verified.%n%n", rounds, warmup, duration));
        for (Scenario sc : SCENARIOS) {
            var byImpl = results.get(sc.name());
            if (byImpl == null) continue;
            boolean bulk = sc.name().contains("download");
            sb.append("## ").append(sc.name()).append("\n\n");
            sb.append(bulk ? "| server | MB/s | CPU µs/req | peak RSS MB | errors |\n|---|---:|---:|---:|---:|\n"
                    : "| server | " + sc.unit() + " | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (Impl impl : IMPLS) {
                List<Result> rs = byImpl.get(impl.name());
                if (rs == null) continue;
                List<Result> sorted = new ArrayList<>(rs);
                sorted.sort(Comparator.comparingDouble(bulk ? Result::mbps : Result::ops));
                Result m = sorted.get(sorted.size() / 2);
                long errors = rs.stream().mapToLong(Result::errors).sum();
                if (bulk) {
                    sb.append(String.format("| %s | %.0f | %.1f | %d | %d |%n", impl.name(), m.mbps(), m.cpuUsPerOp(), m.rssMb(), errors));
                } else {
                    sb.append(String.format("| %s | %.0f | %d | %d | %d | %d | %.1f | %d | %d |%n", impl.name(), m.ops(), m.p50(), m.p99(), m.p999(), m.max(), m.cpuUsPerOp(), m.rssMb(), errors));
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
