package bench;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs every scenario against every implementation (each in its own JVM, same flags), several rounds with the
 * order rotated per round, and prints a Markdown report with the median round per cell.
 * <pre>Driver rounds warmupSec durationSec ioThreads loadThreads outFile</pre>
 * Classpaths come from env vars HYD_CP and XNIO_CP; compiled benchmark classes from BENCH_CP.
 */
public final class Driver {

    record Impl(String name, String mainClass, String cpEnv, String... extraArgs) {
    }

    record Scenario(String name, String mode, int conns, int size) {
    }

    record Result(double ops, double mbps, long p50, long p90, long p99, long p999, long max, long errors,
                  double cpuUsPerOp, double cpuMsPerMB, long rssMb) {
    }

    static final List<Impl> IMPLS = List.of(
            new Impl("xnio", "servers.XnioEchoServer", "XNIO_CP"),
            new Impl("hydrangea", "servers.HydrangeaEchoServer", "HYD_CP"),
            new Impl("hydrangea+pool", "servers.HydrangeaEchoServer", "HYD_CP", "pool"));

    static final List<Scenario> SCENARIOS = List.of(
            new Scenario("ping-pong 1 conn, 64 B", "pingpong", 1, 64),
            new Scenario("ping-pong 100 conns, 64 B", "pingpong", 100, 64),
            new Scenario("ping-pong 1000 conns, 64 B", "pingpong", 1000, 64),
            new Scenario("ping-pong 100 conns, 16 KiB", "pingpong", 100, 16 * 1024),
            new Scenario("stream 16 conns (bulk MB/s)", "stream", 16, 0),
            new Scenario("connect churn 50 slots", "churn", 50, 64));

    public static void main(String[] a) throws Exception {
        int rounds = Integer.parseInt(a[0]);
        int warmup = Integer.parseInt(a[1]);
        int duration = Integer.parseInt(a[2]);
        int ioThreads = Integer.parseInt(a[3]);
        int loadThreads = Integer.parseInt(a[4]);
        Path outFile = Path.of(a[5]);
        int port = 20000 + (int) (ProcessHandle.current().pid() % 20000);

        Map<String, Map<String, List<Result>>> results = new LinkedHashMap<>();
        for (int round = 0; round < rounds; round++) {
            for (int i = 0; i < IMPLS.size(); i++) {
                Impl impl = IMPLS.get((i + round) % IMPLS.size()); // rotate order to cancel warm-machine / thermal bias
                System.err.printf("== round %d/%d: %s%n", round + 1, rounds, impl.name());
                Process server = startServer(impl, ++port, ioThreads);
                try {
                    // throw-away run so the server JIT is warm before the first measured scenario
                    runLoad("pingpong", port, 100, 64, loadThreads, 3, 2);
                    for (Scenario sc : SCENARIOS) {
                        if (sc.mode().equals("churn")) {
                            // Churn burns through kernel socket buffers (macOS reports "No buffer space available" and
                            // "requests for memory denied"). Give the OS time to recover so the result measures the
                            // server, not leftovers from the previous scenario/implementation.
                            Thread.sleep(20_000);
                        }
                        Result r = measure(server, sc, port, loadThreads, warmup, duration);
                        System.err.printf("   %-32s %s%n", sc.name(), r);
                        results.computeIfAbsent(sc.name(), k -> new LinkedHashMap<>())
                                .computeIfAbsent(impl.name(), k -> new ArrayList<>()).add(r);
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
        List<String> cmd = new ArrayList<>(List.of(javaBin(), "-Xms512m", "-Xmx512m", "-XX:+UseG1GC", "-cp",
                System.getenv("BENCH_CP") + File.pathSeparator + System.getenv(impl.cpEnv()), impl.mainClass(),
                String.valueOf(port), String.valueOf(threads)));
        cmd.addAll(List.of(impl.extraArgs()));
        Process p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()));
        var ready = java.util.concurrent.CompletableFuture.supplyAsync(() -> {
            try {
                for (String line; (line = r.readLine()) != null; ) {
                    if (line.equals("READY")) return true;
                }
            } catch (IOException ignored) { }
            return false;
        });
        try {
            if (ready.get(30, java.util.concurrent.TimeUnit.SECONDS)) {
                Thread drain = new Thread(() -> { try { while (r.readLine() != null) { } } catch (IOException ignored) { } });
                drain.setDaemon(true);
                drain.start();
                return p;
            }
        } catch (java.util.concurrent.TimeoutException ignored) {
            // fall through to the failure below
        }
        p.destroyForcibly();
        throw new IllegalStateException("server " + impl.name() + " did not start");
    }

    static Result measure(Process server, Scenario sc, int port, int loadThreads, int warmup, int duration) throws Exception {
        long pid = server.pid();
        double cpu0 = cpuSeconds(pid);
        AtomicLong peakRss = new AtomicLong();
        java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean();
        Thread sampler = new Thread(() -> {
            // a flag, not interrupt(): ps calls swallow InterruptedException and would keep looping forever
            while (!stop.get()) {
                peakRss.accumulateAndGet(rssKb(pid), Math::max);
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        sampler.start();
        // CPU is measured over warmup+measure; ops are only counted in measure, so scale by the measured share
        Map<String, String> kv = runLoad(sc.mode(), port, sc.conns(), sc.size(), loadThreads, warmup, duration);
        stop.set(true);
        sampler.join();
        double cpu = cpuSeconds(pid) - cpu0;
        double ops = Double.parseDouble(kv.get("ops"));
        double mbps = Double.parseDouble(kv.get("mbps"));
        double share = (double) duration / (warmup + duration);
        double cpuMeasure = cpu * share; // approximation: assumes steady load during warmup
        return new Result(ops, mbps, Long.parseLong(kv.get("p50")), Long.parseLong(kv.get("p90")),
                Long.parseLong(kv.get("p99")), Long.parseLong(kv.get("p999")), Long.parseLong(kv.get("max")),
                Long.parseLong(kv.get("errors")),
                ops > 0 ? cpuMeasure * 1e6 / (ops * duration) : 0,
                mbps > 0 ? cpuMeasure * 1000 / (mbps * duration) : 0,
                peakRss.get() / 1024);
    }

    static Map<String, String> runLoad(String mode, int port, int conns, int size, int threads, int warmup, int duration)
            throws Exception {
        Process p = new ProcessBuilder(javaBin(), "-Xmx512m", "-cp", System.getenv("BENCH_CP"), "bench.LoadGenerator",
                mode, String.valueOf(port), String.valueOf(conns), String.valueOf(size), String.valueOf(threads),
                String.valueOf(warmup), String.valueOf(duration)).redirectError(ProcessBuilder.Redirect.INHERIT).start();
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

    // ---- process stats via ps (macOS and Linux compatible) ----

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
        String t = ps(pid, "time"); // [[dd-]hh:]mm:ss[.cc]
        double total = 0;
        if (t.contains("-")) {
            total += Integer.parseInt(t.substring(0, t.indexOf('-'))) * 86400.0;
            t = t.substring(t.indexOf('-') + 1);
        }
        for (String part : t.split(":")) {
            total = total * 60 + Double.parseDouble(part);
        }
        return total;
    }

    // ---- report ----

    static String report(Map<String, Map<String, List<Result>>> results, int rounds, int warmup, int duration, int io, int load) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Benchmark: Hydrangea vs XNIO\n\n");
        sb.append(String.format("%s, %s, %s cores; JDK %s; server %d I/O threads, load generator %d threads (same machine);%n",
                System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                System.getProperty("java.version"), io, load));
        sb.append(String.format("%d rounds, %ds warmup + %ds measured per scenario; cell = median round by throughput.%n%n", rounds, warmup, duration));
        for (var sc : SCENARIOS) {
            Map<String, List<Result>> byImpl = results.get(sc.name());
            if (byImpl == null) continue;
            boolean stream = sc.mode().equals("stream");
            sb.append("## ").append(sc.name()).append("\n\n");
            sb.append(stream
                    ? "| impl | MB/s | CPU ms per MB | peak RSS MB | errors |\n|---|---:|---:|---:|---:|\n"
                    : "| impl | ops/s | p50 µs | p99 µs | p99.9 µs | max µs | CPU µs/op | peak RSS MB | errors |\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            for (Impl impl : IMPLS) {
                List<Result> rs = byImpl.get(impl.name());
                if (rs == null) continue;
                List<Result> sorted = new ArrayList<>(rs);
                sorted.sort(Comparator.comparingDouble(stream ? Result::mbps : Result::ops));
                Result m = sorted.get(sorted.size() / 2);
                long errors = rs.stream().mapToLong(Result::errors).sum();
                if (stream) {
                    sb.append(String.format("| %s | %.0f | %.2f | %d | %d |%n", impl.name(), m.mbps(), m.cpuMsPerMB(), m.rssMb(), errors));
                } else {
                    sb.append(String.format("| %s | %.0f | %d | %d | %d | %d | %.1f | %d | %d |%n", impl.name(), m.ops(), m.p50(),
                            m.p99(), m.p999(), m.max(), m.cpuUsPerOp(), m.rssMb(), errors));
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }
}
