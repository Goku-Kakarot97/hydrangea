package bench;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Long mixed-traffic run against the Hydrangea benchmark server to catch what short tests cannot: leaked connections, file descriptors,
 * threads or live heap. Usage: Soak minutes outFile [host port]   (without host/port it starts its own local server)
 * <p>
 * Traffic: keep-alive HTTP with routing, connection-per-request churn, WebSocket echo, SSE subscribers that reconnect, 1 MiB streamed
 * uploads, downloads, 64 KiB echoes, and "chaos" clients that vanish half way through requests, uploads, handshakes and streams.
 * Verdict: connections, descriptors, threads and live heap must return to / stay at their baseline, and verified workloads must have 0 errors.
 */
public final class Soak {

    record Health(long t, long heapMb, long oldGenMb, long threads, long fds, long active, long opened, long closed, long rejected, long http, long timeouts, long maxBlockMs) {
        static Health parse(String line) {
            Map<String, Long> kv = new HashMap<>();
            for (String tok : line.substring(7).trim().split(" ")) {
                String[] x = tok.split("=");
                kv.put(x[0], Long.parseLong(x[1]));
            }
            return new Health(kv.get("t"), kv.get("heapMB"), kv.get("oldGenAfterGcMB"), kv.get("threads"), kv.get("fds"), kv.get("active"),
                    kv.get("opened"), kv.get("closed"), kv.get("rejected"), kv.get("http"), kv.get("timeouts"), kv.get("maxBlockMs"));
        }
    }

    public static void main(String[] a) throws Exception {
        int minutes = Integer.parseInt(a[0]);
        Path out = Path.of(a[1]);
        boolean local = a.length < 4;
        String host = local ? "127.0.0.1" : a[2];
        int port = local ? 23000 + (int) (ProcessHandle.current().pid() % 5000) : Integer.parseInt(a[3]);
        int secs = minutes * 60;
        List<Health> samples = new CopyOnWriteArrayList<>();
        Process server = null;
        if (local) {
            server = new ProcessBuilder(javaBin(), "-Xms256m", "-Xmx512m", "-XX:+UseG1GC", "-Dhealth.interval.sec=10", "-Dsse.tick.ms=100", "-cp",
                    System.getenv("BENCH_CP") + File.pathSeparator + System.getenv("HYD_CP"), "servers.HydrangeaHttpServer", String.valueOf(port), "4", host)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).start();
            Process s = server;
            var ready = new java.util.concurrent.CompletableFuture<Boolean>();
            Thread reader = new Thread(() -> {
                try (BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()))) {
                    for (String line; (line = r.readLine()) != null; ) {
                        if (line.equals("READY")) ready.complete(true);
                        else if (line.startsWith("health ")) samples.add(Health.parse(line));
                    }
                } catch (IOException ignored) { }
            });
            reader.setDaemon(true);
            reader.start();
            if (!ready.get(30, java.util.concurrent.TimeUnit.SECONDS)) throw new IllegalStateException("server did not start");
        }
        String p = String.valueOf(port);
        String d = String.valueOf(secs);
        Thread.sleep(3000);
        List<String> labels = new ArrayList<>();
        List<Process> loads = new ArrayList<>();
        // label, args
        String[][] plan = {
                {"HTTP keep-alive, routing + query (64 conns, 5 ms think)", "http", p, "64", "2", "0", d, "host=" + host, "path=/users/%d?q=soak", "think=5", "reconnect=true"},
                {"HTTP connection per request (8 conns, 20 ms think)", "http", p, "8", "1", "0", d, "host=" + host, "path=/plain", "close=true", "think=20", "reconnect=true"},
                {"WebSocket echo 200 B (200 conns, 100 ms think)", "ws", p, "200", "1", "0", d, "host=" + host, "size=200", "think=100", "reconnect=true"},
                {"SSE subscribers (300, reconnect)", "sse", p, "300", "1", "0", d, "host=" + host, "path=/sse", "reconnect=true"},
                {"streamed upload 1 MiB (4 conns, 200 ms think)", "http", p, "4", "1", "0", d, "host=" + host, "method=POST", "path=/sink", "body=1048576", "verify=false", "think=200", "reconnect=true"},
                {"download 512 KiB (4 conns, 100 ms think)", "http", p, "4", "1", "0", d, "host=" + host, "path=/download/524288", "verify=false", "think=100", "reconnect=true"},
                {"echo 64 KiB (4 conns, 50 ms think)", "http", p, "4", "1", "0", d, "host=" + host, "method=POST", "path=/echo", "body=65536", "think=50", "reconnect=true"},
        };
        for (String[] row : plan) {
            List<String> cmd = new ArrayList<>(List.of(javaBin(), "-Xmx256m", "-cp", System.getenv("BENCH_CP"), "bench.HttpLoad"));
            cmd.addAll(List.of(row).subList(1, row.length));
            labels.add(row[0]);
            loads.add(new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start());
        }
        Process chaos = new ProcessBuilder(javaBin(), "-Xmx128m", "-cp", System.getenv("BENCH_CP"), "bench.Chaos", host, p, d, "4", "25")
                .redirectError(ProcessBuilder.Redirect.DISCARD).start();
        System.err.printf("soak started: %d min against %s:%d%n", minutes, host, port);

        // progress every minute
        for (int m = 1; m <= minutes; m++) {
            Thread.sleep(60_000);
            Health h = samples.isEmpty() ? null : samples.get(samples.size() - 1);
            System.err.printf("  minute %2d: %s%n", m, h);
        }
        Map<String, String> results = new java.util.LinkedHashMap<>();
        for (int i = 0; i < loads.size(); i++) {
            String outText = new String(loads.get(i).getInputStream().readAllBytes());
            loads.get(i).waitFor();
            results.put(labels.get(i), outText.lines().filter(l -> l.startsWith("RESULT")).findFirst().orElse("RESULT (none)"));
        }
        String chaosOut = new String(chaos.getInputStream().readAllBytes()).trim();
        chaos.waitFor();
        System.err.println("load finished; cooling down 45 s while the server cleans up...");
        Thread.sleep(45_000);
        Health last = samples.isEmpty() ? null : samples.get(samples.size() - 1);
        String report = report(minutes, samples, results, chaosOut, last, local);
        Files.writeString(out, report);
        System.out.println(report);
        if (server != null) {
            server.destroy(); // SIGTERM: exercises the graceful shutdown hook as well
            boolean exited = server.waitFor(30, java.util.concurrent.TimeUnit.SECONDS);
            System.err.println("server exited after SIGTERM: " + exited + (exited ? " (code " + server.exitValue() + ")" : ""));
        }
    }

    static String report(int minutes, List<Health> samples, Map<String, String> results, String chaosOut, Health last, boolean local) {
        StringBuilder sb = new StringBuilder("# Soak test\n\n");
        sb.append(String.format("%d minutes of mixed traffic against the Hydrangea benchmark server (%s), then a 45 s cool-down with no load.%n%n",
                minutes, local ? "local JVM, 512 MB heap, 4 I/O threads" : "remote"));
        sb.append("## Traffic and results\n\n| workload | result |\n|---|---|\n");
        results.forEach((k, v) -> sb.append("| ").append(k).append(" | `").append(v.replace("RESULT ", "")).append("` |\n"));
        sb.append("| chaos clients (vanish mid-request / upload / handshake / stream) | `").append(chaosOut).append("` |\n\n");
        sb.append("## Server health over time\n\n| t (s) | live heap after GC (MB) | heap used (MB) | threads | descriptors | active connections | opened | closed | http responses | max loop block (ms) |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        int step = Math.max(1, samples.size() / 24);
        for (int i = 0; i < samples.size(); i += step) {
            Health h = samples.get(i);
            sb.append(String.format("| %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |%n", h.t(), h.oldGenMb(), h.heapMb(), h.threads(), h.fds(), h.active(), h.opened(), h.closed(), h.http(), h.maxBlockMs()));
        }
        Health lastRow = samples.get(samples.size() - 1);
        if (samples.size() % step != 1 && step > 1) {
            sb.append(String.format("| %d | %d | %d | %d | %d | %d | %d | %d | %d | %d |%n", lastRow.t(), lastRow.oldGenMb(), lastRow.heapMb(), lastRow.threads(), lastRow.fds(), lastRow.active(), lastRow.opened(), lastRow.closed(), lastRow.http(), lastRow.maxBlockMs()));
        }
        sb.append("\n## Verdict\n\n");
        // baseline = first sample once the load is up (after 60 s); steady = median of the last quarter during the load
        List<Health> load = samples.stream().filter(h -> h.t() >= 60).toList();
        List<Health> underLoad = load.subList(0, Math.max(1, load.size() - 5)); // the tail of the list is the cool-down
        List<Health> early = underLoad.subList(0, Math.max(1, underLoad.size() / 4));
        List<Health> late = underLoad.subList(underLoad.size() - Math.max(1, underLoad.size() / 4), underLoad.size());
        long oldEarly = median(early.stream().mapToLong(Health::oldGenMb).toArray());
        long oldLate = median(late.stream().mapToLong(Health::oldGenMb).toArray());
        long maxThreads = underLoad.stream().mapToLong(Health::threads).max().orElse(0);
        long minThreads = underLoad.stream().mapToLong(Health::threads).min().orElse(0);
        long fdsEarly = median(early.stream().mapToLong(Health::fds).toArray());
        long fdsLate = median(late.stream().mapToLong(Health::fds).toArray());
        long maxBlock = underLoad.stream().mapToLong(Health::maxBlockMs).max().orElse(0);
        long errors = 0;
        for (String v : results.values()) {
            for (String tok : v.split(" ")) if (tok.startsWith("errors=")) errors += Long.parseLong(tok.substring(7));
        }
        row(sb, "after cool-down no connection is left", last.active() == 0, "active = " + last.active());
        row(sb, "after cool-down descriptors are back near the start", last.fds() <= fdsEarly - 200 || last.fds() <= 60, "descriptors: " + fdsEarly + " under early load, " + fdsLate + " under late load, " + last.fds() + " after cool-down");
        row(sb, "live heap does not grow (last quarter vs first quarter, +25% allowed)", oldLate <= oldEarly * 1.25 + 8, oldEarly + " MB -> " + oldLate + " MB");
        row(sb, "thread count is stable", maxThreads - minThreads <= 16, "min " + minThreads + ", max " + maxThreads);
        row(sb, "descriptors under load do not creep (+10% allowed)", fdsLate <= fdsEarly * 1.1 + 20, fdsEarly + " -> " + fdsLate);
        row(sb, "the event loops were never blocked for long", maxBlock < 1000, "longest block " + maxBlock + " ms");
        row(sb, "verified workloads report no errors", errors == 0, errors + " errors");
        row(sb, "opened == closed + active at the end", last.opened() == last.closed() + last.active(), last.opened() + " opened, " + last.closed() + " closed, " + last.active() + " active");
        return sb.toString();
    }

    static void row(StringBuilder sb, String check, boolean ok, String detail) {
        sb.append(ok ? "- PASS " : "- **FAIL** ").append(check).append(" (").append(detail).append(")\n");
    }

    static long median(long[] v) {
        long[] c = v.clone();
        java.util.Arrays.sort(c);
        return c.length == 0 ? 0 : c[c.length / 2];
    }

    static String javaBin() {
        return Path.of(System.getProperty("java.home"), "bin", "java").toString();
    }
}
