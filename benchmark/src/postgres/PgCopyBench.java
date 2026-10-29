package postgres;

import com.sun.management.OperatingSystemMXBean;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.postgres.PgClient;
import io.hydrangea.postgres.PgConfig;
import org.postgresql.PGConnection;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * COPY of Hydrangea's {@code PgClient} against pgjdbc's {@code CopyManager}, one connection each. One library and direction per JVM; the last line is
 * {@code RESULT ...}.
 * <pre>
 * java ... postgres.PgCopyBench hydrangea|pgjdbc in|out [rows=1000000] [runs=6] [host=] [port=] [tls=/path/ca.pem]
 * </pre>
 * {@code in}: {@code COPY bench_copy FROM STDIN} of rows built beforehand in memory (id, text of 40 characters, number), in 64 KB pieces; {@code out}: {@code COPY bench_copy TO STDOUT},
 * the bytes counted and dropped. The first run is warm-up. Prints the median of the others.
 */
public final class PgCopyBench {

    static final OperatingSystemMXBean OS = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static void main(String[] a) throws Exception {
        String lib = a[0], dir = a[1];
        Map<String, String> args = new HashMap<>();
        for (int i = 2; i < a.length; i++) {
            int eq = a[i].indexOf('=');
            args.put(a[i].substring(0, eq), a[i].substring(eq + 1));
        }
        int rows = Integer.parseInt(args.getOrDefault("rows", "1000000")), runs = Integer.parseInt(args.getOrDefault("runs", "6"));
        String host = args.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(args.getOrDefault("port", "25432"));
        String tls = args.get("tls");
        String url = "jdbc:postgresql://" + host + ":" + port + "/hydra" + (tls != null ? "?ssl=true&sslmode=verify-full&sslrootcert=" + tls : "");

        StringBuilder sb = new StringBuilder(rows * 60);
        for (int i = 0; i < rows; i++) sb.append(i).append("\trow number ").append(String.format("%028d", i)).append('\t').append(i * 7L % 100003).append('\n');
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        List<byte[]> chunks = new ArrayList<>();
        for (int p = 0; p < data.length; p += 65536) chunks.add(Arrays.copyOfRange(data, p, Math.min(data.length, p + 65536)));

        try (Connection c = DriverManager.getConnection(url, "hydra", "hydra"); Statement s = c.createStatement()) {
            s.execute("CREATE UNLOGGED TABLE IF NOT EXISTS bench_copy (id int, name text, n bigint)");
            s.execute("TRUNCATE bench_copy");
            if (dir.equals("out")) {
                s.execute("INSERT INTO bench_copy SELECT g, 'row number ' || lpad(g::text, 28, '0'), g * 7 % 100003 FROM generate_series(0, " + (rows - 1) + ") g");
            }
        }
        List<Double> secs = new ArrayList<>(), cpus = new ArrayList<>();
        if (lib.equals("hydrangea")) {
            try (EventLoopGroup g = new EventLoopGroup(2)) {
                PgConfig cfg = PgConfig.of(host, port, "hydra", "hydra").withPassword("hydra");
                if (tls != null) cfg = cfg.withTls(PgBench.tlsContextFor(tls));
                PgClient pg = PgClient.connect(g, cfg).get(30, TimeUnit.SECONDS);
                for (int r = 0; r < runs; r++) {
                    if (dir.equals("in")) pg.execute("TRUNCATE bench_copy").get(60, TimeUnit.SECONDS);
                    long cpu0 = OS.getProcessCpuTime(), t0 = System.nanoTime();
                    long n;
                    if (dir.equals("in")) {
                        n = pg.copyIn("COPY bench_copy FROM STDIN", chunks).get(300, TimeUnit.SECONDS);
                    } else {
                        AtomicLong bytes = new AtomicLong();
                        n = pg.copyOut("COPY bench_copy TO STDOUT", chunk -> bytes.addAndGet(chunk.length)).get(300, TimeUnit.SECONDS);
                    }
                    double sec = (System.nanoTime() - t0) / 1e9, cpu = (OS.getProcessCpuTime() - cpu0) / 1e9;
                    if (n != rows) throw new IllegalStateException("copied " + n);
                    if (r > 0) { secs.add(sec); cpus.add(cpu); }
                }
                pg.close();
            }
        } else {
            try (Connection c = DriverManager.getConnection(url, "hydra", "hydra"); Statement s = c.createStatement()) {
                org.postgresql.copy.CopyManager cm = c.unwrap(PGConnection.class).getCopyAPI();
                for (int r = 0; r < runs; r++) {
                    if (dir.equals("in")) s.execute("TRUNCATE bench_copy");
                    long cpu0 = OS.getProcessCpuTime(), t0 = System.nanoTime();
                    long n;
                    if (dir.equals("in")) {
                        n = cm.copyIn("COPY bench_copy FROM STDIN", new ByteArrayInputStream(data), 65536);
                    } else {
                        AtomicLong bytes = new AtomicLong();
                        n = cm.copyOut("COPY bench_copy TO STDOUT", new OutputStream() {
                            public void write(int b) { bytes.incrementAndGet(); }
                            public void write(byte[] b, int off, int len) { bytes.addAndGet(len); }
                        });
                    }
                    double sec = (System.nanoTime() - t0) / 1e9, cpu = (OS.getProcessCpuTime() - cpu0) / 1e9;
                    if (n != rows) throw new IllegalStateException("copied " + n);
                    if (r > 0) { secs.add(sec); cpus.add(cpu); }
                }
            }
        }
        Collections.sort(secs);
        Collections.sort(cpus);
        double sec = secs.get(secs.size() / 2), cpu = cpus.get(cpus.size() / 2);
        System.out.printf("RESULT lib=%s dir=%s tls=%s rows=%d mb=%.1f seconds=%.3f mb_per_s=%.1f cpu_seconds=%.3f%n", lib, dir, tls != null ? "on" : "off", rows, data.length / 1e6, sec,
                data.length / 1e6 / sec, cpu);
        System.exit(0);
    }
}
