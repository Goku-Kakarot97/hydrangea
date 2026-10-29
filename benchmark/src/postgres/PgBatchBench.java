package postgres;

import com.sun.management.OperatingSystemMXBean;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.postgres.PgClient;
import io.hydrangea.postgres.PgConfig;
import org.postgresql.PGConnection;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Bulk insert of one million... of {@code rows} rows into {@code bench_ins(id int, name text, n bigint)} over ONE connection, the ways each library offers. One way per JVM; the last line
 * is {@code RESULT ...}; the first run is warm-up, the rest are timed (median).
 * <pre>
 * java ... postgres.PgBatchBench WAY [rows=200000] [chunk=1000] [runs=6] [host=] [port=] [binary=off]
 * WAY: hydrangea-batch      PgClient.executeBatch in chunks
 *      hydrangea-pipelined  one PgClient.query per row, all in flight (automatic pipelining)
 *      hydrangea-unnest     one INSERT ... SELECT FROM unnest($1::int[], $2::text[], $3::bigint[]) per chunk (array parameters)
 *      hydrangea-copy       COPY FROM STDIN
 *      pgjdbc-batch         addBatch/executeBatch in chunks (the driver's default)
 *      pgjdbc-rewrite       the same with reWriteBatchedInserts=true (the driver turns the batch into multi-row INSERTs)
 *      pgjdbc-unnest        the same statement with pgjdbc (createArrayOf)
 *      pgjdbc-copy          CopyManager
 * </pre>
 */
public final class PgBatchBench {

    static final OperatingSystemMXBean OS = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static void main(String[] a) throws Exception {
        String way = a[0];
        Map<String, String> args = new HashMap<>();
        for (int i = 1; i < a.length; i++) {
            int eq = a[i].indexOf('=');
            args.put(a[i].substring(0, eq), a[i].substring(eq + 1));
        }
        int rows = Integer.parseInt(args.getOrDefault("rows", "200000")), chunk = Integer.parseInt(args.getOrDefault("chunk", "1000")), runs = Integer.parseInt(args.getOrDefault("runs", "6"));
        String host = args.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(args.getOrDefault("port", "25432"));
        String url = "jdbc:postgresql://" + host + ":" + port + "/hydra" + (way.equals("pgjdbc-rewrite") ? "?reWriteBatchedInserts=true" : "");
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/hydra", "hydra", "hydra"); Statement s = c.createStatement()) {
            s.execute("CREATE UNLOGGED TABLE IF NOT EXISTS bench_ins (id int, name text, n bigint)");
        }
        List<Object[]> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) data.add(new Object[]{i, "row number " + i, i * 7L});
        byte[] copyData;
        {
            StringBuilder sb = new StringBuilder();
            for (Object[] r : data) sb.append(r[0]).append('\t').append(r[1]).append('\t').append(r[2]).append('\n');
            copyData = sb.toString().getBytes(StandardCharsets.UTF_8);
        }
        List<Double> secs = new ArrayList<>(), cpus = new ArrayList<>();
        try (EventLoopGroup g = new EventLoopGroup(2); Connection jc = DriverManager.getConnection(url, "hydra", "hydra"); Statement js = jc.createStatement()) {
            PgClient pg = way.startsWith("hydrangea") ? PgClient.connect(g, PgConfig.of(host, port, "hydra", "hydra").withPassword("hydra").withBinaryTransfer(!args.getOrDefault("binary", "on").equals("off"))).get(30, TimeUnit.SECONDS) : null;
            for (int r = 0; r < runs; r++) {
                js.execute("TRUNCATE bench_ins");
                long cpu0 = OS.getProcessCpuTime(), t0 = System.nanoTime();
                switch (way) {
                    case "hydrangea-batch" -> {
                        for (int p = 0; p < rows; p += chunk) {
                            pg.executeBatch("INSERT INTO bench_ins VALUES ($1, $2, $3)", data.subList(p, Math.min(rows, p + chunk))).get(120, TimeUnit.SECONDS);
                        }
                    }
                    case "hydrangea-pipelined" -> {
                        List<CompletableFuture<?>> fs = new ArrayList<>(chunk);
                        for (Object[] row : data) {
                            fs.add(pg.query("INSERT INTO bench_ins VALUES ($1, $2, $3)", row));
                            if (fs.size() == chunk) {
                                for (var f : fs) f.get(120, TimeUnit.SECONDS);
                                fs.clear();
                            }
                        }
                        for (var f : fs) f.get(120, TimeUnit.SECONDS);
                    }
                    case "hydrangea-unnest" -> {
                        for (int p = 0; p < rows; p += chunk) {
                            List<Object[]> part = data.subList(p, Math.min(rows, p + chunk));
                            List<Integer> ids = new ArrayList<>(part.size());
                            List<String> names = new ArrayList<>(part.size());
                            List<Long> ns = new ArrayList<>(part.size());
                            for (Object[] row : part) {
                                ids.add((Integer) row[0]);
                                names.add((String) row[1]);
                                ns.add((Long) row[2]);
                            }
                            pg.query("INSERT INTO bench_ins SELECT * FROM unnest($1::int[], $2::text[], $3::bigint[])", ids, names, ns).get(120, TimeUnit.SECONDS);
                        }
                    }
                    case "pgjdbc-unnest" -> {
                        try (PreparedStatement ps = jc.prepareStatement("INSERT INTO bench_ins SELECT * FROM unnest(?::int[], ?::text[], ?::bigint[])")) {
                            for (int p = 0; p < rows; p += chunk) {
                                List<Object[]> part = data.subList(p, Math.min(rows, p + chunk));
                                Integer[] ids = new Integer[part.size()];
                                String[] names = new String[part.size()];
                                Long[] ns = new Long[part.size()];
                                for (int i = 0; i < ids.length; i++) {
                                    ids[i] = (Integer) part.get(i)[0];
                                    names[i] = (String) part.get(i)[1];
                                    ns[i] = (Long) part.get(i)[2];
                                }
                                ps.setArray(1, jc.createArrayOf("int4", ids));
                                ps.setArray(2, jc.createArrayOf("text", names));
                                ps.setArray(3, jc.createArrayOf("int8", ns));
                                ps.executeUpdate();
                            }
                        }
                    }
                    case "hydrangea-copy" -> pg.copyIn("COPY bench_ins FROM STDIN", copyData).get(120, TimeUnit.SECONDS);
                    case "pgjdbc-batch", "pgjdbc-rewrite" -> {
                        try (PreparedStatement ps = jc.prepareStatement("INSERT INTO bench_ins VALUES (?, ?, ?)")) {
                            int n = 0;
                            for (Object[] row : data) {
                                ps.setInt(1, (Integer) row[0]);
                                ps.setString(2, (String) row[1]);
                                ps.setLong(3, (Long) row[2]);
                                ps.addBatch();
                                if (++n % chunk == 0) ps.executeBatch();
                            }
                            ps.executeBatch();
                        }
                    }
                    case "pgjdbc-copy" -> jc.unwrap(PGConnection.class).getCopyAPI().copyIn("COPY bench_ins FROM STDIN", new ByteArrayInputStream(copyData), 65536);
                    default -> throw new IllegalArgumentException(way);
                }
                double sec = (System.nanoTime() - t0) / 1e9, cpu = (OS.getProcessCpuTime() - cpu0) / 1e9;
                long count;
                try (var rs = js.executeQuery("SELECT count(*) FROM bench_ins")) {
                    rs.next();
                    count = rs.getLong(1);
                }
                if (count != rows) throw new IllegalStateException("inserted " + count);
                if (r > 0) { secs.add(sec); cpus.add(cpu); }
            }
            if (pg != null) pg.close();
        }
        Collections.sort(secs);
        Collections.sort(cpus);
        double sec = secs.get(secs.size() / 2), cpu = cpus.get(cpus.size() / 2);
        System.out.printf("RESULT binary=%s way=%s rows=%d chunk=%d seconds=%.3f rows_per_s=%.0f cpu_seconds=%.3f cpu_us_per_row=%.1f%n", way.startsWith("hydrangea") ? args.getOrDefault("binary", "on") : "-", way, rows, chunk, sec, rows / sec, cpu, cpu * 1e6 / rows);
        System.exit(0);
    }
}
