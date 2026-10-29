package mysql;

import com.sun.management.OperatingSystemMXBean;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.mysql.MyClient;
import io.hydrangea.mysql.MyConfig;
import io.hydrangea.mysql.MyRow;
import io.hydrangea.mysql.MyRowStream;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Bulk work over ONE connection, the ways each library offers: loading {@code rows} rows into {@code bench_ins(id int, name varchar(40), n bigint)} and reading them all back.
 * Every chunk of {@code chunk} rows is one transaction (one commit), for every way, so the disk's fsync is paid once per chunk by all of them. One way per JVM; the last line is
 * {@code RESULT ...}; the first run is warm-up, the rest are timed (median).
 * <pre>
 * java ... mysql.MyBulkBench WAY [rows=200000] [chunk=1000] [runs=4] [host=] [port=]
 * WAY: hydrangea-batch      MyClient.executeBatch in chunks (one PREPARE, the EXECUTEs written back to back)
 *      hydrangea-pipelined  one MyClient.query per row, all of a chunk in flight (automatic pipelining)
 *      hydrangea-multirow   one INSERT ... VALUES (?,?,?),(?,?,?)... per chunk
 *      mysqlj-batch         addBatch/executeBatch, server-side prepared statements (useServerPrepStmts=true)
 *      mysqlj-rewrite       addBatch/executeBatch with rewriteBatchedStatements=true (the driver builds multi-row INSERTs; client-side prepared statements)
 *      mysqlj-multirow      one INSERT ... VALUES (?,?,?),... per chunk, server-side prepared
 *      hydrangea-stream     read every row back with stream(fetchSize = chunk)          (the table must have been loaded: run a load way first)
 *      hydrangea-whole      read every row back with one query (the result held whole)
 *      mysqlj-cursor        read every row back with useCursorFetch=true, fetch size = chunk
 *      mysqlj-whole         read every row back with the default (the result held whole)
 * </pre>
 */
public final class MyBulkBench {

    static final OperatingSystemMXBean OS = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    public static void main(String[] a) throws Exception {
        String way = a[0];
        Map<String, String> args = new HashMap<>();
        for (int i = 1; i < a.length; i++) {
            int eq = a[i].indexOf('=');
            args.put(a[i].substring(0, eq), a[i].substring(eq + 1));
        }
        int rows = Integer.parseInt(args.getOrDefault("rows", "200000")), chunk = Integer.parseInt(args.getOrDefault("chunk", "1000")), runs = Integer.parseInt(args.getOrDefault("runs", "4"));
        String host = args.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(args.getOrDefault("port", "23306"));
        boolean read = way.endsWith("-stream") || way.endsWith("-whole") || way.endsWith("-cursor");
        String base = "jdbc:mysql://" + host + ":" + port + "/hydra?useSSL=false&allowPublicKeyRetrieval=true";
        String url = switch (way) {
            case "mysqlj-rewrite" -> base + "&rewriteBatchedStatements=true";
            case "mysqlj-cursor" -> base + "&useServerPrepStmts=true&useCursorFetch=true&defaultFetchSize=" + chunk;
            default -> base + "&useServerPrepStmts=true&cachePrepStmts=true";
        };
        try (Connection c = DriverManager.getConnection(base, "hydra", "hydra"); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS bench_ins (id INT, name VARCHAR(40), n BIGINT) ENGINE=InnoDB");
            if (read) {
                try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM bench_ins")) {
                    rs.next();
                    if (rs.getLong(1) != rows) {
                        throw new IllegalStateException("the table holds " + rs.getLong(1) + " rows, not " + rows + ": load it first");
                    }
                }
            }
        }
        List<Object[]> data = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) data.add(new Object[]{i, "row number " + i, i * 7L});
        List<Double> secs = new ArrayList<>(), cpus = new ArrayList<>();
        try (EventLoopGroup g = new EventLoopGroup(1); Connection jc = DriverManager.getConnection(url, "hydra", "hydra"); Statement js = jc.createStatement()) {
            MyClient my = way.startsWith("hydrangea") ? MyClient.connect(g, MyConfig.of(host, port, "hydra", "hydra").withPassword("hydra").withAllowPublicKeyRetrieval(true).withResultLimits(10_000_000, 4L << 30)).get(30, TimeUnit.SECONDS) : null;
            for (int r = 0; r < runs; r++) {
                if (!read) {
                    js.execute("TRUNCATE bench_ins");
                }
                long cpu0 = OS.getProcessCpuTime(), t0 = System.nanoTime();
                long seen = 0;
                switch (way) {
                    case "hydrangea-batch" -> {
                        for (int p = 0; p < rows; p += chunk) {
                            List<Object[]> part = data.subList(p, Math.min(rows, p + chunk));
                            my.query("START TRANSACTION").get(120, TimeUnit.SECONDS);
                            my.executeBatch("INSERT INTO bench_ins VALUES (?, ?, ?)", part).get(120, TimeUnit.SECONDS);
                            my.query("COMMIT").get(120, TimeUnit.SECONDS);
                        }
                    }
                    case "hydrangea-pipelined" -> {
                        for (int p = 0; p < rows; p += chunk) {
                            List<CompletableFuture<?>> fs = new ArrayList<>(chunk + 2);
                            fs.add(my.query("START TRANSACTION"));
                            for (Object[] row : data.subList(p, Math.min(rows, p + chunk))) {
                                fs.add(my.query("INSERT INTO bench_ins VALUES (?, ?, ?)", row));
                            }
                            fs.add(my.query("COMMIT"));
                            for (var f : fs) f.get(120, TimeUnit.SECONDS);
                        }
                    }
                    case "hydrangea-multirow" -> {
                        for (int p = 0; p < rows; p += chunk) {
                            List<Object[]> part = data.subList(p, Math.min(rows, p + chunk));
                            Object[] params = new Object[part.size() * 3];
                            StringBuilder sql = new StringBuilder("INSERT INTO bench_ins VALUES ");
                            for (int i = 0; i < part.size(); i++) {
                                sql.append(i == 0 ? "(?, ?, ?)" : ", (?, ?, ?)");
                                params[3 * i] = part.get(i)[0];
                                params[3 * i + 1] = part.get(i)[1];
                                params[3 * i + 2] = part.get(i)[2];
                            }
                            my.query(sql.toString(), params).get(120, TimeUnit.SECONDS);       // autocommit: one statement, one commit
                        }
                    }
                    case "mysqlj-batch", "mysqlj-rewrite" -> {
                        jc.setAutoCommit(false);
                        try (PreparedStatement ps = jc.prepareStatement("INSERT INTO bench_ins VALUES (?, ?, ?)")) {
                            int n = 0;
                            for (Object[] row : data) {
                                ps.setInt(1, (Integer) row[0]);
                                ps.setString(2, (String) row[1]);
                                ps.setLong(3, (Long) row[2]);
                                ps.addBatch();
                                if (++n % chunk == 0) {
                                    ps.executeBatch();
                                    jc.commit();
                                }
                            }
                            ps.executeBatch();
                            jc.commit();
                        }
                        jc.setAutoCommit(true);
                    }
                    case "mysqlj-multirow" -> {
                        StringBuilder sql = new StringBuilder("INSERT INTO bench_ins VALUES ");
                        for (int i = 0; i < chunk; i++) sql.append(i == 0 ? "(?, ?, ?)" : ", (?, ?, ?)");
                        try (PreparedStatement ps = jc.prepareStatement(sql.toString())) {
                            for (int p = 0; p + chunk <= rows; p += chunk) {
                                for (int i = 0; i < chunk; i++) {
                                    Object[] row = data.get(p + i);
                                    ps.setInt(3 * i + 1, (Integer) row[0]);
                                    ps.setString(3 * i + 2, (String) row[1]);
                                    ps.setLong(3 * i + 3, (Long) row[2]);
                                }
                                ps.executeUpdate();
                            }
                        }
                    }
                    case "hydrangea-stream" -> {
                        MyRowStream st = my.stream(chunk, "SELECT id, name, n FROM bench_ins").get(120, TimeUnit.SECONDS);
                        for (List<MyRow> piece = st.next().get(120, TimeUnit.SECONDS); !piece.isEmpty(); piece = st.next().get(120, TimeUnit.SECONDS)) {
                            for (MyRow row : piece) {
                                seen += row.getInt(0) + row.getString(1).length() + row.getLong(2) != 0 ? 1 : 0;
                            }
                        }
                    }
                    case "hydrangea-whole" -> {
                        for (MyRow row : my.query("SELECT id, name, n FROM bench_ins").get(120, TimeUnit.SECONDS).rows()) {
                            seen += row.getInt(0) + row.getString(1).length() + row.getLong(2) != 0 ? 1 : 0;
                        }
                    }
                    case "mysqlj-cursor", "mysqlj-whole" -> {
                        try (PreparedStatement ps = jc.prepareStatement("SELECT id, name, n FROM bench_ins")) {
                            if (way.equals("mysqlj-cursor")) {
                                ps.setFetchSize(chunk);
                            }
                            try (ResultSet rs = ps.executeQuery()) {
                                while (rs.next()) {
                                    seen += rs.getInt(1) + rs.getString(2).length() + rs.getLong(3) != 0 ? 1 : 0;
                                }
                            }
                        }
                    }
                    default -> throw new IllegalArgumentException(way);
                }
                double sec = (System.nanoTime() - t0) / 1e9, cpu = (OS.getProcessCpuTime() - cpu0) / 1e9;
                long count;
                if (read) {
                    count = seen;
                } else {
                    try (ResultSet rs = js.executeQuery("SELECT COUNT(*) FROM bench_ins")) {
                        rs.next();
                        count = rs.getLong(1);
                    }
                }
                if (count != rows) throw new IllegalStateException("rows: " + count);
                if (r > 0) { secs.add(sec); cpus.add(cpu); }
            }
            if (my != null) my.close();
        }
        Collections.sort(secs);
        Collections.sort(cpus);
        double sec = secs.get(secs.size() / 2), cpu = cpus.get(cpus.size() / 2);
        System.out.printf("RESULT way=%s rows=%d chunk=%d seconds=%.3f rows_per_s=%.0f cpu_seconds=%.3f cpu_us_per_row=%.1f%n", way, rows, chunk, sec, rows / sec, cpu, cpu * 1e6 / rows);
        System.exit(0);
    }
}
