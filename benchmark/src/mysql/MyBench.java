package mysql;

import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.mysql.MyConfig;
import io.hydrangea.mysql.MyPool;
import io.hydrangea.mysql.MyResult;
import io.hydrangea.mysql.MyRow;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Hydrangea's MySQL client against MySQL Connector/J on the same server. One library, one scenario per JVM; the last line printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... mysql.MyBench setup host= port=                       (creates and fills the tables)
 * java ... mysql.MyBench LIB SCENARIO [key=value ...]    keys: host= port= seconds= warmup= conns= callers= loops= user= password= db=
 *                                                        (loops: event loops of the Hydrangea pool; default = conns, one loop for each connection, the shape of a thread per connection)
 * LIB:      hydrangea            MyPool, callers are future chains (no thread per caller); calls with parameters are server-prepared
 *           hydrangea-block      MyPool, one platform thread per caller doing query().join()
 *           mysqlj               HikariCP + Connector/J with its defaults (client-side prepared statements: the parameters are put into the SQL text)
 *           mysqlj-server        HikariCP + Connector/J with useServerPrepStmts=true, cachePrepStmts=true (the same protocol as Hydrangea)
 *           mysqlj-virtual       as mysqlj, one virtual thread per caller
 *           mysqlj-dedicated     Connector/J (server-prepared), one connection per caller thread, no pool (callers = connections)
 * SCENARIO: select1 | point | rows | insert | tx
 * </pre>
 */
public final class MyBench {

    static Map<String, String> args = new HashMap<>();

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    static String user, password, db;

    static MyConfig myConfig(String host, int port) {
        return MyConfig.of(host, port, user, db).withPassword(password).withAllowPublicKeyRetrieval(true);
    }

    static final int ACCOUNTS = 100_000;
    static final com.sun.management.OperatingSystemMXBean OS = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

    // ---- latency histogram: 16 sub-buckets per power of two ----

    static final class Histogram {
        final long[] counts = new long[64 * 16];

        void record(long nanos) {
            int msb = 63 - Long.numberOfLeadingZeros(Math.max(nanos, 1));
            int sub = msb >= 4 ? (int) ((nanos >>> (msb - 4)) & 15) : (int) (nanos & 15);
            counts[msb * 16 + sub]++;
        }

        void add(Histogram o) {
            for (int i = 0; i < counts.length; i++) counts[i] += o.counts[i];
        }

        long total() {
            long n = 0;
            for (long c : counts) n += c;
            return n;
        }

        long quantile(double q) {
            long want = (long) Math.ceil(total() * q), seen = 0;
            for (int i = 0; i < counts.length; i++) {
                seen += counts[i];
                if (seen >= want && counts[i] > 0) {
                    int msb = i / 16, sub = i % 16;
                    return msb >= 4 ? ((16L + sub) << (msb - 4)) : sub;
                }
            }
            return 0;
        }
    }

    static final List<Histogram> histograms = Collections.synchronizedList(new ArrayList<>());
    static final ThreadLocal<Histogram> HIST = ThreadLocal.withInitial(() -> {
        Histogram h = new Histogram();
        histograms.add(h);
        return h;
    });
    static final AtomicBoolean measuring = new AtomicBoolean(), stop = new AtomicBoolean();
    static final LongAdder done = new LongAdder(), errors = new LongAdder();

    static final java.util.concurrent.atomic.AtomicLongArray perCaller = new java.util.concurrent.atomic.AtomicLongArray(4096);

    static void record(long t0, Throwable err, int idx) {
        if (!measuring.get()) return;
        if (err != null) {
            errors.increment();
            return;
        }
        done.increment();
        perCaller.incrementAndGet(idx);
        HIST.get().record(System.nanoTime() - t0);
    }

    // ---- scenarios ----

    interface Blocking {
        void run() throws Exception;
    }

    interface Async {
        CompletableFuture<?> start();
    }

    static String sqlOf(String scenario) {
        return switch (scenario) {
            case "select1" -> "SELECT 1";
            case "point" -> "SELECT id, name, balance FROM bench_accounts WHERE id = ?";
            case "rows" -> "SELECT id, name, balance FROM bench_accounts WHERE id BETWEEN ? AND ?";
            case "insert" -> "INSERT INTO bench_log (msg) VALUES (?)";
            case "tx" -> "UPDATE bench_accounts SET balance = balance + 1 WHERE id = ?";
            default -> throw new IllegalArgumentException(scenario);
        };
    }

    static int id() {
        return 1 + ThreadLocalRandom.current().nextInt(ACCOUNTS - 100);
    }

    static Async hydrangeaOp(MyPool pool, String scenario) {
        String sql = sqlOf(scenario);
        return switch (scenario) {
            case "select1" -> () -> pool.query(sql);
            case "point" -> () -> pool.query(sql, id()).thenApply(r -> {
                MyRow row = r.first();
                return row.getInt(0) + row.getString(1).length() + row.getLong(2);
            });
            case "rows" -> () -> {
                int a = id();
                return pool.query(sql, a, a + 99).thenApply(r -> {
                    long sum = 0;
                    for (MyRow row : r.rows()) sum += row.getInt(0) + row.getString(1).length() + row.getLong(2);
                    return sum;
                });
            };
            case "insert" -> () -> pool.query(sql, "message " + ThreadLocalRandom.current().nextInt());
            case "tx" -> () -> {
                int a = id();
                return pool.transaction(db -> db.query(sql, a).thenCompose(x -> db.query("SELECT balance FROM bench_accounts WHERE id = ?", a)));
            };
            default -> throw new IllegalArgumentException(scenario);
        };
    }

    /** What one query does on a JDBC connection, reading every column as the application would. */
    static void jdbcOp(Connection c, String scenario, String sql) throws Exception {
        switch (scenario) {
            case "select1" -> {
                try (PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    rs.getInt(1);
                }
            }
            case "point" -> {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setInt(1, id());
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        long x = rs.getInt(1) + rs.getString(2).length() + rs.getLong(3);
                    }
                }
            }
            case "rows" -> {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int a = id();
                    ps.setInt(1, a);
                    ps.setInt(2, a + 99);
                    try (ResultSet rs = ps.executeQuery()) {
                        long sum = 0;
                        while (rs.next()) sum += rs.getInt(1) + rs.getString(2).length() + rs.getLong(3);
                    }
                }
            }
            case "insert" -> {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    ps.setString(1, "message " + ThreadLocalRandom.current().nextInt());
                    ps.executeUpdate();
                }
            }
            case "tx" -> {
                c.setAutoCommit(false);
                try {
                    int a = id();
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setInt(1, a);
                        ps.executeUpdate();
                    }
                    try (PreparedStatement ps = c.prepareStatement("SELECT balance FROM bench_accounts WHERE id = ?")) {
                        ps.setInt(1, a);
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                            rs.getLong(1);
                        }
                    }
                    c.commit();
                } finally {
                    c.setAutoCommit(true);
                }
            }
            default -> throw new IllegalArgumentException(scenario);
        }
    }

    // ---- main ----

    public static void main(String[] a) throws Exception {
        String lib = a[0];
        for (int i = 1; i < a.length; i++) {
            int eq = a[i].indexOf('=');
            if (eq > 0) args.put(a[i].substring(0, eq), a[i].substring(eq + 1));
        }
        String host = arg("host", "127.0.0.1");
        int port = iarg("port", 23306);
        user = arg("user", "hydra");
        password = arg("password", "hydra");
        db = arg("db", "hydra");
        if (lib.equals("setup")) {
            setup(host, port);
            return;
        }
        String scenario = a[1];
        int seconds = iarg("seconds", 8), warmup = iarg("warmup", 3), conns = iarg("conns", 8), callers = iarg("callers", 32);
        String base = "jdbc:mysql://" + host + ":" + port + "/" + db + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String jdbcUrl = lib.equals("mysqlj-server") || lib.equals("mysqlj-dedicated") ? base + "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=256" : base + "&cachePrepStmts=true&prepStmtCacheSize=256&prepStmtCacheSqlLimit=2048";
        if (scenario.equals("insert") || scenario.equals("tx")) {
            try (Connection c = java.sql.DriverManager.getConnection(jdbcUrl, user, password); Statement s = c.createStatement()) {
                if (scenario.equals("insert")) s.execute("TRUNCATE bench_log");
                if (scenario.equals("tx")) s.execute("OPTIMIZE TABLE bench_accounts");
            }
        }
        List<Thread> threads = new ArrayList<>();
        AutoCloseable cleanup = () -> { };
        switch (lib) {
            case "hydrangea" -> {
                EventLoopGroup group = new EventLoopGroup(iarg("loops", conns));
                MyPool pool = MyPool.create(group, myConfig(host, port), conns).get(30, TimeUnit.SECONDS);
                Async op = hydrangeaOp(pool, scenario);
                for (int i = 0; i < callers; i++) {
                    int idx = i;
                    Runnable[] next = new Runnable[1];
                    next[0] = () -> {
                        if (stop.get()) return;
                        long t0 = System.nanoTime();
                        op.start().whenComplete((r, e) -> {
                            record(t0, e, idx);
                            next[0].run();
                        });
                    };
                    next[0].run();
                }
                cleanup = () -> {
                    io.hydrangea.Metrics.Snapshot snap = group.metrics().snapshot();
                    System.out.printf("LOOPS tasks=%d bytes_written=%d bytes_read=%d%n", snap.tasksExecuted(), snap.bytesWritten(), snap.bytesRead());
                    pool.close();
                    group.close();
                };
            }
            case "hydrangea-block" -> {
                EventLoopGroup group = new EventLoopGroup(iarg("loops", conns));
                MyPool pool = MyPool.create(group, myConfig(host, port), conns).get(30, TimeUnit.SECONDS);
                Async op = hydrangeaOp(pool, scenario);
                for (int i = 0; i < callers; i++) {
                    int idx = i;
                    threads.add(Thread.ofPlatform().start(() -> loop(() -> op.start().join(), idx)));
                }
                cleanup = () -> {
                    pool.close();
                    group.close();
                };
            }
            case "mysqlj", "mysqlj-server", "mysqlj-virtual" -> {
                HikariConfig hc = new HikariConfig();
                hc.setJdbcUrl(jdbcUrl);
                hc.setUsername(user);
                hc.setPassword(password);
                hc.setMaximumPoolSize(conns);
                hc.setMinimumIdle(conns);
                hc.setConnectionTimeout(120_000);
                HikariDataSource ds = new HikariDataSource(hc);
                String sql = sqlOf(scenario);
                boolean virtual = lib.endsWith("virtual");
                for (int i = 0; i < callers; i++) {
                    int idx = i;
                    Runnable body = () -> loop(() -> {
                        try (Connection c = ds.getConnection()) {
                            jdbcOp(c, scenario, sql);
                        }
                    }, idx);
                    threads.add(virtual ? Thread.ofVirtual().start(body) : Thread.ofPlatform().start(body));
                }
                cleanup = ds;
            }
            case "mysqlj-dedicated" -> {
                String sql = sqlOf(scenario);
                List<Connection> all = Collections.synchronizedList(new ArrayList<>());
                for (int i = 0; i < callers; i++) {
                    Connection c = java.sql.DriverManager.getConnection(jdbcUrl, user, password);
                    all.add(c);
                    int idx = i;
                    threads.add(Thread.ofPlatform().start(() -> loop(() -> jdbcOp(c, scenario, sql), idx)));
                }
                cleanup = () -> all.forEach(c -> {
                    try {
                        c.close();
                    } catch (Exception ignored) {
                    }
                });
            }
            default -> throw new IllegalArgumentException(lib);
        }
        Thread.sleep(warmup * 1000L);
        long cpu0 = OS.getProcessCpuTime();
        measuring.set(true);
        Thread.sleep(seconds * 1000L);
        measuring.set(false);
        long cpu = OS.getProcessCpuTime() - cpu0;
        long n = done.sum();
        int peak = ManagementFactory.getThreadMXBean().getPeakThreadCount();
        long rss = rssKb();
        Histogram all = new Histogram();
        synchronized (histograms) {
            for (Histogram h : histograms) all.add(h);
        }
        long[] pc = new long[callers];
        for (int i = 0; i < callers; i++) pc[i] = perCaller.get(i);
        java.util.Arrays.sort(pc);
        long pcMin = pc[0], pcMed = pc[callers / 2], pcMax = pc[callers - 1];
        double top10 = 0; // share of all completions made by the busiest 10% of callers
        for (int i = callers - Math.max(1, callers / 10); i < callers; i++) top10 += pc[i];
        top10 /= Math.max(1, n);
        stop.set(true);
        System.out.printf("RESULT lib=%s scenario=%s conns=%d callers=%d qps=%d err=%d p50_us=%d p99_us=%d cpu_us_per_query=%.1f threads_peak=%d rss_mb=%d caller_min=%d caller_med=%d caller_max=%d top10_share=%.3f%n",
                lib, scenario, conns, callers, n / seconds, errors.sum(), all.quantile(0.5) / 1000, all.quantile(0.99) / 1000,
                cpu / 1000.0 / Math.max(1, n), peak, rss / 1024, pcMin, pcMed, pcMax, top10);
        System.out.flush();
        try {
            cleanup.close();
        } catch (Exception ignored) {
        }
        System.out.flush();
        System.exit(0);
    }

    static void loop(Blocking op, int idx) {
        while (!stop.get()) {
            long t0 = System.nanoTime();
            Throwable err = null;
            try {
                op.run();
            } catch (Throwable t) {
                err = t;
                if (stop.get()) return;
            }
            record(t0, err, idx);
        }
    }

    static long rssKb() {
        try {                                                   // Linux: no need for ps (a slim container may not have it)
            for (String line : java.nio.file.Files.readAllLines(java.nio.file.Path.of("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.replaceAll("[^0-9]", ""));
                }
            }
        } catch (Exception ignored) {
            // not Linux
        }
        try {
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", String.valueOf(ProcessHandle.current().pid())).start();
            return Long.parseLong(new String(p.getInputStream().readAllBytes()).trim());
        } catch (Exception e) {
            return 0;
        }
    }

    static void setup(String host, int port) throws Exception {
        try (Connection c = java.sql.DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db + "?allowPublicKeyRetrieval=true&useSSL=false", user, password); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS bench_accounts");
            s.execute("DROP TABLE IF EXISTS bench_log");
            s.execute("CREATE TABLE bench_accounts (id int PRIMARY KEY, name varchar(64) NOT NULL, balance bigint NOT NULL) ENGINE=InnoDB");
            s.execute("SET SESSION cte_max_recursion_depth = 1000000");
            s.execute("INSERT INTO bench_accounts SELECT n, CONCAT('account number ', n), n * 10 FROM (WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < " + ACCOUNTS
                    + ") SELECT n FROM seq) t");
            s.execute("CREATE TABLE bench_log (id bigint AUTO_INCREMENT PRIMARY KEY, ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, msg varchar(100) NOT NULL) ENGINE=InnoDB");
            s.execute("ANALYZE TABLE bench_accounts");
            System.out.println("setup done: " + ACCOUNTS + " accounts");
        }
    }
}
