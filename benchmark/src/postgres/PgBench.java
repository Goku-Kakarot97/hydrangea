package postgres;

import com.sun.management.OperatingSystemMXBean;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.postgres.PgConfig;
import io.hydrangea.postgres.PgPool;
import io.hydrangea.postgres.PgResult;
import io.hydrangea.postgres.PgRow;

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
 * Hydrangea's PostgreSQL client against pgjdbc on the same server. One library, one scenario per JVM (see {@code benchmark/pg-bench.sh}); the last
 * line printed is {@code RESULT key=value ...}.
 * <pre>
 * java ... postgres.PgBench setup                                                (creates and fills the tables)
 * java ... postgres.PgBench LIB SCENARIO [key=value ...]    keys: host= port= seconds= warmup= conns= callers= loops=
 *                                                        (loops: event loops of the Hydrangea pool; default = conns, one loop for each connection, the shape of a thread per connection)
 * LIB:      hydrangea            PgPool, callers are future chains (no thread per caller)
 *           hydrangea-block      PgPool, one platform thread per caller doing query().join()
 *           pgjdbc               HikariCP + pgjdbc, one platform thread per caller
 *           pgjdbc-virtual       HikariCP + pgjdbc, one virtual thread per caller
 *           pgjdbc-dedicated     pgjdbc, one connection per caller thread, no pool (callers = connections)
 * binary=off        Hydrangea without binary parameters and results (on by default)
 * tls=/path/ca.pem   TLS (both libraries verify the server certificate against the host name)
 * SCENARIO: select1 | point | rows | insert | tx      (these need the tables of {@code setup}; insert and tx write)
 *           calc | series                           read-only, no tables: the same shapes as point / rows computed from generate_series, so they can
 *                                                    run on a database where nothing may be created (user=, password=, db= pick the login)
 * </pre>
 * {@code conns} is the pool size, {@code callers} the number of concurrent callers (a caller sends the next query when the previous one is answered).
 * Defaults of each library are kept: pgjdbc switches to server-side prepared statements after 5 uses and uses binary format for some types;
 * Hydrangea prepares on first use and uses text format.
 */
public final class PgBench {

    static Map<String, String> args = new HashMap<>();

    static String arg(String k, String d) {
        return args.getOrDefault(k, d);
    }

    static int iarg(String k, int d) {
        return Integer.parseInt(arg(k, Integer.toString(d)));
    }

    static String user, password, db;

    /** {@code tls=/path/ca.pem}: TLS on, trusting that CA, the server certificate verified against the host name (pgjdbc: sslmode=verify-full). */
    static javax.net.ssl.SSLContext tlsContext() throws Exception {
        String pem = arg("tls", null);
        if (pem == null) return null;
        java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        ks.load(null, null);
        try (java.io.InputStream in = new java.io.FileInputStream(pem)) {
            ks.setCertificateEntry("ca", java.security.cert.CertificateFactory.getInstance("X.509").generateCertificate(in));
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    static javax.net.ssl.SSLContext tlsContextFor(String pem) throws Exception {
        args.put("tls", pem);
        return tlsContext();
    }

    static PgConfig pgConfig(String host, int port) throws Exception {
        PgConfig c = PgConfig.of(host, port, user, db).withPassword(password).withBinaryTransfer(!arg("binary", "on").equals("off"));
        javax.net.ssl.SSLContext tls = tlsContext();
        return tls == null ? c : c.withTls(tls);
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
            case "calc" -> "SELECT ?::int, 'account number ' || ?::int, ?::int::bigint * 10";
            case "series" -> "SELECT g, 'account number ' || g, g::bigint * 10 FROM generate_series(?::int, ?::int) g";
            default -> throw new IllegalArgumentException(scenario);
        };
    }

    static String pgSql(String sql) { // ? -> $1, $2...
        StringBuilder sb = new StringBuilder();
        int n = 0;
        for (char ch : sql.toCharArray()) sb.append(ch == '?' ? "$" + (++n) : String.valueOf(ch));
        return sb.toString();
    }

    static int id() {
        return 1 + ThreadLocalRandom.current().nextInt(ACCOUNTS - 100);
    }

    static Async hydrangeaOp(PgPool pool, String scenario) {
        String sql = pgSql(sqlOf(scenario));
        return switch (scenario) {
            case "select1" -> () -> pool.query(sql);
            case "point" -> () -> pool.query(sql, id()).thenApply(r -> {
                PgRow row = r.first();
                return row.getInt(0) + row.getString(1).length() + row.getLong(2);
            });
            case "rows" -> () -> {
                int a = id();
                return pool.query(sql, a, a + 99).thenApply(r -> {
                    long sum = 0;
                    for (PgRow row : r.rows()) sum += row.getInt(0) + row.getString(1).length() + row.getLong(2);
                    return sum;
                });
            };
            case "insert" -> () -> pool.query(sql, "message " + ThreadLocalRandom.current().nextInt());
            case "calc" -> () -> {
                int a = id();
                return pool.query(sql, a, a, a).thenApply(r -> {
                    PgRow row = r.first();
                    return row.getInt(0) + row.getString(1).length() + row.getLong(2);
                });
            };
            case "series" -> () -> {
                int a = id();
                return pool.query(sql, a, a + 99).thenApply(r -> {
                    long sum = 0;
                    for (PgRow row : r.rows()) sum += row.getInt(0) + row.getString(1).length() + row.getLong(2);
                    return sum;
                });
            };
            case "tx" -> () -> {
                int a = id();
                return pool.transaction(db -> db.query(sql, a).thenCompose(x -> db.query("SELECT balance FROM bench_accounts WHERE id = $1", a)));
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
            case "calc" -> {
                try (PreparedStatement ps = c.prepareStatement(sql)) {
                    int a = id();
                    ps.setInt(1, a);
                    ps.setInt(2, a);
                    ps.setInt(3, a);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        long x = rs.getInt(1) + rs.getString(2).length() + rs.getLong(3);
                    }
                }
            }
            case "rows", "series" -> {
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
        int port = iarg("port", 25432);
        user = arg("user", "hydra");
        password = arg("password", "hydra");
        db = arg("db", "hydra");
        if (lib.equals("setup")) {
            setup(host, port);
            return;
        }
        String scenario = a[1];
        int seconds = iarg("seconds", 8), warmup = iarg("warmup", 3), conns = iarg("conns", 8), callers = iarg("callers", 32);
        String jdbcUrl = "jdbc:postgresql://" + host + ":" + port + "/" + db
                + (arg("tls", null) != null ? "?ssl=true&sslmode=verify-full&sslrootcert=" + arg("tls", null) : "");
        if (scenario.equals("insert") || scenario.equals("tx")) {
            try (Connection c = java.sql.DriverManager.getConnection(jdbcUrl, user, password); Statement s = c.createStatement()) {
                if (scenario.equals("insert")) s.execute("TRUNCATE bench_log");
                if (scenario.equals("tx")) s.execute("VACUUM bench_accounts");
            }
        }
        List<Thread> threads = new ArrayList<>();
        AutoCloseable cleanup = () -> { };
        switch (lib) {
            case "hydrangea" -> {
                EventLoopGroup group = new EventLoopGroup(iarg("loops", conns));
                PgPool pool = PgPool.create(group, pgConfig(host, port), conns, arg("acquire", "").equals("none") ? io.hydrangea.postgres.PgPoolOptions.defaults().withAcquireTimeout(null) : io.hydrangea.postgres.PgPoolOptions.defaults(), null).get(30, TimeUnit.SECONDS);
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
                    pool.close();
                    group.close();
                };
            }
            case "hydrangea-block" -> {
                EventLoopGroup group = new EventLoopGroup(iarg("loops", conns));
                PgPool pool = PgPool.create(group, pgConfig(host, port), conns, arg("acquire", "").equals("none") ? io.hydrangea.postgres.PgPoolOptions.defaults().withAcquireTimeout(null) : io.hydrangea.postgres.PgPoolOptions.defaults(), null).get(30, TimeUnit.SECONDS);
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
            case "pgjdbc", "pgjdbc-virtual" -> {
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
            case "pgjdbc-dedicated" -> {
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
        System.out.printf("RESULT binary=%s tls=%s lib=%s scenario=%s conns=%d callers=%d qps=%d err=%d p50_us=%d p99_us=%d cpu_us_per_query=%.1f threads_peak=%d rss_mb=%d caller_min=%d caller_med=%d caller_max=%d top10_share=%.3f%n",
                arg("binary", "on"), arg("tls", null) != null ? "on" : "off", lib, scenario, conns, callers, n / seconds, errors.sum(), all.quantile(0.5) / 1000, all.quantile(0.99) / 1000,
                cpu / 1000.0 / Math.max(1, n), peak, rss / 1024, pcMin, pcMed, pcMax, top10);
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
        try (Connection c = java.sql.DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/hydra", "hydra", "hydra"); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS bench_accounts");
            s.execute("DROP TABLE IF EXISTS bench_log");
            s.execute("CREATE TABLE bench_accounts (id int PRIMARY KEY, name text NOT NULL, balance bigint NOT NULL)");
            s.execute("INSERT INTO bench_accounts SELECT g, 'account number ' || g, g * 10 FROM generate_series(1, " + ACCOUNTS + ") g");
            s.execute("CREATE TABLE bench_log (id bigserial PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now(), msg text NOT NULL)");
            s.execute("VACUUM ANALYZE bench_accounts");
            System.out.println("setup done: " + ACCOUNTS + " accounts");
        }
    }
}
