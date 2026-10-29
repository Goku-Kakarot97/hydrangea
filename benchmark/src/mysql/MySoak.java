package mysql;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.mysql.*;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A long run of a mixed load on the MySQL client, to see what a short test cannot: memory growing, the statement cache leaking prepared statements on the server, connections and
 * threads piling up, errors that appear only now and then. Workers (virtual threads) pick at random from: queries over {@code distinct} different SQL strings (more than the statement
 * cache holds, so it churns), inserts, batches inside transactions, streams, queries with a time limit that sometimes expires, transactions with and without a deliberate failure,
 * big rows. Every {@code report} seconds it prints heap in use after a GC, threads, the sessions on the server and the prepared statements a pool connection holds, and at the end
 * checks the numbers. The pool replaces its connections every 25 s and pings them, so that path runs too.
 * <pre>
 * java ... mysql.MySoak [seconds=120] [workers=32] [conns=8] [cache=64] [distinct=300] [host=] [port=] [report=15] [only=query,insert,batch,stream,timeout,tx,big]
 * </pre>
 */
public final class MySoak {

    public static void main(String[] a) throws Exception {
        Map<String, String> args = new HashMap<>();
        for (String s : a) {
            int eq = s.indexOf('=');
            args.put(s.substring(0, eq), s.substring(eq + 1));
        }
        int seconds = Integer.parseInt(args.getOrDefault("seconds", "120")), workers = Integer.parseInt(args.getOrDefault("workers", "32")), conns = Integer.parseInt(args.getOrDefault("conns", "8"));
        int cache = Integer.parseInt(args.getOrDefault("cache", "64")), distinct = Integer.parseInt(args.getOrDefault("distinct", "300")), report = Integer.parseInt(args.getOrDefault("report", "15"));
        Set<String> only = args.containsKey("only") ? new HashSet<>(Arrays.asList(args.get("only").split(","))) : null;
        String host = args.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(args.getOrDefault("port", "23306"));
        MyConfig config = MyConfig.of(host, port, "hydra", "hydra").withPassword("hydra").withAllowPublicKeyRetrieval(true).withStatementCacheSize(cache);
        AtomicBoolean stop = new AtomicBoolean();
        Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
        Map<String, AtomicLong> unexpected = new ConcurrentHashMap<>();
        AtomicLong strayKills = new AtomicLong();          // a KILL QUERY meant for a timed-out call hit the next one (error 1317): documented, counted apart
        try (EventLoopGroup group = new EventLoopGroup(conns)) {
            MyPoolOptions options = MyPoolOptions.defaults().withMaxLifetime(Duration.ofSeconds(25)).withPing(Duration.ofSeconds(5), Duration.ofSeconds(3));
            MyPool pool = MyPool.create(group, config, conns, options).get(30, TimeUnit.SECONDS);
            pool.query("CREATE TABLE IF NOT EXISTS soak (id BIGINT AUTO_INCREMENT PRIMARY KEY, k INT, v VARCHAR(100), b MEDIUMBLOB) ENGINE=InnoDB").get();
            pool.query("TRUNCATE soak").get();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                long seed = 1000 + w;
                threads.add(Thread.ofVirtual().start(() -> {
                    Random rnd = new Random(seed);
                    while (!stop.get()) {
                        String what = "?";
                        try {
                            int pick = rnd.nextInt(100);
                            String kind = pick < 40 ? "query" : pick < 55 ? "insert" : pick < 65 ? "batch" : pick < 75 ? "stream" : pick < 85 ? "timeout" : pick < 95 ? "tx" : "big";
                            if (only != null && !only.contains(kind)) {
                                continue;
                            }
                            what = kind;
                            switch (kind) {
                                case "query" -> {
                                    int d = rnd.nextInt(distinct);
                                    pool.query("SELECT ? + " + d + " AS r" + d, rnd.nextInt(1000)).get(30, TimeUnit.SECONDS);
                                }
                                case "insert" -> pool.query("INSERT INTO soak (k, v) VALUES (?, ?)", rnd.nextInt(100), "value " + rnd.nextInt()).get(30, TimeUnit.SECONDS);
                                case "batch" -> {
                                    List<Object[]> sets = new ArrayList<>();
                                    for (int i = 0; i < 100; i++) sets.add(new Object[]{rnd.nextInt(100), "batch " + i});
                                    pool.transaction(db -> db.executeBatch("INSERT INTO soak (k, v) VALUES (?, ?)", sets)).get(30, TimeUnit.SECONDS);
                                }
                                case "stream" -> {
                                    MyRowStream s = pool.stream(50, "SELECT id, v FROM soak WHERE k = ? ORDER BY id LIMIT 500", rnd.nextInt(100)).get(30, TimeUnit.SECONDS);
                                    for (List<MyRow> piece = s.next().get(30, TimeUnit.SECONDS); !piece.isEmpty(); piece = s.next().get(30, TimeUnit.SECONDS)) {
                                        for (MyRow r : piece) r.getString(1);
                                    }
                                }
                                case "timeout" -> {
                                    try {
                                        pool.query(Duration.ofMillis(50), "SELECT SLEEP(?)", rnd.nextInt(3) * 0.05).get(30, TimeUnit.SECONDS);
                                    } catch (ExecutionException e) {
                                        if (!(e.getCause() instanceof MyException me && (me.isTimeout() || me.errorCode() == 1317))) throw e;
                                    }
                                }
                                case "tx" -> {
                                    boolean fail = rnd.nextInt(4) == 0;
                                    try {
                                        pool.<MyResult>transactionWithRetry(MyTxOptions.defaults(), 3, db -> db.query("INSERT INTO soak (k, v) VALUES (?, 'tx')", rnd.nextInt(100))
                                                .thenCompose(x -> fail ? db.query("SELECT * FROM soak_no_such_table") : db.query("SELECT COUNT(*) FROM soak WHERE k = 1"))).get(30, TimeUnit.SECONDS);
                                    } catch (ExecutionException e) {
                                        if (!(fail && e.getCause() instanceof MyException me && me.errorCode() == 1146)) throw e;
                                    }
                                }
                                default -> {
                                    byte[] blob = new byte[20_000 + rnd.nextInt(100_000)];
                                    rnd.nextBytes(blob);
                                    pool.query("INSERT INTO soak (k, v, b) VALUES (?, ?, ?)", rnd.nextInt(100), "big", blob).get(30, TimeUnit.SECONDS);
                                    pool.query("SELECT b FROM soak WHERE k = ? AND b IS NOT NULL ORDER BY id DESC LIMIT 3", rnd.nextInt(100)).get(30, TimeUnit.SECONDS);
                                }
                            }
                            counts.computeIfAbsent(what, k -> new AtomicLong()).incrementAndGet();
                        } catch (Throwable t) {
                            Throwable c = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
                            if (c instanceof MyException me && me.errorCode() == 1317) {
                                strayKills.incrementAndGet();
                                continue;
                            }
                            String key = what + ": " + c.getClass().getSimpleName() + ": " + c.getMessage();
                            unexpected.computeIfAbsent(key.length() > 200 ? key.substring(0, 200) : key, k -> new AtomicLong()).incrementAndGet();
                        }
                    }
                }));
            }
            Runtime rt = Runtime.getRuntime();
            List<Long> heaps = new ArrayList<>();
            long start = System.nanoTime();
            while ((System.nanoTime() - start) / 1_000_000_000L < seconds) {
                Thread.sleep(report * 1000L);
                System.gc();
                long heap = (rt.totalMemory() - rt.freeMemory()) >> 20;
                heaps.add(heap);
                int sessions;
                try (MyClient probe = MyClient.connect(group, MyConfig.of(host, port, "hydra", "hydra").withPassword("hydra").withAllowPublicKeyRetrieval(true)).get(10, TimeUnit.SECONDS)) {
                    sessions = probe.query("SELECT COUNT(*) FROM information_schema.processlist WHERE user = 'hydra'").get(10, TimeUnit.SECONDS).first().getInt(0);
                }
                long ops = counts.values().stream().mapToLong(AtomicLong::get).sum();
                System.out.printf("t=%3ds heap=%4d MB threads=%3d sessions=%2d ops=%d unexpected=%d%n", (System.nanoTime() - start) / 1_000_000_000L, heap, ManagementFactory.getThreadMXBean().getThreadCount(),
                        sessions, ops, unexpected.values().stream().mapToLong(AtomicLong::get).sum());
            }
            stop.set(true);
            for (Thread t : threads) t.join(40_000);
            int worst = 0;
            for (int i = 0; i < conns * 3; i++) {
                try {
                    worst = Math.max(worst, pool.query("SELECT COUNT(*) FROM performance_schema.prepared_statements_instances WHERE OWNER_THREAD_ID = PS_CURRENT_THREAD_ID()").get(10, TimeUnit.SECONDS).first().getInt(0));
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof MyException me && me.errorCode() == 1317)) throw e;     // a kill meant for a timed-out call of the run, landing late: ask again
                    i--;
                }
            }
            long ops = counts.values().stream().mapToLong(AtomicLong::get).sum();
            double first = heaps.subList(0, Math.max(1, heaps.size() / 3)).stream().mapToLong(Long::longValue).average().orElse(0);
            double last = heaps.subList(heaps.size() - Math.max(1, heaps.size() / 3), heaps.size()).stream().mapToLong(Long::longValue).average().orElse(0);
            System.out.println("operations: " + new TreeMap<>(counts));
            System.out.println("stray kills that hit the next call: " + strayKills.get());
            System.out.println("unexpected: " + (unexpected.isEmpty() ? "none" : new TreeMap<>(unexpected)));
            System.out.printf("RESULT seconds=%d workers=%d conns=%d cache=%d distinct=%d ops=%d ops_per_s=%d heap_first_third_mb=%.0f heap_last_third_mb=%.0f max_prepared_on_a_connection=%d stray_kills=%d unexpected=%d%n",
                    seconds, workers, conns, cache, distinct, ops, ops / seconds, first, last, worst, strayKills.get(), unexpected.values().stream().mapToLong(AtomicLong::get).sum());
            pool.query("DROP TABLE soak").get();
            pool.close();
        }
        System.exit(0);
    }
}
