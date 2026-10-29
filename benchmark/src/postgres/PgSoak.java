package postgres;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.postgres.*;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A long run of a mixed load, to see what a short test cannot: memory growing, the statement cache leaking prepared statements on the server, connections and threads piling up, errors
 * that appear only now and then. Workers (virtual threads) pick at random from: queries over {@code distinct} different SQL strings (more than the statement cache holds, so it churns),
 * batches, small COPYs, streams, queries with a time limit that sometimes expires, transactions with and without a deliberate failure, arrays. Every {@code report} seconds it prints heap in use
 * after a GC, threads, the pool's connections as the server sees them and the prepared statements the server holds, and at the end checks the numbers.
 * <pre>
 * java ... postgres.PgSoak [seconds=120] [workers=32] [conns=8] [cache=64] [distinct=300] [host=] [port=] [report=15] [only=query,insert,batch,copy,stream,timeout,tx,array]
 * </pre>
 */
public final class PgSoak {

    public static void main(String[] a) throws Exception {
        Map<String, String> args = new HashMap<>();
        for (String s : a) {
            int eq = s.indexOf('=');
            args.put(s.substring(0, eq), s.substring(eq + 1));
        }
        int seconds = Integer.parseInt(args.getOrDefault("seconds", "120")), workers = Integer.parseInt(args.getOrDefault("workers", "32")), conns = Integer.parseInt(args.getOrDefault("conns", "8"));
        int cache = Integer.parseInt(args.getOrDefault("cache", "64")), distinct = Integer.parseInt(args.getOrDefault("distinct", "300")), report = Integer.parseInt(args.getOrDefault("report", "15"));
        Set<String> only = args.containsKey("only") ? new HashSet<>(Arrays.asList(args.get("only").split(","))) : null;      // e.g. only=query,tx to run just those kinds
        String host = args.getOrDefault("host", "127.0.0.1");
        int port = Integer.parseInt(args.getOrDefault("port", "25432"));
        PgConfig config = PgConfig.of(host, port, "hydra", "hydra").withPassword("hydra").withStatementCacheSize(cache);
        AtomicBoolean stop = new AtomicBoolean();
        Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
        Map<String, AtomicLong> unexpected = new ConcurrentHashMap<>();
        AtomicLong strayCancels = new AtomicLong();       // a cancel meant for a timed-out query hit another one that could not be retried (a COPY, a stream, a transaction): documented, counted apart
        try (EventLoopGroup group = new EventLoopGroup(2)) {
            PgPoolOptions options = PgPoolOptions.defaults().withMaxLifetime(Duration.ofSeconds(25)).withPing(Duration.ofSeconds(5), Duration.ofSeconds(3));
            PgPool pool = PgPool.create(group, config, conns, options, null).get(30, TimeUnit.SECONDS);
            pool.execute("CREATE TABLE IF NOT EXISTS soak (id serial PRIMARY KEY, k int, v text, tags text[]); TRUNCATE soak").get();
            List<Thread> threads = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                long seed = 1000 + w;
                threads.add(Thread.ofVirtual().start(() -> {
                    Random rnd = new Random(seed);
                    while (!stop.get()) {
                        String what = "?";
                        try {
                            int pick = rnd.nextInt(100);
                            String kind = pick < 40 ? "query" : pick < 55 ? "insert" : pick < 65 ? "batch" : pick < 72 ? "copy" : pick < 80 ? "stream" : pick < 87 ? "timeout" : pick < 95 ? "tx" : "array";
                            if (only != null && !only.contains(kind)) {
                                continue;
                            }
                            if (pick < 40) {
                                what = "query";
                                int d = rnd.nextInt(distinct);
                                pool.query("SELECT $1::int + " + d + " AS r" + d, rnd.nextInt(1000)).get(30, TimeUnit.SECONDS);
                            } else if (pick < 55) {
                                what = "insert";
                                pool.query("INSERT INTO soak (k, v, tags) VALUES ($1, $2, $3)", rnd.nextInt(100), "value " + rnd.nextInt(), List.of("a", "b c", "d\"e")).get(30, TimeUnit.SECONDS);
                            } else if (pick < 65) {
                                what = "batch";
                                List<Object[]> sets = new ArrayList<>();
                                for (int i = 0; i < 100; i++) sets.add(new Object[]{rnd.nextInt(100), "batch " + i, List.of("x")});
                                pool.executeBatch("INSERT INTO soak (k, v, tags) VALUES ($1, $2, $3)", sets).get(30, TimeUnit.SECONDS);
                            } else if (pick < 72) {
                                what = "copy";
                                pool.copyIn("COPY soak (k, v) FROM STDIN", "1\tcopy a\n2\tcopy b\n".getBytes()).get(30, TimeUnit.SECONDS);
                            } else if (pick < 80) {
                                what = "stream";
                                pool.stream(50, "SELECT id, v FROM soak WHERE k = $1 ORDER BY id LIMIT 500", rnd.nextInt(100)).get(30, TimeUnit.SECONDS).forEach(r -> { }).get(30, TimeUnit.SECONDS);
                            } else if (pick < 87) {
                                what = "timeout";
                                try {
                                    pool.query(Duration.ofMillis(50), "SELECT pg_sleep($1::float8)", rnd.nextInt(3) * 0.05).get(30, TimeUnit.SECONDS);
                                } catch (ExecutionException e) {
                                    if (!(e.getCause() instanceof PgException pe && (pe.isTimeout() || pe.retriable()))) throw e;     // a timeout, or a stray cancel: both expected here
                                }
                            } else if (pick < 95) {
                                what = "tx";
                                boolean fail = rnd.nextInt(4) == 0;
                                try {
                                    pool.<PgResult>transactionWithRetry(TxOptions.defaults(), 3, db -> db.query("INSERT INTO soak (k, v) VALUES ($1, 'tx')", rnd.nextInt(100))
                                            .thenCompose(x -> fail ? db.query("SELECT 1/0") : db.query("SELECT count(*) FROM soak WHERE k = 1"))).get(30, TimeUnit.SECONDS);
                                } catch (ExecutionException e) {
                                    if (!(fail && e.getCause() instanceof PgException pe && "22012".equals(pe.sqlState()))) throw e;
                                }
                            } else {
                                what = "array";
                                pool.query("SELECT count(*) FROM soak WHERE k = ANY($1)", List.of(1, 2, 3, rnd.nextInt(100))).get(30, TimeUnit.SECONDS);
                            }
                            counts.computeIfAbsent(what, k -> new AtomicLong()).incrementAndGet();
                        } catch (Throwable t) {
                            Throwable c = t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
                            if (c instanceof PgException pe && pe.retriable() && "57014".equals(pe.sqlState())) {
                                strayCancels.incrementAndGet();
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
            int maxPrepared = 0;
            while ((System.nanoTime() - start) / 1_000_000_000L < seconds) {
                Thread.sleep(report * 1000L);
                System.gc();
                long heap = (rt.totalMemory() - rt.freeMemory()) >> 20;
                heaps.add(heap);
                int prepared = 0, sessions = 0;
                try (PgClient probe = PgClient.connect(group, PgConfig.of(host, port, "hydra", "hydra").withPassword("hydra")).get(10, TimeUnit.SECONDS)) {
                    sessions = probe.query("SELECT count(*) FROM pg_stat_activity WHERE datname = 'hydra'").get(10, TimeUnit.SECONDS).first().getInt(0);
                    prepared = probe.query("SELECT coalesce(max(c), 0) FROM (SELECT count(*) c FROM pg_prepared_statements) x").get(10, TimeUnit.SECONDS).first().getInt(0);
                }
                long ops = counts.values().stream().mapToLong(AtomicLong::get).sum();
                System.out.printf("t=%3ds heap=%4d MB threads=%3d sessions=%2d ops=%d unexpected=%d%n", (System.nanoTime() - start) / 1_000_000_000L, heap, ManagementFactory.getThreadMXBean().getThreadCount(),
                        sessions, ops, unexpected.values().stream().mapToLong(AtomicLong::get).sum());
            }
            stop.set(true);
            for (Thread t : threads) t.join(40_000);
            // each pool connection holds at most `cache` prepared statements (+ the transient unnamed one): measure on a connection that did the same work
            int worst = 0;
            for (int i = 0; i < conns * 3; i++) {
                worst = Math.max(worst, pool.query("SELECT count(*) FROM pg_prepared_statements").get(10, TimeUnit.SECONDS).first().getInt(0));
            }
            maxPrepared = worst;
            long ops = counts.values().stream().mapToLong(AtomicLong::get).sum();
            double first = heaps.subList(0, Math.max(1, heaps.size() / 3)).stream().mapToLong(Long::longValue).average().orElse(0);
            double last = heaps.subList(heaps.size() - Math.max(1, heaps.size() / 3), heaps.size()).stream().mapToLong(Long::longValue).average().orElse(0);
            System.out.println("operations: " + new TreeMap<>(counts));
            System.out.println("stray cancels that reached a COPY, a stream or a transaction: " + strayCancels.get());
        System.out.println("unexpected: " + (unexpected.isEmpty() ? "none" : new TreeMap<>(unexpected)));
            System.out.printf("RESULT seconds=%d workers=%d conns=%d cache=%d distinct=%d ops=%d ops_per_s=%d heap_first_third_mb=%.0f heap_last_third_mb=%.0f max_prepared_on_a_connection=%d stray_cancels=%d unexpected=%d%n",
                    seconds, workers, conns, cache, distinct, ops, ops / seconds, first, last, maxPrepared, strayCancels.get(), unexpected.values().stream().mapToLong(AtomicLong::get).sum());
            pool.execute("DROP TABLE soak").get();
            pool.close();
        }
        System.exit(0);
    }
}
