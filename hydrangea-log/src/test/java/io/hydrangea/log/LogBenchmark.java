package io.hydrangea.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.logging.FileHandler;
import java.util.logging.SimpleFormatter;

/**
 * A quick measurement, not a benchmark suite: {@code java -cp target/classes:target/test-classes io.hydrangea.log.LogBenchmark}.
 * Compares a disabled call, an enabled call into the async buffer with a file behind it, the same synchronously, the JDK's
 * {@code java.util.logging} to a file, and the same from several threads.
 */
public final class LogBenchmark {

    private static final int WARMUP = 200_000;
    private static final int N = 2_000_000;

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("hydrangea-log-bench");
        try {
            Logger log = Logger.get("bench");

            Log.configure().async(false).level(Level.WARN).stage(Appender.memory()).apply();
            run("disabled call (level off)", 1, () -> log.info("value {} and {}", 1, 2));

            Path f1 = dir.resolve("async.log");
            Log.configure().async(true).buffer(1 << 16).level(Level.INFO).file(f1, 0, 0).apply();
            run("async -> file, 1 thread", 1, () -> log.info("value {} and {}", 1, 2));
            run("async -> file, 4 threads", 4, () -> log.info("value {} and {}", 1, 2));
            run("async -> file, structured, 1 thread", 1, () -> log.at(Level.INFO).kv("a", 1).kv("b", "two").log("value"));
            Log.configure().async(true).buffer(1 << 16).level(Level.INFO).format(LogFormat.json()).file(f1, 0, 0).apply();
            run("async -> file JSON, 1 thread", 1, () -> log.info("value {} and {}", 1, 2));

            Path f2 = dir.resolve("sync.log");
            Log.configure().async(false).level(Level.INFO).file(f2, 0, 0).apply();
            run("sync -> file, 1 thread", 1, () -> log.info("value {} and {}", 1, 2));
            run("sync -> file, 4 threads", 4, () -> log.info("value {} and {}", 1, 2));

            Log.configure().async(true).level(Level.INFO).file(f1, 0, 0).apply();
            System.Logger sys = System.getLogger("bench.system");
            run("System.Logger bridge, async -> file", 1, () -> sys.log(System.Logger.Level.INFO, "value {0} and {1}", 1, 2));

            Log.configure().async(false).level(Level.WARN).stage(Appender.memory()).apply();
            java.util.logging.Logger jul = java.util.logging.Logger.getLogger("bench.jul");
            jul.setUseParentHandlers(false);
            FileHandler handler = new FileHandler(dir.resolve("jul.log").toString());
            handler.setFormatter(new SimpleFormatter());
            jul.addHandler(handler);
            run("java.util.logging -> file, 1 thread", 1, () -> jul.log(java.util.logging.Level.INFO, "value {0} and {1}", new Object[] {1, 2}));
            run("java.util.logging -> file, 4 threads", 4, () -> jul.log(java.util.logging.Level.INFO, "value {0} and {1}", new Object[] {1, 2}));
            handler.close();
            System.out.println("files: " + Files.size(f1) / 1_000_000 + " MB async, " + Files.size(f2) / 1_000_000 + " MB sync, " + Files.size(dir.resolve("jul.log")) / 1_000_000 + " MB jul");
        } finally {
            Log.shutdown();
            try (var files = Files.walk(dir)) {
                files.sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    private static void run(String name, int threads, Runnable call) throws InterruptedException {
        loop(WARMUP, 1, call);
        Log.flush();
        long t0 = System.nanoTime();
        loop(N, threads, call);
        Log.flush();
        long t1 = System.nanoTime();
        double perCall = (double) (t1 - t0) / N;
        System.out.printf("%-42s %8.0f ns/call  %10.0f calls/s%n", name, perCall, 1e9 / perCall);
    }

    private static void loop(int n, int threads, Runnable call) throws InterruptedException {
        if (threads == 1) {
            for (int i = 0; i < n; i++) {
                call.run();
            }
            return;
        }
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                for (int i = 0; i < n / threads; i++) {
                    call.run();
                }
                done.countDown();
            }).start();
        }
        done.await();
    }
}
