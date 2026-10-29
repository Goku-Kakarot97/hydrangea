package io.hydrangea;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

/** A {@link MetricsSink} that keeps what was registered so a test can read the numbers: key is {@code name} or {@code name{tag,value,...}}. */
public final class CaptureSink implements MetricsSink {

    public final Map<String, DoubleSupplier> meters = new ConcurrentHashMap<>();

    private static String key(String name, String[] tags) {
        return tags.length == 0 ? name : name + "{" + String.join(",", tags) + "}";
    }

    @Override
    public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
        meters.put(key(name, tags), total);
    }

    @Override
    public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
        meters.put(key(name, tags), value);
    }

    @Override
    public Counter counter(String name, String unit, String... tags) {
        return NOOP.counter(name, unit, tags);
    }

    @Override
    public Timer timer(String name, String... tags) {
        return NOOP.timer(name, tags);
    }

    public double get(String key) {
        DoubleSupplier s = meters.get(key);
        if (s == null) {
            throw new AssertionError("no meter " + key + "; have " + meters.keySet());
        }
        return s.getAsDouble();
    }

    public static void eventually(BooleanSupplier ok, String what) throws InterruptedException {
        long end = System.nanoTime() + 5_000_000_000L;
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > end) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }
}
