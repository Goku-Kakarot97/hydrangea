package io.hydrangea;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsSinkTest {

    /** Remembers what was registered, keyed by "name{tags}". */
    private static final class Recording implements MetricsSink {
        final Map<String, DoubleSupplier> pull = new HashMap<>();
        final List<String> units = new ArrayList<>();

        private static String key(String name, String[] tags) {
            return name + String.join(",", tags);
        }

        @Override
        public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
            pull.put(key(name, tags), total);
            units.add(name + ":" + unit);
        }

        @Override
        public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
            pull.put(key(name, tags), value);
            units.add(name + ":" + unit);
        }

        @Override
        public Counter counter(String name, String unit, String... tags) {
            return NOOP.counter(name, unit, tags);
        }

        @Override
        public Timer timer(String name, String... tags) {
            return NOOP.timer(name, tags);
        }
    }

    @Test
    void bindRegistersEveryNumberAndReadsThemLive() {
        Metrics m = new Metrics();
        Recording sink = new Recording();
        m.bindTo(sink);

        assertEquals(0.0, sink.pull.get("connections.rejected").getAsDouble());
        m.recordRejected();
        m.recordHttpResponse(404);
        m.recordHttpResponse(200);
        m.recordHttpTimeout();
        assertEquals(1.0, sink.pull.get("connections.rejected").getAsDouble());
        assertEquals(1.0, sink.pull.get("http.responses" + "class,4xx").getAsDouble());
        assertEquals(1.0, sink.pull.get("http.responses" + "class,2xx").getAsDouble());
        assertEquals(1.0, sink.pull.get("http.timeouts").getAsDouble());
        assertTrue(sink.units.contains("network.read:bytes"));
        assertTrue(sink.units.contains("loop.max.block:seconds"));
        assertEquals(26, sink.pull.size(), "4 connections, 2 network, 3 loop, 3 udp, 1 requests, 4 responses, 1 timeouts, 1 active, and the HTTP duration: count, sum, 5 quantiles");
    }

    @Test
    void noopSinkAcceptsEverything() {
        new Metrics().bindTo(MetricsSink.NOOP);
        MetricsSink.NOOP.counter("x", null).increment();
        MetricsSink.NOOP.timer("y").record(1);
    }
}
