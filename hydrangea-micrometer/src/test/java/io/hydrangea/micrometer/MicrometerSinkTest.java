package io.hydrangea.micrometer;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.MetricsSink;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicrometerSinkTest {

    @Test
    void groupMetricsAreReadLiveAtScrapeTime() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            group.metrics().bindTo(new MicrometerSink(registry, Tags.of("service", "t")));

            group.metrics().recordRejected();
            group.metrics().recordRejected();
            group.metrics().recordHttpResponse(200);
            group.metrics().recordHttpResponse(503);
            group.metrics().recordHttpTimeout();
            CompletableFuture<Void> ran = new CompletableFuture<>();
            group.next().execute(() -> ran.complete(null));
            ran.get(5, TimeUnit.SECONDS);
            System.gc();   // the registry holds function meters weakly unless told otherwise; the sink is already garbage here

            assertEquals(2.0, registry.get("hydrangea.connections.rejected").tag("service", "t").functionCounter().count());
            assertEquals(1.0, registry.get("hydrangea.http.responses").tag("class", "2xx").functionCounter().count());
            assertEquals(1.0, registry.get("hydrangea.http.responses").tag("class", "5xx").functionCounter().count());
            assertEquals(0.0, registry.get("hydrangea.http.responses").tag("class", "4xx").functionCounter().count());
            assertEquals(1.0, registry.get("hydrangea.http.timeouts").functionCounter().count());
            assertTrue(registry.get("hydrangea.loop.tasks").functionCounter().count() >= 1);
            assertEquals(0.0, registry.get("hydrangea.connections.active").gauge().value());
            assertTrue(registry.get("hydrangea.loop.max.block").gauge().value() >= 0);

            group.metrics().recordHttpResponse(404);   // later changes show up without binding again
            assertEquals(1.0, registry.get("hydrangea.http.responses").tag("class", "4xx").functionCounter().count());
        }
    }

    @Test
    void pushMetersGoToTheRegistry() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        MicrometerSink sink = new MicrometerSink(registry);
        MetricsSink.Counter errors = sink.counter("redis.errors", null, "kind", "timeout");
        MetricsSink.Timer latency = sink.timer("redis.command", "command", "GET");
        errors.increment();
        errors.add(2);
        latency.record(TimeUnit.MILLISECONDS.toNanos(5));
        latency.record(TimeUnit.MILLISECONDS.toNanos(15));

        assertEquals(3.0, registry.get("hydrangea.redis.errors").tag("kind", "timeout").counter().count());
        var timer = registry.get("hydrangea.redis.command").tag("command", "GET").timer();
        assertEquals(2, timer.count());
        assertEquals(20.0, timer.totalTime(TimeUnit.MILLISECONDS), 0.001);
    }

    @Test
    void prometheusRegistryRendersThem() {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            group.metrics().bindTo(new MicrometerSink(registry));
            String text = registry.scrape();
            assertTrue(text.contains("hydrangea_connections_opened_total"), text);
            assertTrue(text.contains("hydrangea_http_responses_total{class=\"2xx\""), text);
            assertTrue(text.contains("hydrangea_loop_max_block_seconds"), text);
            assertTrue(text.contains("hydrangea_network_read_bytes_total"), text);
        }
    }

    @Test
    void latencyShowsUpAsCountSumAndQuantiles() {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            group.metrics().bindTo(new MicrometerSink(registry, Tags.of("service", "t")));
            io.hydrangea.Latency l = group.metrics().latency("redis.command.duration");   // created after the sink was bound
            for (int i = 0; i < 10; i++) {
                l.record(TimeUnit.MILLISECONDS.toNanos(2));
            }
            String text = registry.scrape();
            assertTrue(text.contains("hydrangea_redis_command_duration_count_total{service=\"t\"} 10.0"), text);
            assertTrue(text.contains("hydrangea_redis_command_duration_sum_seconds_total{service=\"t\"} 0.02"), text);
            assertTrue(text.contains("hydrangea_redis_command_duration_seconds{quantile=\"0.99\",service=\"t\"}"), text);
            assertTrue(text.contains("hydrangea_http_server_duration_seconds{quantile=\"0.5\""), text);
        }
    }
}
