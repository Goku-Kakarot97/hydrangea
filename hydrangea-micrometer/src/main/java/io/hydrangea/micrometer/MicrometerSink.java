package io.hydrangea.micrometer;

import io.hydrangea.MetricsSink;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

/**
 * A {@link MetricsSink} that writes to a Micrometer {@link MeterRegistry}, so the numbers reach Prometheus, OTLP, Datadog, JMX ...
 * through whichever registry you use.
 *
 * <pre>{@code
 * group.metrics().bindTo(new MicrometerSink(registry));
 * group.metrics().bindTo(new MicrometerSink(registry, Tags.of("service", "api")));   // extra tags on every meter
 * }</pre>
 *
 * Every name gets the prefix {@code hydrangea.} (a Prometheus registry shows {@code hydrangea_connections_opened_total}).
 * Timers are plain Micrometer timers: configure percentiles or histograms with a {@code MeterFilter} on the registry.
 */
public final class MicrometerSink implements MetricsSink {

    private final MeterRegistry registry;
    private final Tags common;
    private final String prefix;
    /** A Micrometer function meter holds the object it reads only weakly, so the suppliers are kept here; whoever binds this sink keeps it alive ({@code Metrics.bindTo} does). */
    private final List<Object> keep = new CopyOnWriteArrayList<>();

    public MicrometerSink(MeterRegistry registry) {
        this(registry, Tags.empty());
    }

    public MicrometerSink(MeterRegistry registry, Iterable<Tag> commonTags) {
        this(registry, commonTags, "hydrangea.");
    }

    public MicrometerSink(MeterRegistry registry, Iterable<Tag> commonTags, String prefix) {
        this.registry = registry;
        this.common = Tags.of(commonTags);
        this.prefix = prefix;
    }

    @Override
    public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
        keep.add(total);
        FunctionCounter.builder(prefix + name, total, DoubleSupplier::getAsDouble)
                .baseUnit(unit).tags(common).tags(tags).register(registry);
    }

    @Override
    public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
        keep.add(value);
        Gauge.builder(prefix + name, value, DoubleSupplier::getAsDouble)
                .baseUnit(unit).tags(common).tags(tags).register(registry);
    }

    @Override
    public Counter counter(String name, String unit, String... tags) {
        io.micrometer.core.instrument.Counter c = io.micrometer.core.instrument.Counter.builder(prefix + name)
                .baseUnit(unit).tags(common).tags(tags).register(registry);
        return amount -> c.increment(amount);
    }

    @Override
    public Timer timer(String name, String... tags) {
        io.micrometer.core.instrument.Timer t = io.micrometer.core.instrument.Timer.builder(prefix + name)
                .tags(common).tags(tags).register(registry);
        return nanos -> t.record(nanos, TimeUnit.NANOSECONDS);
    }
}
