package io.hydrangea.mqtt;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A publish is a PRODUCER span; with MQTT 5 the message carries traceparent as a user property and the subscriber continues the trace. */
class MqttTraceTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        Broker.require(Broker.PORT);
        group = Broker.newGroup();
    }

    @AfterAll
    static void stop() throws Exception {
        Tracing.bindTo(TraceSink.NOOP);
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void unbind() {
        Tracing.bindTo(TraceSink.NOOP);
    }

    @Test
    void mqtt5CarriesTheTraceToTheSubscriber() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (MqttClient sub = MqttClient.connect(group, Broker.config5()).get(10, TimeUnit.SECONDS);
             MqttClient pub = MqttClient.connect(group, Broker.config5()).get(10, TimeUnit.SECONDS)) {
            String topic = Broker.id("trace");
            CompletableFuture<MqttMessage> received = new CompletableFuture<>();
            sub.subscribe(topic, QoS.AT_LEAST_ONCE, received::complete).get(10, TimeUnit.SECONDS);

            Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
            try (Span.Scope ignored = outer.makeCurrent()) {
                pub.publish(topic, "hi".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, false, MqttProperties.NONE.withContentType("text/plain")).get(10, TimeUnit.SECONDS);
            }
            outer.end();
            MqttMessage m = received.get(10, TimeUnit.SECONDS);
            assertEquals("text/plain", m.properties().contentType(), "the caller's properties are kept");

            MemoryTraceSink.Recorded publish = spans.spans().stream().filter(s -> s.kind() == Span.Kind.PRODUCER).findFirst().orElseThrow();
            assertEquals(topic + " publish", publish.name());
            assertEquals(outer.context().spanId(), publish.parentSpanId());
            assertEquals("mqtt", publish.attributes().get("messaging.system"));
            assertEquals(1L, publish.attributes().get("messaging.mqtt.qos"));
            assertTrue(publish.ended());
            assertFalse(publish.failed());

            assertEquals(publish.context(), m.spanContext(), "the user property travelled: " + m.properties().userProperties());
            try (Span processing = m.startProcessing(); Span.Scope ignored = processing.makeCurrent()) {
                assertEquals(outer.context().traceId(), processing.context().traceId());
            }
            MemoryTraceSink.Recorded consumer = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CONSUMER).findFirst().orElseThrow();
            assertEquals(topic + " process", consumer.name());
            assertEquals(publish.context().spanId(), consumer.parentSpanId());
        }
    }

    @Test
    void mqtt3HasASpanButNothingToCarryItIn() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (MqttClient sub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS);
             MqttClient pub = MqttClient.connect(group, Broker.config()).get(10, TimeUnit.SECONDS)) {
            String topic = Broker.id("trace3");
            CompletableFuture<MqttMessage> received = new CompletableFuture<>();
            sub.subscribe(topic, QoS.AT_LEAST_ONCE, received::complete).get(10, TimeUnit.SECONDS);
            pub.publish(topic, "hi".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE).get(10, TimeUnit.SECONDS);
            MqttMessage m = received.get(10, TimeUnit.SECONDS);
            assertNull(m.spanContext());
            MemoryTraceSink.Recorded publish = spans.spans().stream().filter(s -> s.kind() == Span.Kind.PRODUCER).findFirst().orElseThrow();
            assertTrue(publish.ended());
            Span processing = m.startProcessing();
            assertTrue(processing.isRecording());
            processing.end();
        }
    }
}
