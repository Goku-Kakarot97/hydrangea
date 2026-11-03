package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A publish is a PRODUCER span and the message carries traceparent in its headers; the consumer continues the trace. */
class AmqpTraceTest {

    private static final String HOST = System.getProperty("amqp.host", "127.0.0.1");
    private static final int PORT = Integer.getInteger("amqp.port", 25672);
    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no RabbitMQ on " + HOST + ":" + PORT);
        group = new EventLoopGroup(2);
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
    void publisherAndConsumerSpansFormOneTrace() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        try (AmqpConnection conn = AmqpConnection.connect(group, AmqpConfig.of(HOST, PORT)).get(10, TimeUnit.SECONDS)) {
            AmqpChannel ch = conn.openChannel().get(10, TimeUnit.SECONDS);
            String q = "hydrangea-trace-" + UUID.randomUUID();
            ch.queueDeclare(q, false, false, true, null).get(10, TimeUnit.SECONDS);
            CompletableFuture<Delivery> delivered = new CompletableFuture<>();
            ch.basicConsume(q, true, delivered::complete).get(10, TimeUnit.SECONDS);

            Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
            try (Span.Scope ignored = outer.makeCurrent()) {
                ch.basicPublish("", q, false, BasicProperties.EMPTY.withContentType("text/plain"), "hello".getBytes(StandardCharsets.UTF_8)).get(10, TimeUnit.SECONDS);
            }
            outer.end();
            Delivery d = delivered.get(10, TimeUnit.SECONDS);
            assertEquals("hello", new String(d.body(), StandardCharsets.UTF_8));
            assertEquals("text/plain", d.properties().contentType(), "the caller's properties are kept");

            MemoryTraceSink.Recorded publish = spans.spans().stream().filter(s -> s.kind() == Span.Kind.PRODUCER).findFirst().orElseThrow();
            assertEquals(q + " publish", publish.name());
            assertEquals(outer.context().spanId(), publish.parentSpanId());
            assertEquals("rabbitmq", publish.attributes().get("messaging.system"));
            assertEquals(q, publish.attributes().get("messaging.rabbitmq.destination.routing_key"));
            assertTrue(publish.ended());
            assertFalse(publish.failed());

            assertEquals(publish.context(), d.spanContext(), "the header travelled: " + d.properties().headers());
            try (Span processing = d.startProcessing(); Span.Scope ignored = processing.makeCurrent()) {
                assertEquals(outer.context().traceId(), processing.context().traceId());
            }
            MemoryTraceSink.Recorded consumer = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CONSUMER).findFirst().orElseThrow();
            assertEquals(q + " process", consumer.name());
            assertEquals(publish.context().spanId(), consumer.parentSpanId());
            assertTrue(consumer.ended());
        }
    }
}
