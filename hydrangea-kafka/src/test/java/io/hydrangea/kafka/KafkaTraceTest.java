package io.hydrangea.kafka;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.trace.MemoryTraceSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.SpanContext;
import io.hydrangea.trace.TraceSink;
import io.hydrangea.trace.Tracing;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A send is a PRODUCER span and the record carries traceparent; the consumer continues the trace with a CONSUMER span. */
class KafkaTraceTest {

    private static EventLoopGroup group;
    private static KafkaClient kafka;

    @BeforeAll
    static void start() {
        Kafka.require(Kafka.PORT);
        group = new EventLoopGroup(2);
        kafka = KafkaClient.create(group, Kafka.config());
    }

    @AfterAll
    static void stop() throws Exception {
        Tracing.bindTo(TraceSink.NOOP);
        if (kafka != null) {
            kafka.close();
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void unbind() {
        Tracing.bindTo(TraceSink.NOOP);
    }

    @Test
    void producerAndConsumerSpansFormOneTrace() throws Exception {
        MemoryTraceSink spans = new MemoryTraceSink();
        Tracing.bindTo(spans);
        String topic = Kafka.topic("trace");
        kafka.createTopic(topic, 1, 1).get(30, TimeUnit.SECONDS);
        KafkaConsumer consumer = kafka.consumer(ConsumerConfig.defaults());
        consumer.assign(List.of(new TopicPartition(topic, 0))).get(30, TimeUnit.SECONDS);

        Span outer = Tracing.start("outer", Span.Kind.INTERNAL, null);
        RecordMetadata meta;
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults()); Span.Scope ignored = outer.makeCurrent()) {
            meta = p.send(topic, "k".getBytes(StandardCharsets.UTF_8), "v".getBytes(StandardCharsets.UTF_8)).get(30, TimeUnit.SECONDS);
        }
        outer.end();
        MemoryTraceSink.Recorded producer = spans.spans().stream().filter(s -> s.kind() == Span.Kind.PRODUCER).findFirst().orElseThrow();
        assertEquals(topic + " publish", producer.name());
        assertEquals(outer.context().spanId(), producer.parentSpanId());
        assertEquals("kafka", producer.attributes().get("messaging.system"));
        assertEquals(topic, producer.attributes().get("messaging.destination.name"));
        assertEquals(String.valueOf(meta.partition()), producer.attributes().get("messaging.destination.partition.id"));
        assertEquals(meta.offset(), producer.attributes().get("messaging.kafka.offset"));
        assertTrue(producer.ended());
        assertFalse(producer.failed());

        List<ConsumerRecord> records = new ArrayList<>();
        long end = System.currentTimeMillis() + 20_000;
        while (records.isEmpty() && System.currentTimeMillis() < end) {
            records.addAll(consumer.poll(Duration.ofSeconds(1)).get(30, TimeUnit.SECONDS));
        }
        assertEquals(1, records.size());
        ConsumerRecord r = records.get(0);
        assertTrue(r.headers().stream().anyMatch(h -> h.key().equals("traceparent")), "the header travelled: " + r.headers());
        SpanContext fromRecord = r.spanContext();
        assertEquals(producer.context(), fromRecord);
        try (Span processing = r.startProcessing(); Span.Scope ignored = processing.makeCurrent()) {
            assertTrue(processing.isRecording());
            assertEquals(outer.context().traceId(), processing.context().traceId());
        }
        MemoryTraceSink.Recorded consumerSpan = spans.spans().stream().filter(s -> s.kind() == Span.Kind.CONSUMER).findFirst().orElseThrow();
        assertEquals(topic + " process", consumerSpan.name());
        assertEquals(producer.context().spanId(), consumerSpan.parentSpanId());
        assertEquals("0", consumerSpan.attributes().get("messaging.destination.partition.id"));
        assertTrue(consumerSpan.ended());
        consumer.close();
    }

    @Test
    void aRecordWithoutTheHeaderStartsItsOwnTrace() {
        ConsumerRecord r = new ConsumerRecord("t", 0, 0, 0, null, null, List.of());
        assertNull(r.spanContext());
        Span s = r.startProcessing();
        assertNotNull(s.context().traceId());
        s.end();
    }
}
