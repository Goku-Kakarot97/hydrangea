package io.hydrangea.kafka;

/** Where the broker put a record. */
public record RecordMetadata(String topic, int partition, long offset, long timestamp) {
}
