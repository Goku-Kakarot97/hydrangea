package io.hydrangea.kafka;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** A record read from a partition. */
public record ConsumerRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value, List<Header> headers) {

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }
}
