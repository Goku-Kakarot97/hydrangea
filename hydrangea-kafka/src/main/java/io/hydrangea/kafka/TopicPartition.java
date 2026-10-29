package io.hydrangea.kafka;

public record TopicPartition(String topic, int partition) {
    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
