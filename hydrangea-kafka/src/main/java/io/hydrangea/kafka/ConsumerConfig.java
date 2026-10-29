package io.hydrangea.kafka;

import java.time.Duration;

/**
 * Consumer settings. {@code groupId} null: no group, partitions are given with {@code assign}. {@code earliest}: where to start a partition that has no committed offset
 * (true: the oldest record, false: the end). {@code bufferedRecords} bounds what is fetched ahead of {@code poll}: fetching pauses above it.
 * {@code autoCommit} non-null commits the positions of the records already returned by {@code poll} every so often (at-least-once).
 */
public record ConsumerConfig(String groupId, boolean earliest, Duration sessionTimeout, Duration rebalanceTimeout, Duration heartbeat, Duration fetchMaxWait, int fetchMinBytes,
                             int fetchMaxBytes, int partitionMaxBytes, int bufferedRecords, int maxPollRecords, Duration autoCommit) {

    public ConsumerConfig {
        if (fetchMaxBytes < 1 || partitionMaxBytes < 1 || bufferedRecords < 1 || maxPollRecords < 1) {
            throw new IllegalArgumentException("sizes must be positive");
        }
    }

    public static ConsumerConfig defaults() {
        return new ConsumerConfig(null, true, Duration.ofSeconds(10), Duration.ofSeconds(20), Duration.ofSeconds(3), Duration.ofMillis(500), 1, 16 * 1024 * 1024, 4 * 1024 * 1024,
                100_000, 5_000, null);
    }

    public ConsumerConfig withGroup(String g) {
        return new ConsumerConfig(g, earliest, sessionTimeout, rebalanceTimeout, heartbeat, fetchMaxWait, fetchMinBytes, fetchMaxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, autoCommit);
    }

    public ConsumerConfig withEarliest(boolean e) {
        return new ConsumerConfig(groupId, e, sessionTimeout, rebalanceTimeout, heartbeat, fetchMaxWait, fetchMinBytes, fetchMaxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, autoCommit);
    }

    public ConsumerConfig withSession(Duration session, Duration heartbeat) {
        return new ConsumerConfig(groupId, earliest, session, rebalanceTimeout, heartbeat, fetchMaxWait, fetchMinBytes, fetchMaxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, autoCommit);
    }

    public ConsumerConfig withFetch(Duration maxWait, int minBytes, int maxBytes, int partitionMaxBytes) {
        return new ConsumerConfig(groupId, earliest, sessionTimeout, rebalanceTimeout, heartbeat, maxWait, minBytes, maxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, autoCommit);
    }

    public ConsumerConfig withBuffer(int bufferedRecords, int maxPollRecords) {
        return new ConsumerConfig(groupId, earliest, sessionTimeout, rebalanceTimeout, heartbeat, fetchMaxWait, fetchMinBytes, fetchMaxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, autoCommit);
    }

    public ConsumerConfig withAutoCommit(Duration interval) {
        return new ConsumerConfig(groupId, earliest, sessionTimeout, rebalanceTimeout, heartbeat, fetchMaxWait, fetchMinBytes, fetchMaxBytes, partitionMaxBytes, bufferedRecords, maxPollRecords, interval);
    }
}
