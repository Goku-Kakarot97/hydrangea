package io.hydrangea.kafka;

import java.util.Collection;

/** Told when the group gives the consumer other partitions. Runs on the event loop: do not block. */
public interface RebalanceListener {

    /** The partitions taken away; their offsets have been committed (with a group and manual or auto commit) before this is called. */
    default void revoked(Collection<TopicPartition> partitions) {
    }

    default void assigned(Collection<TopicPartition> partitions) {
    }

    /** The consumer cannot go on (for instance the group refuses it). It stops fetching. */
    default void failed(Throwable cause) {
    }
}
