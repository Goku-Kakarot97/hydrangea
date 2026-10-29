package io.hydrangea.kafka;

import java.time.Duration;

/**
 * Producer settings. {@code acks}: 0 fire and forget, 1 the leader has it, -1 all in-sync replicas have it. A batch is sent when it holds {@code batchBytes},
 * or {@code linger} after its first record, or at once on {@code flush()}. {@code bufferBytes} bounds what is waiting or in flight: a send over it fails with
 * a retriable exception ("buffer full") instead of using memory without limit. {@code compression}: 0 none, 1 gzip.
 * <p>
 * <b>The default is idempotent</b> (as in the Java client since 3.0): the broker checks a producer id and sequence numbers, so a retry after a lost answer cannot store a
 * record twice and nothing is reordered within a partition, with up to 5 batches in flight per partition. That needs {@code acks=-1} (all in-sync replicas) and Kafka 0.11 or
 * later. {@link #withoutIdempotence()} switches it off (and goes back to one batch in flight per partition, so that a retry can never overtake another batch), after which
 * {@code acks} can be 0 or 1. {@code maxInFlight} above 1 <em>without</em> idempotence lets a retried batch be stored after a later one and a lost answer store a batch twice.
 */
public record ProducerConfig(int acks, Duration linger, int batchBytes, int maxRequestBytes, long bufferBytes, int compression, Duration deliveryTimeout, int retries,
                             boolean idempotent, int maxInFlight) {

    public ProducerConfig {
        if (acks != 0 && acks != 1 && acks != -1) {
            throw new IllegalArgumentException("acks must be 0, 1 or -1");
        }
        if (compression != 0 && compression != 1) {
            throw new IllegalArgumentException("compression must be 0 (none) or 1 (gzip)");
        }
        if (batchBytes < 1 || maxRequestBytes < batchBytes && maxRequestBytes < 1024) {
            throw new IllegalArgumentException("batchBytes / maxRequestBytes");
        }
        if (maxInFlight < 1) {
            throw new IllegalArgumentException("maxInFlight");
        }
        if (idempotent && (acks != -1 || maxInFlight > 5 || retries < 1)) {
            throw new IllegalArgumentException("an idempotent producer (the default) needs acks=-1, maxInFlight <= 5 and retries >= 1: call withoutIdempotence() first to use acks 0 or 1");
        }
    }

    /** Idempotent, {@code acks=-1}, 5 batches in flight per partition, linger 5 ms, batches of 64 KiB. */
    public static ProducerConfig defaults() {
        return new ProducerConfig(-1, Duration.ofMillis(5), 64 * 1024, 1024 * 1024, 64L * 1024 * 1024, 0, Duration.ofSeconds(30), 10, true, 5);
    }

    /** Not idempotent: {@code acks} may be 0 or 1 again; one batch in flight per partition unless {@link #withMaxInFlight} says otherwise. */
    public ProducerConfig withoutIdempotence() {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, retries, false, 1);
    }

    public ProducerConfig withAcks(int a) {
        return new ProducerConfig(a, linger, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withLinger(Duration l) {
        return new ProducerConfig(acks, l, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withBatchBytes(int b) {
        return new ProducerConfig(acks, linger, b, maxRequestBytes, bufferBytes, compression, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withMaxRequestBytes(int b) {
        return new ProducerConfig(acks, linger, batchBytes, b, bufferBytes, compression, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withBufferBytes(long b) {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, b, compression, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withCompression(int c) {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, bufferBytes, c, deliveryTimeout, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withDeliveryTimeout(Duration d) {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, bufferBytes, compression, d, retries, idempotent, maxInFlight);
    }

    public ProducerConfig withRetries(int r) {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, r, idempotent, maxInFlight);
    }

    public ProducerConfig withMaxInFlight(int n) {
        return new ProducerConfig(acks, linger, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, retries, idempotent, n);
    }

    /** Idempotent producer: sets acks=-1 and up to 5 batches in flight per partition. */
    public ProducerConfig withIdempotence() {
        return new ProducerConfig(-1, linger, batchBytes, maxRequestBytes, bufferBytes, compression, deliveryTimeout, Math.max(retries, 1), true, Math.max(maxInFlight, 5));
    }
}
