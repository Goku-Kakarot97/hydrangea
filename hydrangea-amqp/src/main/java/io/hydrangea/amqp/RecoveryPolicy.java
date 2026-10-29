package io.hydrangea.amqp;

import java.time.Duration;

/**
 * How {@link RecoveringConnection} retries after losing the broker: the wait before attempt n is {@code initialDelay * factor^(n-1)},
 * at most {@code maxDelay}, varied by up to {@code jitter} (0.2 = plus or minus 20 %) so that many clients do not all reconnect in the same
 * instant when a broker restarts.
 *
 * @param maxAttempts consecutive failed attempts before giving up; 0 = never give up
 */
public record RecoveryPolicy(Duration initialDelay, Duration maxDelay, double factor, double jitter, int maxAttempts) {

    public RecoveryPolicy {
        if (initialDelay == null || initialDelay.isNegative() || maxDelay == null || maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException("require 0 <= initialDelay <= maxDelay");
        }
        if (factor < 1 || jitter < 0 || jitter > 1 || maxAttempts < 0) {
            throw new IllegalArgumentException("require factor >= 1, 0 <= jitter <= 1, maxAttempts >= 0");
        }
    }

    /** 1 s, doubling up to 30 s, 20 % jitter, never gives up. */
    public static RecoveryPolicy defaults() {
        return new RecoveryPolicy(Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0, 0.2, 0);
    }

    public RecoveryPolicy withDelays(Duration initial, Duration max) {
        return new RecoveryPolicy(initial, max, factor, jitter, maxAttempts);
    }

    public RecoveryPolicy withMaxAttempts(int n) {
        return new RecoveryPolicy(initialDelay, maxDelay, factor, jitter, n);
    }

    public RecoveryPolicy withJitter(double j) {
        return new RecoveryPolicy(initialDelay, maxDelay, factor, j, maxAttempts);
    }

    /** The wait before the given attempt (1 = the first retry). */
    Duration delayFor(int attempt) {
        return new io.hydrangea.Backoff(initialDelay, maxDelay, factor, jitter, maxAttempts).delayFor(attempt);
    }
}
