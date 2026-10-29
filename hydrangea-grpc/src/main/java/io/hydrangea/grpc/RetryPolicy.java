package io.hydrangea.grpc;

import java.time.Duration;
import java.util.Set;

/**
 * When and how a {@link GrpcChannel} retries a failed call by itself (gRFC A6, without hedging and throttling).
 * <p>A call is retried only while it can be done invisibly: the failure status is in {@code retryableCodes} <em>and</em> nothing of the
 * response has been delivered yet (no headers, no message) <em>and</em> the request messages sent so far still fit in the channel's retry
 * buffer. The deadline of the call spans all attempts. Retrying a call that is not idempotent can run it twice, so a policy is opt-in and
 * the application decides which codes are safe.
 *
 * @param maxAttempts      total attempts including the first (2..10)
 * @param initialBackoff   wait before the first retry; each later wait is multiplied by {@code multiplier}, capped at {@code maxBackoff},
 *                         and the actual wait is a random value up to that (jitter)
 */
public record RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, double multiplier, Set<Status.Code> retryableCodes) {

    public RetryPolicy {
        if (maxAttempts < 2 || maxAttempts > 10) {
            throw new IllegalArgumentException("maxAttempts must be 2..10");
        }
        if (initialBackoff.isNegative() || initialBackoff.isZero() || maxBackoff.compareTo(initialBackoff) < 0 || multiplier < 1) {
            throw new IllegalArgumentException("invalid backoff");
        }
        retryableCodes = Set.copyOf(retryableCodes);
        if (retryableCodes.isEmpty() || retryableCodes.contains(Status.Code.OK)) {
            throw new IllegalArgumentException("retryableCodes must be non-empty and not contain OK");
        }
    }

    /** 3 attempts, 100 ms to 5 s backoff, retrying UNAVAILABLE only (the status of a connection that could not be used). */
    public static RetryPolicy defaults() {
        return new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(5), 2.0, Set.of(Status.Code.UNAVAILABLE));
    }

    public RetryPolicy withMaxAttempts(int n) {
        return new RetryPolicy(n, initialBackoff, maxBackoff, multiplier, retryableCodes);
    }

    public RetryPolicy withBackoff(Duration initial, Duration max, double multiplier) {
        return new RetryPolicy(maxAttempts, initial, max, multiplier, retryableCodes);
    }

    public RetryPolicy withRetryableCodes(Set<Status.Code> codes) {
        return new RetryPolicy(maxAttempts, initialBackoff, maxBackoff, multiplier, codes);
    }
}
