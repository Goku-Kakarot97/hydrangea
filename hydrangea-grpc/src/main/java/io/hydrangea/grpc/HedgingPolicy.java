package io.hydrangea.grpc;

import java.time.Duration;
import java.util.Set;

/**
 * Hedging (gRFC A6): instead of waiting for a slow call to fail, send the same call again to (usually) another server after
 * {@code hedgingDelay}, and use whichever answers first; the others are cancelled. It cuts tail latency at the cost of extra load, and the
 * server may run the call more than once, so use it only for idempotent methods. Mutually exclusive with a {@link RetryPolicy}.
 * <ul>
 *   <li>If an attempt fails with one of {@code nonFatalCodes}, the next one starts at once (not after the delay).</li>
 *   <li>Any other failure status ends the call right away and cancels the other attempts.</li>
 *   <li>The first attempt to deliver a message (or to finish with OK) wins.</li>
 * </ul>
 *
 * @param maxAttempts total attempts including the first (2..5)
 */
public record HedgingPolicy(int maxAttempts, Duration hedgingDelay, Set<Status.Code> nonFatalCodes) {

    public HedgingPolicy {
        if (maxAttempts < 2 || maxAttempts > 5) {
            throw new IllegalArgumentException("maxAttempts must be 2..5");
        }
        if (hedgingDelay.isNegative()) {
            throw new IllegalArgumentException("hedgingDelay must not be negative");
        }
        nonFatalCodes = Set.copyOf(nonFatalCodes);
        if (nonFatalCodes.contains(Status.Code.OK)) {
            throw new IllegalArgumentException("OK is not a failure");
        }
    }

    /** 2 attempts, the second after 100 ms; UNAVAILABLE does not stop the hedging. */
    public static HedgingPolicy defaults() {
        return new HedgingPolicy(2, Duration.ofMillis(100), Set.of(Status.Code.UNAVAILABLE));
    }

    public HedgingPolicy withMaxAttempts(int n) {
        return new HedgingPolicy(n, hedgingDelay, nonFatalCodes);
    }

    public HedgingPolicy withDelay(Duration d) {
        return new HedgingPolicy(maxAttempts, d, nonFatalCodes);
    }

    public HedgingPolicy withNonFatalCodes(Set<Status.Code> codes) {
        return new HedgingPolicy(maxAttempts, hedgingDelay, codes);
    }
}
