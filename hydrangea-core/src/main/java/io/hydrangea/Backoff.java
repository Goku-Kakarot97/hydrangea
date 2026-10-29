package io.hydrangea;

import java.time.Duration;

/**
 * How long a client waits between attempts to reconnect: the wait before attempt n is {@code initialDelay * factor^(n-1)}, at most
 * {@code maxDelay}, varied by up to {@code jitter} (0.2 = plus or minus 20 %) so that many clients do not all come back in the same instant
 * when a server restarts. Shared by the clients that reconnect by themselves.
 *
 * @param maxAttempts consecutive failed attempts before giving up; 0 = never give up
 */
public record Backoff(Duration initialDelay, Duration maxDelay, double factor, double jitter, int maxAttempts) {

    public Backoff {
        if (initialDelay == null || initialDelay.isNegative() || maxDelay == null || maxDelay.compareTo(initialDelay) < 0) {
            throw new IllegalArgumentException("require 0 <= initialDelay <= maxDelay");
        }
        if (factor < 1 || jitter < 0 || jitter > 1 || maxAttempts < 0) {
            throw new IllegalArgumentException("require factor >= 1, 0 <= jitter <= 1, maxAttempts >= 0");
        }
    }

    /** 1 s, doubling up to 30 s, 20 % jitter, never gives up. */
    public static Backoff defaults() {
        return new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0, 0.2, 0);
    }

    public Backoff withDelays(Duration initial, Duration max) {
        return new Backoff(initial, max, factor, jitter, maxAttempts);
    }

    public Backoff withMaxAttempts(int n) {
        return new Backoff(initialDelay, maxDelay, factor, jitter, n);
    }

    public Backoff withJitter(double j) {
        return new Backoff(initialDelay, maxDelay, factor, j, maxAttempts);
    }

    /** The wait before the given attempt (1 = the first retry). */
    public Duration delayFor(int attempt) {
        double ms = initialDelay.toMillis() * Math.pow(factor, Math.max(0, attempt - 1));
        ms = Math.min(ms, maxDelay.toMillis());
        double spread = ms * jitter;
        ms += (java.util.concurrent.ThreadLocalRandom.current().nextDouble() * 2 - 1) * spread;
        return Duration.ofMillis(Math.max(0, Math.round(ms)));
    }
}
