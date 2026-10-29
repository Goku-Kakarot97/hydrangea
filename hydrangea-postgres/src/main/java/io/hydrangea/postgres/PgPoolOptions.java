package io.hydrangea.postgres;

import java.time.Duration;

/**
 * How a {@link PgPool} treats its connections and the callers that find none free.
 *
 * @param acquireTimeout how long a call waits when every connection is taken by a transaction (or broken and being replaced) before it fails with a retriable
 *                       {@link PgException}; null = wait without limit
 * @param maxWaiters     how many calls may wait at once; more fail at once with a retriable exception, so a stuck database does not pile up unbounded work
 * @param maxLifetime    a connection older than this (with 10% jitter so they do not all go at once) is replaced by a new one when it is idle; null = keep until it breaks.
 *                       Spreads load after a database failover and frees server memory that long sessions grow
 * @param pingInterval   an idle connection gets a {@code SELECT 1} this often, and is closed and replaced if it does not answer within {@code pingTimeout}
 *                       (a connection that a firewall dropped silently looks open until something is sent); null = no pings
 * @param pingTimeout    how long a ping may take
 */
public record PgPoolOptions(Duration acquireTimeout, int maxWaiters, Duration maxLifetime, Duration pingInterval, Duration pingTimeout) {

    public PgPoolOptions {
        if (maxWaiters < 0) {
            throw new IllegalArgumentException("maxWaiters");
        }
        for (Duration d : new Duration[]{acquireTimeout, maxLifetime, pingInterval, pingTimeout}) {
            if (d != null && (d.isZero() || d.isNegative())) {
                throw new IllegalArgumentException("durations must be positive");
            }
        }
        if (pingInterval != null && pingTimeout == null) {
            throw new IllegalArgumentException("pings need a pingTimeout");
        }
    }

    /** Wait up to 30 s for a connection (10 000 callers at most), replace connections after 30 minutes, ping idle ones every 30 s with 5 s to answer. */
    public static PgPoolOptions defaults() {
        return new PgPoolOptions(Duration.ofSeconds(30), 10_000, Duration.ofMinutes(30), Duration.ofSeconds(30), Duration.ofSeconds(5));
    }

    public PgPoolOptions withAcquireTimeout(Duration d) {
        return new PgPoolOptions(d, maxWaiters, maxLifetime, pingInterval, pingTimeout);
    }

    public PgPoolOptions withMaxWaiters(int n) {
        return new PgPoolOptions(acquireTimeout, n, maxLifetime, pingInterval, pingTimeout);
    }

    public PgPoolOptions withMaxLifetime(Duration d) {
        return new PgPoolOptions(acquireTimeout, maxWaiters, d, pingInterval, pingTimeout);
    }

    public PgPoolOptions withPing(Duration interval, Duration timeout) {
        return new PgPoolOptions(acquireTimeout, maxWaiters, maxLifetime, interval, timeout);
    }
}
