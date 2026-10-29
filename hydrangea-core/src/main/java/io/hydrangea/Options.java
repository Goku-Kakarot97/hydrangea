package io.hydrangea;

import java.time.Duration;

/**
 * Immutable tuning knobs shared by {@link Server} and {@link Client}.
 *
 * @param highWaterMark queued outbound bytes above which a connection becomes non-writable
 * @param lowWaterMark  queued outbound bytes at or below which it becomes writable again
 * @param idleTimeout   close connections with no read/write activity for this long; {@code null} disables
 * @param connectTimeout client connect timeout; {@code null} disables
 * @param backlog       server accept backlog
 * @param tcpNoDelay    TCP_NODELAY
 * @param keepAlive     SO_KEEPALIVE
 * @param autoRead      start reading as soon as the connection is active
 * @param maxConnections server: connections open at once across all clients; further ones are accepted and closed at
 *                      once (counted in {@link Metrics}); 0 = unlimited
 * @param maxConnectionsPerAddress server: the same per remote IP address (the usual first defence against one host
 *                      exhausting the server); 0 = unlimited. Behind a proxy every client shares the proxy's address.
 */
public record Options(
        int highWaterMark,
        int lowWaterMark,
        Duration idleTimeout,
        Duration connectTimeout,
        int backlog,
        boolean tcpNoDelay,
        boolean keepAlive,
        boolean autoRead,
        int maxConnections,
        int maxConnectionsPerAddress) {

    public Options {
        if (lowWaterMark < 0 || highWaterMark < lowWaterMark) {
            throw new IllegalArgumentException("require 0 <= lowWaterMark <= highWaterMark");
        }
        if (maxConnections < 0 || maxConnectionsPerAddress < 0) {
            throw new IllegalArgumentException("connection limits must be >= 0");
        }
    }

    public static Options defaults() {
        return new Options(64 * 1024, 32 * 1024, null, Duration.ofSeconds(10), 1024, true, false, true, 0, 0);
    }

    public Options withWaterMarks(int low, int high) {
        return new Options(high, low, idleTimeout, connectTimeout, backlog, tcpNoDelay, keepAlive, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withIdleTimeout(Duration d) {
        return new Options(highWaterMark, lowWaterMark, d, connectTimeout, backlog, tcpNoDelay, keepAlive, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withConnectTimeout(Duration d) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, d, backlog, tcpNoDelay, keepAlive, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withBacklog(int n) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, n, tcpNoDelay, keepAlive, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withTcpNoDelay(boolean b) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, backlog, b, keepAlive, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withKeepAlive(boolean b) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, backlog, tcpNoDelay, b, autoRead, maxConnections, maxConnectionsPerAddress);
    }

    public Options withMaxConnections(int n) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, backlog, tcpNoDelay, keepAlive, autoRead, n, maxConnectionsPerAddress);
    }

    public Options withMaxConnectionsPerAddress(int n) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, backlog, tcpNoDelay, keepAlive, autoRead, maxConnections, n);
    }

    public Options withAutoRead(boolean b) {
        return new Options(highWaterMark, lowWaterMark, idleTimeout, connectTimeout, backlog, tcpNoDelay, keepAlive, b, maxConnections, maxConnectionsPerAddress);
    }
}
