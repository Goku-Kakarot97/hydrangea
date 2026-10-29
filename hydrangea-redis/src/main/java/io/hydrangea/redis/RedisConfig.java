package io.hydrangea.redis;

import io.hydrangea.Backoff;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Connection settings.
 *
 * @param username        ACL user (Redis 6 and later); null = the default user
 * @param password        null = no authentication
 * @param database        the database to {@code SELECT} after connecting (0 = none needed). Connection-wide state: see {@link RedisClient}
 * @param sslContext      non-null = TLS; the server certificate is verified against {@code host}
 * @param connectTimeout  limit for the TCP connection and the handshake that follows
 * @param protocol        3 = RESP3, with automatic fallback to RESP2 when the server does not know {@code HELLO} (before Redis 6); 2 = RESP2 only
 * @param maxBulkBytes    the largest single value accepted from the server (a bigger one closes the connection; Redis itself caps a value at 512 MB)
 * @param maxElements     the most elements accepted in one array, map or set
 * @param maxPending      commands sent and not yet answered; beyond it a command fails at once with a retriable exception
 * @param reconnect       how to retry after an established connection is lost; null = do not reconnect, the client closes
 * @param clientName      sent with {@code CLIENT SETNAME} (shows in {@code CLIENT LIST}); null = none
 */
public record RedisConfig(
        String host,
        int port,
        String username,
        String password,
        int database,
        SSLContext sslContext,
        Duration connectTimeout,
        int protocol,
        int maxBulkBytes,
        int maxElements,
        int maxPending,
        Backoff reconnect,
        String clientName) {

    public RedisConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port");
        }
        if (database < 0) {
            throw new IllegalArgumentException("database");
        }
        if (protocol != 2 && protocol != 3) {
            throw new IllegalArgumentException("protocol must be 2 or 3");
        }
        if (maxBulkBytes < 1024 || maxElements < 1 || maxPending < 1) {
            throw new IllegalArgumentException("limits");
        }
        if (username != null && password == null) {
            throw new IllegalArgumentException("a user name needs a password");
        }
    }

    /** No authentication, no TLS, RESP3, 10 s to connect, 512 MiB values, 1 000 000 pending commands, reconnecting with {@link Backoff#defaults()}. */
    public static RedisConfig of(String host, int port) {
        return new RedisConfig(host, port, null, null, 0, null, Duration.ofSeconds(10), 3, 512 * 1024 * 1024, 100_000_000, 1_000_000, Backoff.defaults(), null);
    }

    public RedisConfig withAuth(String username, String password) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    /** The default user with a password ({@code requirepass}). */
    public RedisConfig withPassword(String password) {
        return new RedisConfig(host, port, null, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    public RedisConfig withDatabase(int database) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    public RedisConfig withTls(SSLContext context) {
        return new RedisConfig(host, port, username, password, database, context, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    public RedisConfig withConnectTimeout(Duration d) {
        return new RedisConfig(host, port, username, password, database, sslContext, d, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    public RedisConfig withProtocol(int protocol) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    public RedisConfig withLimits(int maxBulkBytes, int maxElements, int maxPending) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, clientName);
    }

    /** Null = never reconnect. */
    public RedisConfig withReconnect(Backoff b) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, b, clientName);
    }

    public RedisConfig withClientName(String name) {
        return new RedisConfig(host, port, username, password, database, sslContext, connectTimeout, protocol, maxBulkBytes, maxElements, maxPending, reconnect, name);
    }
}
