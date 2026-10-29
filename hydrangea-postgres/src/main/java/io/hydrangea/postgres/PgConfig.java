package io.hydrangea.postgres;

import io.hydrangea.Backoff;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Connection settings.
 *
 * @param user               the database role
 * @param password           null = the server must not ask for one (trust / peer authentication)
 * @param database           the database to open
 * @param connectTimeout     limit for the TCP connection and the login that follows
 * @param statementCacheSize prepared statements kept per connection (least recently used leave first); 0 = no cache: every query is parsed
 *                           again with the unnamed statement
 * @param maxPending         queries sent and not yet answered; beyond it a query fails at once with a retriable exception
 * @param maxMessageBytes    the largest single message accepted from the server (a bigger one closes the connection)
 * @param applicationName    shows in {@code pg_stat_activity}; null = none
 * @param sslContext         non-null = TLS is required: the client sends {@code SSLRequest}, a server that answers no is an error (no fallback to plain text), and the
 *                           server certificate is verified against {@code host} (as {@code sslmode=verify-full}); null = no TLS
 * @param maxResultRows      the most rows one query may return into memory; past it the query fails (the rest is read and dropped, the connection is fine) and the message
 *                           points to {@link PgClient#stream}. A result is held whole until the caller has it, so without a limit one careless {@code SELECT} could exhaust the heap
 * @param maxResultBytes     the same for the bytes of the values of one result (the sum of the sizes of the {@code DataRow} messages)
 * @param binaryTransfer     send parameters in binary when the server has said what type they are, and ask for results in binary for the types where that changes nothing the caller
 *                           can see (booleans, integers, numeric, uuid, bytea and arrays of them): less to convert on both sides, and no hex doubling of a bytea. The first run of
 *                           a statement is always in text (its types are not known yet). false = text everywhere
 * @param reconnect          how a {@link PgClient} behaves when an established connection is lost: null (the default) = it closes, and a {@link PgPool} replaces it; with a
 *                           {@link Backoff} it reconnects by itself, and queries sent meanwhile fail at once with a retriable exception. The session is new, so everything that lived in
 *                           it is gone: the statement cache is rebuilt, channels given to {@link PgClient#listen} are listened to again, but a transaction, {@code SET} values and
 *                           temporary tables are not restored (see {@link PgClient})
 * @param queryTimeout       how long a query may take from the moment the client takes it up, waiting behind other queries included; null = no limit. When it runs
 *                           out the future fails with a {@link PgException#isTimeout() timeout} and, if the query is the one the server is running, the server is
 *                           asked to cancel it. See {@link PgClient#query(java.time.Duration, String, Object...)}
 */
public record PgConfig(
        String host,
        int port,
        String user,
        String password,
        String database,
        Duration connectTimeout,
        int statementCacheSize,
        int maxPending,
        int maxMessageBytes,
        String applicationName,
        SSLContext sslContext,
        Duration queryTimeout,
        int maxResultRows,
        int maxResultBytes,
        boolean binaryTransfer,
        Backoff reconnect, boolean allowClearTextPassword) {

    public PgConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port");
        }
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException("user");
        }
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("database");
        }
        if (maxResultRows < 1 || maxResultBytes < 1024) {
            throw new IllegalArgumentException("result limits");
        }
        if (statementCacheSize < 0 || maxPending < 1 || maxMessageBytes < 1024) {
            throw new IllegalArgumentException("limits");
        }
    }

    /** No password, 10 s to connect, 256 cached statements, 100 000 pending queries, messages up to 256 MiB. */
    public static PgConfig of(String host, int port, String user, String database) {
        return new PgConfig(host, port, user, null, database, Duration.ofSeconds(10), 256, 100_000, 256 * 1024 * 1024, null, null, null, 1_000_000, 256 * 1024 * 1024, true, null, false);
    }

    public PgConfig withPassword(String password) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    public PgConfig withConnectTimeout(Duration d) {
        return new PgConfig(host, port, user, password, database, d, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    public PgConfig withStatementCacheSize(int n) {
        return new PgConfig(host, port, user, password, database, connectTimeout, n, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    public PgConfig withLimits(int maxPending, int maxMessageBytes) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    /** TLS is required, with the server certificate verified against the host name; see {@link #sslContext()}. */
    public PgConfig withTls(SSLContext context) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, context, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    /** The client reconnects by itself after losing an established connection, waiting as {@code backoff} says (null = it does not); see {@link #reconnect()}. */
    public PgConfig withReconnect(Backoff backoff) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, backoff, allowClearTextPassword);
    }

    /** Binary parameters and results where they are exact; see {@link #binaryTransfer()}. On by default. */
    public PgConfig withBinaryTransfer(boolean on) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, on, reconnect, allowClearTextPassword);
    }

    /** What one query may bring into memory; larger results fail, and can be read with {@link PgClient#stream}. */
    public PgConfig withResultLimits(int maxRows, int maxBytes) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxRows, maxBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    /** Every query gets this limit unless it is given its own; see {@link #queryTimeout()}. */
    public PgConfig withQueryTimeout(Duration timeout) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, timeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    public PgConfig withApplicationName(String name) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, name, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allowClearTextPassword);
    }

    /**
     * Whether the password may be sent in the clear when the server asks for it with {@code AuthenticationCleartextPassword} on a connection without TLS. Off by default: a
     * man in the middle, or a server that is not the one expected, could ask and read it. With TLS the question does not arise (the server's certificate was verified).
     */
    public PgConfig withAllowClearTextPassword(boolean allow) {
        return new PgConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxMessageBytes, applicationName, sslContext, queryTimeout, maxResultRows, maxResultBytes, binaryTransfer, reconnect, allow);
    }

    /** Without the password. */
    @Override
    public String toString() {
        return "PgConfig[host=" + host + ", port=" + port + ", user=" + user + ", password=" + (password == null ? "null" : "***") + ", database=" + database + ", tls=" + (sslContext != null)
                + ", connectTimeout=" + connectTimeout + ", statementCacheSize=" + statementCacheSize + ", queryTimeout=" + queryTimeout + ", binaryTransfer=" + binaryTransfer
                + ", reconnect=" + reconnect + ", allowClearTextPassword=" + allowClearTextPassword + "]";
    }
}
