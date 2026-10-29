package io.hydrangea.mysql;

import io.hydrangea.Backoff;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Where and how to connect.
 *
 * @param statementCacheSize prepared statements kept for each connection (0: every call with parameters prepares, executes and closes)
 * @param maxPending         calls a connection accepts while its answers are still due; more are refused with a retriable error
 * @param maxPacketBytes     the biggest packet accepted from the server (what a row or a command may weigh)
 * @param sslContext         non-null: connect with TLS (the server must offer it)
 * @param queryTimeout       how long a call may run before it fails (null: no limit)
 * @param maxResultRows      the most rows a call may return (a result is held whole; over it the call fails and the rows are read and dropped: use {@link MyClient#stream})
 * @param maxResultBytes     the most bytes of row data a call may return
 * @param reconnect          non-null: a connection that is lost is made again after the back-off's delays (see {@link MyClient}); null (the default): it stays closed
 * @param maxInFlight        the most commands written to a connection before their answers come back; 0 (the default) is no limit (pipelining). A proxy that reads one
 *                           command at a time (ProxySQL) needs 1, which makes the client wait for each answer, as a JDBC driver does
 */
public record MyConfig(String host, int port, String user, String password, String database, Duration connectTimeout, int statementCacheSize,
                       int maxPending, int maxPacketBytes, SSLContext sslContext, Duration queryTimeout, long maxResultRows, long maxResultBytes, Backoff reconnect, int maxInFlight, boolean allowPublicKeyRetrieval) {

    public MyConfig {
        if (host == null || user == null) {
            throw new IllegalArgumentException("host and user are required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port");
        }
        if (statementCacheSize < 0 || maxPending < 1 || maxPacketBytes < 1024 || maxResultRows < 1 || maxResultBytes < 1 || maxInFlight < 0) {
            throw new IllegalArgumentException("statementCacheSize, maxPending, maxPacketBytes or the result limits out of range");
        }
    }

    public static MyConfig of(String host, int port, String user, String database) {
        return new MyConfig(host, port, user, null, database, Duration.ofSeconds(10), 256, 10_000, 64 << 20, null, null, 1_000_000, 256L << 20, null, 0, false);
    }

    public MyConfig withPassword(String p) {
        return new MyConfig(host, port, user, p, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    public MyConfig withConnectTimeout(Duration d) {
        return new MyConfig(host, port, user, password, database, d, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    public MyConfig withStatementCacheSize(int n) {
        return new MyConfig(host, port, user, password, database, connectTimeout, n, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    public MyConfig withMaxPending(int n) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, n, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    public MyConfig withTls(SSLContext c) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, c, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    public MyConfig withQueryTimeout(Duration d) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, d, maxResultRows, maxResultBytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    /** The most a call may return (default a million rows, 256 MiB); a bigger result fails the call. {@link MyClient#stream} has no limit. */
    public MyConfig withResultLimits(long rows, long bytes) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, rows, bytes, reconnect, maxInFlight, allowPublicKeyRetrieval);
    }

    /** A lost connection is made again, waiting as {@code b} says between attempts (null: never). A {@link MyPool} replaces its own connections and turns this off for them. */
    public MyConfig withReconnect(Backoff b) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, b, maxInFlight, allowPublicKeyRetrieval);
    }

    /** At most {@code n} commands written before their answers are back (0: no limit). ProxySQL needs 1: it does not read a second command before it has answered the first. */
    public MyConfig withMaxInFlight(int n) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, n, allowPublicKeyRetrieval);
    }

    /**
     * Whether, on a connection without TLS, the client may ask the server for its RSA public key and send the password encrypted with it ({@code caching_sha2_password} "full
     * authentication", needed the first time a user logs in after the server started). Off by default, as in Connector/J ({@code allowPublicKeyRetrieval}): a man in the middle who
     * answers with his own key reads the password. With TLS the password goes inside the session and the question does not arise.
     */
    public MyConfig withAllowPublicKeyRetrieval(boolean allow) {
        return new MyConfig(host, port, user, password, database, connectTimeout, statementCacheSize, maxPending, maxPacketBytes, sslContext, queryTimeout, maxResultRows, maxResultBytes, reconnect, maxInFlight, allow);
    }

    /** Without the password. */
    @Override
    public String toString() {
        return "MyConfig[host=" + host + ", port=" + port + ", user=" + user + ", password=" + (password == null ? "null" : "***") + ", database=" + database + ", tls=" + (sslContext != null)
                + ", connectTimeout=" + connectTimeout + ", statementCacheSize=" + statementCacheSize + ", queryTimeout=" + queryTimeout + ", maxInFlight=" + maxInFlight
                + ", reconnect=" + reconnect + ", allowPublicKeyRetrieval=" + allowPublicKeyRetrieval + "]";
    }
}
