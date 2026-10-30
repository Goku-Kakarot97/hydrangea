package io.hydrangea.mongodb;

import javax.net.ssl.SSLContext;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * How to reach a server or a replica set. {@link #of} for one host, port and the default database; {@link #parse} for a {@code mongodb://} URI
 * ({@code mongodb://user:pw@h1:27017,h2:27017,h3:27017/db?replicaSet=rs0&authSource=admin&tls=true}; no {@code mongodb+srv}).
 * <ul>
 *   <li>{@link #hosts} are seeds: the client asks each for {@code hello}; a replica set member answers with the whole set and its primary, and the client uses the
 *       primary (every read and write goes there). When the primary changes, the client finds the new one and goes on; see {@link MongoClient}. A standalone
 *       server or a mongos is used as it is.</li>
 *   <li>{@link #username}/{@link #password} log in with SCRAM-SHA-256, or SCRAM-SHA-1 when the user has only that; the user lives in {@link #authSource} ({@code admin} by default).</li>
 *   <li>{@link #database} is where collections named without a database go.</li>
 *   <li>{@link #connections}: connections to the primary. A MongoDB connection runs its commands one after the other, so this is the server-side parallelism;
 *       the client pipelines over each.</li>
 *   <li>{@link #commandTimeout}: a command without an answer by then fails (its result, if it comes, is dropped; the server may still be working on it, give
 *       long reads a {@code maxTimeMS} too). {@link #serverSelectionTimeout}: how long a command waits for a primary to be found.</li>
 *   <li>{@link #retryWrites}: on a replica set or mongos, a write that fails because the connection was lost or the primary stepped down is sent once more,
 *       with the same transaction number, so it runs exactly once. Off, such a write fails with a retriable {@link MongoException} and the caller decides.</li>
 * </ul>
 */
public record MongoConfig(
        List<String> hosts,
        String replicaSet,
        String username,
        String password,
        String authSource,
        String database,
        SSLContext sslContext,
        Duration connectTimeout,
        Duration commandTimeout,
        Duration serverSelectionTimeout,
        int connections,
        int maxPending,
        int maxMessageBytes,
        boolean retryWrites,
        String appName) {

    public MongoConfig {
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("at least one host");
        }
        List<String> normalized = new ArrayList<>(hosts.size());
        for (String h : hosts) {
            normalized.add(normalize(h));
        }
        hosts = List.copyOf(normalized);
        if (database == null || database.isEmpty() || database.contains(".") || database.contains("$") || database.contains("/") || database.contains("\0")) {
            throw new IllegalArgumentException("a database name without . $ / or NUL is required");
        }
        if (connections < 1 || maxPending < 1 || maxMessageBytes < 1024) {
            throw new IllegalArgumentException("connections and maxPending must be positive, maxMessageBytes at least 1024");
        }
        if ((username == null) != (password == null)) {
            throw new IllegalArgumentException("username and password go together");
        }
        if (connectTimeout == null || serverSelectionTimeout == null) {
            throw new IllegalArgumentException("connectTimeout and serverSelectionTimeout are required");
        }
        authSource = authSource == null || authSource.isEmpty() ? "admin" : authSource;
    }

    /** {@code host:port} with the default port added; the host may be an IPv6 literal in brackets. */
    static String normalize(String hostPort) {
        String h = hostPort.trim();
        if (h.isEmpty()) {
            throw new IllegalArgumentException("an empty host");
        }
        int colon = h.lastIndexOf(':');
        if (h.startsWith("[")) {
            int close = h.indexOf(']');
            if (close < 0) {
                throw new IllegalArgumentException("bad IPv6 host: " + h);
            }
            return close == h.length() - 1 ? h + ":27017" : h;
        }
        if (colon < 0) {
            return h + ":27017";
        }
        int port = Integer.parseInt(h.substring(colon + 1));
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("bad port in " + h);
        }
        return h;
    }

    static String hostOf(String hostPort) {
        int colon = hostPort.lastIndexOf(':');
        String h = hostPort.substring(0, colon);
        return h.startsWith("[") ? h.substring(1, h.length() - 1) : h;
    }

    static int portOf(String hostPort) {
        return Integer.parseInt(hostPort.substring(hostPort.lastIndexOf(':') + 1));
    }

    /** Defaults for one server: no login, 10 s to connect, 30 s per command and to find a primary, 1 connection, 1000 commands pending per connection, messages up to 48 MB, writes retried. */
    public static MongoConfig of(String host, int port, String database) {
        return new MongoConfig(List.of(host + ":" + port), null, null, null, "admin", database, null, Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(30),
                1, 1000, 48 * 1024 * 1024, true, null);
    }

    /** Defaults for a seed list of {@code host:port} (or {@code host}, port 27017). */
    public static MongoConfig of(List<String> hosts, String database) {
        return of("x", 27017, database).withHosts(hosts);
    }

    /**
     * From a {@code mongodb://} URI: hosts, credentials (percent-encoded), the database path, and the options {@code replicaSet}, {@code authSource}, {@code tls}/{@code ssl},
     * {@code appName}, {@code connectTimeoutMS}, {@code socketTimeoutMS}, {@code serverSelectionTimeoutMS}, {@code maxPoolSize}, {@code retryWrites}. Other options are ignored.
     */
    public static MongoConfig parse(String uri) {
        String masked = uri.replaceAll("://[^@/]*@", "://***@");
        if (!uri.startsWith("mongodb://")) {
            throw new IllegalArgumentException("a mongodb:// URI is expected (mongodb+srv is not supported): " + masked);
        }
        String rest = uri.substring("mongodb://".length());
        String user = null, pw = null;
        int at = rest.indexOf('@');
        int slashOrQuery = firstOf(rest, '/', '?');
        if (at >= 0 && (slashOrQuery < 0 || at < slashOrQuery)) {
            String[] up = rest.substring(0, at).split(":", 2);
            user = URLDecoder.decode(up[0], StandardCharsets.UTF_8);
            pw = up.length > 1 ? URLDecoder.decode(up[1], StandardCharsets.UTF_8) : "";
            rest = rest.substring(at + 1);
        }
        int end = firstOf(rest, '/', '?');
        String hostList = end < 0 ? rest : rest.substring(0, end);
        List<String> hosts = new ArrayList<>();
        for (String h : hostList.split(",")) {
            hosts.add(h);
        }
        String db = "test";
        String query = null;
        if (end >= 0) {
            String tail = rest.substring(end);
            int q = tail.indexOf('?');
            String path = q < 0 ? tail : tail.substring(0, q);
            query = q < 0 ? null : tail.substring(q + 1);
            if (path.length() > 1) {
                db = URLDecoder.decode(path.substring(1), StandardCharsets.UTF_8);
            }
        }
        MongoConfig c;
        try {
            c = of(hosts, db);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(e.getMessage() + " in " + masked, e);
        }
        if (user != null) {
            c = c.withCredentials(user, pw);
        }
        if (query != null) {
            for (String kv : query.split("&")) {
                int eq = kv.indexOf('=');
                String k = eq < 0 ? kv : kv.substring(0, eq);
                String v = eq < 0 ? "" : URLDecoder.decode(kv.substring(eq + 1), StandardCharsets.UTF_8);
                switch (k.toLowerCase(java.util.Locale.ROOT)) {
                    case "replicaset" -> c = c.withReplicaSet(v);
                    case "authsource" -> c = c.withAuthSource(v);
                    case "tls", "ssl" -> {
                        if (Boolean.parseBoolean(v)) {
                            try {
                                c = c.withTls(SSLContext.getDefault());
                            } catch (java.security.NoSuchAlgorithmException e) {
                                throw new IllegalStateException(e);
                            }
                        }
                    }
                    case "appname" -> c = c.withAppName(v);
                    case "connecttimeoutms" -> c = c.withConnectTimeout(Duration.ofMillis(Long.parseLong(v)));
                    case "sockettimeoutms" -> c = c.withCommandTimeout(Duration.ofMillis(Long.parseLong(v)));
                    case "serverselectiontimeoutms" -> c = c.withServerSelectionTimeout(Duration.ofMillis(Long.parseLong(v)));
                    case "maxpoolsize" -> c = c.withConnections(Integer.parseInt(v));
                    case "retrywrites" -> c = c.withRetryWrites(Boolean.parseBoolean(v));
                    default -> {
                        // readPreference, w, journal, compressors...: not supported, ignored
                    }
                }
            }
        }
        return c;
    }

    private static int firstOf(String s, char a, char b) {
        int i = s.indexOf(a), j = s.indexOf(b);
        return i < 0 ? j : j < 0 ? i : Math.min(i, j);
    }

    /** The first seed's host. */
    public String host() {
        return hostOf(hosts.get(0));
    }

    /** The first seed's port. */
    public int port() {
        return portOf(hosts.get(0));
    }

    public MongoConfig withHosts(List<String> hs) {
        return new MongoConfig(hs, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    /** The replica set's name: members of another set are ignored. Null to take whatever the seeds belong to. */
    public MongoConfig withReplicaSet(String name) {
        return new MongoConfig(hosts, name, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withCredentials(String user, String pw) {
        return new MongoConfig(hosts, replicaSet, user, pw, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withAuthSource(String db) {
        return new MongoConfig(hosts, replicaSet, username, password, db, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withDatabase(String db) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, db, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withTls(SSLContext context) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, context, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withConnectTimeout(Duration d) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, d, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    /** Null for none. */
    public MongoConfig withCommandTimeout(Duration d) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, d, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withServerSelectionTimeout(Duration d) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, d, connections, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withConnections(int n) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, n, maxPending, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withMaxPending(int n) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, n, maxMessageBytes, retryWrites, appName);
    }

    public MongoConfig withMaxMessageBytes(int n) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, n, retryWrites, appName);
    }

    public MongoConfig withRetryWrites(boolean on) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, on, appName);
    }

    /** Shown in the server's logs and {@code currentOp}. */
    public MongoConfig withAppName(String name) {
        return new MongoConfig(hosts, replicaSet, username, password, authSource, database, sslContext, connectTimeout, commandTimeout, serverSelectionTimeout, connections, maxPending, maxMessageBytes, retryWrites, name);
    }

    /** Without the password. */
    @Override
    public String toString() {
        return "MongoConfig[hosts=" + hosts + ", replicaSet=" + replicaSet + ", username=" + username + ", password=" + (password == null ? "null" : "***") + ", authSource=" + authSource
                + ", database=" + database + ", tls=" + (sslContext != null) + ", connectTimeout=" + connectTimeout + ", commandTimeout=" + commandTimeout
                + ", serverSelectionTimeout=" + serverSelectionTimeout + ", connections=" + connections + ", maxPending=" + maxPending + ", maxMessageBytes=" + maxMessageBytes
                + ", retryWrites=" + retryWrites + ", appName=" + appName + "]";
    }
}
