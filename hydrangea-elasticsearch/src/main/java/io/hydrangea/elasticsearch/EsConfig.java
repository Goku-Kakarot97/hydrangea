package io.hydrangea.elasticsearch;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

/**
 * How to reach a cluster. {@link #of} takes the node URLs ({@code http://host:9200}, {@code https://...}); a request goes to the nodes in turn, and a node that could not
 * be reached is set aside for {@link #nodeDeadTime} while the others take its requests.
 * <ul>
 *   <li>Credentials: {@link #withBasicAuth} or {@link #withApiKey}; they travel in the {@code Authorization} header and are sent to the configured nodes only
 *       (the HTTP client drops the header on a redirect to another origin). {@link #toString} masks them.</li>
 *   <li>TLS: an {@code https} node uses {@link #withTls the given SSLContext} or the JDK default (which trusts public CAs, not a self-signed cluster certificate).</li>
 *   <li>{@link #maxResponseBytes} bounds every answer (searches with large {@code size}, {@code _source} of big documents): what is past it fails the request.</li>
 *   <li>{@link #compression}: ask for gzip answers; {@link #compressRequests}: gzip request bodies of 1 KB or more ({@code Content-Encoding: gzip}; the cluster
 *       accepts them unless {@code http.compression} was turned off). Cheaper on a slow network, dearer in CPU on a fast one; both off by default.</li>
 * </ul>
 *
 * @param nodes                 base URLs, at least one; no path, no credentials in the URL
 * @param authorization         the {@code Authorization} header value, or null
 * @param sslContext            for https nodes, or null for the JDK default
 * @param connectTimeout        per TCP connection
 * @param requestTimeout        per HTTP request, queueing in the connection pool included
 * @param maxConnectionsPerNode the connection pool size for one node (one request at a time per connection)
 * @param maxResponseBytes      the largest answer accepted
 * @param nodeDeadTime          how long a node that failed to answer is left out
 * @param compression           ask for compressed answers
 * @param compressRequests      gzip request bodies of 1 KB or more
 */
public record EsConfig(
        List<URI> nodes,
        String authorization,
        SSLContext sslContext,
        Duration connectTimeout,
        Duration requestTimeout,
        int maxConnectionsPerNode,
        int maxResponseBytes,
        Duration nodeDeadTime,
        boolean compression,
        boolean compressRequests) {

    public EsConfig {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("at least one node");
        }
        for (URI n : nodes) {
            String scheme = n.getScheme();
            if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https")) || n.getHost() == null) {
                throw new IllegalArgumentException("a node is an http(s) URL with a host: " + n);
            }
            if (n.getUserInfo() != null) {
                throw new IllegalArgumentException("credentials do not go in the URL (withBasicAuth): " + n.getHost());
            }
            if (n.getRawQuery() != null || n.getRawFragment() != null) {
                throw new IllegalArgumentException("a node URL has no query or fragment: " + n);
            }
        }
        nodes = List.copyOf(nodes);
        if (maxConnectionsPerNode < 1 || maxResponseBytes < 1) {
            throw new IllegalArgumentException("maxConnectionsPerNode and maxResponseBytes must be positive");
        }
        if (connectTimeout == null || requestTimeout == null || nodeDeadTime == null) {
            throw new IllegalArgumentException("timeouts are required");
        }
    }

    /** Defaults for these nodes: 5 s to connect, 30 s per request, 8 connections per node, answers up to 64 MB, a dead node left out for 30 s. */
    public static EsConfig of(String... nodeUrls) {
        List<URI> uris = new java.util.ArrayList<>(nodeUrls.length);
        for (String u : nodeUrls) {
            String t = u.trim();
            while (t.endsWith("/")) {
                t = t.substring(0, t.length() - 1);
            }
            uris.add(URI.create(t));
        }
        return new EsConfig(uris, null, null, Duration.ofSeconds(5), Duration.ofSeconds(30), 8, 64 << 20, Duration.ofSeconds(30), false, false);
    }

    public EsConfig withBasicAuth(String user, String password) {
        String token = Base64.getEncoder().encodeToString((user + ":" + password).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return withAuthorization("Basic " + token);
    }

    /** An API key as the console shows it: the base64 {@code encoded} value, or {@code id} and {@code key} separately. */
    public EsConfig withApiKey(String encoded) {
        return withAuthorization("ApiKey " + encoded);
    }

    public EsConfig withApiKey(String id, String key) {
        return withApiKey(Base64.getEncoder().encodeToString((id + ":" + key).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    /** Any other {@code Authorization} value (a bearer token). */
    public EsConfig withAuthorization(String value) {
        return new EsConfig(nodes, value, sslContext, connectTimeout, requestTimeout, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withTls(SSLContext context) {
        return new EsConfig(nodes, authorization, context, connectTimeout, requestTimeout, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withConnectTimeout(Duration d) {
        return new EsConfig(nodes, authorization, sslContext, d, requestTimeout, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withRequestTimeout(Duration d) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, d, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withMaxConnectionsPerNode(int n) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, requestTimeout, n, maxResponseBytes, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withMaxResponseBytes(int n) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, requestTimeout, maxConnectionsPerNode, n, nodeDeadTime, compression, compressRequests);
    }

    public EsConfig withNodeDeadTime(Duration d) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, requestTimeout, maxConnectionsPerNode, maxResponseBytes, d, compression, compressRequests);
    }

    public EsConfig withCompression(boolean on) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, requestTimeout, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, on, compressRequests);
    }

    public EsConfig withRequestCompression(boolean on) {
        return new EsConfig(nodes, authorization, sslContext, connectTimeout, requestTimeout, maxConnectionsPerNode, maxResponseBytes, nodeDeadTime, compression, on);
    }

    /** Without the credentials. */
    @Override
    public String toString() {
        return "EsConfig[nodes=" + nodes + ", authorization=" + (authorization == null ? "none" : authorization.split(" ", 2)[0] + " ***") + ", tls=" + (sslContext != null)
                + ", connectTimeout=" + connectTimeout + ", requestTimeout=" + requestTimeout + ", maxConnectionsPerNode=" + maxConnectionsPerNode
                + ", maxResponseBytes=" + maxResponseBytes + ", nodeDeadTime=" + nodeDeadTime + ", compression=" + compression + ", compressRequests=" + compressRequests + "]";
    }
}
