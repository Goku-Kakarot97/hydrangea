package io.hydrangea.elasticsearch;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Latency;
import io.hydrangea.Metrics;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * An Elasticsearch (and OpenSearch) client over the HTTP client: a pool of keep-alive connections per node, every call a {@link CompletableFuture} completed on an
 * event loop (continuations must not block), request bodies and answers as plain {@code Map}/{@code List} JSON ({@link Json}).
 * <pre>{@code
 * EsClient es = EsClient.create(group, EsConfig.of("http://localhost:9200").withBasicAuth("elastic", password));
 * es.index("products", "1", Map.of("name", "lamp", "price", 20)).join();
 * es.refresh("products").join();
 * EsSearchResponse r = es.search("products", Json.obj("query", Json.obj("match", Json.obj("name", "lamp")))).join();
 * r.hits().forEach(h -> System.out.println(h.id() + " " + h.source()));
 * }</pre>
 * <ul>
 *   <li><b>Nodes</b>: requests go round-robin over the configured nodes. When a node cannot be reached (connection refused, name unknown, the connection dropped
 *       before an answer) the request is sent to the next node and the failed one is left out for {@link EsConfig#nodeDeadTime}. A request timeout is not retried
 *       (the node may be working on it). As with every Elasticsearch client, a request that the node had already applied when the connection dropped is applied
 *       again on the next node: give writes an id (or {@code if_seq_no}) when that matters.</li>
 *   <li><b>Errors</b>: an error status becomes an {@link EsException} with the status and the cluster's {@code error.type}/{@code reason}; what is normal for a
 *       call is not an error ({@link #get} of a missing id, {@link #delete} of one, {@link #exists}).</li>
 *   <li><b>Typed calls</b> cover documents, search, scroll, count, bulk and index management; {@link #request} sends anything else (aliases, mappings, ingest
 *       pipelines, SQL, ...) and gives the parsed answer.</li>
 *   <li><b>Metrics</b> on the group: {@code es.requests{outcome=ok|error|failed}}, {@code es.request.duration}, {@code es.nodes.failures}; the HTTP client's own
 *       {@code http.client.*} underneath.</li>
 * </ul>
 */
public final class EsClient implements AutoCloseable {

    private volatile io.hydrangea.observe.CallObserver<String, EsResponse> observer;

    /**
     * Who is told about every request (as {@code METHOD /path}; one per logical request, retries across nodes included): {@code started} on the calling thread before it goes out, {@code ended} with the answer or the
     * failure. The client records no span itself. {@code null} (the default) observes nothing.
     */
    public EsClient observer(io.hydrangea.observe.CallObserver<String, EsResponse> observer) {
        this.observer = observer;
        return this;
    }

    private io.hydrangea.observe.CallObserver.Observation<EsResponse> observe(String request) {
        io.hydrangea.observe.CallObserver<String, EsResponse> o = observer;
        if (o == null) {
            return null;
        }
        try {
            return o.started(request);
        } catch (Throwable t) {
            return null;
        }
    }

    private static <A> void observed(java.util.concurrent.CompletableFuture<? extends A> f, io.hydrangea.observe.CallObserver.Observation<A> o) {
        if (o != null) {
            f.whenComplete((r, err) -> {
                try {
                    o.ended(r, err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err);
                } catch (Throwable ignored) {
                    // an observer must not break the client
                }
            });
        }
    }

    private static final String JSON = "application/json";
    private static final String NDJSON = "application/x-ndjson";
    private static final int[] OK = {};
    private static final int[] OK_OR_404 = {404};

    private final EsConfig config;
    private final HttpClient http;
    private final List<URI> nodes;
    private final AtomicLongArray deadUntil;                       // System.nanoTime() until which the node is left out; 0 = fine
    private final AtomicInteger next = new AtomicInteger();
    private final Metrics.Count mOk, mError, mFailed, mNodeFailures;
    private final Latency mDuration;
    private volatile boolean closed;

    private EsClient(EventLoopGroup group, EsConfig config) {
        this.config = config;
        this.nodes = config.nodes();
        this.deadUntil = new AtomicLongArray(nodes.size());
        HttpClientConfig hc = HttpClientConfig.defaults()
                .withConnectTimeout(config.connectTimeout())
                .withRequestTimeout(config.requestTimeout())
                .withIdleTimeout(config.requestTimeout().plusSeconds(5))
                .withMaxConnectionsPerHost(config.maxConnectionsPerNode())
                .withMaxIdlePerHost(config.maxConnectionsPerNode())
                .withMaxResponseBytes(config.maxResponseBytes())
                .withMaxRedirects(0)                                 // a cluster does not redirect; an answer that does is handed over as the error it is
                .withDecompression(config.compression())
                .withUserAgent("hydrangea-elasticsearch");
        if (config.sslContext() != null) {
            hc = hc.withSslContext(config.sslContext());
        }
        this.http = new HttpClient(group, hc);
        Metrics m = group.metrics();
        mOk = m.counter("es.requests", null, "outcome", "ok");
        mError = m.counter("es.requests", null, "outcome", "error");
        mFailed = m.counter("es.requests", null, "outcome", "failed");
        mNodeFailures = m.counter("es.nodes.failures", null);
        mDuration = m.latency("es.request.duration");
    }

    /** A client; no request is made until the first call. */
    public static EsClient create(EventLoopGroup group, EsConfig config) {
        return new EsClient(group, config);
    }

    /** A client that has checked the cluster answers ({@link #info}): the future fails if no node does or the credentials are refused. */
    public static CompletableFuture<EsClient> connect(EventLoopGroup group, EsConfig config) {
        EsClient c = new EsClient(group, config);
        return c.info().handle((info, err) -> {
            if (err != null) {
                c.close();
                throw err instanceof CompletionException ce ? ce : new CompletionException(err);
            }
            return c;
        });
    }

    public EsConfig config() {
        return config;
    }

    /** A loop of the group, for timers built on top of the client. */
    public EventLoop eventLoop() {
        return http.eventLoop();
    }

    /** Closes the connections; requests under way fail. */
    @Override
    public void close() {
        closed = true;
        http.close();
    }

    // ---- cluster ----

    /** {@code GET /}: cluster name, version ({@code version.number}), tagline. */
    public CompletableFuture<Map<String, Object>> info() {
        return request("GET", "/").thenApply(r -> Json.map(r.json()));
    }

    /** {@code GET /_cluster/health}: {@code status} green/yellow/red, node and shard counts. */
    public CompletableFuture<Map<String, Object>> health() {
        return request("GET", "/_cluster/health").thenApply(r -> Json.map(r.json()));
    }

    // ---- indices ----

    /** Creates an index with {@code settings}/{@code mappings}/{@code aliases} in {@code body} (null for the defaults). Fails with 400 {@code resource_already_exists_exception} if it exists. */
    public CompletableFuture<Void> createIndex(String index, Object body) {
        return request("PUT", "/" + encodeIndex(index), null, body).thenApply(r -> null);
    }

    /** Deletes the index (or indices: a comma list or wildcard); fails with 404 when there is none. */
    public CompletableFuture<Void> deleteIndex(String index) {
        return request("DELETE", "/" + encodeIndex(index)).thenApply(r -> null);
    }

    public CompletableFuture<Boolean> indexExists(String index) {
        return exchange("HEAD", "/" + encodeIndex(index), null, null, null, OK_OR_404).thenApply(r -> r.status() == 200);
    }

    /** Makes the writes so far visible to search (a write is searchable within {@code refresh_interval}, 1 s by default, without this). */
    public CompletableFuture<Void> refresh(String index) {
        return request("POST", "/" + encodeIndex(index) + "/_refresh").thenApply(r -> null);
    }

    // ---- documents ----

    /** Writes the document under {@code id} (replacing an existing one), or under an id the cluster makes up when {@code id} is null. */
    public CompletableFuture<EsWriteResponse> index(String index, String id, Object document) {
        return index(index, id, document, null);
    }

    /** With parameters: {@code refresh=true|wait_for}, {@code routing}, {@code if_seq_no}/{@code if_primary_term}, {@code pipeline}, {@code op_type=create}, ... */
    public CompletableFuture<EsWriteResponse> index(String index, String id, Object document, Map<String, String> params) {
        String path = "/" + encodeIndex(index) + "/_doc" + (id == null ? "" : "/" + encodeId(id));
        return request(id == null ? "POST" : "PUT", path, params, document).thenApply(r -> EsWriteResponse.of(r.json()));
    }

    /** Writes the document; fails with 409 if the id exists. */
    public CompletableFuture<EsWriteResponse> create(String index, String id, Object document) {
        return create(index, id, document, null);
    }

    public CompletableFuture<EsWriteResponse> create(String index, String id, Object document, Map<String, String> params) {
        return request("PUT", "/" + encodeIndex(index) + "/_create/" + encodeId(id), params, document).thenApply(r -> EsWriteResponse.of(r.json()));
    }

    /** The document, or one with {@link EsDocument#found} false. */
    public CompletableFuture<EsDocument> get(String index, String id) {
        return get(index, id, (Map<String, String>) null);
    }

    /** With parameters: {@code _source=false}, {@code _source_includes=a,b}, {@code routing}, {@code preference}, {@code realtime=false}, ... */
    public CompletableFuture<EsDocument> get(String index, String id, Map<String, String> params) {
        return exchange("GET", "/" + encodeIndex(index) + "/_doc/" + encodeId(id), params, null, null, OK_OR_404).thenApply(r -> EsDocument.of(r.json()));
    }

    /** The document as a record ({@link Json#to}), or null when there is none. */
    public <T> CompletableFuture<T> get(String index, String id, Class<T> type) {
        return get(index, id, (Map<String, String>) null).thenApply(d -> d.source(type));
    }

    public CompletableFuture<Boolean> exists(String index, String id) {
        return exchange("HEAD", "/" + encodeIndex(index) + "/_doc/" + encodeId(id), null, null, null, OK_OR_404).thenApply(r -> r.status() == 200);
    }

    /** Merges {@code partialDocument} into the document; fails with 404 when there is none ({@link #updateWith} with {@code doc_as_upsert} to create it then). */
    public CompletableFuture<EsWriteResponse> update(String index, String id, Object partialDocument) {
        return updateWith(index, id, Map.of("doc", partialDocument), null);
    }

    public CompletableFuture<EsWriteResponse> update(String index, String id, Object partialDocument, Map<String, String> params) {
        return updateWith(index, id, Map.of("doc", partialDocument), params);
    }

    /** An update with the whole update body: {@code doc}, {@code script}, {@code upsert}, {@code doc_as_upsert}, {@code detect_noop}... */
    public CompletableFuture<EsWriteResponse> updateWith(String index, String id, Map<String, Object> updateBody, Map<String, String> params) {
        return request("POST", "/" + encodeIndex(index) + "/_update/" + encodeId(id), params, updateBody).thenApply(r -> EsWriteResponse.of(r.json()));
    }

    /** Deletes the document; {@link EsWriteResponse#result} is {@code not_found} when there was none (no exception). */
    public CompletableFuture<EsWriteResponse> delete(String index, String id) {
        return delete(index, id, null);
    }

    public CompletableFuture<EsWriteResponse> delete(String index, String id, Map<String, String> params) {
        return exchange("DELETE", "/" + encodeIndex(index) + "/_doc/" + encodeId(id), params, null, null, OK_OR_404).thenApply(r -> EsWriteResponse.of(r.json()));
    }

    // ---- search ----

    /** The sources of a search as records, when the hits are all that matters ({@link EsSearchResponse#sources} keeps the total and the aggregations too). */
    public <T> CompletableFuture<List<T>> search(String index, Object body, Class<T> type) {
        return search(index, body, (Map<String, String>) null).thenApply(r -> r.sources(type));
    }

    /** A search of {@code index} (a name, a comma list, a wildcard, or null for every index) with the request {@code body} ({@code query}, {@code size}, {@code sort}, {@code aggs}, ...). */
    public CompletableFuture<EsSearchResponse> search(String index, Object body) {
        return search(index, body, (Map<String, String>) null);
    }

    /** With parameters: {@code routing}, {@code preference}, {@code scroll}, {@code track_total_hits}, {@code ignore_unavailable}, ... */
    public CompletableFuture<EsSearchResponse> search(String index, Object body, Map<String, String> params) {
        return request("POST", (index == null ? "" : "/" + encodeIndex(index)) + "/_search", params, body == null ? Map.of() : body).thenApply(r -> EsSearchResponse.of(r.json()));
    }

    /** The number of documents matching {@code body} (a {@code query} object; null for all). Exact, unlike a search's {@code total}. */
    public CompletableFuture<Long> count(String index, Object body) {
        return request("POST", (index == null ? "" : "/" + encodeIndex(index)) + "/_count", null, body == null ? Map.of() : body).thenApply(r -> Json.lng(r.at("count")));
    }

    /**
     * Every hit of a search, page by page; {@code body} carries the page {@code size} (1000 is a usual choice). Each page must arrive within {@code keepAlive} of
     * the previous one. See {@link EsScroll}.
     */
    public EsScroll scroll(String index, Object body, Duration keepAlive) {
        return new EsScroll(this, index, body, keepAlive);
    }

    /** Deletes what matches {@code body} (a {@code query} object); the number deleted. Runs to the end before answering ({@code wait_for_completion}). */
    public CompletableFuture<Long> deleteByQuery(String index, Object body) {
        return request("POST", "/" + encodeIndex(index) + "/_delete_by_query", null, body).thenApply(r -> Json.lng(r.at("deleted")));
    }

    // ---- bulk ----

    /** Sends the actions in one request. The future succeeds when the cluster answered, even if items failed: check {@link EsBulkResponse#errors}. */
    public CompletableFuture<EsBulkResponse> bulk(List<BulkOp> ops) {
        return bulk(ops, null);
    }

    /** With parameters: {@code refresh}, {@code routing}, {@code pipeline}, {@code timeout}... */
    public CompletableFuture<EsBulkResponse> bulk(List<BulkOp> ops, Map<String, String> params) {
        if (ops.isEmpty()) {
            return CompletableFuture.completedFuture(new EsBulkResponse(0, false, List.of()));
        }
        StringBuilder sb = new StringBuilder(ops.size() * 128);
        for (BulkOp op : ops) {
            op.writeTo(sb);
        }
        return exchange("POST", "/_bulk", params, sb.toString().getBytes(StandardCharsets.UTF_8), NDJSON, OK).thenApply(r -> EsBulkResponse.of(r.json()));
    }

    /** An indexer that gathers actions into bulk requests of a given size, sends them as they fill or on a timer, and retries rejected ones. */
    public EsBulkIndexer bulkIndexer(EsBulkIndexer.Options options) {
        return new EsBulkIndexer(this, options);
    }

    // ---- raw ----

    /** A request without body; {@code path} is the already-encoded path, with its query string if any ({@code /_cat/indices?v=true}). */
    public CompletableFuture<EsResponse> request(String method, String path) {
        return request(method, path, null, null);
    }

    /**
     * Any request. {@code params} become the query string (encoded here), {@code body} is written as JSON (a {@code Map}, {@code List}, or a {@link String}
     * taken as JSON text already); the answer is parsed. An error status fails the future with {@link EsException}.
     */
    public CompletableFuture<EsResponse> request(String method, String path, Map<String, String> params, Object body) {
        byte[] bytes = body == null ? null : body instanceof String s ? s.getBytes(StandardCharsets.UTF_8) : Json.writeBytes(body);
        return exchange(method, path, params, bytes, bytes == null ? null : JSON, OK);
    }

    /** A request with any body and content type ({@code text/plain} for SQL, {@code application/x-ndjson} for {@code _msearch}). */
    public CompletableFuture<EsResponse> request(String method, String path, Map<String, String> params, byte[] body, String contentType) {
        return exchange(method, path, params, body, contentType, OK);
    }

    // ---- transport ----

    private CompletableFuture<EsResponse> exchange(String method, String path, Map<String, String> params, byte[] body, String contentType, int[] acceptedErrors) {
        if (closed) {
            return CompletableFuture.failedFuture(EsException.transport("client is closed", null));
        }
        String target = params == null || params.isEmpty() ? path : path + (path.indexOf('?') >= 0 ? "&" : "?") + query(params);
        String encoding = null;
        if (body != null && config.compressRequests() && body.length >= COMPRESS_FROM) {
            body = gzip(body);
            encoding = "gzip";
        }
        long start = mDuration.begin();
        CompletableFuture<EsResponse> result = new CompletableFuture<>();
        Attempt a = new Attempt(method, target, body, contentType, encoding, result);
        if (observer != null) {      // one observation for the logical request, however many nodes it takes
            observed(result, observe(method + " " + path));
        }
        result.whenComplete((r, err) -> {
            mDuration.end(start);
            if (err == null) {
                mOk.increment();
            } else if (err instanceof EsException e && e.status() != 0) {
                mError.increment();
            } else {
                mFailed.increment();
                CompletableFuture<?> inner = a.inFlight;
                if (inner != null && result.isCancelled()) {
                    inner.cancel(true);
                }
            }
        });
        a.acceptedErrors = acceptedErrors;
        a.send(0, null);
        return result;
    }

    /** Bodies shorter than this go uncompressed even when compression is on: the gzip header and the CPU would cost more than the bytes saved. */
    static final int COMPRESS_FROM = 1024;

    private static byte[] gzip(byte[] body) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(body.length / 4 + 64);
        try (java.util.zip.GZIPOutputStream gz = new java.util.zip.GZIPOutputStream(out, 8192)) {
            gz.write(body);
        } catch (IOException e) {
            throw new IllegalStateException(e);                    // a memory stream does not fail
        }
        return out.toByteArray();
    }

    /** One logical request, over as many nodes as it takes. */
    private final class Attempt {
        final String method, target, contentType, encoding;
        final byte[] body;
        final CompletableFuture<EsResponse> result;
        int[] acceptedErrors;
        volatile CompletableFuture<ClientResponse> inFlight;

        Attempt(String method, String target, byte[] body, String contentType, String encoding, CompletableFuture<EsResponse> result) {
            this.method = method;
            this.target = target;
            this.body = body;
            this.contentType = contentType;
            this.encoding = encoding;
            this.result = result;
        }

        void send(int tried, Throwable last) {
            if (result.isDone()) {
                return;
            }
            if (tried >= nodes.size()) {
                result.completeExceptionally(EsException.transport("no node answered (" + nodes.size() + " tried): " + last, last));
                return;
            }
            int node = pickNode();
            ClientRequest req = ClientRequest.of(method, nodes.get(node) + target).header("Accept", JSON);
            if (config.authorization() != null) {
                req = req.header("Authorization", config.authorization());
            }
            if (body != null) {
                req = req.header("Content-Type", contentType).body(body);
                if (encoding != null) {
                    req = req.header("Content-Encoding", encoding);
                }
            }
            CompletableFuture<ClientResponse> f = http.send(req);
            inFlight = f;
            f.whenComplete((resp, err) -> {
                inFlight = null;
                if (err != null) {
                    Throwable e = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
                    if (result.isCancelled() || closed) {
                        result.completeExceptionally(EsException.transport(closed ? "client is closed" : "cancelled", e));
                        return;
                    }
                    if (e instanceof SocketTimeoutException || !(e instanceof IOException)) {
                        // a timeout: the node may be working on the request, another node would double it; a bug: no point in another node
                        result.completeExceptionally(EsException.transport(String.valueOf(e.getMessage()), e));
                        return;
                    }
                    markDead(node);
                    send(tried + 1, e);
                    return;
                }
                deadUntil.set(node, 0);
                try {
                    result.complete(parse(resp, acceptedErrors));
                } catch (EsException e) {
                    result.completeExceptionally(e);
                } catch (RuntimeException e) {
                    result.completeExceptionally(EsException.transport("unreadable answer: " + e.getMessage(), e));
                }
            });
        }
    }

    private int pickNode() {
        int n = nodes.size();
        int first = Math.floorMod(next.getAndIncrement(), n);
        long now = System.nanoTime();
        for (int i = 0; i < n; i++) {
            int idx = (first + i) % n;
            long dead = deadUntil.get(idx);
            if (dead == 0 || now - dead >= 0) {
                return idx;
            }
        }
        return first;                                              // every node is marked dead: try one anyway, a success clears the mark
    }

    private void markDead(int node) {
        mNodeFailures.increment();
        if (nodes.size() > 1) {
            deadUntil.set(node, System.nanoTime() + config.nodeDeadTime().toNanos());
        }
    }

    private static EsResponse parse(ClientResponse resp, int[] acceptedErrors) {
        String type = resp.header("content-type");
        boolean json = type != null && type.toLowerCase(java.util.Locale.ROOT).contains("json") && resp.body().length > 0;
        Object parsed = null;
        String text = null;
        if (json) {
            try {
                parsed = Json.parse(resp.bodyAsString());
            } catch (IllegalArgumentException e) {
                if (resp.isSuccessful()) {
                    throw e;
                }
                text = resp.bodyAsString();                        // an error page that calls itself JSON: keep the text for the exception
            }
        } else if (resp.body().length > 0) {
            text = resp.bodyAsString();
        }
        if (resp.status() >= 400) {
            for (int ok : acceptedErrors) {
                if (ok == resp.status()) {
                    return new EsResponse(resp.status(), resp.headers(), parsed, text);
                }
            }
            throw EsException.of(resp.status(), parsed != null ? parsed : text);
        }
        if (resp.status() >= 300) {
            throw new EsException(resp.status(), null, "HTTP " + resp.status() + " (a redirect: not a cluster node?)", parsed != null ? parsed : text, null);
        }
        return new EsResponse(resp.status(), resp.headers(), parsed, text);
    }

    // ---- encoding ----

    /** An index name, list or pattern as a path segment: {@code ,} and {@code *} kept, everything else outside the unreserved set percent-encoded. */
    static String encodeIndex(String index) {
        if (index == null || index.isEmpty()) {
            throw new IllegalArgumentException("index is required");
        }
        return encode(index, ",*");
    }

    /** A document id as a path segment: everything outside the unreserved set percent-encoded ({@code /} included, so an id cannot change the route). */
    static String encodeId(String id) {
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("id is required");
        }
        return encode(id, "");
    }

    static String query(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (sb.length() > 0) {
                sb.append('&');
            }
            sb.append(encode(e.getKey(), "")).append('=').append(encode(String.valueOf(e.getValue()), ""));
        }
        return sb.toString();
    }

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    static String encode(String s, String alsoKept) {
        StringBuilder sb = null;
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xFF;
            boolean keep = (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '-' || b == '.' || b == '_' || b == '~' || alsoKept.indexOf(b) >= 0;
            if (keep) {
                if (sb != null) {
                    sb.append((char) b);
                }
            } else {
                if (sb == null) {
                    sb = new StringBuilder(bytes.length + 16).append(s, 0, i);  // s is ASCII up to here: the kept bytes are single characters
                }
                sb.append('%').append(HEX[b >> 4]).append(HEX[b & 15]);
            }
        }
        return sb == null ? s : sb.toString();
    }
}
