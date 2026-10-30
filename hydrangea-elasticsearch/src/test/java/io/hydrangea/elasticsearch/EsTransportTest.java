package io.hydrangea.elasticsearch;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/** Node failover, error mapping and what goes on the wire, against small scripted HTTP servers (no Elasticsearch needed). */
class EsTransportTest {

    /** One request as a fake node saw it. */
    record Seen(String method, String target, Map<String, String> headers, String body) {
    }

    /** A fake node: answers every request with what {@code answer} says (status, content type, body); records what it saw. */
    final class FakeNode implements AutoCloseable {
        final ServerSocket server;
        final List<Seen> seen = new CopyOnWriteArrayList<>();
        volatile Function<Seen, String[]> answer;

        FakeNode(Function<Seen, String[]> answer) throws IOException {
            this.answer = answer;
            server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
            Thread.ofVirtual().start(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket s = server.accept();
                        Thread.ofVirtual().start(() -> serve(s));
                    } catch (IOException e) {
                        return;
                    }
                }
            });
        }

        String url() {
            return "http://127.0.0.1:" + server.getLocalPort();
        }

        private void serve(Socket s) {
            try (s) {
                s.setSoTimeout(5000);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.ISO_8859_1));
                OutputStream out = s.getOutputStream();
                for (;;) {
                    String line = in.readLine();
                    if (line == null) {
                        return;
                    }
                    String[] req = line.split(" ");
                    Map<String, String> headers = new java.util.LinkedHashMap<>();
                    int length = 0;
                    for (String h = in.readLine(); h != null && !h.isEmpty(); h = in.readLine()) {
                        int c = h.indexOf(':');
                        headers.put(h.substring(0, c).toLowerCase(java.util.Locale.ROOT), h.substring(c + 1).trim());
                    }
                    if (headers.containsKey("content-length")) {
                        length = Integer.parseInt(headers.get("content-length"));
                    }
                    char[] body = new char[length];
                    for (int got = 0; got < length; ) {
                        int n = in.read(body, got, length - got);
                        if (n < 0) {
                            return;
                        }
                        got += n;
                    }
                    Seen seenReq = new Seen(req[0], req[1], headers, new String(body));
                    seen.add(seenReq);
                    String[] a = answer.apply(seenReq);
                    if (a == null) {
                        return;                                    // hang up without an answer
                    }
                    byte[] bytes = a[2].getBytes(StandardCharsets.UTF_8);
                    out.write(("HTTP/1.1 " + a[0] + " X\r\nContent-Type: " + a[1] + "\r\nContent-Length: " + bytes.length + "\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
                    out.write(bytes);
                    out.flush();
                }
            } catch (IOException e) {
                // the client went away
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private static final Function<Seen, String[]> OK_JSON = r -> new String[]{"200", "application/json", "{\"ok\":true,\"target\":" + Json.write(r.target()) + "}"};

    private EventLoopGroup group;
    private final List<AutoCloseable> toClose = new ArrayList<>();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(1);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AutoCloseable c : toClose) {
            c.close();
        }
        group.close();
    }

    private FakeNode node(Function<Seen, String[]> answer) throws IOException {
        FakeNode n = new FakeNode(answer);
        toClose.add(n);
        return n;
    }

    private EsClient client(EsConfig config) {
        EsClient c = EsClient.create(group, config);
        toClose.add(c);
        return c;
    }

    private static EsException failure(CompletableFuture<?> f) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        return assertInstanceOf(EsException.class, e.getCause(), String.valueOf(e.getCause()));
    }

    /** A port nothing listens on. */
    private static int closedPort() throws IOException {
        try (ServerSocket s = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            return s.getLocalPort();
        }
    }

    @Test
    void aNodeThatRefusesConnectionsIsSkippedAndRemembered() throws Exception {
        FakeNode good = node(OK_JSON);
        int dead = closedPort();
        EsClient es = client(EsConfig.of("http://127.0.0.1:" + dead, good.url()).withNodeDeadTime(Duration.ofSeconds(30)));
        for (int i = 0; i < 6; i++) {
            EsResponse r = es.request("GET", "/").get(10, TimeUnit.SECONDS);
            assertEquals(true, r.at("ok"));
        }
        assertEquals(6, good.seen.size(), "every request ended up on the live node");
        assertEquals(1, group.metrics().counter("es.nodes.failures", null).value(), "the dead node was tried once, then left out");
    }

    @Test
    void aNodeThatHangsUpMidRequestIsRetriedOnTheNext() throws Exception {
        FakeNode good = node(OK_JSON);
        FakeNode hangsUp = node(r -> null);
        EsClient es = client(EsConfig.of(hangsUp.url(), good.url()));
        EsResponse r = es.request("POST", "/x/_doc/1", null, Json.obj("a", 1)).get(10, TimeUnit.SECONDS);
        assertEquals(true, r.at("ok"));
        assertEquals(1, hangsUp.seen.size());
        assertEquals(1, good.seen.size());
        assertEquals("{\"a\":1}", good.seen.get(0).body(), "the body went again, whole");
    }

    @Test
    void whenNoNodeAnswersTheRequestFailsWithStatusZero() throws Exception {
        int a = closedPort(), b = closedPort();
        EsClient es = client(EsConfig.of("http://127.0.0.1:" + a, "http://127.0.0.1:" + b));
        EsException e = failure(es.request("GET", "/"));
        assertEquals(0, e.status());
        assertTrue(e.isRetriable());
        assertTrue(e.getMessage().contains("2 tried"), e.getMessage());
        assertNotNull(e.getCause());
    }

    @Test
    void aTimeoutIsNotRetriedOnAnotherNode() throws Exception {
        FakeNode slow = node(r -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException ignored) {
                // closing
            }
            return OK_JSON.apply(r);
        });
        FakeNode other = node(OK_JSON);
        EsClient es = client(EsConfig.of(slow.url(), other.url()).withRequestTimeout(Duration.ofMillis(300)));
        EsException e = failure(es.request("POST", "/x/_doc", null, Json.obj("a", 1)));
        assertEquals(0, e.status());
        assertInstanceOf(java.net.SocketTimeoutException.class, e.getCause());
        assertEquals(0, other.seen.size(), "a write that timed out may have been applied: it must not be sent to another node");
    }

    @Test
    void errorAnswersBecomeExceptionsWithTheClustersType() throws Exception {
        FakeNode n = node(r -> switch (r.target()) {
            case "/typed" -> new String[]{"404", "application/json", "{\"error\":{\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index [x]\"}],\"type\":\"index_not_found_exception\",\"reason\":\"no such index [x]\"},\"status\":404}"};
            case "/string" -> new String[]{"400", "application/json", "{\"error\":\"just text\",\"status\":400}"};
            case "/html" -> new String[]{"502", "text/html", "<html>bad gateway</html>"};
            case "/garbage" -> new String[]{"500", "application/json", "{not json"};
            case "/redirect" -> new String[]{"302", "text/plain", ""};
            case "/okgarbage" -> new String[]{"200", "application/json", "{not json"};
            default -> new String[]{"429", "application/json", "{\"error\":{\"type\":\"es_rejected_execution_exception\",\"reason\":\"queue full\"}}"};
        });
        EsClient es = client(EsConfig.of(n.url()));
        EsException typed = failure(es.request("GET", "/typed"));
        assertEquals(404, typed.status());
        assertEquals("index_not_found_exception", typed.type());
        assertTrue(typed.isNotFound());
        assertTrue(typed.getMessage().contains("no such index [x]"), typed.getMessage());
        assertNotNull(Json.at(typed.body(), "error"));

        EsException str = failure(es.request("GET", "/string"));
        assertEquals(400, str.status());
        assertNull(str.type());
        assertTrue(str.getMessage().contains("just text"));

        EsException html = failure(es.request("GET", "/html"));
        assertEquals(502, html.status());
        assertEquals("<html>bad gateway</html>", html.body());

        EsException garbage = failure(es.request("GET", "/garbage"));
        assertEquals(500, garbage.status());
        assertEquals("{not json", garbage.body());

        EsException redirect = failure(es.request("GET", "/redirect"));
        assertEquals(302, redirect.status());

        EsException okGarbage = failure(es.request("GET", "/okgarbage"));
        assertEquals(0, okGarbage.status());
        assertTrue(okGarbage.getMessage().contains("unreadable"), okGarbage.getMessage());

        EsException busy = failure(es.request("GET", "/other"));
        assertEquals(429, busy.status());
        assertTrue(busy.isRetriable());
        assertEquals(1, n.seen.stream().filter(s -> s.target().equals("/other")).count(), "a 429 is reported, not silently retried by the plain client");
    }

    @Test
    void credentialsGoInTheHeaderToEveryNodeAndNowhereElse() throws Exception {
        FakeNode n = node(OK_JSON);
        EsClient basic = client(EsConfig.of(n.url()).withBasicAuth("elastic", "s3cret"));
        basic.request("GET", "/").get(10, TimeUnit.SECONDS);
        assertEquals("Basic " + java.util.Base64.getEncoder().encodeToString("elastic:s3cret".getBytes()), n.seen.get(0).headers().get("authorization"));
        EsClient apiKey = client(EsConfig.of(n.url()).withApiKey("id", "key"));
        apiKey.request("GET", "/").get(10, TimeUnit.SECONDS);
        assertEquals("ApiKey " + java.util.Base64.getEncoder().encodeToString("id:key".getBytes()), n.seen.get(1).headers().get("authorization"));
        EsClient none = client(EsConfig.of(n.url()));
        none.request("GET", "/").get(10, TimeUnit.SECONDS);
        assertNull(n.seen.get(2).headers().get("authorization"));

        String shown = basic.config().toString();
        assertFalse(shown.contains("s3cret"), shown);
        assertFalse(shown.contains("ZWxhc3RpYzpzM2NyZXQ="), "nor its base64: " + shown);
        assertTrue(shown.contains("Basic ***"), shown);
        assertThrows(IllegalArgumentException.class, () -> EsConfig.of("http://elastic:pw@localhost:9200"), "credentials in the URL are refused");
    }

    @Test
    void idsCannotChangeTheRouteAndParamsAreEncoded() throws Exception {
        FakeNode n = node(OK_JSON);
        EsClient es = client(EsConfig.of(n.url()));
        es.get("idx", "../../_cluster/settings").get(10, TimeUnit.SECONDS);
        assertEquals("/idx/_doc/..%2F..%2F_cluster%2Fsettings", n.seen.get(0).target());
        es.get("idx", "a?b=1&c=2#f").get(10, TimeUnit.SECONDS);
        assertEquals("/idx/_doc/a%3Fb%3D1%26c%3D2%23f", n.seen.get(1).target());
        es.index("idx", "sp ace", Json.obj(), Map.of("routing", "a&b=c", "refresh", "wait_for")).get(10, TimeUnit.SECONDS);
        String t = n.seen.get(2).target();
        assertTrue(t.startsWith("/idx/_doc/sp%20ace?"), t);
        assertTrue(t.contains("routing=a%26b%3Dc") && t.contains("refresh=wait_for"), t);
        es.search("logs-*,metrics-*", Json.obj()).get(10, TimeUnit.SECONDS);
        assertEquals("/logs-*,metrics-*/_search", n.seen.get(3).target(), "index lists and patterns keep their , and *");
        es.search("ü", Json.obj()).get(10, TimeUnit.SECONDS);
        assertEquals("/%C3%BC/_search", n.seen.get(4).target());
        es.request("GET", "/_cat/indices?v=true", Map.of("s", "index"), null).get(10, TimeUnit.SECONDS);
        assertEquals("/_cat/indices?v=true&s=index", n.seen.get(5).target());
        assertThrows(IllegalArgumentException.class, () -> es.get("idx", ""));
        assertThrows(IllegalArgumentException.class, () -> es.index("", "1", Json.obj()));
    }

    @Test
    void anOversizedAnswerFailsTheRequestInsteadOfFillingTheHeap() throws Exception {
        FakeNode n = node(r -> new String[]{"200", "application/json", "{\"big\":\"" + "x".repeat(200_000) + "\"}"});
        EsClient es = client(EsConfig.of(n.url()).withMaxResponseBytes(100_000));
        EsException e = failure(es.request("GET", "/"));
        assertEquals(0, e.status());
        assertNotNull(e.getCause(), e.getMessage());
        EsClient roomy = client(EsConfig.of(n.url()).withMaxResponseBytes(1_000_000));
        assertEquals(200_000, Json.str(roomy.request("GET", "/").get(10, TimeUnit.SECONDS).at("big")).length());
    }

    @Test
    void bulkIsNdjsonAndTheIndexerRetriesRejectedItems() throws Exception {
        int[] calls = {0};
        FakeNode n = node(r -> {
            calls[0]++;
            String[] lines = r.body().split("\n");
            StringBuilder items = new StringBuilder();
            for (int i = 0; i < lines.length; i += 2) {
                Map<String, Object> action = Json.map(Json.parse(lines[i]));
                String kind = action.keySet().iterator().next();
                String id = Json.str(Json.at(action, kind, "_id"));
                if (kind.equals("delete")) {
                    i--;                                           // no document line after a delete
                }
                boolean reject = "busy".equals(id) && calls[0] == 2;           // the indexer's first request: one item is rejected under load
                items.append(items.length() > 0 ? "," : "").append(reject
                        ? "{\"index\":{\"_index\":\"i\",\"_id\":\"busy\",\"status\":429,\"error\":{\"type\":\"es_rejected_execution_exception\",\"reason\":\"queue full\"}}}"
                        : "{\"index\":{\"_index\":\"i\",\"_id\":\"" + id + "\",\"status\":201,\"result\":\"created\",\"_version\":1,\"_seq_no\":" + i + "}}");
            }
            return new String[]{"200", "application/json", "{\"took\":1,\"errors\":true,\"items\":[" + items + "]}"};
        });
        EsClient es = client(EsConfig.of(n.url()));
        EsBulkResponse direct = es.bulk(List.of(BulkOp.index("i", "1", Json.obj("a", 1)), BulkOp.delete("i", "2"), BulkOp.update("i", "3", Json.obj("b", 2)).withRouting("r"))).get(10, TimeUnit.SECONDS);
        Seen s = n.seen.get(0);
        assertEquals("/_bulk", s.target());
        assertEquals("application/x-ndjson", s.headers().get("content-type"));
        assertEquals("{\"index\":{\"_index\":\"i\",\"_id\":\"1\"}}\n{\"a\":1}\n{\"delete\":{\"_index\":\"i\",\"_id\":\"2\"}}\n{\"update\":{\"_index\":\"i\",\"_id\":\"3\",\"routing\":\"r\"}}\n{\"doc\":{\"b\":2}}\n", s.body());
        assertEquals(3, direct.items().size());

        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withFlushInterval(null).withMaxRetries(2)
                .withBackoff(io.hydrangea.Backoff.defaults().withDelays(Duration.ofMillis(50), Duration.ofMillis(50))));
        CompletableFuture<EsBulkResponse.Item> fine = ix.add(BulkOp.index("i", "fine", Json.obj()));
        CompletableFuture<EsBulkResponse.Item> busy = ix.add(BulkOp.index("i", "busy", Json.obj()));
        ix.close().get(10, TimeUnit.SECONDS);
        assertEquals("created", fine.get(1, TimeUnit.SECONDS).result());
        assertEquals("created", busy.get(1, TimeUnit.SECONDS).result(), "rejected once, sent again, taken");
        assertEquals(3, n.seen.size(), "the direct bulk, the indexer's first request, the retry of the one rejected item");
        assertEquals(1, n.seen.get(2).body().split("\n").length / 2, "only the rejected item went again");
    }

    @Test
    void theIndexerGivesUpAfterMaxRetriesAndFailsOnTransportErrors() throws Exception {
        FakeNode n = node(r -> new String[]{"200", "application/json", "{\"took\":1,\"errors\":true,\"items\":[{\"index\":{\"_index\":\"i\",\"_id\":\"x\",\"status\":429,\"error\":{\"type\":\"es_rejected_execution_exception\",\"reason\":\"queue full\"}}}]}"});
        EsClient es = client(EsConfig.of(n.url()));
        EsBulkIndexer ix = es.bulkIndexer(EsBulkIndexer.Options.defaults().withFlushInterval(null).withMaxRetries(2).withBackoff(io.hydrangea.Backoff.defaults().withDelays(Duration.ofMillis(10), Duration.ofMillis(10))));
        CompletableFuture<EsBulkResponse.Item> f = ix.add(BulkOp.index("i", "x", Json.obj()));
        ix.close().get(10, TimeUnit.SECONDS);
        EsException e = assertInstanceOf(EsException.class, assertThrows(ExecutionException.class, () -> f.get(1, TimeUnit.SECONDS)).getCause());
        assertEquals(429, e.status());
        assertEquals(3, n.seen.size(), "the first send and two retries");

        int dead = closedPort();
        EsClient down = client(EsConfig.of("http://127.0.0.1:" + dead));
        EsBulkIndexer ix2 = down.bulkIndexer(EsBulkIndexer.Options.defaults().withFlushInterval(null).withMaxRetries(1).withBackoff(io.hydrangea.Backoff.defaults().withDelays(Duration.ofMillis(10), Duration.ofMillis(10))));
        CompletableFuture<EsBulkResponse.Item> f2 = ix2.add(BulkOp.index("i", "y", Json.obj()));
        ix2.close().get(10, TimeUnit.SECONDS);
        assertEquals(0, assertInstanceOf(EsException.class, assertThrows(ExecutionException.class, () -> f2.get(1, TimeUnit.SECONDS)).getCause()).status());
        assertEquals(0, ix2.pending());
    }

    @Test
    void requestBodiesAreGzippedFromOneKilobyteWhenAsked() throws Exception {
        FakeNode n = node(OK_JSON);
        EsClient es = client(EsConfig.of(n.url()).withRequestCompression(true));
        String big = "x".repeat(5000);
        es.index("i", "1", Json.obj("big", big)).get(10, TimeUnit.SECONDS);
        Seen s = n.seen.get(0);
        assertEquals("gzip", s.headers().get("content-encoding"));
        assertEquals("application/json", s.headers().get("content-type"));
        int length = Integer.parseInt(s.headers().get("content-length"));
        assertTrue(length < 200, "5 KB of x compress to a few dozen bytes: " + length);
        byte[] raw = s.body().getBytes(StandardCharsets.ISO_8859_1);
        try (java.util.zip.GZIPInputStream in = new java.util.zip.GZIPInputStream(new java.io.ByteArrayInputStream(raw))) {
            assertEquals(Json.write(Json.obj("big", big)), new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
        es.index("i", "2", Json.obj("small", 1)).get(10, TimeUnit.SECONDS);
        assertNull(n.seen.get(1).headers().get("content-encoding"), "under 1 KB: as it is");
        assertEquals("{\"small\":1}", n.seen.get(1).body());
        EsClient plain = client(EsConfig.of(n.url()));
        plain.index("i", "3", Json.obj("big", big)).get(10, TimeUnit.SECONDS);
        assertNull(n.seen.get(2).headers().get("content-encoding"), "off by default");
    }

    @Test
    void aClosedClientFailsFast() throws Exception {
        FakeNode n = node(OK_JSON);
        EsClient es = EsClient.create(group, EsConfig.of(n.url()));
        es.close();
        assertEquals(0, failure(es.request("GET", "/")).status());
    }
}
