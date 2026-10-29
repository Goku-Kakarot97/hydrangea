package io.hydrangea;

import io.hydrangea.http.Headers;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.HttpStatusException;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RouterTest {

    private static HttpRequest req(String method, String target) {
        return new HttpRequest(method, target, "HTTP/1.1", new Headers().add("Host", "x"), new byte[0]);
    }

    private static HttpRequest form(String target, String body) {
        return new HttpRequest("POST", target, "HTTP/1.1",
                new Headers().add("Host", "x").add("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8"),
                body.getBytes(StandardCharsets.ISO_8859_1));
    }

    private static HttpResponse call(HttpRouter router, HttpRequest request) throws Exception {
        return router.handle(request).toCompletableFuture().get(2, TimeUnit.SECONDS);
    }

    private static String body(HttpResponse r) {
        return new String(r.body(), StandardCharsets.UTF_8);
    }

    private static HttpRouter router() {
        return new HttpRouter()
                .get("/", r -> HttpResponse.ok("root"))
                .get("/users/{id}", r -> HttpResponse.ok("user:" + r.pathVariable("id")))
                .get("/users/me", r -> HttpResponse.ok("me"))
                .get("/users/{userId}/posts/{postId}", r -> HttpResponse.ok(r.pathVariable("userId") + "/" + r.pathVariable("postId")))
                .get("/files/{path...}", r -> HttpResponse.ok("file:" + r.pathVariable("path")))
                .post("/users/{id}", r -> HttpResponse.ok("created:" + r.pathVariable("id")))
                .get("/num/{n}", r -> HttpResponse.ok("n+1=" + (r.pathInt("n") + 1)))
                .get("/search", r -> HttpResponse.ok(
                        "q=" + r.param("q", "-") + " tags=" + r.params("tag") + " limit=" + r.paramInt("limit", 10)
                                + " all=" + r.paramBool("all", false)))
                .post("/login", r -> HttpResponse.ok(r.requiredParam("user") + ":" + r.param("lang", "en")))
                .get("/gone", r -> { throw new HttpStatusException(410, "it is gone"); })
                .get("/bug", r -> { throw new IllegalStateException("kaboom"); })
                .getAsync("/async/{x}", r -> CompletableFuture.supplyAsync(() -> HttpResponse.ok("async:" + r.pathVariable("x"))))
                .getAsync("/async-status", r -> CompletableFuture.failedFuture(new HttpStatusException(404, "nope")));
    }

    // ---- path variables ----

    @Test
    void pathVariables() throws Exception {
        var r = router();
        assertEquals("root", body(call(r, req("GET", "/"))));
        assertEquals("user:42", body(call(r, req("GET", "/users/42"))));
        assertEquals("7/99", body(call(r, req("GET", "/users/7/posts/99"))));
        assertEquals("n+1=6", body(call(r, req("GET", "/num/5"))));
    }

    @Test
    void literalBeatsVariableRegardlessOfRegistrationOrder() throws Exception {
        assertEquals("me", body(call(router(), req("GET", "/users/me"))));
        var reversed = new HttpRouter()
                .get("/users/me", r -> HttpResponse.ok("me"))
                .get("/users/{id}", r -> HttpResponse.ok("id"));
        assertEquals("me", body(call(reversed, req("GET", "/users/me"))));
        assertEquals("id", body(call(reversed, req("GET", "/users/mee"))));
    }

    @Test
    void catchAllTakesRestOfPath() throws Exception {
        var r = router();
        assertEquals("file:a", body(call(r, req("GET", "/files/a"))));
        assertEquals("file:a/b/c.txt", body(call(r, req("GET", "/files/a/b/c.txt"))));
        assertEquals(404, call(r, req("GET", "/files")).status(), "catch-all needs at least one segment");
    }

    @Test
    void pathIsPercentDecodedAfterSplitting() throws Exception {
        var r = router();
        assertEquals("user:a b", body(call(r, req("GET", "/users/a%20b"))));
        assertEquals("user:a/b", body(call(r, req("GET", "/users/a%2Fb"))), "%2F must not create a new segment");
        assertEquals("user:a+b", body(call(r, req("GET", "/users/a+b"))), "'+' is literal in paths");
        assertEquals("user:Việt", body(call(r, req("GET", "/users/Vi%E1%BB%87t"))));
        assertEquals(400, call(r, req("GET", "/users/%zz")).status());
        assertEquals(400, call(r, req("GET", "/users/%C3%28")).status(), "invalid UTF-8");
        assertEquals(400, call(r, req("GET", "/users/%4")).status(), "truncated escape");
    }

    @Test
    void trailingSlashIgnoredAndEmptyVariableRejected() throws Exception {
        var r = router();
        assertEquals("user:5", body(call(r, req("GET", "/users/5/"))));
        assertEquals(404, call(r, req("GET", "/users//posts/3")).status());
    }

    @Test
    void typedPathVariableErrorIs400() throws Exception {
        var res = call(router(), req("GET", "/num/abc"));
        assertEquals(400, res.status());
        assertTrue(body(res).contains("'n'"));
    }

    // ---- request parameters ----

    @Test
    void queryParameters() throws Exception {
        var r = router();
        assertEquals("q=hello world tags=[a, b] limit=5 all=true",
                body(call(r, req("GET", "/search?q=hello+world&tag=a&tag=b&limit=5&all"))));
        assertEquals("q=- tags=[] limit=10 all=false", body(call(r, req("GET", "/search"))));
        assertEquals("q=a&b tags=[] limit=10 all=false", body(call(r, req("GET", "/search?q=a%26b"))));
        assertEquals("q=x tags=[] limit=10 all=false", body(call(r, req("GET", "/search?&&q=x&&"))), "empty pairs ignored");
        assertEquals("q=Việt tags=[] limit=10 all=false", body(call(r, req("GET", "/search?q=Vi%E1%BB%87t"))));
    }

    @Test
    void badParameterValuesAre400() throws Exception {
        var r = router();
        assertEquals(400, call(r, req("GET", "/search?limit=ten")).status());
        assertEquals(400, call(r, req("GET", "/search?all=maybe")).status());
        assertEquals(400, call(r, req("GET", "/search?q=%zz")).status());
    }

    @Test
    void formBodyParametersAreMergedWithQuery() throws Exception {
        var r = router();
        assertEquals("sinh:vi", body(call(r, form("/login?lang=vi", "user=sinh&x=1"))));
        assertEquals("sinh:en", body(call(r, form("/login", "user=sinh"))));
        var missing = call(r, form("/login", "other=1"));
        assertEquals(400, missing.status());
        assertTrue(body(missing).contains("'user'"));
        // a body is only parsed as a form when the content type says so
        var json = new HttpRequest("POST", "/login", "HTTP/1.1", new Headers().add("Host", "x").add("Content-Type", "application/json"),
                "user=sinh".getBytes(StandardCharsets.UTF_8));
        assertEquals(400, call(r, json).status());
    }

    @Test
    void tooManyParametersRejected() throws Exception {
        StringBuilder q = new StringBuilder("/search?");
        for (int i = 0; i < 1001; i++) q.append('p').append(i).append("=1&");
        assertEquals(400, call(router(), req("GET", q.toString())).status());
    }

    // ---- method handling ----

    @Test
    void wrongMethodGives405WithAllowHeader() throws Exception {
        var res = call(router(), req("DELETE", "/users/5"));
        assertEquals(405, res.status());
        assertEquals("GET, HEAD, POST", res.headers().get("Allow"));
    }

    @Test
    void headUsesGetRoute() throws Exception {
        assertEquals("user:5", body(call(router(), req("HEAD", "/users/5")))); // the server strips the body on the wire
    }

    @Test
    void unknownPathIs404AndCanBeCustomised() throws Exception {
        assertEquals(404, call(router(), req("GET", "/nothing")).status());
        var r = router().notFound(rr -> HttpResponse.status(404).text("no such page: " + rr.path()));
        assertEquals("no such page: /zzz", body(call(r, req("GET", "/zzz"))));
    }

    @Test
    void nonOriginFormTargetIs400() throws Exception {
        assertEquals(400, call(router(), req("GET", "http://evil/x")).status());
    }

    // ---- errors ----

    @Test
    void statusExceptionBecomesResponseOtherExceptionsPropagate() throws Exception {
        var r = router();
        var gone = call(r, req("GET", "/gone"));
        assertEquals(410, gone.status());
        assertEquals("it is gone", body(gone));
        assertThrows(IllegalStateException.class, () -> r.handle(req("GET", "/bug")));
        assertEquals(404, call(r, req("GET", "/async-status")).status());
        assertEquals("async:z", body(call(r, req("GET", "/async/z"))));
    }

    // ---- registration validation ----

    @Test
    void invalidPatternsAndDuplicatesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("users", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("/a//b", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("/a/{}", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("/a/{x}/{x}", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("/a/{rest...}/b", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter().get("/a/x{y}", r -> null));
        assertThrows(IllegalArgumentException.class, () -> new HttpRouter()
                .get("/u/{id}", r -> null).get("/u/{other}", r -> null), "same shape, different variable name");
        new HttpRouter().get("/u/{id}", r -> null).post("/u/{id}", r -> null); // different method is fine
    }

    @Test
    void unknownPathVariableNameIsAProgrammingError() {
        var r = new HttpRouter().get("/a/{id}", rr -> HttpResponse.ok(rr.pathVariable("typo")));
        assertThrows(IllegalArgumentException.class, () -> r.handle(req("GET", "/a/1")));
    }

    // ---- end to end ----

    @Test
    void worksThroughTheRealServer() throws Exception {
        try (var group = new EventLoopGroup(2); var server = HttpServer.bind(group, new InetSocketAddress("127.0.0.1", 0), router())) {
            int port = ((InetSocketAddress) server.localAddress()).getPort();
            var client = HttpClient.newHttpClient();
            var get = client.send(java.net.http.HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/users/7/posts/99?x=1")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals("7/99", get.body());
            var search = client.send(java.net.http.HttpRequest.newBuilder(
                    URI.create("http://127.0.0.1:" + port + "/search?q=h%C3%A9llo&tag=a&tag=b&limit=3")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals("q=héllo tags=[a, b] limit=3 all=false", search.body());
            var post = client.send(java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/login"))
                            .header("Content-Type", "application/x-www-form-urlencoded")
                            .POST(java.net.http.HttpRequest.BodyPublishers.ofString("user=sinh&lang=vi")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals("sinh:vi", post.body());
            var bug = client.send(java.net.http.HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/bug")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            assertEquals(500, bug.statusCode());
            assertFalse(bug.body().contains("kaboom"), "internal error details must not leak");
        }
    }
}
