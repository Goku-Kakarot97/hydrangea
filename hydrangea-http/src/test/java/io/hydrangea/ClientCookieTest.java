package io.hydrangea;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.InMemoryCookieJar;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The client's cookie jar, end to end over HTTP/1.1 and HTTP/2. */
class ClientCookieTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Server server;

    @BeforeEach
    void setUp() throws Exception {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
        HttpRouter router = new HttpRouter()
                .get("/login", r -> {
                    HttpResponse resp = HttpResponse.ok("welcome");
                    resp.headers().add("Set-Cookie", "session=s3cret; Path=/; HttpOnly");
                    resp.headers().add("Set-Cookie", "theme=dark; Path=/app; Max-Age=3600");
                    return resp;
                })
                .get("/whoami", r -> HttpResponse.ok(String.valueOf(r.request().headers().get("cookie"))))
                .get("/app/whoami", r -> HttpResponse.ok(String.valueOf(r.request().headers().get("cookie"))))
                .get("/hop", r -> {
                    HttpResponse resp = HttpResponse.status(302).header("Location", "/whoami").text("");
                    resp.headers().add("Set-Cookie", "hop=1; Path=/");
                    return resp;
                })
                .get("/logout", r -> {
                    HttpResponse resp = HttpResponse.ok("bye");
                    resp.headers().add("Set-Cookie", "session=; Path=/; Max-Age=0");
                    return resp;
                });
        server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null,
                new HttpConfig(16 * 1024, 1024 * 1024).withHttp2(Http2Config.defaults()), router);
    }

    @AfterEach
    void tearDown() {
        clientGroup.close();
        serverGroup.close();
    }

    private int port() {
        return ((InetSocketAddress) server.localAddress()).getPort();
    }

    private HttpClient client(boolean h2, InMemoryCookieJar jar) {
        HttpClientConfig cfg = HttpClientConfig.defaults().withCookieJar(jar);
        if (h2) {
            cfg = cfg.withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        }
        return new HttpClient(clientGroup, cfg);
    }

    private String get(HttpClient c, String host, String path) throws Exception {
        return c.get("http://" + host + ":" + port() + path).get(10, TimeUnit.SECONDS).bodyAsString();
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void cookiesSetByOneResponseAreSentWithLaterRequests(boolean h2) throws Exception {
        try (HttpClient c = client(h2, new InMemoryCookieJar())) {
            assertEquals("null", get(c, "127.0.0.1", "/whoami"), "nothing yet");
            assertEquals("welcome", get(c, "127.0.0.1", "/login"));
            assertEquals("session=s3cret", get(c, "127.0.0.1", "/whoami"));
            assertEquals("theme=dark; session=s3cret", get(c, "127.0.0.1", "/app/whoami"), "the longer path first");
            assertEquals("null", get(c, "localhost", "/whoami"), "another host does not get the cookies");
            get(c, "127.0.0.1", "/logout");
            assertEquals("null", get(c, "127.0.0.1", "/whoami"), "Max-Age=0 deleted it");
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void aCookieSetByARedirectIsSentOnTheNextHop(boolean h2) throws Exception {
        try (HttpClient c = client(h2, new InMemoryCookieJar())) {
            assertEquals("hop=1", get(c, "127.0.0.1", "/hop"), "/hop sets a cookie and redirects to /whoami, which must see it");
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void aCookieHeaderTheCallerSetItselfIsLeftAlone(boolean h2) throws Exception {
        try (HttpClient c = client(h2, new InMemoryCookieJar())) {
            get(c, "127.0.0.1", "/login");
            String seen = c.send(ClientRequest.get("http://127.0.0.1:" + port() + "/whoami").header("Cookie", "mine=1")).get(10, TimeUnit.SECONDS).bodyAsString();
            assertEquals("mine=1", seen);
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void withoutAJarNothingIsRemembered(boolean h2) throws Exception {
        HttpClientConfig cfg = HttpClientConfig.defaults();
        if (h2) {
            cfg = cfg.withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        }
        try (HttpClient c = new HttpClient(clientGroup, cfg)) {
            get(c, "127.0.0.1", "/login");
            assertEquals("null", get(c, "127.0.0.1", "/whoami"));
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void manyConcurrentRequestsShareOneJob(boolean h2) throws Exception {
        try (HttpClient c = client(h2, new InMemoryCookieJar())) {
            get(c, "127.0.0.1", "/login");
            var all = new java.util.ArrayList<java.util.concurrent.CompletableFuture<io.hydrangea.http.ClientResponse>>();
            for (int i = 0; i < 100; i++) {
                all.add(c.get("http://127.0.0.1:" + port() + "/whoami"));
            }
            for (var f : all) {
                assertEquals("session=s3cret", f.get(10, TimeUnit.SECONDS).bodyAsString());
            }
        }
    }
}
