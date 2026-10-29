package io.hydrangea;

import io.hydrangea.testing.TestTls;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.ProxyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** HTTP proxy support: absolute-form requests, CONNECT tunnels (TLS and HTTP/2 through them), credentials, bypass, name resolution. */
class ProxyTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private TestProxy proxy;

    @BeforeEach
    void setUp() throws Exception {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
        proxy = new TestProxy();
    }

    @AfterEach
    void tearDown() throws Exception {
        proxy.close();
        clientGroup.close();
        serverGroup.close();
    }

    private HttpRouter router() {
        return new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version() + " host=" + r.request().headers().get("host")))
                .get("/headers", r -> HttpResponse.ok(r.request().headers().toString()));
    }

    private Server server(SSLContext tls) throws Exception {
        return HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), tls,
                new HttpConfig(16 * 1024, 1024 * 1024).withHttp2(Http2Config.defaults()), router());
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private HttpClient client(HttpClientConfig cfg) {
        return new HttpClient(clientGroup, cfg);
    }

    private ProxyConfig viaProxy() {
        return ProxyConfig.all("127.0.0.1", proxy.port());
    }

    // ---- plain http ----

    @Test
    void plainHttpGoesToTheProxyInAbsoluteForm() throws Exception {
        try (Server origin = server(null); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy()))) {
            String url = "http://127.0.0.1:" + port(origin) + "/hello?x=1";
            assertEquals("hello over HTTP/1.1 host=127.0.0.1:" + port(origin), c.get(url).get(10, TimeUnit.SECONDS).bodyAsString());
            assertEquals("GET " + url + " HTTP/1.1", proxy.requestLines.get(0));
            assertEquals(0, proxy.connects.get());
        }
    }

    @Test
    void theOriginsNameIsLeftForTheProxyToResolve() throws Exception {
        try (Server origin = server(null); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy()))) {
            proxy.hostMap.put("origin.test", new InetSocketAddress("127.0.0.1", port(origin)));
            // "origin.test" does not exist in DNS: only a proxy that resolves it can make this work
            ClientResponse r = c.get("http://origin.test:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS);
            assertEquals(200, r.status());
            assertTrue(r.bodyAsString().contains("host=origin.test:"), r.bodyAsString());
            // and without a proxy the same URL cannot be reached
            try (HttpClient direct = client(HttpClientConfig.defaults())) {
                assertThrows(ExecutionException.class, () -> direct.get("http://origin.test:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS));
            }
        }
    }

    // ---- https through CONNECT ----

    @Test
    void httpsIsTunnelledAndTheOriginsCertificateIsTheOneChecked(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server origin = server(tls[0])) {
            String url = "https://localhost:" + port(origin) + "/hello";
            try (HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy()).withSslContext(tls[1]))) {
                assertEquals("hello over HTTP/1.1 host=localhost:" + port(origin), c.get(url).get(10, TimeUnit.SECONDS).bodyAsString());
                assertEquals("CONNECT localhost:" + port(origin) + " HTTP/1.1", proxy.requestLines.get(0));
                assertEquals(0, proxy.forwards.get());
            }
            // a client that does not trust the origin's certificate must fail: the TLS session is with the origin, not the proxy
            try (HttpClient untrusting = client(HttpClientConfig.defaults().withProxy(viaProxy()))) {
                ExecutionException e = assertThrows(ExecutionException.class, () -> untrusting.get(url).get(10, TimeUnit.SECONDS));
                assertTrue(e.getCause().toString().toLowerCase().contains("ssl") || e.getCause().toString().toLowerCase().contains("handshake")
                        || e.getCause().toString().toLowerCase().contains("certificate") || e.getCause().toString().toLowerCase().contains("closed"), e.getCause().toString());
            }
        }
    }

    @Test
    void oneTunnelServesManyKeepAliveRequests(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        try (Server origin = server(tls[0]); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy()).withSslContext(tls[1]))) {
            for (int i = 0; i < 6; i++) {
                assertEquals(200, c.get("https://localhost:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS).status());
            }
            assertEquals(1, proxy.connects.get(), "the tunnel and the connection inside it are reused");
        }
    }

    @Test
    void http2WorksInsideTheTunnel(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        HttpClientConfig cfg = HttpClientConfig.defaults().withProxy(viaProxy()).withSslContext(tls[1]).withHttp2(Http2ClientConfig.defaults());
        try (Server origin = server(tls[0]); HttpClient c = client(cfg)) {
            assertTrue(c.get("https://localhost:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS).bodyAsString().startsWith("hello over HTTP/2"));
            assertEquals(1, proxy.connects.get());
        }
    }

    @Test
    void cleartextHttp2ByPriorKnowledgeUsesATunnel() throws Exception {
        HttpClientConfig cfg = HttpClientConfig.defaults().withProxy(viaProxy())
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        try (Server origin = server(null); HttpClient c = client(cfg)) {
            assertTrue(c.get("http://127.0.0.1:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS).bodyAsString().startsWith("hello over HTTP/2"));
            assertEquals("CONNECT 127.0.0.1:" + port(origin) + " HTTP/1.1", proxy.requestLines.get(0), "HTTP/2 needs a byte stream, not absolute-form requests");
        }
    }

    // ---- authentication and refusal ----

    @Test
    void credentialsAreSentPreemptivelyAndRequiredByTheProxy(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        proxy.requiredAuthorization = "Basic " + Base64.getEncoder().encodeToString("alice:p@ss".getBytes());
        try (Server plain = server(null); Server secure = server(tls[0])) {
            String plainUrl = "http://127.0.0.1:" + port(plain) + "/hello";
            String secureUrl = "https://localhost:" + port(secure) + "/hello";
            try (HttpClient anonymous = client(HttpClientConfig.defaults().withProxy(viaProxy()).withSslContext(tls[1]))) {
                // on a plain request the proxy's 407 is an ordinary response; for a tunnel it is a refusal to open one
                assertEquals(407, anonymous.get(plainUrl).get(10, TimeUnit.SECONDS).status());
                ExecutionException refused = assertThrows(ExecutionException.class, () -> anonymous.get(secureUrl).get(10, TimeUnit.SECONDS));
                assertTrue(refused.getCause().getMessage().contains("407"), refused.getCause().toString());
                assertTrue(refused.getCause().getMessage().contains("credentials"), "the message says what to do: " + refused.getCause());
            }
            ProxyConfig withCredentials = viaProxy().withCredentials("alice", "p@ss");
            try (HttpClient c = client(HttpClientConfig.defaults().withProxy(withCredentials).withSslContext(tls[1]))) {
                assertEquals(200, c.get(plainUrl).get(10, TimeUnit.SECONDS).status());
                assertEquals(200, c.get(secureUrl).get(10, TimeUnit.SECONDS).status());
            }
            assertTrue(proxy.proxyAuthorizations.contains(proxy.requiredAuthorization));
        }
    }

    @Test
    void theProxyCredentialsNeverReachTheOrigin() throws Exception {
        try (Server origin = server(null); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy().withCredentials("u", "p")))) {
            String seen = c.get("http://127.0.0.1:" + port(origin) + "/headers").get(10, TimeUnit.SECONDS).bodyAsString().toLowerCase();
            assertFalse(seen.contains("proxy-authorization"), seen);
        }
    }

    @Test
    void aRefusedTunnelFailsWithTheProxysAnswer(@TempDir Path dir) throws Exception {
        SSLContext[] tls = TestTls.tls(dir);
        proxy.refuseConnectWith = 403;
        try (Server origin = server(tls[0]); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy()).withSslContext(tls[1]))) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.get("https://localhost:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().contains("403"), e.getCause().toString());
        }
    }

    // ---- bypass and configuration ----

    @Test
    void hostsInNoProxyAreReachedDirectly() throws Exception {
        try (Server origin = server(null); HttpClient c = client(HttpClientConfig.defaults().withProxy(viaProxy().withNoProxy("127.0.0.1")))) {
            assertEquals(200, c.get("http://127.0.0.1:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS).status());
            assertTrue(proxy.requestLines.isEmpty(), "the proxy must not have seen it");
        }
    }

    @Test
    void anUnreachableProxyFailsTheRequest() throws Exception {
        int dead = proxy.port();
        proxy.close();
        try (Server origin = server(null); HttpClient c = client(HttpClientConfig.defaults().withProxy(ProxyConfig.all("127.0.0.1", dead)))) {
            assertThrows(ExecutionException.class, () -> c.get("http://127.0.0.1:" + port(origin) + "/hello").get(10, TimeUnit.SECONDS));
        }
    }
}
