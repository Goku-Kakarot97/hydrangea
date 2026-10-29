package io.hydrangea;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The client's side of "Upgrade: h2c": offer it, switch on 101, and carry on as HTTP/1.1 when the server does not play along. */
class Http2ClientUpgradeTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;

    @BeforeEach
    void setUp() {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        clientGroup.close();
        serverGroup.close();
    }

    private Server server(boolean http2) throws Exception {
        HttpRouter router = new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version() + " upgradeHeader=" + r.request().headers().get("upgrade"))
                        .header("X-Seen", "yes"))
                .post("/echo", r -> HttpResponse.status(200).body(r.body()))
                .postStream("/sink", (r, body) -> {
                    CompletableFuture<HttpResponse> done = new CompletableFuture<>();
                    long[] n = {0};
                    body.listen(new io.hydrangea.http.BodyListener() {
                        public void onData(ByteBuffer d, io.hydrangea.http.BodyControl c) {
                            n[0] += d.remaining();
                        }

                        public void onComplete() {
                            done.complete(HttpResponse.ok(r.request().version() + ":" + n[0] + ":upgrade=" + r.request().headers().get("upgrade")));
                        }
                    });
                    return done;
                });
        HttpConfig cfg = new HttpConfig(16 * 1024, 4 * 1024 * 1024);
        return HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null, http2 ? cfg.withHttp2(Http2Config.defaults()) : cfg, router);
    }

    private HttpClient client() {
        return new HttpClient(clientGroup, HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextUpgrade(true)));
    }

    private static String url(Server s, String path) {
        return "http://127.0.0.1:" + ((InetSocketAddress) s.localAddress()).getPort() + path;
    }

    @Test
    void switchesToHttp2WhenTheServerAgrees() throws Exception {
        try (Server s = server(true); HttpClient c = client()) {
            ClientResponse first = c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS);
            assertEquals("hello over HTTP/2 upgradeHeader=null", first.bodyAsString(), "the request that carried the offer is answered over HTTP/2, without the offer headers");
            assertEquals("hello over HTTP/2 upgradeHeader=null", c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS).bodyAsString());
            assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened());
        }
    }

    @Test
    void aPostWithABodyUpgradesToo() throws Exception {
        try (Server s = server(true); HttpClient c = client()) {
            ClientResponse r = c.send(ClientRequest.post(url(s, "/echo")).body("a body that travels with the offer")).get(10, TimeUnit.SECONDS);
            assertEquals("a body that travels with the offer", r.bodyAsString());
            assertEquals("hello over HTTP/2 upgradeHeader=null", c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS).bodyAsString());
        }
    }

    @Test
    void aHeadRequestCanCarryTheOffer() throws Exception {
        try (Server s = server(true); HttpClient c = client()) {
            ClientResponse r = c.send(ClientRequest.head(url(s, "/hello"))).get(10, TimeUnit.SECONDS);
            assertEquals(200, r.status());
            assertEquals("yes", r.header("x-seen"));
            assertEquals(0, r.body().length);
        }
    }

    @Test
    void aBurstSharesTheOneUpgradedConnection() throws Exception {
        try (Server s = server(true); HttpClient c = client()) {
            List<CompletableFuture<ClientResponse>> all = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                all.add(c.get(url(s, "/hello")));
            }
            for (var f : all) {
                assertTrue(f.get(15, TimeUnit.SECONDS).bodyAsString().startsWith("hello over HTTP/2"));
            }
            assertEquals(1, serverGroup.metrics().snapshot().connectionsOpened(), "the first request decided; the rest waited and multiplexed");
        }
    }

    @Test
    void carriesOnAsHttp11WhenTheServerDoesNotDoHttp2() throws Exception {
        try (Server s = server(false); HttpClient c = client()) {
            ClientResponse r = c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS);
            assertTrue(r.bodyAsString().startsWith("hello over HTTP/1.1"), r.bodyAsString());
            // the offer headers reached a server that ignored them, and the connection keeps working as HTTP/1.1
            assertTrue(c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS).bodyAsString().contains("upgradeHeader=null"), "the offer is made once per connection");
            List<CompletableFuture<ClientResponse>> burst = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                burst.add(c.get(url(s, "/hello")));
            }
            for (var f : burst) {
                assertEquals(200, f.get(10, TimeUnit.SECONDS).status());
            }
        }
    }

    @Test
    void aStreamedBodyMakesNoOffer() throws Exception {
        try (Server s = server(true); HttpClient c = client()) {
            RequestBody body = RequestBody.producer(100_000, false, sink -> {
                sink.write(ByteBuffer.wrap(new byte[100_000]));
                sink.finish();
            });
            ClientResponse r = c.send(ClientRequest.post(url(s, "/sink")).body(body)).get(10, TimeUnit.SECONDS);
            assertEquals("HTTP/1.1:100000:upgrade=null", r.bodyAsString(), "the whole request has to be out before a switch, so a streamed body makes no offer");
        }
    }

    @Test
    void offStaysOff() throws Exception {
        try (Server s = server(true); HttpClient c = new HttpClient(clientGroup, HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults()))) {
            assertTrue(c.get(url(s, "/hello")).get(10, TimeUnit.SECONDS).bodyAsString().startsWith("hello over HTTP/1.1"));
        }
    }
}
