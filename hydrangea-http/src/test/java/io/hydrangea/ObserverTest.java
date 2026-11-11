package io.hydrangea;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.observe.CallObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The server and client observers: what they are told, in which order, and that the server's end comes after the response bytes left. */
class ObserverTest {

    /** One observation, recording every call with the thread it came on. */
    static final class Rec implements CallObserver.Observation<HttpResponse> {
        final HttpRequest request;
        final List<String> events;
        final long startedNanos = System.nanoTime();
        volatile long endedNanos;
        volatile HttpResponse response;
        volatile Throwable error;

        Rec(HttpRequest request, List<String> events) {
            this.request = request;
            this.events = events;
            events.add("started " + request.method() + " " + request.path());
        }

        public void handling() { events.add("handling"); }
        public void handled() { events.add("handled"); }
        public void ended(HttpResponse answer, Throwable e) {
            endedNanos = System.nanoTime();
            response = answer;
            error = e;
            events.add("ended " + (answer == null ? "-" : answer.status()) + (e == null ? "" : " " + e.getClass().getSimpleName()));
        }
    }

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<Rec> recs = new CopyOnWriteArrayList<>();
    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Server server;
    private volatile Object seenInRoute;
    private volatile String patternInRoute;
    private volatile long routeReturnedNanos;

    @BeforeEach
    void setUp() throws Exception {
        serverGroup = new EventLoopGroup(1);
        clientGroup = new EventLoopGroup(1);
        HttpRouter router = new HttpRouter()
                .get("/hello/{name}", r -> {
                    seenInRoute = r.attachment();
                    patternInRoute = r.pattern();
                    events.add("route");
                    routeReturnedNanos = System.nanoTime();
                    return HttpResponse.ok("hi " + r.pathVariable("name"));
                })
                .getAsync("/slow", r -> CompletableFuture.supplyAsync(() -> {
                    try { Thread.sleep(50); } catch (InterruptedException ignored) { }
                    return HttpResponse.ok("late");
                }))
                .get("/boom", r -> { throw new IllegalStateException("kaboom"); })
                .getAsync("/fail", r -> CompletableFuture.failedFuture(new IllegalStateException("later")));
        CallObserver<HttpRequest, HttpResponse> observer = request -> {
            Rec rec = new Rec(request, events);
            recs.add(rec);
            return rec;
        };
        server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null,
                HttpConfig.defaults().withObserver(observer), router);
    }

    @AfterEach
    void tearDown() {
        clientGroup.close();
        serverGroup.close();
    }

    private String url(String path) {
        return "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + path;
    }

    @Test
    void theServerObserverSeesStartHandlingHandledAndEndsAfterTheResponseLeft() throws Exception {
        try (HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults())) {
            ClientResponse resp = client.send(ClientRequest.get(url("/hello/ann"))).get(5, TimeUnit.SECONDS);
            assertEquals(200, resp.status());
            assertEquals("hi ann", resp.bodyAsString());
            waitFor(() -> recs.size() == 1 && recs.get(0).endedNanos != 0);
            Rec rec = recs.get(0);
            assertEquals(List.of("started GET /hello/ann", "handling", "route", "handled", "ended 200"), events);
            assertSame(rec, seenInRoute, "the route sees its own observation as the request's attachment");
            assertEquals("/hello/{name}", patternInRoute);
            assertSame(rec.response, rec.response, "ended got the response that went out");
            assertEquals(200, rec.response.status());
            assertNull(rec.error);
            assertTrue(rec.endedNanos > routeReturnedNanos, "ended comes after the route returned (once the bytes were handed to the socket)");
        }
    }

    @Test
    void anAsyncRouteEndsWhenItsLateResponseLeftNotWhenTheHandlerReturned() throws Exception {
        try (HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults())) {
            assertEquals("late", client.send(ClientRequest.get(url("/slow"))).get(5, TimeUnit.SECONDS).bodyAsString());
            waitFor(() -> recs.size() == 1 && recs.get(0).endedNanos != 0);
            assertEquals(List.of("started GET /slow", "handling", "handled", "ended 200"), events);
            assertTrue(recs.get(0).endedNanos - recs.get(0).startedNanos >= TimeUnit.MILLISECONDS.toNanos(45), "the observation spans the wait");
        }
    }

    @Test
    void aHandlerThatThrowsOrFailsEndsWithTheErrorAndTheFiveHundredThatWentOut() throws Exception {
        try (HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults())) {
            assertEquals(500, client.send(ClientRequest.get(url("/boom"))).get(5, TimeUnit.SECONDS).status());
            waitFor(() -> recs.size() == 1 && recs.get(0).endedNanos != 0);
            assertEquals(List.of("started GET /boom", "handling", "handled", "ended 500 IllegalStateException"), events);
            assertEquals("kaboom", recs.get(0).error.getMessage());
            events.clear();
            assertEquals(500, client.send(ClientRequest.get(url("/fail"))).get(5, TimeUnit.SECONDS).status());
            waitFor(() -> recs.size() == 2 && recs.get(1).endedNanos != 0);
            assertEquals(List.of("started GET /fail", "handling", "handled", "ended 500 IllegalStateException"), events);
            assertEquals("later", recs.get(1).error.getMessage(), "the cause, not the CompletionException around it");
        }
    }

    @Test
    void theClientObserverSeesTheRequestGoAndTheResponseArrive() throws Exception {
        List<String> client_events = new CopyOnWriteArrayList<>();
        CallObserver<ClientRequest, ClientResponse> observer = request -> {
            client_events.add("started " + request.method() + " " + request.uri().getPath());
            return (resp, err) -> client_events.add("ended " + (resp == null ? "-" : resp.status()) + (err == null ? "" : " " + err.getClass().getSimpleName()));
        };
        try (HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults().withObserver(observer))) {
            assertEquals(200, client.send(ClientRequest.get(url("/hello/bob"))).get(5, TimeUnit.SECONDS).status());
            waitFor(() -> client_events.size() == 2);
            assertEquals(List.of("started GET /hello/bob", "ended 200"), client_events);
            client_events.clear();
            assertThrows(Exception.class, () -> client.send(ClientRequest.get("http://127.0.0.1:1/nobody")).get(5, TimeUnit.SECONDS));   // nothing listens on port 1
            waitFor(() -> client_events.size() == 2);
            assertEquals("started GET /nobody", client_events.get(0));
            assertTrue(client_events.get(1).startsWith("ended - "), client_events.get(1));
        }
    }

    @Test
    void withoutAnObserverTheRequestCarriesNoAttachment() throws Exception {
        Server plain = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), new HttpRouter().get("/x", r -> HttpResponse.ok(String.valueOf(r.attachment()))));
        try (HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults())) {
            String url = "http://127.0.0.1:" + ((InetSocketAddress) plain.localAddress()).getPort() + "/x";
            assertEquals("null", client.send(ClientRequest.get(url)).get(5, TimeUnit.SECONDS).bodyAsString());
        } finally {
            plain.close();
        }
    }

    private static void waitFor(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("condition not met in time");
            }
            Thread.sleep(5);
        }
    }
}
