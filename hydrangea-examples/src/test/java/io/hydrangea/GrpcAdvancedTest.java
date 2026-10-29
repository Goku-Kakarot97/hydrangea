package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** Interceptors, retries, name resolution, load balancing and keep-alive. */
class GrpcAdvancedTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    private Server serve(GrpcServer g) throws Exception {
        return g.bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    /** A server whose Unary fails with {@code code} for the first {@code failures} calls, then answers "ok:<n>" with n the call number. */
    private Server flaky(int failures, Status.Code code, AtomicInteger calls) throws Exception {
        return serve(GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            int n = calls.incrementAndGet();
            if (n <= failures) {
                return CompletableFuture.failedFuture(new StatusException(Status.of(code).withDescription("failure " + n)));
            }
            return CompletableFuture.completedFuture(new Msg("ok:" + n));
        }).build());
    }

    private static StatusException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(15, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return (StatusException) e.getCause();
        }
    }

    private static RetryPolicy quick() {
        return RetryPolicy.defaults().withBackoff(Duration.ofMillis(5), Duration.ofMillis(20), 2);
    }

    // ---- server interceptors ----

    @Test
    void aServerInterceptorCanRejectACallBeforeTheHandler() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        try (Server s = serve(GrpcServer.builder()
                .intercept((m, ctx) -> {
                    if (ctx.requestMetadata().get("authorization") == null) {
                        throw new StatusException(Status.of(Status.Code.UNAUTHENTICATED).withDescription("who are you"));
                    }
                    return null;
                })
                .unary(UNARY, (req, ctx) -> {
                    handled.incrementAndGet();
                    return CompletableFuture.completedFuture(new Msg("secret"));
                }).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).build()) {
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT));
            assertEquals(Status.Code.UNAUTHENTICATED, e.status().code());
            assertEquals("who are you", e.status().description());
            assertEquals(0, handled.get(), "the handler never ran");
            Msg ok = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMetadata(new Metadata().put("authorization", "t"))).get(10, TimeUnit.SECONDS);
            assertEquals("secret", ok.text());
            assertEquals(1, handled.get());
        }
    }

    @Test
    void serverInterceptorListenersSeeMessagesAndExactlyOneClose() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        try (Server s = serve(GrpcServer.builder()
                .intercept((m, ctx) -> new ServerInterceptor.Listener() {
                    public void onRequest(Object msg) { events.add("req:" + ((Msg) msg).text()); }
                    public void onResponse(Object msg) { events.add("resp:" + ((Msg) msg).text()); }
                    public void onClose(Status st) { events.add("close:" + st.code()); }
                })
                .unary(UNARY, (req, ctx) -> req.text().equals("SLOW")
                        ? new CompletableFuture<>()
                        : CompletableFuture.completedFuture(new Msg("echo:" + req.text()))).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).build()) {
            ch.unary(UNARY, new Msg("a"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            assertEquals(List.of("req:a", "resp:echo:a", "close:OK"), events);
            events.clear();
            ClientCall<Msg, Msg> call = ch.newCall(UNARY, CallOptions.DEFAULT, new ClientCall.Listener<>() { });
            call.send(new Msg("SLOW"));
            call.halfClose();
            Thread.sleep(200);
            call.cancel("bye");
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!events.contains("close:CANCELLED") && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            assertEquals(List.of("req:SLOW", "close:CANCELLED"), events, "a cancelled call is reported once");
        }
    }

    @Test
    void aRejectedStreamingCallNeverCreatesItsHandler() throws Exception {
        AtomicInteger created = new AtomicInteger();
        try (Server s = serve(GrpcServer.builder()
                .intercept((m, ctx) -> { throw new StatusException(Status.of(Status.Code.PERMISSION_DENIED)); })
                .bidiStreaming(GrpcEchoExample.BIDI, (ctx, out) -> {
                    created.incrementAndGet();
                    return new StreamObserver<Msg>() {
                        public void onNext(Msg m) { }
                        public void onError(Throwable t) { }
                        public void onCompleted() { }
                    };
                }).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).build()) {
            CompletableFuture<Void> done = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.bidiStreaming(GrpcEchoExample.BIDI, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            call.send(new Msg("x"));
            assertEquals(Status.Code.PERMISSION_DENIED, failure(done).status().code());
            assertEquals(0, created.get());
        }
    }

    @Test
    void eachAttemptGetsOnlyTheTimeThatIsLeft() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = serve(GrpcServer.builder().unary(UNARY, (req, ctx) -> {
                 if (calls.incrementAndGet() == 1) {
                     CompletableFuture<Msg> late = new CompletableFuture<>();
                     CompletableFuture.delayedExecutor(300, TimeUnit.MILLISECONDS).execute(() ->
                             late.completeExceptionally(new StatusException(Status.of(Status.Code.UNAVAILABLE))));
                     return late;
                 }
                 return new CompletableFuture<>();   // the second attempt hangs
             }).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).build()) {
            long t0 = System.nanoTime();
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(600))));
            long took = (System.nanoTime() - t0) / 1_000_000;
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.status().code());
            assertTrue(took < 800, "the deadline counts from the start of the call, not of each attempt: " + took + " ms");
            assertEquals(2, calls.get());
        }
    }

    @Test
    void interceptorsRunInTheOrderTheyWereAdded() throws Exception {
        List<String> order = new CopyOnWriteArrayList<>();
        try (Server s = serve(GrpcServer.builder()
                .intercept((m, c) -> { order.add("first"); return null; })
                .intercept((m, c) -> { order.add("second"); return null; })
                .unary(UNARY, (req, ctx) -> CompletableFuture.completedFuture(req)).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).build()) {
            ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            assertEquals(List.of("first", "second"), order);
        }
    }

    // ---- client interceptors ----

    @Test
    void aClientInterceptorCanAddMetadataAndObserveTheResult() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();
        try (Server s = serve(GrpcEchoExample.builder().build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s))
                     .intercept(new ClientInterceptor() {
                         public <Req, Resp> ClientCall<Req, Resp> intercept(MethodDescriptor<Req, Resp> m, CallOptions o, ClientCall.Listener<Resp> l, Channel next) {
                             log.add("outer:" + m.fullName());
                             return next.newCall(m, o.withMetadata(o.metadata().copy().put("x-test", "from-interceptor")), new ClientCall.Listener<Resp>() {
                                 public void onMessage(Resp r) { l.onMessage(r); }
                                 public void onClose(Status st, Metadata t) { log.add("closed:" + st.code()); l.onClose(st, t); }
                             });
                         }
                     })
                     .intercept(new ClientInterceptor() {
                         public <Req, Resp> ClientCall<Req, Resp> intercept(MethodDescriptor<Req, Resp> m, CallOptions o, ClientCall.Listener<Resp> l, Channel next) {
                             log.add("inner");
                             return next.newCall(m, o, l);
                         }
                     }).build()) {
            assertEquals("from-interceptor", ch.unary(UNARY, new Msg("META"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            assertEquals(List.of("outer:hydrangea.test.Echo/Unary", "inner", "closed:OK"), log);
        }
    }

    // ---- retries ----

    @Test
    void unavailableIsRetriedUntilItWorks() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(2, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).build()) {
            assertEquals("ok:3", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            assertEquals(3, calls.get());
        }
    }

    @Test
    void retriesStopAtMaxAttemptsAndReportTheLastStatus() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(100, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick().withMaxAttempts(4)).build()) {
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT));
            assertEquals(Status.Code.UNAVAILABLE, e.status().code());
            assertEquals("failure 4", e.status().description());
            assertEquals(4, calls.get());
        }
    }

    @Test
    void codesOutsideThePolicyAreNeverRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(100, Status.Code.INVALID_ARGUMENT, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).build()) {
            assertEquals(Status.Code.INVALID_ARGUMENT, failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)).status().code());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void withoutAPolicyNothingIsRetried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(1, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).build()) {
            assertEquals(Status.Code.UNAVAILABLE, failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)).status().code());
            assertEquals(1, calls.get());
        }
    }

    @Test
    void aCallIsNotRetriedOnceTheResponseHasStarted() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = serve(GrpcServer.builder().serverStreaming(GrpcEchoExample.SERVER_STREAM, (req, ctx, out) -> {
                 calls.incrementAndGet();
                 out.onNext(new Msg("first"));
                 out.onError(new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("died mid-stream")));
             }).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).build()) {
            List<String> got = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("x", 1), CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { got.add(m.text()); }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            assertEquals(Status.Code.UNAVAILABLE, failure(done).status().code());
            assertEquals(List.of("first"), got, "no duplicate of an already delivered message");
            assertEquals(1, calls.get());
        }
    }

    @Test
    void streamedRequestsAreReplayedOnRetry() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = serve(GrpcServer.builder().clientStreaming(GrpcEchoExample.CLIENT_STREAM, (ctx, out) -> {
                 int attempt = calls.incrementAndGet();
                 List<String> texts = new ArrayList<>();
                 return new StreamObserver<Msg>() {
                     public void onNext(Msg m) { texts.add(m.text()); }
                     public void onError(Throwable t) { }
                     public void onCompleted() {
                         if (attempt == 1) {
                             out.onError(new StatusException(Status.of(Status.Code.UNAVAILABLE)));
                         } else {
                             out.onNext(new Msg(String.join(",", texts), attempt));
                             out.onCompleted();
                         }
                     }
                 };
             }).build());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).build()) {
            CompletableFuture<Msg> result = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.clientStreaming(GrpcEchoExample.CLIENT_STREAM, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { result.complete(m); }
                public void onError(Throwable t) { result.completeExceptionally(t); }
                public void onCompleted() { }
            });
            for (String t : new String[]{"a", "b", "c"}) call.send(new Msg(t));
            call.halfClose();
            Msg r = result.get(10, TimeUnit.SECONDS);
            assertEquals("a,b,c", r.text(), "the second attempt got every message again");
            assertEquals(2, r.n());
        }
    }

    @Test
    void aBufferOverflowCommitsTheCallSoItIsNotReplayedPartially() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(1, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s)).retryPolicy(quick()).retryBufferBytes(100).build()) {
            StatusException e = failure(ch.unary(UNARY, new Msg("big", 0, new byte[500]), CallOptions.DEFAULT));
            assertEquals(Status.Code.UNAVAILABLE, e.status().code(), "too big to replay: the first failure is final");
            assertEquals(1, calls.get());
        }
    }

    @Test
    void theDeadlineSpansAllAttempts() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(1000, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s))
                     .retryPolicy(RetryPolicy.defaults().withMaxAttempts(10).withBackoff(Duration.ofMillis(200), Duration.ofMillis(200), 1)).build()) {
            long t0 = System.nanoTime();
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(350))));
            long took = (System.nanoTime() - t0) / 1_000_000;
            assertNotNull(e);
            assertTrue(took < 1500, "gave up near the deadline instead of using all attempts: " + took + " ms");
            assertTrue(calls.get() < 10);
        }
    }

    @Test
    void cancellingDuringTheBackoffEndsTheCall() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (Server s = flaky(1000, Status.Code.UNAVAILABLE, calls);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port(s))
                     .retryPolicy(RetryPolicy.defaults().withBackoff(Duration.ofSeconds(2), Duration.ofSeconds(2), 1)).build()) {
            CompletableFuture<Status> end = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.newCall(UNARY, CallOptions.DEFAULT, new ClientCall.Listener<>() {
                public void onClose(Status st, Metadata t) { end.complete(st); }
            });
            call.send(new Msg("x"));
            call.halfClose();
            Thread.sleep(300);
            call.cancel("stop");
            assertEquals(Status.Code.CANCELLED, end.get(2, TimeUnit.SECONDS).code());
            int seen = calls.get();
            Thread.sleep(500);
            assertEquals(seen, calls.get(), "no further attempts after the cancel");
        }
    }

    // ---- name resolution and load balancing ----

    private Server tagged(String tag, AtomicInteger counter) throws Exception {
        return serve(GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            counter.incrementAndGet();
            return CompletableFuture.completedFuture(new Msg(tag));
        }).build());
    }

    @Test
    void roundRobinSpreadsCallsAndPickFirstSticks() throws Exception {
        AtomicInteger a = new AtomicInteger(), b = new AtomicInteger(), c = new AtomicInteger();
        try (Server sa = tagged("a", a); Server sb = tagged("b", b); Server sc = tagged("c", c)) {
            NameResolver three = NameResolver.fixed(new InetSocketAddress("127.0.0.1", port(sa)), new InetSocketAddress("127.0.0.1", port(sb)),
                    new InetSocketAddress("127.0.0.1", port(sc)));
            try (GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(three).loadBalancing(LoadBalancing.ROUND_ROBIN).build()) {
                for (int i = 0; i < 30; i++) {
                    ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
                }
                assertEquals(List.of(10, 10, 10), List.of(a.get(), b.get(), c.get()));
            }
            a.set(0); b.set(0); c.set(0);
            try (GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(three).build()) {
                for (int i = 0; i < 10; i++) {
                    ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
                }
                assertEquals(List.of(10, 0, 0), List.of(a.get(), b.get(), c.get()));
            }
        }
    }

    @Test
    void aDeadBackendIsSkippedAfterItFailsOnce() throws Exception {
        AtomicInteger live = new AtomicInteger();
        int deadPort;
        try (ServerSocket tmp = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            deadPort = tmp.getLocalPort();
        }
        try (Server good = tagged("good", live)) {
            NameResolver both = NameResolver.fixed(new InetSocketAddress("127.0.0.1", deadPort), new InetSocketAddress("127.0.0.1", port(good)));
            try (GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(both).loadBalancing(LoadBalancing.ROUND_ROBIN).build()) {
                int failed = 0;
                for (int i = 0; i < 20; i++) {
                    try {
                        ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
                    } catch (ExecutionException e) {
                        failed++;
                    }
                }
                assertEquals(1, failed, "only the call that found the dead address failed; later ones avoid it");
                assertEquals(19, live.get());
            }
        }
    }

    @Test
    void retryPlusRoundRobinHidesADeadBackendCompletely() throws Exception {
        AtomicInteger live = new AtomicInteger();
        int deadPort;
        try (ServerSocket tmp = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            deadPort = tmp.getLocalPort();
        }
        try (Server good = tagged("good", live)) {
            NameResolver both = NameResolver.fixed(new InetSocketAddress("127.0.0.1", deadPort), new InetSocketAddress("127.0.0.1", port(good)));
            try (GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(both).loadBalancing(LoadBalancing.ROUND_ROBIN).retryPolicy(quick()).build()) {
                for (int i = 0; i < 20; i++) {
                    assertEquals("good", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
                }
            }
        }
    }

    @Test
    void theResolverIsRunAgainAndNewAddressesAreUsed() throws Exception {
        AtomicInteger a = new AtomicInteger(), b = new AtomicInteger();
        try (Server sa = tagged("a", a); Server sb = tagged("b", b)) {
            var current = new java.util.concurrent.atomic.AtomicReference<>(new InetSocketAddress("127.0.0.1", port(sa)));
            try (GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver((h, p) -> List.of(current.get()))
                    .resolveInterval(Duration.ofMillis(100)).build()) {
                assertEquals("a", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
                current.set(new InetSocketAddress("127.0.0.1", port(sb)));
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                String last = "a";
                while (last.equals("a") && System.nanoTime() < end) {
                    Thread.sleep(50);
                    last = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text();
                }
                assertEquals("b", last);
            }
        }
    }

    @Test
    void aTargetThatDoesNotResolveIsUnavailable() throws Exception {
        try (GrpcChannel ch = GrpcChannel.forTarget("http://no-such-host.invalid:50051").build()) {
            assertEquals(Status.Code.UNAVAILABLE, failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)).status().code());
        }
    }

    // ---- keep-alive ----

    @Test
    void aPeerThatStopsAnsweringIsDroppedByKeepAlive() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).keepAlive(Duration.ofMillis(100), Duration.ofMillis(200)).build()) {
            long t0 = System.nanoTime();
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT);
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                boolean pinged = false;
                while (!pinged) {
                    Frame f = p.nextInteresting();
                    pinged = f.type() == 6 && (f.flags() & 1) == 0;
                }
                StatusException e = failure(call);
                assertEquals(Status.Code.UNAVAILABLE, e.status().code());
                assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000);
            }
        }
    }

    @Test
    void aPeerThatAnswersPingsKeepsItsConnection() throws Exception {
        try (ServerSocket ss = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ss.getLocalPort()).keepAlive(Duration.ofMillis(100), Duration.ofMillis(300)).build()) {
            CompletableFuture<Msg> call = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT);
            try (Peer p = new Peer(ss.accept(), false, true)) {
                p.settings();
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1200);
                int pings = 0;
                while (System.nanoTime() < until) {
                    Frame f = p.pollOrNull(150);
                    if (f != null && f.type() == 6 && (f.flags() & 1) == 0) {
                        pings++;
                        p.ping(f.payload());
                    }
                }
                assertTrue(pings >= 3, "pinged repeatedly while quiet: " + pings);
                assertFalse(call.isDone(), "the connection and the call survived");
                call.cancel(true);
            }
        }
    }
}
