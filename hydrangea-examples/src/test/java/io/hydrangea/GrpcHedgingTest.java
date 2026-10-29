package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

class GrpcHedgingTest {

    private EventLoopGroup group;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    /** A server whose n-th call (1-based) is answered by {@code behaviour}; it may return a future that never completes. */
    private Server server(IntFunction<CompletableFuture<Msg>> behaviour) throws Exception {
        return GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            int n = calls.incrementAndGet();
            ctx.onCancel(cancelled::incrementAndGet);
            return behaviour.apply(n);
        }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    private static String url(Server s) {
        return "http://127.0.0.1:" + ((InetSocketAddress) s.localAddress()).getPort();
    }

    private static StatusException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(15, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return (StatusException) e.getCause();
        }
    }

    private static CompletableFuture<Msg> failed(Status.Code code) {
        return CompletableFuture.failedFuture(new StatusException(Status.of(code).withDescription("n")));
    }

    private static void eventually(java.util.function.BooleanSupplier cond, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!cond.getAsBoolean() && System.nanoTime() < end) {
            Thread.sleep(10);
        }
        assertTrue(cond.getAsBoolean(), what);
    }

    @Test
    void aSlowFirstAttemptIsOvertakenByTheHedgeAndCancelled() throws Exception {
        try (Server s = server(n -> n == 1 ? new CompletableFuture<>() : CompletableFuture.completedFuture(new Msg("attempt " + n)));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofMillis(50))).build()) {
            long t0 = System.nanoTime();
            assertEquals("attempt 2", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 1500);
            eventually(() -> cancelled.get() == 1, "the slow attempt was cancelled on the server");
            assertEquals(2, calls.get());
        }
    }

    @Test
    void aFastFirstAnswerSendsNoHedge() throws Exception {
        try (Server s = server(n -> CompletableFuture.completedFuture(new Msg("fast")));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofMillis(500))).build()) {
            assertEquals("fast", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            Thread.sleep(700);
            assertEquals(1, calls.get(), "the hedge timer was cancelled by the answer");
        }
    }

    @Test
    void aNonFatalFailureStartsTheNextAttemptAtOnce() throws Exception {
        try (Server s = server(n -> n == 1 ? failed(Status.Code.UNAVAILABLE) : CompletableFuture.completedFuture(new Msg("second")));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofSeconds(30))).build()) {
            long t0 = System.nanoTime();
            assertEquals("second", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 3000, "did not wait for the 30 s hedging delay");
        }
    }

    @Test
    void aFatalFailureEndsTheCallAndCancelsTheOthers() throws Exception {
        try (Server s = server(n -> n == 1 ? new CompletableFuture<>() : failed(Status.Code.INVALID_ARGUMENT));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofMillis(50))).build()) {
            assertEquals(Status.Code.INVALID_ARGUMENT, failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)).status().code());
            eventually(() -> cancelled.get() >= 1, "the first, still-running attempt was cancelled");
        }
    }

    @Test
    void whenEveryAttemptFailsTheLastStatusIsReported() throws Exception {
        try (Server s = server(n -> CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("down " + n))));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withMaxAttempts(3).withDelay(Duration.ofMillis(10))).build()) {
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT));
            assertEquals(Status.Code.UNAVAILABLE, e.status().code());
            assertEquals(3, calls.get());
            assertEquals("down 3", e.status().description());
        }
    }

    @Test
    void noMoreThanMaxAttemptsAreStarted() throws Exception {
        try (Server s = server(n -> new CompletableFuture<>());
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withMaxAttempts(3).withDelay(Duration.ofMillis(40))).build()) {
            CompletableFuture<Msg> f = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT);
            Thread.sleep(600);
            assertEquals(3, calls.get());
            f.cancel(true);
            eventually(() -> cancelled.get() == 3, "cancelling the call cancels every attempt");
        }
    }

    @Test
    void theDeadlineStillAppliesToAHedgedCall() throws Exception {
        try (Server s = server(n -> new CompletableFuture<>());
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofMillis(50))).build()) {
            long t0 = System.nanoTime();
            StatusException e = failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(300))));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, e.status().code());
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 1500);
        }
    }

    @Test
    void streamedRequestsReachTheHedgeToo() throws Exception {
        AtomicInteger seen = new AtomicInteger();
        try (Server s = GrpcServer.builder().clientStreaming(io.hydrangea.examples.GrpcEchoExample.CLIENT_STREAM, (ctx, out) -> {
                 int n = calls.incrementAndGet();
                 return new StreamObserver<Msg>() {
                     int got;
                     public void onNext(Msg m) { got++; }
                     public void onError(Throwable t) { }
                     public void onCompleted() {
                         if (n == 1) return;   // the first attempt never answers
                         seen.set(got);
                         out.onNext(new Msg("n=" + got));
                         out.onCompleted();
                     }
                 };
             }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).hedgingPolicy(HedgingPolicy.defaults().withDelay(Duration.ofMillis(100))).build()) {
            CompletableFuture<Msg> result = new CompletableFuture<>();
            ClientCall<Msg, Msg> call = ch.clientStreaming(io.hydrangea.examples.GrpcEchoExample.CLIENT_STREAM, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { result.complete(m); }
                public void onError(Throwable t) { result.completeExceptionally(t); }
                public void onCompleted() { }
            });
            call.send(new Msg("a"));
            call.send(new Msg("b"));
            Thread.sleep(250);    // the hedge starts now, after both messages were sent
            call.send(new Msg("c"));
            call.halfClose();
            assertEquals("n=3", result.get(10, TimeUnit.SECONDS).text(), "the hedge was replayed a, b and then got c live");
        }
    }

    @Test
    void cancellingOneCallLeavesTheOthersOnTheSameConnectionAlone() throws Exception {
        AtomicInteger connections = new AtomicInteger();
        try (Server s = server(n -> n <= 2 ? new CompletableFuture<>() : CompletableFuture.completedFuture(new Msg("fast")));
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).build()) {
            CompletableFuture<Msg> a = ch.unary(UNARY, new Msg("a"), CallOptions.DEFAULT);
            CompletableFuture<Msg> b = ch.unary(UNARY, new Msg("b"), CallOptions.DEFAULT);
            eventually(() -> calls.get() == 2, "both calls reached the server");
            b.cancel(true);
            eventually(() -> cancelled.get() == 1, "only the cancelled call was cancelled on the server");
            assertEquals("fast", ch.unary(UNARY, new Msg("c"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            Thread.sleep(200);
            assertFalse(a.isDone(), "call a was not disturbed: " + (a.isDone() ? a.handle((r, e) -> e).get() : ""));
            assertEquals(1, cancelled.get());
            a.cancel(true);
        }
    }

    @Test
    void retryAndHedgingCannotBeCombined() {
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("http://127.0.0.1:1")
                .retryPolicy(RetryPolicy.defaults()).hedgingPolicy(HedgingPolicy.defaults()).build());
        assertThrows(IllegalArgumentException.class, () -> new HedgingPolicy(1, Duration.ZERO, Set.of()));
    }

    // ---- throttling ----

    @Test
    void throttlingStopsRetriesWhileTheServerKeepsFailing() throws Exception {
        try (Server s = server(n -> failed(Status.Code.UNAVAILABLE));
             GrpcChannel ch = GrpcChannel.forTarget(url(s))
                     .retryPolicy(RetryPolicy.defaults().withBackoff(Duration.ofMillis(5), Duration.ofMillis(10), 1))
                     .retryThrottling(new RetryThrottling(4, 0.1)).build()) {
            // 4 tokens, retries need more than 2: the first call fails twice (4 -> 3 -> 2) and its second retry is refused
            assertEquals(Status.Code.UNAVAILABLE, failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)).status().code());
            assertEquals(2, calls.get());
            assertNotNull(failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT)));
            assertEquals(3, calls.get(), "the next call gets its first attempt only");
        }
    }

    @Test
    void successesRefillTheBucket() throws Exception {
        AtomicInteger phase = new AtomicInteger();   // 0 = failing, 1 = healthy
        try (Server s = server(n -> phase.get() == 0 ? failed(Status.Code.UNAVAILABLE) : CompletableFuture.completedFuture(new Msg("ok")));
             GrpcChannel ch = GrpcChannel.forTarget(url(s))
                     .retryPolicy(RetryPolicy.defaults().withBackoff(Duration.ofMillis(5), Duration.ofMillis(10), 1))
                     .retryThrottling(new RetryThrottling(4, 1)).build()) {
            failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT));   // drains the bucket to 2
            phase.set(1);
            for (int i = 0; i < 3; i++) {
                ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);   // +1 each
            }
            phase.set(0);
            int before = calls.get();
            failure(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT));
            assertTrue(calls.get() - before >= 2, "retries are allowed again: " + (calls.get() - before));
        }
    }
}
