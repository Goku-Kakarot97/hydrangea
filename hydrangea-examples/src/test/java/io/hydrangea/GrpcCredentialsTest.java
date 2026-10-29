package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

class GrpcCredentialsTest {

    private EventLoopGroup group;
    private Server server;
    private final List<String> seenAuth = new CopyOnWriteArrayList<>();
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger starts = new AtomicInteger();   // streams that reached the server at all, handled or not

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        server = GrpcServer.builder().intercept((m, c) -> { starts.incrementAndGet(); return null; }).unary(UNARY, (req, ctx) -> {
            calls.incrementAndGet();
            seenAuth.add(String.valueOf(ctx.requestMetadata().get("authorization")));
            return CompletableFuture.completedFuture(new Msg("ok:" + req.text()));
        }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private GrpcChannel.Builder channel() {
        return GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).allowCredentialsOverPlaintext();
    }

    @Test
    void theTokenIsFetchedForEachCall() throws Exception {
        AtomicInteger n = new AtomicInteger();
        try (GrpcChannel ch = channel().callCredentials(CallCredentials.bearer(() -> "t" + n.incrementAndGet())).build()) {
            ch.unary(UNARY, new Msg("a"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            ch.unary(UNARY, new Msg("b"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            assertEquals(List.of("Bearer t1", "Bearer t2"), seenAuth);
        }
    }

    @Test
    void aSlowCredentialDelaysTheCallAndKeepsTheMessagesSentMeanwhile() throws Exception {
        CompletableFuture<Metadata> gate = new CompletableFuture<>();
        try (GrpcChannel ch = channel().callCredentials((path, authority) -> gate).build()) {
            CompletableFuture<Msg> result = ch.unary(UNARY, new Msg("held"), CallOptions.DEFAULT);
            Thread.sleep(300);
            assertEquals(0, calls.get(), "nothing reaches the server before the credentials are known");
            assertFalse(result.isDone());
            gate.complete(new Metadata().put("authorization", "late-token"));
            assertEquals("ok:held", result.get(10, TimeUnit.SECONDS).text());
            assertEquals(List.of("late-token"), seenAuth);
        }
    }

    @Test
    void failingCredentialsFailTheCallWithoutContactingTheServer() throws Exception {
        try (GrpcChannel ch = channel().callCredentials((p, a) -> CompletableFuture.failedFuture(new IllegalStateException("token service down"))).build()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.UNAUTHENTICATED, ((StatusException) e.getCause()).status().code());
            assertEquals(0, calls.get());
        }
        try (GrpcChannel ch = channel().callCredentials((p, a) -> { throw new IllegalStateException("boom"); }).build()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.UNAUTHENTICATED, ((StatusException) e.getCause()).status().code());
        }
    }

    @Test
    void aCredentialsFailureCanCarryItsOwnStatus() throws Exception {
        try (GrpcChannel ch = channel().callCredentials((p, a) ->
                CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("try later")))).build()) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.UNAVAILABLE, ((StatusException) e.getCause()).status().code());
        }
    }

    @Test
    void cancellingWhileTheCredentialsLoadSendsNothing() throws Exception {
        CompletableFuture<Metadata> gate = new CompletableFuture<>();
        try (GrpcChannel ch = channel().callCredentials((path, authority) -> gate).build()) {
            CompletableFuture<Msg> result = ch.unary(UNARY, new Msg("never"), CallOptions.DEFAULT);
            result.cancel(true);
            gate.complete(new Metadata().put("authorization", "x"));
            Thread.sleep(300);
            assertEquals(0, calls.get());
            assertEquals(0, starts.get(), "not even a request head was sent");
        }
    }

    @Test
    void theDeadlineCoversTheTimeSpentGettingCredentials() throws Exception {
        try (GrpcChannel ch = channel().callCredentials((path, authority) -> new CompletableFuture<>()).build()) {
            ExecutionException e = assertThrows(ExecutionException.class, () ->
                    ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(200))).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusException) e.getCause()).status().code());
        }
    }

    @Test
    void everyRetryAttemptFetchesFreshCredentials() throws Exception {
        AtomicInteger fetched = new AtomicInteger();
        try (Server flaky = GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            seenAuth.add(String.valueOf(ctx.requestMetadata().get("authorization")));
            return seenAuth.size() < 3 ? CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.UNAVAILABLE)))
                    : CompletableFuture.completedFuture(new Msg("ok"));
        }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) flaky.localAddress()).getPort())
                     .allowCredentialsOverPlaintext()
                     .callCredentials(CallCredentials.bearer(() -> "tok" + fetched.incrementAndGet()))
                     .retryPolicy(RetryPolicy.defaults().withBackoff(Duration.ofMillis(5), Duration.ofMillis(10), 1)).build()) {
            ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            assertEquals(List.of("Bearer tok1", "Bearer tok2", "Bearer tok3"), seenAuth);
        }
    }

    @Test
    void credentialsRefuseToTravelInTheClearByDefault() {
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("http://127.0.0.1:1").callCredentials(CallCredentials.bearer(() -> "x")).build());
        // https is fine (building does not connect)
        assertDoesNotThrow(() -> GrpcChannel.forTarget("https://localhost:1").callCredentials(CallCredentials.bearer(() -> "x")).build().close());
    }
}
