package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
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

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

class GrpcServiceConfigTest {

    private static final String SERVICE = "hydrangea.test.Echo";

    // ---- parsing ----

    @Test
    void parsesAFullConfig() {
        ServiceConfig c = ServiceConfig.parse("""
                {"loadBalancingConfig": [{"unknown_policy": {}}, {"round_robin": {}}],
                 "methodConfig": [
                   {"name": [{"service": "a.B", "method": "Get"}], "timeout": "1.5s",
                    "retryPolicy": {"maxAttempts": 4, "initialBackoff": "0.1s", "maxBackoff": "2s", "backoffMultiplier": 3,
                                    "retryableStatusCodes": ["UNAVAILABLE", 4]}},
                   {"name": [{"service": "a.B"}], "hedgingPolicy": {"maxAttempts": 3, "hedgingDelay": "0.25s", "nonFatalStatusCodes": ["UNAVAILABLE"]},
                    "maxRequestMessageBytes": 1000, "maxResponseMessageBytes": 2000},
                   {"name": [{}], "timeout": "30s", "futureField": [1, 2, {"x": null}]}],
                 "retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1},
                 "healthCheckConfig": {"serviceName": "a.B"}}
                """);
        assertEquals(LoadBalancing.ROUND_ROBIN, c.loadBalancing(), "the first policy we know");
        assertEquals(new RetryThrottling(10, 0.1), c.retryThrottling());
        assertEquals("a.B", c.healthCheckServiceName());
        ServiceConfig.MethodConfig get = c.forMethod("a.B", "Get");
        assertEquals(Duration.ofMillis(1500), get.timeout());
        assertEquals(4, get.retry().maxAttempts());
        assertEquals(Duration.ofMillis(100), get.retry().initialBackoff());
        assertEquals(3.0, get.retry().multiplier());
        assertEquals(Set.of(Status.Code.UNAVAILABLE, Status.Code.DEADLINE_EXCEEDED), get.retry().retryableCodes());
        ServiceConfig.MethodConfig other = c.forMethod("a.B", "Other");
        assertEquals(3, other.hedging().maxAttempts());
        assertEquals(Duration.ofMillis(250), other.hedging().hedgingDelay());
        assertEquals(1000, other.maxRequestMessageBytes());
        assertEquals(2000, other.maxResponseMessageBytes());
        assertEquals(Duration.ofSeconds(30), c.forMethod("x.Y", "Z").timeout(), "the empty selector matches everything");
    }

    @Test
    void theMostSpecificSelectorWins() {
        ServiceConfig c = ServiceConfig.parse("""
                {"methodConfig": [
                  {"name": [{}], "timeout": "1s"},
                  {"name": [{"service": "s.S"}], "timeout": "2s"},
                  {"name": [{"service": "s.S", "method": "M"}], "timeout": "3s"}]}
                """);
        assertEquals(Duration.ofSeconds(3), c.forMethod("s.S", "M").timeout());
        assertEquals(Duration.ofSeconds(2), c.forMethod("s.S", "N").timeout());
        assertEquals(Duration.ofSeconds(1), c.forMethod("t.T", "M").timeout());
        assertNull(ServiceConfig.parse("{}").forMethod("s.S", "M"));
    }

    @Test
    void invalidConfigsAreRejectedWithAClearReason() {
        String[] bad = {
                "not json", "[]", "{\"methodConfig\": [{\"name\": [{\"method\": \"M\"}]}]}", "{\"methodConfig\": [{\"name\": []}]}",
                "{\"methodConfig\": [{\"name\": [{}], \"timeout\": \"fast\"}]}",
                "{\"methodConfig\": [{\"name\": [{}], \"timeout\": \"-1s\"}]}",
                "{\"methodConfig\": [{\"name\": [{}], \"retryPolicy\": {\"maxAttempts\": 1, \"initialBackoff\": \"1s\", \"maxBackoff\": \"1s\", \"backoffMultiplier\": 2, \"retryableStatusCodes\": [\"UNAVAILABLE\"]}}]}",
                "{\"methodConfig\": [{\"name\": [{}], \"retryPolicy\": {\"maxAttempts\": 3, \"initialBackoff\": \"1s\", \"maxBackoff\": \"1s\", \"backoffMultiplier\": 2, \"retryableStatusCodes\": [\"NOPE\"]}}]}",
                "{\"methodConfig\": [{\"name\": [{}], \"retryPolicy\": {\"maxAttempts\": 3, \"initialBackoff\": \"1s\", \"maxBackoff\": \"1s\", \"backoffMultiplier\": 2, \"retryableStatusCodes\": [\"UNAVAILABLE\"]}, \"hedgingPolicy\": {\"maxAttempts\": 2}}]}",
                "{\"retryThrottling\": {\"maxTokens\": 0, \"tokenRatio\": 0.1}}", "{\"a\": 1,}", "{\"a\": \"unterminated}",
        };
        for (String b : bad) {
            assertThrows(IllegalArgumentException.class, () -> ServiceConfig.parse(b), b);
        }
    }

    @Test
    void jsonStringsHandleEscapesAndDeepNesting() {
        ServiceConfig c = ServiceConfig.parse("{\"healthCheckConfig\": {\"serviceName\": \"caf\\u00e9\\n\\\"q\\\"\"}}");
        assertEquals("café\n\"q\"", c.healthCheckServiceName());
        assertThrows(IllegalArgumentException.class, () -> ServiceConfig.parse("[".repeat(200) + "]".repeat(200)));
    }

    // ---- applied by a channel ----

    private EventLoopGroup group;
    private final AtomicInteger unaryCalls = new AtomicInteger(), streamCalls = new AtomicInteger();

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private Server failing() throws Exception {
        return GrpcServer.builder()
                .unary(UNARY, (req, ctx) -> {
                    unaryCalls.incrementAndGet();
                    return CompletableFuture.failedFuture(new StatusException(Status.of(Status.Code.UNAVAILABLE)));
                })
                .serverStreaming(GrpcEchoExample.SERVER_STREAM, (req, ctx, out) -> {
                    streamCalls.incrementAndGet();
                    out.onError(new StatusException(Status.of(Status.Code.UNAVAILABLE)));
                }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    private static String url(Server s) {
        return "http://127.0.0.1:" + ((InetSocketAddress) s.localAddress()).getPort();
    }

    private static final String RETRY_UNARY_ONLY = """
            {"methodConfig": [{"name": [{"service": "hydrangea.test.Echo", "method": "Unary"}],
               "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.005s", "maxBackoff": "0.01s", "backoffMultiplier": 1,
                               "retryableStatusCodes": ["UNAVAILABLE"]}}]}
            """;

    @Test
    void aMethodsRetryPolicyAppliesToThatMethodOnly() throws Exception {
        try (Server s = failing(); GrpcChannel ch = GrpcChannel.forTarget(url(s)).serviceConfig(RETRY_UNARY_ONLY).build()) {
            assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(3, unaryCalls.get());
            CompletableFuture<Void> done = new CompletableFuture<>();
            ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("x", 1), CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(Msg m) { }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            assertThrows(ExecutionException.class, () -> done.get(10, TimeUnit.SECONDS));
            assertEquals(1, streamCalls.get(), "no policy for ServerStream: not retried");
        }
    }

    @Test
    void theConfiguredTimeoutIsTheDefaultDeadlineButACallerCanShortenIt() throws Exception {
        try (Server s = GrpcServer.builder().unary(UNARY, (req, ctx) -> new CompletableFuture<>()).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget(url(s)).serviceConfig("{\"methodConfig\": [{\"name\": [{}], \"timeout\": \"0.3s\"}]}").build()) {
            long t0 = System.nanoTime();
            ExecutionException e = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusException) e.getCause()).status().code());
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000);
            t0 = System.nanoTime();
            assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(100))).get(10, TimeUnit.SECONDS));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 250, "the caller's shorter deadline wins");
        }
    }

    @Test
    void messageSizeLimitsFromTheConfigAreEnforced() throws Exception {
        try (Server s = GrpcServer.builder().unary(UNARY, (req, ctx) -> CompletableFuture.completedFuture(new Msg("big", 0, new byte[5000]))).build()
                .bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             GrpcChannel ch = GrpcChannel.forTarget(url(s))
                     .serviceConfig("{\"methodConfig\": [{\"name\": [{}], \"maxRequestMessageBytes\": 100, \"maxResponseMessageBytes\": 1000}]}").build()) {
            ExecutionException big = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x", 0, new byte[500]), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, ((StatusException) big.getCause()).status().code());
            assertTrue(((StatusException) big.getCause()).status().description().contains("request"));
            ExecutionException resp = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, ((StatusException) resp.getCause()).status().code(), String.valueOf(((StatusException) resp.getCause()).status()) + " cause=" + ((StatusException) resp.getCause()).status().cause());
            // a caller can lift the response limit for one call
            assertEquals(5000, ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMaxResponseBytes(100_000)).get(10, TimeUnit.SECONDS).blob().length);
        }
    }

    @Test
    void theLoadBalancingPolicyComesFromTheConfigUnlessTheBuilderSaysOtherwise() throws Exception {
        AtomicInteger a = new AtomicInteger(), b = new AtomicInteger();
        try (Server sa = GrpcServer.builder().unary(UNARY, (r, c) -> { a.incrementAndGet(); return CompletableFuture.completedFuture(new Msg("a")); }).build()
                .bind(group, new InetSocketAddress("127.0.0.1", 0), null);
             Server sb = GrpcServer.builder().unary(UNARY, (r, c) -> { b.incrementAndGet(); return CompletableFuture.completedFuture(new Msg("b")); }).build()
                     .bind(group, new InetSocketAddress("127.0.0.1", 0), null)) {
            NameResolver two = NameResolver.fixed(new InetSocketAddress("127.0.0.1", ((InetSocketAddress) sa.localAddress()).getPort()),
                    new InetSocketAddress("127.0.0.1", ((InetSocketAddress) sb.localAddress()).getPort()));
            String rr = "{\"loadBalancingConfig\": [{\"round_robin\": {}}]}";
            try (GrpcChannel ch = GrpcChannel.forTarget("http://x.invalid:1").nameResolver(two).serviceConfig(rr).build()) {
                for (int i = 0; i < 10; i++) ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            }
            assertEquals(5, a.get());
            assertEquals(5, b.get());
            a.set(0); b.set(0);
            try (GrpcChannel ch = GrpcChannel.forTarget("http://x.invalid:1").nameResolver(two).serviceConfig(rr).loadBalancing(LoadBalancing.PICK_FIRST).build()) {
                for (int i = 0; i < 10; i++) ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            }
            assertEquals(10, a.get(), "an explicit builder setting wins");
        }
    }

    @Test
    void aBadConfigFailsTheBuilderNotTheCalls() {
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("http://127.0.0.1:1").serviceConfig("{oops"));
    }
}
