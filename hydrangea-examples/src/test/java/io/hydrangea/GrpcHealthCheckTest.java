package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

class GrpcHealthCheckTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private Server backend(String name, HealthService health, AtomicInteger hits) throws Exception {
        GrpcServer.Builder b = GrpcServer.builder().unary(UNARY, (req, ctx) -> {
            hits.incrementAndGet();
            return CompletableFuture.completedFuture(new Msg(name));
        });
        if (health != null) {
            b.addService(health);
        }
        return b.build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    private static InetSocketAddress addr(Server s) {
        return new InetSocketAddress("127.0.0.1", ((InetSocketAddress) s.localAddress()).getPort());
    }

    private static Set<String> whoAnswers(GrpcChannel ch, int calls) throws Exception {
        Set<String> names = new HashSet<>();
        for (int i = 0; i < calls; i++) {
            names.add(ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
        }
        return names;
    }

    private static void eventually(java.util.function.BooleanSupplier cond, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
        while (!cond.getAsBoolean() && System.nanoTime() < end) {
            Thread.sleep(25);
        }
        assertTrue(cond.getAsBoolean(), what);
    }

    @Test
    void aServerThatSaysNotServingStopsGettingCallsAndComesBackWhenItRecovers() throws Exception {
        HealthService ha = new HealthService(), hb = new HealthService();
        AtomicInteger hitsA = new AtomicInteger(), hitsB = new AtomicInteger();
        try (Server a = backend("a", ha, hitsA); Server b = backend("b", hb, hitsB);
             GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(NameResolver.fixed(addr(a), addr(b)))
                     .loadBalancing(LoadBalancing.ROUND_ROBIN).healthCheck(Duration.ofMillis(100)).build()) {
            ha.setStatus("", HealthService.ServingStatus.SERVING);
            hb.setStatus("", HealthService.ServingStatus.SERVING);
            eventually(() -> { try { return whoAnswers(ch, 6).equals(Set.of("a", "b")); } catch (Exception e) { return false; } }, "both serve at first");

            hb.setStatus("", HealthService.ServingStatus.NOT_SERVING);
            eventually(() -> { try { return whoAnswers(ch, 6).equals(Set.of("a")); } catch (Exception e) { return false; } }, "b is taken out of rotation");
            int before = hitsB.get();
            assertEquals(Set.of("a"), whoAnswers(ch, 20));
            assertEquals(before, hitsB.get());

            hb.setStatus("", HealthService.ServingStatus.SERVING);
            eventually(() -> { try { return whoAnswers(ch, 6).equals(Set.of("a", "b")); } catch (Exception e) { return false; } }, "b is back");
        }
    }

    @Test
    void aDeadAddressIsAvoidedBeforeAnyCallFailsOnIt() throws Exception {
        int dead;
        try (ServerSocket tmp = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            dead = tmp.getLocalPort();
        }
        HealthService h = new HealthService();
        AtomicInteger hits = new AtomicInteger();
        try (Server live = backend("live", h, hits);
             GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1")
                     .nameResolver(NameResolver.fixed(new InetSocketAddress("127.0.0.1", dead), addr(live)))
                     .loadBalancing(LoadBalancing.ROUND_ROBIN).healthCheck(Duration.ofMillis(100)).build()) {
            h.setStatus("", HealthService.ServingStatus.SERVING);
            Thread.sleep(500);   // a few probe rounds
            for (int i = 0; i < 20; i++) {
                assertEquals("live", ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
            }
        }
    }

    @Test
    void aServerWithoutAHealthServiceStaysInRotation() throws Exception {
        HealthService h = new HealthService();
        AtomicInteger hitsA = new AtomicInteger(), hitsB = new AtomicInteger();
        try (Server withHealth = backend("a", h, hitsA); Server without = backend("b", null, hitsB);
             GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(NameResolver.fixed(addr(withHealth), addr(without)))
                     .loadBalancing(LoadBalancing.ROUND_ROBIN).healthCheck(Duration.ofMillis(100)).build()) {
            h.setStatus("", HealthService.ServingStatus.SERVING);
            Thread.sleep(500);
            assertEquals(Set.of("a", "b"), whoAnswers(ch, 10));
        }
    }

    @Test
    void aServerThatWasDownAndComesBackIsUsedAgainWithoutWaitingOutTheBackoff() throws Exception {
        int port;
        try (ServerSocket tmp = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            port = tmp.getLocalPort();
        }
        HealthService hLive = new HealthService(), hBack = new HealthService();
        AtomicInteger hits = new AtomicInteger();
        try (Server live = backend("live", hLive, hits);
             GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1")
                     .nameResolver(NameResolver.fixed(new InetSocketAddress("127.0.0.1", port), addr(live)))
                     .loadBalancing(LoadBalancing.ROUND_ROBIN).healthCheck(Duration.ofMillis(100)).build()) {
            hLive.setStatus("", HealthService.ServingStatus.SERVING);
            Thread.sleep(1200);   // a dozen failed probes: the dead address is now deep into its (up to 30 s) backoff
            assertEquals(Set.of("live"), whoAnswers(ch, 6));
            Server back = GrpcServer.builder().addService(hBack).unary(UNARY, (req, ctx) -> CompletableFuture.completedFuture(new Msg("back")))
                    .build().bind(group, new InetSocketAddress("127.0.0.1", port), null);
            try {
                hBack.setStatus("", HealthService.ServingStatus.SERVING);
                eventually(() -> { try { return whoAnswers(ch, 6).contains("back"); } catch (Exception e) { return false; } }, "the recovered server gets calls again");
            } finally {
                back.close();
            }
        }
    }

    @Test
    void theCheckedServiceNameIsConfigurable() throws Exception {
        HealthService h = new HealthService();
        AtomicInteger hits = new AtomicInteger(), other = new AtomicInteger();
        try (Server a = backend("a", h, hits); Server b = backend("b", new HealthService(), other);
             GrpcChannel ch = GrpcChannel.forTarget("http://backend.invalid:1").nameResolver(NameResolver.fixed(addr(a), addr(b)))
                     .loadBalancing(LoadBalancing.ROUND_ROBIN).healthCheck(Duration.ofMillis(100)).healthCheckService("my.Service").build()) {
            h.setStatus("my.Service", HealthService.ServingStatus.SERVING);
            // b does not know "my.Service": it answers NOT_FOUND, an error, which marks it failed
            eventually(() -> { try { return whoAnswers(ch, 6).equals(Set.of("a")); } catch (Exception e) { return false; } }, "only the server that serves my.Service gets calls");
        }
    }
}
