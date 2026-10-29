import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Message;
import com.google.protobuf.UInt32Value;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.cluster.v3.Cluster;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.core.v3.Address;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.core.v3.AggregatedConfigSource;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.core.v3.ConfigSource;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.core.v3.HealthStatus;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.core.v3.SocketAddress;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.endpoint.v3.ClusterLoadAssignment;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.endpoint.v3.Endpoint;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.endpoint.v3.LbEndpoint;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.endpoint.v3.LocalityLbEndpoints;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.listener.v3.ApiListener;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.listener.v3.Listener;
import io.grpc.xds.shaded.io.envoyproxy.envoy.config.route.v3.*;
import io.grpc.xds.shaded.io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager;
import io.grpc.xds.shaded.io.envoyproxy.envoy.extensions.filters.network.http_connection_manager.v3.Rds;
import io.grpc.xds.shaded.io.envoyproxy.envoy.service.discovery.v3.AggregatedDiscoveryServiceGrpc;
import io.grpc.xds.shaded.io.envoyproxy.envoy.service.discovery.v3.DiscoveryRequest;
import io.grpc.xds.shaded.io.envoyproxy.envoy.service.discovery.v3.DiscoveryResponse;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static io.hydrangea.examples.GrpcEchoExample.SERVER_STREAM;

/**
 * Hydrangea's xDS client against a control plane built from grpc-java's generated Envoy API classes (an independent encoder of every message
 * the client has to read), routing to Hydrangea echo servers. Everything runs in one JVM.
 */
public class XdsInterop {
    static final String LDS = "type.googleapis.com/envoy.config.listener.v3.Listener";
    static final String RDS = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
    static final String CDS = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
    static final String EDS = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";

    static int failures;
    static void check(String what, boolean ok, Object detail) { System.out.println((ok ? "  ok   " : "  FAIL ") + what + (ok ? "" : "  -> " + detail)); if (!ok) failures++; }

    // ---- the control plane ----

    static class ControlPlane extends AggregatedDiscoveryServiceGrpc.AggregatedDiscoveryServiceImplBase {
        final Map<String, Map<String, Message>> resources = new ConcurrentHashMap<>();
        final AtomicInteger version = new AtomicInteger();
        final List<Conn> conns = new CopyOnWriteArrayList<>();
        final List<String> nacks = new CopyOnWriteArrayList<>();
        final List<DiscoveryRequest> firstRequests = new CopyOnWriteArrayList<>();

        class Conn {
            final StreamObserver<DiscoveryResponse> out;
            final Map<String, Set<String>> subscribed = new ConcurrentHashMap<>();
            final Map<String, String> lastNonce = new ConcurrentHashMap<>();
            final AtomicInteger nonce = new AtomicInteger();
            Conn(StreamObserver<DiscoveryResponse> out) { this.out = out; }

            synchronized void send(String type) {
                Set<String> names = subscribed.getOrDefault(type, Set.of());
                if (names.isEmpty()) return;
                DiscoveryResponse.Builder r = DiscoveryResponse.newBuilder().setTypeUrl(type).setVersionInfo(String.valueOf(version.get()));
                Map<String, Message> all = resources.getOrDefault(type, Map.of());
                for (String n : names) { Message m = all.get(n); if (m != null) r.addResources(Any.pack(m)); }
                String nn = String.valueOf(nonce.incrementAndGet());
                lastNonce.put(type, nn);
                r.setNonce(nn);
                try { out.onNext(r.build()); } catch (RuntimeException closed) { }
            }
        }

        @Override public StreamObserver<DiscoveryRequest> streamAggregatedResources(StreamObserver<DiscoveryResponse> out) {
            Conn c = new Conn(out);
            conns.add(c);
            return new StreamObserver<>() {
                boolean first = true;
                public void onNext(DiscoveryRequest q) {
                    if (first) { firstRequests.add(q); first = false; }
                    String type = q.getTypeUrl();
                    if (q.hasErrorDetail()) { nacks.add(type + ": " + q.getErrorDetail().getMessage()); return; }
                    Set<String> names = new TreeSet<>(q.getResourceNamesList());
                    boolean ack = !q.getResponseNonce().isEmpty() && q.getResponseNonce().equals(c.lastNonce.get(type)) && names.equals(c.subscribed.get(type));
                    c.subscribed.put(type, names);
                    if (!ack) c.send(type);
                }
                public void onError(Throwable t) { conns.remove(c); }
                public void onCompleted() { conns.remove(c); out.onCompleted(); }
            };
        }

        void set(String type, String name, Message m) {
            resources.computeIfAbsent(type, k -> new ConcurrentHashMap<>()).put(name, m);
            version.incrementAndGet();
            for (Conn c : conns) c.send(type);
        }

        void remove(String type, String name) {
            Optional.ofNullable(resources.get(type)).ifPresent(m -> m.remove(name));
            version.incrementAndGet();
            for (Conn c : conns) c.send(type);
        }
    }

    // ---- resource builders ----

    static Listener listener(String name, String rdsName) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder()
                .setRds(Rds.newBuilder().setRouteConfigName(rdsName).setConfigSource(ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))).build();
        return Listener.newBuilder().setName(name).setApiListener(ApiListener.newBuilder().setApiListener(Any.pack(hcm))).build();
    }

    static Listener inlineListener(String name, RouteConfiguration rc) {
        HttpConnectionManager hcm = HttpConnectionManager.newBuilder().setRouteConfig(rc).build();
        return Listener.newBuilder().setName(name).setApiListener(ApiListener.newBuilder().setApiListener(Any.pack(hcm))).build();
    }

    static Route route(RouteMatch.Builder match, RouteAction.Builder action) {
        return Route.newBuilder().setMatch(match).setRoute(action).build();
    }

    static RouteConfiguration routes(String name, String domain, Route... routes) {
        return RouteConfiguration.newBuilder().setName(name).addVirtualHosts(VirtualHost.newBuilder().setName("vh").addDomains(domain).addAllRoutes(Arrays.asList(routes))).build();
    }

    static Cluster edsCluster(String name) {
        return Cluster.newBuilder().setName(name).setType(Cluster.DiscoveryType.EDS)
                .setEdsClusterConfig(Cluster.EdsClusterConfig.newBuilder().setServiceName(name + "-eds").setEdsConfig(ConfigSource.newBuilder().setAds(AggregatedConfigSource.getDefaultInstance()))).build();
    }

    static LbEndpoint endpoint(int port, int weight, HealthStatus health) {
        return LbEndpoint.newBuilder().setHealthStatus(health).setLoadBalancingWeight(UInt32Value.of(weight))
                .setEndpoint(Endpoint.newBuilder().setAddress(Address.newBuilder().setSocketAddress(SocketAddress.newBuilder().setAddress("127.0.0.1").setPortValue(port)))).build();
    }

    static ClusterLoadAssignment assignment(String cluster, LbEndpoint... eps) {
        return ClusterLoadAssignment.newBuilder().setClusterName(cluster + "-eds").addEndpoints(LocalityLbEndpoints.newBuilder().addAllLbEndpoints(Arrays.asList(eps))).build();
    }

    // ---- scenario ----

    static String who(GrpcChannel ch, MethodDescriptor<Msg, Msg> m, Metadata md) throws Exception {
        if (m == UNARY) return ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMetadata(md)).get(10, TimeUnit.SECONDS).text();
        throw new IllegalArgumentException();
    }

    static String name(EchoServers s, String text) { return text; }

    record EchoServers(Server a, Server b, int portA, int portB) { }

    static Server backend(EventLoopGroup g, String name, AtomicInteger hits) throws Exception {
        return GrpcServer.builder()
                .unary(UNARY, (req, ctx) -> { hits.incrementAndGet(); return req.text().startsWith("SLOW") ? new CompletableFuture<Msg>() : CompletableFuture.completedFuture(new Msg(name)); })
                .serverStreaming(SERVER_STREAM, (req, ctx, out) -> { hits.incrementAndGet(); out.onNext(new Msg(name)); out.onCompleted(); })
                .build().bind(g, new InetSocketAddress("127.0.0.1", 0), null);
    }

    static int port(Server s) { return ((InetSocketAddress) s.localAddress()).getPort(); }

    static String streamWho(GrpcChannel ch) throws Exception {
        CompletableFuture<String> f = new CompletableFuture<>();
        ch.serverStreaming(SERVER_STREAM, new Msg("x", 1), CallOptions.DEFAULT, new io.hydrangea.grpc.StreamObserver<Msg>() {
            String got;
            public void onNext(Msg m) { got = m.text(); }
            public void onError(Throwable t) { f.completeExceptionally(t); }
            public void onCompleted() { f.complete(got); }
        });
        return f.get(10, TimeUnit.SECONDS);
    }

    static void eventually(String what, java.util.function.BooleanSupplier c) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        boolean ok;
        while (!(ok = c.getAsBoolean()) && System.nanoTime() < end) Thread.sleep(50);
        check(what, ok, "timed out");
    }

    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new EventLoopGroup(2);
        AtomicInteger hitsA = new AtomicInteger(), hitsB = new AtomicInteger();
        Server a = backend(group, "A", hitsA), b = backend(group, "B", hitsB);
        int pa = port(a), pb = port(b);

        ControlPlane cp = new ControlPlane();
        io.grpc.Server xds = ServerBuilder.forPort(0).addService(cp).build().start();
        int xdsPort = xds.getPort();
        System.out.println("control plane on " + xdsPort + ", backends A=" + pa + " B=" + pb);

        // initial config: Unary -> c1 (A), everything else -> c2 (B)
        cp.set(LDS, "echo-service", listener("echo-service", "routes"));
        cp.set(RDS, "routes", routes("routes", "echo-service",
                route(RouteMatch.newBuilder().setPath("/hydrangea.test.Echo/Unary"), RouteAction.newBuilder().setCluster("c1")),
                route(RouteMatch.newBuilder().setPrefix(""), RouteAction.newBuilder().setCluster("c2"))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpoint(pa, 1, HealthStatus.HEALTHY)));
        cp.set(EDS, "c2-eds", assignment("c2", endpoint(pb, 1, HealthStatus.UNKNOWN)));

        String bootstrap = "{\"xds_servers\":[{\"server_uri\":\"127.0.0.1:" + xdsPort + "\",\"channel_creds\":[{\"type\":\"insecure\"}]}],"
                + "\"node\":{\"id\":\"hydrangea-test\",\"cluster\":\"interop\",\"locality\":{\"region\":\"r1\",\"zone\":\"z1\"}}}";
        try (GrpcChannel ch = GrpcChannel.forTarget("xds:///echo-service").xdsBootstrap(bootstrap).build()) {
            System.out.println("== routing by path");
            check("Unary goes to A (path route -> c1)", who(ch, UNARY, new Metadata()).equals("A"), "");
            check("ServerStream goes to B (prefix route -> c2)", streamWho(ch).equals("B"), "");
            DiscoveryRequest first = cp.firstRequests.get(0);
            check("the first request carries the node", first.getNode().getId().equals("hydrangea-test") && first.getNode().getCluster().equals("interop")
                    && first.getNode().getLocality().getRegion().equals("r1") && first.getTypeUrl().equals(LDS) && first.getResourceNamesList().equals(List.of("echo-service")), first);

            System.out.println("== endpoint weights");
            cp.set(EDS, "c1-eds", assignment("c1", endpoint(pa, 1, HealthStatus.HEALTHY), endpoint(pb, 3, HealthStatus.HEALTHY)));
            hitsA.set(0); hitsB.set(0);
            eventually("new endpoints arrive", () -> { try { Set<String> seen = new HashSet<>(); for (int i = 0; i < 4; i++) seen.add(who(ch, UNARY, new Metadata())); return seen.size() == 2; } catch (Exception e) { return false; } });
            hitsA.set(0); hitsB.set(0);
            for (int i = 0; i < 40; i++) who(ch, UNARY, new Metadata());
            check("1:3 weights give 10:30 over 40 calls", hitsA.get() == 10 && hitsB.get() == 30, hitsA.get() + ":" + hitsB.get());

            System.out.println("== health status");
            cp.set(EDS, "c1-eds", assignment("c1", endpoint(pa, 1, HealthStatus.UNHEALTHY), endpoint(pb, 1, HealthStatus.HEALTHY)));
            eventually("an UNHEALTHY endpoint stops receiving calls", () -> { try { hitsA.set(0); for (int i = 0; i < 6; i++) who(ch, UNARY, new Metadata()); return hitsA.get() == 0; } catch (Exception e) { return false; } });

            System.out.println("== weighted clusters and header matching");
            cp.set(EDS, "c1-eds", assignment("c1", endpoint(pa, 1, HealthStatus.HEALTHY)));
            cp.set(RDS, "routes", routes("routes", "echo-service",
                    route(RouteMatch.newBuilder().setPrefix("/").addHeaders(HeaderMatcher.newBuilder().setName("x-canary").setExactMatch("yes")), RouteAction.newBuilder().setCluster("c2")),
                    route(RouteMatch.newBuilder().setPrefix("/").addHeaders(HeaderMatcher.newBuilder().setName("x-region").setStringMatch(io.grpc.xds.shaded.io.envoyproxy.envoy.type.matcher.v3.StringMatcher.newBuilder().setPrefix("eu-"))), RouteAction.newBuilder().setCluster("c2")),
                    route(RouteMatch.newBuilder().setPrefix("/"), RouteAction.newBuilder().setWeightedClusters(WeightedCluster.newBuilder()
                            .addClusters(WeightedCluster.ClusterWeight.newBuilder().setName("c1").setWeight(UInt32Value.of(75)))
                            .addClusters(WeightedCluster.ClusterWeight.newBuilder().setName("c2").setWeight(UInt32Value.of(25)))))));
            eventually("the new route table is in use (x-canary routes to B)", () -> { try { return who(ch, UNARY, new Metadata().put("x-canary", "yes")).equals("B"); } catch (Exception e) { return false; } });
            check("a string_match header (prefix eu-) routes to B", who(ch, UNARY, new Metadata().put("x-region", "eu-west")).equals("B"), "");
            check("a header that does not match falls through", !who(ch, UNARY, new Metadata().put("x-canary", "no")).isEmpty(), "");
            int toA = 0;
            for (int i = 0; i < 400; i++) if (who(ch, UNARY, new Metadata()).equals("A")) toA++;
            check("75/25 weighted clusters send roughly 75% to A (got " + toA + "/400)", toA > 270 && toA < 330, toA);

            System.out.println("== route timeout");
            cp.set(RDS, "routes", routes("routes", "*", route(RouteMatch.newBuilder().setPrefix(""),
                    RouteAction.newBuilder().setCluster("c2").setTimeout(com.google.protobuf.Duration.newBuilder().setNanos(200_000_000)))));
            eventually("the wildcard domain and the timeout route are in use", () -> { try { for (int i = 0; i < 12; i++) if (!"B".equals(streamWho(ch))) return false; return true; } catch (Exception e) { return false; } });
            long t0 = System.nanoTime();
            try { ch.unary(UNARY, new Msg("SLOW:5000"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS); check("slow call exceeds the route timeout", false, "no error"); }
            catch (ExecutionException e) { check("route timeout becomes DEADLINE_EXCEEDED quickly", ((StatusException) e.getCause()).status().code() == Status.Code.DEADLINE_EXCEEDED, e.getCause()); }
            check("...in well under a second", (System.nanoTime() - t0) / 1_000_000 < 1500, (System.nanoTime() - t0) / 1_000_000);
            cp.set(RDS, "routes", routes("routes", "echo-service", route(RouteMatch.newBuilder().setPrefix(""), RouteAction.newBuilder().setCluster("c2"))));

            System.out.println("== NACK keeps the last good configuration");
            cp.nacks.clear();
            cp.set(CDS, "c2", edsCluster("c2").toBuilder().setLbPolicy(Cluster.LbPolicy.RING_HASH).build());
            eventually("the control plane receives a NACK that says why", () -> cp.nacks.stream().anyMatch(n -> n.contains("CDS".toLowerCase()) || n.contains("Cluster")) && cp.nacks.stream().anyMatch(n -> n.contains("lb_policy")));
            check("calls still work with the old configuration", who(ch, UNARY, new Metadata()).equals("B"), "");
            cp.set(CDS, "c2", edsCluster("c2"));

            System.out.println("== removing and re-adding the listener");
            cp.remove(LDS, "echo-service");
            eventually("without a listener calls fail UNAVAILABLE", () -> { try { who(ch, UNARY, new Metadata()); return false; } catch (ExecutionException e) { return ((StatusException) e.getCause()).status().code() == Status.Code.UNAVAILABLE; } catch (Exception e) { return false; } });
            cp.set(LDS, "echo-service", listener("echo-service", "routes"));
            eventually("and work again when it returns", () -> { try { return who(ch, UNARY, new Metadata()).equals("B"); } catch (Exception e) { return false; } });

            System.out.println("== the control plane goes away and comes back");
            xds.shutdownNow().awaitTermination();
            for (int i = 0; i < 5; i++) check("calls keep working from the last snapshot (" + i + ")", who(ch, UNARY, new Metadata()).equals("B"), "");
            io.grpc.Server xds2 = ServerBuilder.forPort(xdsPort).addService(cp).build().start();
            cp.set(RDS, "routes", routes("routes", "echo-service", route(RouteMatch.newBuilder().setPrefix(""), RouteAction.newBuilder().setCluster("c1"))));
            eventually("after it returns, the client reconnects and applies new routes", () -> { try { return who(ch, UNARY, new Metadata()).equals("A"); } catch (Exception e) { return false; } });
            check("the reconnected stream started from scratch (version empty)", cp.firstRequests.size() >= 2 && cp.firstRequests.get(cp.firstRequests.size() - 1).getVersionInfo().isEmpty(), cp.firstRequests.size());
            xds2.shutdownNow();
        }
        group.close();
        System.out.println(failures == 0 ? "ALL OK" : failures + " FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }
}
