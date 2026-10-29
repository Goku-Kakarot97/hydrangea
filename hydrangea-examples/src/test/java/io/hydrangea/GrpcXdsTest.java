package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The xDS client against a small management server written with Hydrangea itself. The messages are encoded by hand here, so the field numbers
 * are checked separately, against grpc-java's Envoy classes, by interop/XdsInterop.java.
 */
class GrpcXdsTest {

    static final String LDS = "type.googleapis.com/envoy.config.listener.v3.Listener";
    static final String RDS = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
    static final String CDS = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
    static final String EDS = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
    static final String HCM = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";

    private static final Marshaller<byte[]> RAW = Marshaller.of(b -> b, b -> b);

    // ---- a management server ----

    static final class ControlPlane {
        final Map<String, Map<String, byte[]>> resources = new ConcurrentHashMap<>();
        final AtomicInteger version = new AtomicInteger();
        final List<Conn> conns = new CopyOnWriteArrayList<>();
        final List<String> nacks = new CopyOnWriteArrayList<>();
        final List<byte[]> firstRequests = new CopyOnWriteArrayList<>();
        final List<String> firstVersions = new CopyOnWriteArrayList<>();   // version_info of the first request of each stream
        final List<List<String>> requestedNames = new CopyOnWriteArrayList<>();

        final class Conn {
            final StreamObserver<byte[]> out;
            final Map<String, Set<String>> subscribed = new ConcurrentHashMap<>();
            final Map<String, String> lastNonce = new ConcurrentHashMap<>();
            final AtomicInteger nonce = new AtomicInteger();

            Conn(StreamObserver<byte[]> out) { this.out = out; }

            synchronized void send(String type) {
                Set<String> names = subscribed.getOrDefault(type, Set.of());
                if (names.isEmpty()) return;
                ProtoWriter w = new ProtoWriter().string(1, String.valueOf(version.get()));
                Map<String, byte[]> all = resources.getOrDefault(type, Map.of());
                for (String n : names) {
                    byte[] r = all.get(n);
                    if (r != null) w.message(2, new ProtoWriter().string(1, type).bytes(2, r).toByteArray());
                }
                String nn = String.valueOf(nonce.incrementAndGet());
                lastNonce.put(type, nn);
                w.string(4, type).string(5, nn);
                out.onNext(w.toByteArray());
            }
        }

        void install(GrpcServer.Builder b) {
            MethodDescriptor<byte[], byte[]> ads = MethodDescriptor.of("envoy.service.discovery.v3.AggregatedDiscoveryService", "StreamAggregatedResources",
                    MethodDescriptor.MethodType.BIDI_STREAMING, RAW, RAW);
            b.bidiStreaming(ads, (ctx, out) -> {
                Conn c = new Conn(out);
                conns.add(c);
                return new StreamObserver<byte[]>() {
                    boolean first = true;

                    public void onNext(byte[] q) {
                        String type = "", nonce = "";
                        Set<String> names = new TreeSet<>();
                        String error = null;
                        byte[] node = null;
                        String versionInfo = "";
                        ProtoReader r = new ProtoReader(q);
                        while (r.next()) {
                            switch (r.field()) {
                                case 1 -> versionInfo = r.string();
                                case 2 -> node = r.bytes();
                                case 3 -> names.add(r.string());
                                case 4 -> type = r.string();
                                case 5 -> nonce = r.string();
                                case 6 -> {
                                    ProtoReader st = r.message();
                                    while (st.next()) { if (st.field() == 2) error = st.string(); else st.skip(); }
                                }
                                default -> r.skip();
                            }
                        }
                        if (first) { firstRequests.add(node == null ? new byte[0] : node); firstVersions.add(versionInfo); first = false; }
                        requestedNames.add(List.of(type.substring(type.lastIndexOf('.') + 1), String.join(",", names)));
                        if (error != null) { nacks.add(type.substring(type.lastIndexOf('.') + 1) + ": " + error); return; }
                        boolean ack = !nonce.isEmpty() && nonce.equals(c.lastNonce.get(type)) && names.equals(c.subscribed.get(type));
                        c.subscribed.put(type, names);
                        if (!ack) c.send(type);
                    }

                    public void onError(Throwable t) { conns.remove(c); }

                    public void onCompleted() { conns.remove(c); out.onCompleted(); }
                };
            });
        }

        void set(String type, String name, byte[] resource) {
            resources.computeIfAbsent(type, k -> new ConcurrentHashMap<>()).put(name, resource);
            version.incrementAndGet();
            for (Conn c : conns) c.send(type);
        }

        void remove(String type, String name) {
            Optional.ofNullable(resources.get(type)).ifPresent(m -> m.remove(name));
            version.incrementAndGet();
            for (Conn c : conns) c.send(type);
        }
    }

    // ---- encoders (envoy.config.*.v3) ----

    static byte[] listener(String name, String rds) {
        byte[] hcm = new ProtoWriter().message(3, new ProtoWriter().string(2, rds).toByteArray()).toByteArray();
        return listenerWith(name, hcm);
    }

    static byte[] inlineListener(String name, byte[] routeConfig) {
        return listenerWith(name, new ProtoWriter().message(4, routeConfig).toByteArray());
    }

    private static byte[] listenerWith(String name, byte[] hcm) {
        byte[] any = new ProtoWriter().string(1, HCM).bytes(2, hcm).toByteArray();
        return new ProtoWriter().string(1, name).message(19, new ProtoWriter().message(1, any).toByteArray()).toByteArray();
    }

    static byte[] routeConfig(String name, byte[]... virtualHosts) {
        ProtoWriter w = new ProtoWriter().string(1, name);
        for (byte[] vh : virtualHosts) w.message(2, vh);
        return w.toByteArray();
    }

    static byte[] vhost(String name, List<String> domains, byte[]... routes) {
        ProtoWriter w = new ProtoWriter().string(1, name);
        for (String d : domains) w.string(2, d);
        for (byte[] r : routes) w.message(3, r);
        return w.toByteArray();
    }

    static byte[] route(byte[] match, byte[] action) {
        return new ProtoWriter().message(1, match).message(2, action).toByteArray();
    }

    static byte[] prefix(String p) { return new ProtoWriter().string(1, p).toByteArray(); }
    static byte[] exactPath(String p) { return new ProtoWriter().string(2, p).toByteArray(); }
    static byte[] regex(String re) { return new ProtoWriter().message(10, new ProtoWriter().string(2, re).toByteArray()).toByteArray(); }
    static byte[] withHeader(byte[] match, byte[] headerMatcher) {
        return concat(match, new ProtoWriter().message(6, headerMatcher).toByteArray());
    }
    static byte[] headerExact(String name, String value) { return new ProtoWriter().string(1, name).string(4, value).toByteArray(); }
    static byte[] headerPresent(String name, boolean invert) { return new ProtoWriter().string(1, name).bool(7, true).bool(8, invert).toByteArray(); }
    static byte[] headerPrefixIgnoreCase(String name, String v) {
        return new ProtoWriter().string(1, name).message(13, new ProtoWriter().string(2, v).bool(6, true).toByteArray()).toByteArray();
    }
    static byte[] toCluster(String c) { return new ProtoWriter().string(1, c).toByteArray(); }
    static byte[] toClusterTimeout(String c, long nanos) {
        return concat(toCluster(c), new ProtoWriter().message(8, new ProtoWriter().varint(2, nanos).toByteArray()).toByteArray());
    }
    static byte[] weighted(Object... nameWeight) {
        ProtoWriter wc = new ProtoWriter();
        for (int i = 0; i < nameWeight.length; i += 2) {
            wc.message(1, new ProtoWriter().string(1, (String) nameWeight[i]).message(2, new ProtoWriter().varint(1, (Integer) nameWeight[i + 1]).toByteArray()).toByteArray());
        }
        return new ProtoWriter().message(3, wc.toByteArray()).toByteArray();
    }

    static byte[] edsCluster(String name) {
        return new ProtoWriter().string(1, name).varint(2, 3).message(3, new ProtoWriter().string(2, name + "-eds").toByteArray()).toByteArray();
    }

    static byte[] endpointAt(int port, int weight, int health) {
        byte[] socket = new ProtoWriter().string(2, "127.0.0.1").varint(3, port).toByteArray();
        byte[] addr = new ProtoWriter().message(1, socket).toByteArray();
        ProtoWriter lb = new ProtoWriter().message(1, new ProtoWriter().message(1, addr).toByteArray());
        if (health != 0) lb.varint(2, health);
        if (weight != 0) lb.message(4, new ProtoWriter().varint(1, weight).toByteArray());
        return lb.toByteArray();
    }

    static byte[] assignment(String cluster, byte[]... endpoints) {
        ProtoWriter loc = new ProtoWriter();
        for (byte[] e : endpoints) loc.message(2, e);
        return new ProtoWriter().string(1, cluster + "-eds").message(2, loc.toByteArray()).toByteArray();
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] out = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    // ---- fixtures ----

    private EventLoopGroup group;
    private ControlPlane cp;
    private Server xdsServer;
    private final AtomicInteger hitsA = new AtomicInteger(), hitsB = new AtomicInteger();
    private Server a, b;
    private int pa, pb;
    private final List<GrpcChannel> open = new ArrayList<>();

    private Server backend(String name, AtomicInteger hits) throws Exception {
        return GrpcServer.builder()
                .unary(UNARY, (req, ctx) -> {
                    hits.incrementAndGet();
                    return req.text().startsWith("SLOW") ? new CompletableFuture<Msg>() : CompletableFuture.completedFuture(new Msg(name));
                })
                .serverStreaming(GrpcEchoExample.SERVER_STREAM, (req, ctx, out) -> {
                    hits.incrementAndGet();
                    out.onNext(new Msg(name));
                    out.onCompleted();
                }).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
    }

    private static int port(Server s) {
        return ((InetSocketAddress) s.localAddress()).getPort();
    }

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        cp = new ControlPlane();
        GrpcServer.Builder b0 = GrpcServer.builder();
        cp.install(b0);
        xdsServer = b0.build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        a = backend("A", hitsA);
        b = backend("B", hitsB);
        pa = port(a);
        pb = port(b);
    }

    @AfterEach
    void tearDown() {
        for (GrpcChannel c : open) c.close();
        group.close();
    }

    private String bootstrap() {
        return "{\"xds_servers\":[{\"server_uri\":\"127.0.0.1:" + port(xdsServer) + "\",\"channel_creds\":[{\"type\":\"insecure\"}]}],"
                + "\"node\":{\"id\":\"test-node\",\"cluster\":\"test-cluster\",\"locality\":{\"region\":\"eu\",\"zone\":\"z1\",\"sub_zone\":\"s1\"}}}";
    }

    private GrpcChannel channel(String service) {
        GrpcChannel ch = GrpcChannel.forTarget("xds:///" + service).xdsBootstrap(bootstrap()).build();
        open.add(ch);
        return ch;
    }

    /** One route to cluster c1 (A), one to c2 (B): the baseline most tests start from. */
    private void baseline() {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"),
                route(exactPath("/hydrangea.test.Echo/Unary"), toCluster("c1")),
                route(prefix(""), toCluster("c2")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        cp.set(EDS, "c2-eds", assignment("c2", endpointAt(pb, 1, 0)));
    }

    private String unary(GrpcChannel ch, Metadata md) throws Exception {
        return ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withMetadata(md)).get(10, TimeUnit.SECONDS).text();
    }

    private String unary(GrpcChannel ch) throws Exception {
        return unary(ch, new Metadata());
    }

    private String stream(GrpcChannel ch) throws Exception {
        CompletableFuture<String> f = new CompletableFuture<>();
        ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("x", 1), CallOptions.DEFAULT, new StreamObserver<>() {
            String got;
            public void onNext(Msg m) { got = m.text(); }
            public void onError(Throwable t) { f.completeExceptionally(t); }
            public void onCompleted() { f.complete(got); }
        });
        return f.get(10, TimeUnit.SECONDS);
    }

    private static void eventually(java.util.function.BooleanSupplier c, String what) throws Exception {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        boolean ok;
        while (!(ok = c.getAsBoolean()) && System.nanoTime() < end) Thread.sleep(25);
        assertTrue(ok, what);
    }

    /** True once {@code n} calls in a row answered {@code expected}: a random or rotating choice cannot pass this by luck. */
    private boolean steadily(java.util.concurrent.Callable<String> call, String expected, int n) {
        try {
            for (int i = 0; i < n; i++) if (!expected.equals(call.call())) return false;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ---- bootstrap ----

    @Test
    void bootstrapParsingAndItsErrors() {
        XdsBootstrap b = XdsBootstrap.parse(bootstrap());
        assertEquals("test-node", b.nodeId());
        assertEquals("eu", b.region());
        assertEquals("s1", b.subZone());
        for (String bad : new String[]{"[]", "{}", "{\"xds_servers\": []}", "{\"xds_servers\": [{\"server_uri\": \"x:1\"}]}",
                "{\"xds_servers\": [{\"server_uri\": \"x:1\", \"channel_creds\": [{\"type\": \"google_default\"}]}]}",
                "{\"xds_servers\": [{\"channel_creds\": [{\"type\": \"insecure\"}]}]}"}) {
            assertThrows(IllegalArgumentException.class, () -> XdsBootstrap.parse(bad), bad);
        }
    }

    @Test
    void targetsMustBeWellFormed() {
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("xds:///").xdsBootstrap(bootstrap()).build());
        assertThrows(IllegalArgumentException.class, () -> GrpcChannel.forTarget("xds://authority/name").xdsBootstrap(bootstrap()).build());
    }

    // ---- routing ----

    @Test
    void routesByPathAndPrefix() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        assertEquals("B", stream(ch));
    }

    @Test
    void theFirstRequestIdentifiesTheClientAndAsksForTheTargetListener() throws Exception {
        baseline();
        unary(channel("svc"));
        ProtoReader r = new ProtoReader(cp.firstRequests.get(0));
        String id = "", cluster = "", region = "", agent = "";
        while (r.next()) {
            switch (r.field()) {
                case 1 -> id = r.string();
                case 2 -> cluster = r.string();
                case 4 -> { ProtoReader l = r.message(); while (l.next()) { if (l.field() == 1) region = l.string(); else l.skip(); } }
                case 6 -> agent = r.string();
                default -> r.skip();
            }
        }
        assertEquals("test-node", id);
        assertEquals("test-cluster", cluster);
        assertEquals("eu", region);
        assertEquals("grpc-hydrangea", agent);
        assertEquals(List.of("Listener", "svc"), cp.requestedNames.get(0));
        // and nothing else is asked for until the listener has said what it needs
        assertTrue(cp.requestedNames.stream().anyMatch(n -> n.equals(List.of("RouteConfiguration", "rc"))));
        assertTrue(cp.requestedNames.stream().anyMatch(n -> n.get(0).equals("Cluster") && n.get(1).equals("c1,c2")));
        assertTrue(cp.requestedNames.stream().anyMatch(n -> n.get(0).equals("ClusterLoadAssignment") && n.get(1).equals("c1-eds,c2-eds")));
    }

    @Test
    void headersAndRegexAndCaseMatching() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"),
                route(withHeader(prefix(""), headerExact("x-canary", "yes")), toCluster("c2")),
                route(withHeader(prefix(""), headerPrefixIgnoreCase("x-region", "EU-")), toCluster("c2")),
                route(withHeader(prefix(""), headerPresent("x-skip", true)), toCluster("c2")),   // matches calls WITHOUT x-skip
                route(regex("/hydrangea\\.test\\.Echo/(Unary|Other)"), toCluster("c1")),
                route(prefix(""), toCluster("c1")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        cp.set(EDS, "c2-eds", assignment("c2", endpointAt(pb, 1, 1)));
        GrpcChannel ch = channel("svc");
        assertEquals("B", unary(ch, new Metadata().put("x-canary", "yes")));
        assertEquals("B", unary(ch, new Metadata().put("x-region", "eu-west")), "case-insensitive prefix from string_match");
        assertEquals("B", unary(ch), "invert_match + present: the call has no x-skip");
        assertEquals("A", unary(ch, new Metadata().put("x-skip", "1")), "x-skip present: the regex route takes it");
    }

    @Test
    void stringMatchCanIgnoreCase() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"),
                route(withHeader(prefix(""), headerPrefixIgnoreCase("x-region", "EU-")), toCluster("c2")),
                route(prefix(""), toCluster("c1")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        cp.set(EDS, "c2-eds", assignment("c2", endpointAt(pb, 1, 1)));
        GrpcChannel ch = channel("svc");
        assertEquals("B", unary(ch, new Metadata().put("x-region", "eu-west")));
        assertEquals("A", unary(ch, new Metadata().put("x-region", "us-east")));
    }

    @Test
    void weightedClustersSplitTraffic() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), weighted("c1", 80, "c2", 20)))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        cp.set(EDS, "c2-eds", assignment("c2", endpointAt(pb, 1, 1)));
        GrpcChannel ch = channel("svc");
        for (int i = 0; i < 500; i++) unary(ch);
        assertTrue(hitsA.get() > 340 && hitsA.get() < 460, "about 80% of 500: " + hitsA.get());
        assertEquals(500, hitsA.get() + hitsB.get());
    }

    @Test
    void theBestMatchingVirtualHostIsChosen() throws Exception {
        cp.set(LDS, "svc.example.com", listener("svc.example.com", "rc"));
        cp.set(RDS, "rc", routeConfig("rc",
                vhost("any", List.of("*"), route(prefix(""), toCluster("c2"))),
                vhost("suffix", List.of("*.example.com"), route(prefix(""), toCluster("c2"))),
                vhost("longer-suffix", List.of("*.svc.example.com", "*e.com"), route(prefix(""), toCluster("c2"))),
                vhost("exact", List.of("SVC.example.com"), route(prefix(""), toCluster("c1")))));   // domains compare without case
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(CDS, "c2", edsCluster("c2"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        cp.set(EDS, "c2-eds", assignment("c2", endpointAt(pb, 1, 1)));
        assertEquals("A", unary(channel("svc.example.com")), "exact beats wildcards");
    }

    @Test
    void anInlineRouteConfigNeedsNoRdsRound() throws Exception {
        cp.set(LDS, "svc", inlineListener("svc", routeConfig("inline", vhost("vh", List.of("*"), route(prefix(""), toCluster("c1"))))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        assertEquals("A", unary(channel("svc")));
        assertTrue(cp.requestedNames.stream().noneMatch(n -> n.get(0).equals("RouteConfiguration")));
    }

    @Test
    void aStaticClusterCarriesItsEndpointsInline() throws Exception {
        cp.set(LDS, "svc", inlineListener("svc", routeConfig("inline", vhost("vh", List.of("*"), route(prefix(""), toCluster("s"))))));
        byte[] staticCluster = new ProtoWriter().string(1, "s").varint(2, 0).message(33, assignment("s", endpointAt(pb, 1, 1))).toByteArray();
        cp.set(CDS, "s", staticCluster);
        assertEquals("B", unary(channel("svc")));
        assertTrue(cp.requestedNames.stream().noneMatch(n -> n.get(0).equals("ClusterLoadAssignment")));
    }

    @Test
    void noMatchingRouteIsUnavailable() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(exactPath("/other.Service/Method"), toCluster("c1")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        ExecutionException e = assertThrows(ExecutionException.class, () -> unary(channel("svc")));
        assertEquals(Status.Code.UNAVAILABLE, ((StatusException) e.getCause()).status().code());
        assertTrue(((StatusException) e.getCause()).status().description().contains("no route"));
    }

    @Test
    void theRouteTimeoutIsTheDefaultDeadline() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), toClusterTimeout("c1", 250_000_000)))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        GrpcChannel ch = channel("svc");
        long t0 = System.nanoTime();
        ExecutionException e = assertThrows(ExecutionException.class, () -> ch.unary(UNARY, new Msg("SLOW"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusException) e.getCause()).status().code());
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 2000);
    }

    // ---- endpoints ----

    @Test
    void endpointWeightsAreHonouredExactlyOverACycle() throws Exception {
        baseline();
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1), endpointAt(pb, 3, 1)));
        GrpcChannel ch = channel("svc");
        eventually(() -> { try { Set<String> s = new HashSet<>(); for (int i = 0; i < 4; i++) s.add(unary(ch)); return s.size() == 2; } catch (Exception e) { return false; } }, "both endpoints in use");
        hitsA.set(0);
        hitsB.set(0);
        for (int i = 0; i < 40; i++) unary(ch);
        assertEquals(10, hitsA.get());
        assertEquals(30, hitsB.get());
    }

    @Test
    void unhealthyAndDrainingEndpointsAreSkipped() throws Exception {
        baseline();
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 2 /* UNHEALTHY */), endpointAt(pb, 1, 1)));
        GrpcChannel ch = channel("svc");
        eventually(() -> steadily(() -> unary(ch), "B", 8), "traffic leaves the unhealthy endpoint");
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 3 /* DRAINING */), endpointAt(pb, 1, 5 /* DEGRADED: still used */)));
        eventually(() -> steadily(() -> unary(ch), "B", 8), "a draining endpoint gets nothing, a degraded one still does");
    }

    @Test
    void onlyTheBestPriorityIsUsedAndAZeroWeightLocalityGetsNothing() throws Exception {
        baseline();
        byte[] backup = new ProtoWriter().message(2, endpointAt(pb, 1, 1)).varint(5, 1).toByteArray();
        byte[] primary = new ProtoWriter().message(2, endpointAt(pa, 1, 1)).varint(5, 0).toByteArray();
        byte[] silent = new ProtoWriter().message(2, endpointAt(pb, 1, 1)).message(3, new ProtoWriter().varint(1, 0).toByteArray()).varint(5, 0).toByteArray();
        cp.set(EDS, "c1-eds", new ProtoWriter().string(1, "c1-eds").message(2, backup).message(2, primary).message(2, silent).toByteArray());
        GrpcChannel ch = channel("svc");
        eventually(() -> steadily(() -> unary(ch), "A", 8), "priority 0 only; weight-0 locality excluded");
    }

    @Test
    void aDeadEndpointIsAvoidedAfterItFailsOnce() throws Exception {
        baseline();
        int dead;
        try (java.net.ServerSocket tmp = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) { dead = tmp.getLocalPort(); }
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(dead, 1, 1), endpointAt(pa, 1, 1)));
        GrpcChannel ch = channel("svc");
        int failures = 0;
        for (int i = 0; i < 20; i++) {
            try { unary(ch); } catch (ExecutionException e) { failures++; }
        }
        assertEquals(1, failures, "one call found the dead endpoint; later ones avoid it");
    }

    // ---- updates, errors, recovery ----

    @Test
    void newConfigurationIsPickedUpWithoutReconnecting() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), toCluster("c2")))));
        eventually(() -> steadily(() -> unary(ch), "B", 6), "route change applies");
        assertEquals(1, cp.firstRequests.size(), "one ADS stream throughout");
    }

    @Test
    void anInvalidResourceIsNackedAndTheLastGoodConfigurationKeepsServing() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("B", stream(ch));
        cp.set(CDS, "c2", concat(edsCluster("c2"), new ProtoWriter().varint(6, 2 /* RING_HASH */).toByteArray()));
        eventually(() -> cp.nacks.stream().anyMatch(n -> n.startsWith("Cluster") && n.contains("lb_policy")), "NACK with the reason");
        assertEquals("B", stream(ch), "still routes with the old cluster");
        cp.set(CDS, "c2", concat(edsCluster("c2"), new ProtoWriter().varint(6, 0).toByteArray()));
    }

    @Test
    void thingsTheClientCannotHonourAreRejectedWithAReason() throws Exception {
        cp.set(LDS, "svc", listener("svc", "rc"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), toCluster("c1")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        cp.set(EDS, "c1-eds", assignment("c1", endpointAt(pa, 1, 1)));
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        cp.nacks.clear();
        byte[] redirect = new ProtoWriter().message(1, prefix("")).message(3, new ProtoWriter().string(1, "elsewhere").toByteArray()).toByteArray();
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), redirect)));
        eventually(() -> cp.nacks.stream().anyMatch(n -> n.startsWith("RouteConfiguration") && n.contains("not a plain route")), "redirect routes are refused");
        byte[] queryMatch = route(concat(prefix(""), new ProtoWriter().message(7, new ProtoWriter().string(1, "q").toByteArray()).toByteArray()), toCluster("c1"));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), queryMatch)));
        eventually(() -> cp.nacks.stream().anyMatch(n -> n.contains("query parameters")), "query parameter matching is refused");
        cp.set(CDS, "c1", new ProtoWriter().string(1, "c1").message(38, new byte[0]).toByteArray());
        eventually(() -> cp.nacks.stream().anyMatch(n -> n.contains("aggregate")), "aggregate clusters are refused");
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), toCluster("c1")))));
        cp.set(CDS, "c1", edsCluster("c1"));
        eventually(() -> steadily(() -> unary(ch), "A", 5), "valid configuration is accepted again");
    }

    @Test
    void aMalformedRegexIsNacked() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(regex("(unclosed"), toCluster("c1")))));
        eventually(() -> cp.nacks.stream().anyMatch(n -> n.contains("bad regex")), "bad regex refused");
        assertEquals("A", unary(ch));
    }

    @Test
    void withoutAListenerCallsFailUnavailableAndRecoverWhenItReturns() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        cp.remove(LDS, "svc");
        eventually(() -> { try { unary(ch); return false; } catch (ExecutionException e) { return ((StatusException) e.getCause()).status().code() == Status.Code.UNAVAILABLE; } catch (Exception e) { return false; } }, "UNAVAILABLE");
        cp.set(LDS, "svc", listener("svc", "rc"));
        eventually(() -> steadily(() -> unary(ch), "A", 3), "back to normal");
    }

    @Test
    void callsWaitForTheFirstConfigurationAndFailWithAReasonIfItNeverComes() throws Exception {
        GrpcChannel ch = channel("svc");     // nothing configured yet
        CompletableFuture<Msg> pending = ch.unary(UNARY, new Msg("x"), CallOptions.DEFAULT);
        Thread.sleep(300);
        assertFalse(pending.isDone(), "the call waits for configuration instead of failing at once");
        baseline();
        assertEquals("A", pending.get(10, TimeUnit.SECONDS).text(), "and then goes through");
        GrpcChannel quick = channel("unknown-service");
        ExecutionException e = assertThrows(ExecutionException.class, () -> quick.unary(UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(400))).get(10, TimeUnit.SECONDS));
        assertEquals(Status.Code.DEADLINE_EXCEEDED, ((StatusException) e.getCause()).status().code(), "the call's own deadline ends the wait");
    }

    @Test
    void routingSurvivesTheManagementServerGoingAwayAndTheStreamIsReopened() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        assertEquals("A", unary(ch));
        for (ControlPlane.Conn c : cp.conns) c.out.onCompleted();    // the server ends the stream
        for (int i = 0; i < 5; i++) assertEquals("A", unary(ch), "the last snapshot keeps serving");
        eventually(() -> cp.firstRequests.size() >= 2, "a new stream is opened");
        assertEquals("", cp.firstVersions.get(1), "a new stream starts from no version, as the protocol says");
        assertTrue(cp.firstRequests.get(1).length > 0, "and says who the client is again");
        cp.set(RDS, "rc", routeConfig("rc", vhost("vh", List.of("svc"), route(prefix(""), toCluster("c2")))));
        eventually(() -> steadily(() -> unary(ch), "B", 5), "updates flow again over the new stream");
    }

    @Test
    void channelzShowsTheXdsServersAsSubchannels() throws Exception {
        baseline();
        GrpcChannel ch = channel("svc");
        unary(ch);
        stream(ch);
        ChannelzService cz = new ChannelzService();
        assertNotNull(cz);
        Server czServer = GrpcServer.builder().addService(cz).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        try (GrpcChannel reader = GrpcChannel.forTarget("http://127.0.0.1:" + port(czServer)).build()) {
            MethodDescriptor<byte[], byte[]> top = MethodDescriptor.of("grpc.channelz.v1.Channelz", "GetTopChannels", MethodDescriptor.MethodType.UNARY, RAW, RAW);
            byte[] resp = reader.unary(top, new byte[0], CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            ProtoReader r = new ProtoReader(resp);
            int subchannels = 0;
            boolean found = false;
            while (r.next()) {
                if (r.field() == 1) {
                    ProtoReader c = r.message();
                    String target = "";
                    int subs = 0;
                    while (c.next()) {
                        if (c.field() == 1) { ProtoReader ref = c.message(); while (ref.next()) { if (ref.field() == 2) target = ref.string(); else ref.skip(); } }
                        else if (c.field() == 4) { subs++; c.skip(); }
                        else c.skip();
                    }
                    if (target.equals("xds:///svc")) { found = true; subchannels = subs; }
                } else r.skip();
            }
            assertTrue(found);
            assertEquals(2, subchannels, "A and B");
        } finally {
            czServer.close();
        }
    }
}
