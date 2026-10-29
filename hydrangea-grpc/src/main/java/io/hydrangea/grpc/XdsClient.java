package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An xDS client (state-of-the-world, aggregated discovery, v3): it follows one service name through a management server: the listener named
 * like the target, its route table, the clusters the routes use, and their endpoints, and keeps an up-to-date routing {@link Snapshot}.
 * <p>What a response is judged by: if any resource in it is invalid (or one this client cannot honour, see {@link XdsDecoder}) the whole
 * response is NACKed with the reason, and the last good state stays in force. The ADS stream is reopened with backoff when it breaks, and
 * routing keeps working from the last snapshot meanwhile.
 * <p>Not supported: load reporting (LRS), TLS to the management server or between peers (transport sockets are ignored), priorities beyond
 * the best one, locality failover, ring-hash / least-request balancing, circuit breaking, outlier detection, HTTP filters, delta xDS.
 */
final class XdsClient implements AutoCloseable {

    private static final Marshaller<byte[]> RAW = Marshaller.of(b -> b, b -> b);
    private static final MethodDescriptor<byte[], byte[]> ADS = MethodDescriptor.of("envoy.service.discovery.v3.AggregatedDiscoveryService",
            "StreamAggregatedResources", MethodDescriptor.MethodType.BIDI_STREAMING, RAW, RAW);

    /** The endpoints of one cluster, with a counter that makes the weighted round robin deterministic. */
    static final class ClusterEndpoints {
        final String name;
        final List<XdsDecoder.Endpoint> endpoints;
        private final AtomicLong counter = new AtomicLong();

        ClusterEndpoints(String name, List<XdsDecoder.Endpoint> endpoints) {
            this.name = name;
            this.endpoints = endpoints;
        }

        /** The next endpoint in weighted round-robin order among those {@code usable} says yes to; if none is usable, among all of them. */
        XdsDecoder.Endpoint pick(Predicate<InetSocketAddress> usable) {
            if (endpoints.isEmpty()) {
                return null;
            }
            long total = 0;
            for (XdsDecoder.Endpoint e : endpoints) {
                if (usable.test(e.address())) {
                    total += e.weight();
                }
            }
            boolean all = total == 0;
            if (all) {
                for (XdsDecoder.Endpoint e : endpoints) {
                    total += e.weight();
                }
            }
            long at = Math.floorMod(counter.getAndIncrement(), total);
            for (XdsDecoder.Endpoint e : endpoints) {
                if (all || usable.test(e.address())) {
                    if (at < e.weight()) {
                        return e;
                    }
                    at -= e.weight();
                }
            }
            return endpoints.get(0);
        }
    }

    record Snapshot(List<XdsDecoder.Route> routes, Map<String, ClusterEndpoints> clusters) {
    }

    /** The outcome of routing one call. */
    record Selection(ClusterEndpoints cluster, Duration timeout) {
    }

    private final String target;
    private final XdsBootstrap bootstrap;
    private final GrpcChannel management;
    private final ExecutorService thread = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "hydrangea-xds");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "hydrangea-xds-timer");
        t.setDaemon(true);
        return t;
    });

    private volatile Snapshot snapshot;
    private volatile String problem = "no response from the xDS server yet";
    private final CompletableFuture<Void> ready = new CompletableFuture<>();
    private volatile Consumer<Snapshot> onSnapshot = s -> { };
    private volatile boolean closed;

    // thread-confined below
    private static final class Subscription {
        Set<String> names = new TreeSet<>();
        String version = "";
        String nonce = "";
    }

    private final Map<String, Subscription> subs = new HashMap<>();
    private final Map<String, XdsDecoder.Listener> listeners = new HashMap<>();
    private final Map<String, XdsDecoder.RouteConfig> routeConfigs = new HashMap<>();
    private final Map<String, XdsDecoder.Cluster> clusterDefs = new HashMap<>();
    private final Map<String, XdsDecoder.ClusterLoadAssignment> assignments = new HashMap<>();
    private ClientCall<byte[], byte[]> stream;
    private boolean firstOnStream;
    private long backoffMillis = 1000;
    private int streamGeneration;

    XdsClient(XdsBootstrap bootstrap, String target) {
        this.bootstrap = bootstrap;
        this.target = target;
        this.management = GrpcChannel.forTarget("http://" + bootstrap.serverUri()).build();
        for (String t : List.of(XdsDecoder.LDS, XdsDecoder.RDS, XdsDecoder.CDS, XdsDecoder.EDS)) {
            subs.put(t, new Subscription());
        }
        subs.get(XdsDecoder.LDS).names.add(target);
        thread.execute(this::connect);
    }

    void onSnapshot(Consumer<Snapshot> listener) {
        this.onSnapshot = listener;
    }

    boolean isReady() {
        return ready.isDone();
    }

    /** Completes when a first complete routing table exists; fails after {@code timeout} with what is missing. */
    CompletableFuture<Void> whenReady(Duration timeout) {
        if (ready.isDone()) {
            return ready;
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        ready.whenComplete((v, e) -> f.complete(null));
        timers.schedule(() -> f.completeExceptionally(new StatusException(Status.of(Status.Code.UNAVAILABLE)
                .withDescription("no xDS configuration for " + target + " after " + timeout.toSeconds() + " s: " + problem))), timeout.toMillis(), TimeUnit.MILLISECONDS);
        return f;
    }

    Snapshot snapshot() {
        return snapshot;
    }

    /** Routes one call. @throws StatusException UNAVAILABLE when there is no configuration or no route matches (UNAVAILABLE, as gRPC does) */
    Selection select(String methodPath, Metadata md) throws StatusException {
        Snapshot s = snapshot;
        if (s == null) {
            throw new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("no xDS configuration for " + target + ": " + problem));
        }
        for (XdsDecoder.Route r : s.routes()) {
            if (!r.path().matches(methodPath)) {
                continue;
            }
            boolean headersOk = true;
            for (XdsDecoder.HeaderMatch h : r.headers()) {
                if (!h.matches(md)) {
                    headersOk = false;
                    break;
                }
            }
            if (!headersOk) {
                continue;
            }
            String cluster = r.clusters().get(0).name();
            if (r.clusters().size() > 1) {
                long at = ThreadLocalRandom.current().nextLong(r.totalWeight());
                for (XdsDecoder.WeightedCluster c : r.clusters()) {
                    if (at < c.weight()) {
                        cluster = c.name();
                        break;
                    }
                    at -= c.weight();
                }
            }
            ClusterEndpoints ce = s.clusters().get(cluster);
            if (ce == null) {
                throw new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("cluster " + cluster + " is not known"));
            }
            Duration t = r.timeout() == null || r.timeout().isZero() ? null : r.timeout();
            return new Selection(ce, t);
        }
        throw new StatusException(Status.of(Status.Code.UNAVAILABLE).withDescription("no route matches " + methodPath));
    }

    @Override
    public void close() {
        closed = true;
        thread.execute(() -> {
            if (stream != null) {
                stream.cancel("closing");
            }
        });
        thread.shutdown();
        timers.shutdownNow();
        management.close();
    }

    // ---- the ADS stream (xds thread) ----

    private void connect() {
        if (closed) {
            return;
        }
        int generation = ++streamGeneration;
        for (Subscription s : subs.values()) {   // a new stream starts from nothing
            s.version = "";
            s.nonce = "";
        }
        firstOnStream = true;
        stream = management.bidiStreaming(ADS, CallOptions.DEFAULT, new StreamObserver<>() {
            @Override
            public void onNext(byte[] response) {
                thread.execute(() -> {
                    if (generation == streamGeneration) {
                        onResponse(response);
                    }
                });
            }

            @Override
            public void onError(Throwable t) {
                thread.execute(() -> streamEnded(generation, String.valueOf(t.getMessage())));
            }

            @Override
            public void onCompleted() {
                thread.execute(() -> streamEnded(generation, "the xDS server closed the stream"));
            }
        });
        for (String type : List.of(XdsDecoder.LDS, XdsDecoder.RDS, XdsDecoder.CDS, XdsDecoder.EDS)) {
            if (!subs.get(type).names.isEmpty()) {
                request(type, null);
            }
        }
    }

    private void streamEnded(int generation, String why) {
        if (closed || generation != streamGeneration) {
            return;
        }
        if (snapshot == null) {
            problem = "cannot reach the xDS server " + bootstrap.serverUri() + ": " + why;
        }
        long wait = backoffMillis;
        backoffMillis = Math.min(backoffMillis * 2, 30_000);
        try {
            timers.schedule(() -> thread.execute(this::connect), wait + ThreadLocalRandom.current().nextLong(wait / 4 + 1), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException closing) {
            // shutting down
        }
    }

    /** Sends a DiscoveryRequest for {@code type}: a subscription change or an ACK, or a NACK when {@code error} is given. */
    private void request(String type, String error) {
        if (stream == null) {
            return;
        }
        Subscription s = subs.get(type);
        ProtoWriter w = new ProtoWriter().string(1, s.version);
        if (firstOnStream) {
            ProtoWriter node = new ProtoWriter().string(1, bootstrap.nodeId()).string(2, bootstrap.nodeCluster());
            if (!bootstrap.region().isEmpty() || !bootstrap.zone().isEmpty() || !bootstrap.subZone().isEmpty()) {
                node.message(4, new ProtoWriter().string(1, bootstrap.region()).string(2, bootstrap.zone()).string(3, bootstrap.subZone()).toByteArray());
            }
            node.string(6, "grpc-hydrangea").string(7, "0.1").string(10, "envoy.lb.does_not_support_overprovisioning");
            w.message(2, node.toByteArray());
            firstOnStream = false;
        }
        for (String n : s.names) {
            w.string(3, n);
        }
        w.string(4, type).string(5, s.nonce);
        if (error != null) {
            w.message(6, new ProtoWriter().varint(1, 3).string(2, error).toByteArray());   // google.rpc.Status INVALID_ARGUMENT
        }
        stream.send(w.toByteArray());
    }

    private void onResponse(byte[] bytes) {
        String version = "", type = "", nonce = "";
        List<byte[]> anys = new ArrayList<>();
        ProtoReader r = new ProtoReader(bytes);
        while (r.next()) {
            switch (r.field()) {
                case 1 -> version = r.string();
                case 2 -> anys.add(r.bytes());
                case 4 -> type = r.string();
                case 5 -> nonce = r.string();
                default -> r.skip();
            }
        }
        Subscription sub = subs.get(type);
        if (sub == null) {
            return;   // a type we did not ask for: nothing to acknowledge
        }
        sub.nonce = nonce;
        Map<String, Object> decoded = new HashMap<>();
        try {
            for (byte[] any : anys) {
                String typeUrl = "";
                byte[] value = new byte[0];
                ProtoReader a = new ProtoReader(any);
                while (a.next()) {
                    switch (a.field()) {
                        case 1 -> typeUrl = a.string();
                        case 2 -> value = a.bytes();
                        default -> a.skip();
                    }
                }
                if (!typeUrl.equals(type)) {
                    throw new XdsDecoder.Invalid("a resource of type " + typeUrl + " in a response for " + type);
                }
                switch (type) {
                    case XdsDecoder.LDS -> {
                        XdsDecoder.Listener l = target.equals(nameOf(value)) ? XdsDecoder.listener(value) : null;
                        if (l != null) decoded.put(l.name(), l);
                    }
                    case XdsDecoder.RDS -> {
                        XdsDecoder.RouteConfig rc = XdsDecoder.routeConfig(value);
                        decoded.put(rc.name(), rc);
                    }
                    case XdsDecoder.CDS -> {
                        XdsDecoder.Cluster c = XdsDecoder.cluster(value);
                        decoded.put(c.name(), c);
                    }
                    default -> {
                        XdsDecoder.ClusterLoadAssignment cla = XdsDecoder.clusterLoadAssignment(value);
                        decoded.put(cla.clusterName(), cla);
                    }
                }
            }
        } catch (XdsDecoder.Invalid | RuntimeException e) {
            problem = "the xDS server sent an invalid " + type.substring(type.lastIndexOf('.') + 1) + ": " + e.getMessage();
            request(type, e.getMessage());   // NACK: the version stays the last good one
            return;
        }
        backoffMillis = 1000;
        applyResources(type, decoded);
        sub.version = version;
        request(type, null);   // ACK
        recompute();
    }

    private static String nameOf(byte[] resource) {
        ProtoReader r = new ProtoReader(resource);
        while (r.next()) {
            if (r.field() == 1) return r.string(); else r.skip();
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private void applyResources(String type, Map<String, Object> decoded) {
        switch (type) {
            case XdsDecoder.LDS -> {   // the full set of subscribed listeners: absent means gone
                listeners.clear();
                decoded.forEach((k, v) -> listeners.put(k, (XdsDecoder.Listener) v));
            }
            case XdsDecoder.CDS -> {
                clusterDefs.clear();
                decoded.forEach((k, v) -> clusterDefs.put(k, (XdsDecoder.Cluster) v));
            }
            case XdsDecoder.RDS -> decoded.forEach((k, v) -> routeConfigs.put(k, (XdsDecoder.RouteConfig) v));   // may be a subset
            default -> decoded.forEach((k, v) -> assignments.put(k, (XdsDecoder.ClusterLoadAssignment) v));
        }
    }

    // ---- from resources to a routing snapshot ----

    private void recompute() {
        XdsDecoder.Listener l = listeners.get(target);
        if (l == null) {
            publish(null, snapshot == null ? "the xDS server has no listener named " + target : "the listener " + target + " was removed");
            return;
        }
        XdsDecoder.RouteConfig rc = l.inline() != null ? l.inline() : routeConfigs.get(l.routeConfigName());
        Set<String> wantRds = l.inline() != null ? Set.of() : Set.of(l.routeConfigName());
        updateSubscription(XdsDecoder.RDS, wantRds);
        if (rc == null) {
            problem = "waiting for the route configuration " + l.routeConfigName();
            return;
        }
        XdsDecoder.VirtualHost vh = virtualHostFor(rc);
        if (vh == null) {
            publish(null, "route configuration " + rc.name() + " has no virtual host for " + target);
            return;
        }
        Set<String> clusters = new TreeSet<>();
        for (XdsDecoder.Route r : vh.routes()) {
            for (XdsDecoder.WeightedCluster c : r.clusters()) {
                clusters.add(c.name());
            }
        }
        updateSubscription(XdsDecoder.CDS, clusters);
        Set<String> wantEds = new TreeSet<>();
        for (String c : clusters) {
            XdsDecoder.Cluster def = clusterDefs.get(c);
            if (def != null && def.usesEds()) {
                wantEds.add(def.edsServiceName());
            }
        }
        updateSubscription(XdsDecoder.EDS, wantEds);
        Map<String, ClusterEndpoints> built = new HashMap<>();
        for (String c : clusters) {
            XdsDecoder.Cluster def = clusterDefs.get(c);
            if (def == null) {
                problem = "waiting for cluster " + c;
                return;
            }
            XdsDecoder.ClusterLoadAssignment cla = def.usesEds() ? assignments.get(def.edsServiceName()) : def.staticEndpoints();
            if (cla == null) {
                problem = "waiting for the endpoints of cluster " + c;
                return;
            }
            built.put(c, new ClusterEndpoints(c, cla.endpoints()));
        }
        publish(new Snapshot(vh.routes(), Map.copyOf(built)), null);
    }

    private void publish(Snapshot s, String why) {
        problem = why == null ? "" : why;
        Snapshot old = snapshot;
        snapshot = s;
        if (s != null) {
            ready.complete(null);
        }
        if (old != s) {
            try {
                onSnapshot.accept(s);
            } catch (RuntimeException ignored) {
                // a failing listener must not stop discovery
            }
        }
    }

    private void updateSubscription(String type, Set<String> wanted) {
        Subscription s = subs.get(type);
        if (!s.names.equals(wanted)) {
            s.names = new TreeSet<>(wanted);
            if (!wanted.isEmpty()) {
                request(type, null);
            }
        }
    }

    /** The virtual host whose domain matches the target best: exact, then the longest "*.suffix", then "prefix.*", then "*". */
    private XdsDecoder.VirtualHost virtualHostFor(XdsDecoder.RouteConfig rc) {
        String host = target.toLowerCase(java.util.Locale.ROOT);
        XdsDecoder.VirtualHost best = null;
        int bestScore = -1;
        for (XdsDecoder.VirtualHost vh : rc.virtualHosts()) {
            for (String d : vh.domains()) {
                String domain = d.toLowerCase(java.util.Locale.ROOT);
                int score = -1;
                if (domain.equals(host)) {
                    score = 4_000_000;
                } else if (domain.startsWith("*") && domain.length() > 1 && host.endsWith(domain.substring(1))) {
                    score = 3_000_000 + domain.length();
                } else if (domain.endsWith("*") && domain.length() > 1 && host.startsWith(domain.substring(0, domain.length() - 1))) {
                    score = 2_000_000 + domain.length();
                } else if (domain.equals("*")) {
                    score = 1_000_000;
                }
                if (score > bestScore) {
                    bestScore = score;
                    best = vh;
                }
            }
        }
        return best;
    }

    /** All addresses in the current snapshot (for housekeeping by the channel). */
    static Set<InetSocketAddress> addressesIn(Snapshot s) {
        Set<InetSocketAddress> out = new HashSet<>();
        if (s != null) {
            for (ClusterEndpoints c : s.clusters().values()) {
                for (XdsDecoder.Endpoint e : c.endpoints) {
                    out.add(e.address());
                }
            }
        }
        return out;
    }
}
