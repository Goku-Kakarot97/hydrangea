package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Reads the xDS resources the client uses from their protobuf form (envoy.config.{listener,route,cluster,endpoint}.v3). Only what routing
 * needs is read; fields that would change the meaning of a resource in a way this client cannot honour make it invalid (so the management
 * server gets a NACK and keeps its idea of the last good version), instead of being ignored in silence.
 */
final class XdsDecoder {
    private XdsDecoder() {
    }

    static final String LDS = "type.googleapis.com/envoy.config.listener.v3.Listener";
    static final String RDS = "type.googleapis.com/envoy.config.route.v3.RouteConfiguration";
    static final String CDS = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
    static final String EDS = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
    static final String HCM = "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";

    static final class Invalid extends Exception {
        Invalid(String message) {
            super(message);
        }
    }

    // ---- results ----

    /** What a listener says about routing: an inline route table, or the name of one to fetch. */
    record Listener(String name, String routeConfigName, RouteConfig inline) {
    }

    record RouteConfig(String name, List<VirtualHost> virtualHosts) {
    }

    record VirtualHost(String name, List<String> domains, List<Route> routes) {
    }

    record Route(PathMatch path, List<HeaderMatch> headers, List<WeightedCluster> clusters, int totalWeight, Duration timeout) {
    }

    /** Exactly one of prefix, path, regex is set. */
    record PathMatch(String prefix, String path, Pattern regex, boolean caseSensitive) {
        boolean matches(String p) {
            if (prefix != null) {
                return caseSensitive ? p.startsWith(prefix) : p.regionMatches(true, 0, prefix, 0, prefix.length());
            }
            if (path != null) {
                return caseSensitive ? p.equals(path) : p.equalsIgnoreCase(path);
            }
            return regex.matcher(p).matches();
        }
    }

    enum HeaderKind { EXACT, PREFIX, SUFFIX, CONTAINS, REGEX, PRESENT, RANGE }

    record HeaderMatch(String name, HeaderKind kind, String value, Pattern regex, long rangeStart, long rangeEnd, boolean present, boolean invert, boolean ignoreCase) {
        boolean matches(Metadata md) {
            String v = md.get(name);
            String want = value;
            if (ignoreCase && v != null && want != null) {
                v = v.toLowerCase(Locale.ROOT);
                want = want.toLowerCase(Locale.ROOT);
            }
            boolean m;
            switch (kind) {
                case PRESENT -> m = (v != null) == present;
                case EXACT -> m = v != null && v.equals(want);
                case PREFIX -> m = v != null && v.startsWith(want);
                case SUFFIX -> m = v != null && v.endsWith(want);
                case CONTAINS -> m = v != null && v.contains(want);
                case REGEX -> m = v != null && regex.matcher(v).matches();
                default -> {
                    boolean in = false;
                    if (v != null) {
                        try {
                            long n = Long.parseLong(v);
                            in = n >= rangeStart && n < rangeEnd;
                        } catch (NumberFormatException ignored) {
                            // not a number: no match
                        }
                    }
                    m = in;
                }
            }
            return m != invert;
        }
    }

    record WeightedCluster(String name, int weight) {
    }

    record Cluster(String name, String edsServiceName, ClusterLoadAssignment staticEndpoints) {
        boolean usesEds() {
            return staticEndpoints == null;
        }
    }

    record Endpoint(InetSocketAddress address, int weight) {
    }

    record ClusterLoadAssignment(String clusterName, List<Endpoint> endpoints) {
    }

    // ---- listener ----

    static Listener listener(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "";
        byte[] apiListener = null;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 19 -> apiListener = r.bytes();
                default -> r.skip();
            }
        }
        if (apiListener == null) {
            throw new Invalid("listener " + name + " has no api_listener (a gRPC client needs one)");
        }
        byte[] any = null;
        ProtoReader a = new ProtoReader(apiListener);
        while (a.next()) {
            if (a.field() == 1) any = a.bytes(); else a.skip();
        }
        if (any == null) {
            throw new Invalid("api_listener of " + name + " is empty");
        }
        String typeUrl = "";
        byte[] value = new byte[0];
        ProtoReader any0 = new ProtoReader(any);
        while (any0.next()) {
            switch (any0.field()) {
                case 1 -> typeUrl = any0.string();
                case 2 -> value = any0.bytes();
                default -> any0.skip();
            }
        }
        if (!typeUrl.equals(HCM)) {
            throw new Invalid("api_listener of " + name + " is a " + typeUrl + ", not an HttpConnectionManager");
        }
        String rdsName = null;
        RouteConfig inline = null;
        ProtoReader h = new ProtoReader(value);
        while (h.next()) {
            switch (h.field()) {
                case 3 -> {
                    ProtoReader rds = h.message();
                    while (rds.next()) {
                        if (rds.field() == 2) rdsName = rds.string(); else rds.skip();
                    }
                }
                case 4 -> inline = routeConfig(h.bytes());
                default -> h.skip();
            }
        }
        if (rdsName == null && inline == null) {
            throw new Invalid("the HttpConnectionManager of " + name + " names neither rds nor route_config");
        }
        return new Listener(name, rdsName, inline);
    }

    // ---- routes ----

    static RouteConfig routeConfig(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "";
        List<VirtualHost> hosts = new ArrayList<>();
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> hosts.add(virtualHost(r.bytes()));
                default -> r.skip();
            }
        }
        return new RouteConfig(name, List.copyOf(hosts));
    }

    private static VirtualHost virtualHost(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "";
        List<String> domains = new ArrayList<>();
        List<Route> routes = new ArrayList<>();
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> domains.add(r.string());
                case 3 -> routes.add(route(r.bytes(), name));
                default -> r.skip();
            }
        }
        if (domains.isEmpty()) {
            throw new Invalid("virtual host " + name + " has no domains");
        }
        return new VirtualHost(name, List.copyOf(domains), List.copyOf(routes));
    }

    private static Route route(byte[] bytes, String vhost) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        byte[] match = null, action = null;
        boolean otherAction = false;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> match = r.bytes();
                case 2 -> action = r.bytes();
                case 3, 7, 17, 18 -> { otherAction = true; r.skip(); }   // redirect, direct_response, filter_action, non_forwarding_action
                default -> r.skip();
            }
        }
        if (match == null) {
            throw new Invalid("a route in " + vhost + " has no match");
        }
        if (action == null || otherAction) {
            throw new Invalid("a route in " + vhost + " is not a plain route to a cluster (redirects and direct responses are not supported)");
        }
        PathMatch path = null;
        boolean caseSensitive = true;
        List<HeaderMatch> headers = new ArrayList<>();
        ProtoReader m = new ProtoReader(match);
        while (m.next()) {
            switch (m.field()) {
                case 1, 2, 10 -> path = pathMatch(m.field(), m, path);
                case 4 -> caseSensitive = boolWrapper(m.bytes());
                case 6 -> headers.add(headerMatch(m.bytes()));
                case 7, 9, 12, 14 -> throw new Invalid("route matches on query parameters, runtime fractions, tokens or path_separated_prefix are not supported");
                default -> m.skip();
            }
        }
        if (path == null) {
            throw new Invalid("a route match in " + vhost + " has no prefix, path or safe_regex");
        }
        if (!caseSensitive) {
            path = new PathMatch(path.prefix, path.path, path.regex, false);
        }
        String cluster = null;
        List<WeightedCluster> weighted = new ArrayList<>();
        int total = 0;
        Duration timeout = null;
        ProtoReader a = new ProtoReader(action);
        while (a.next()) {
            switch (a.field()) {
                case 1 -> cluster = a.string();
                case 2 -> throw new Invalid("cluster_header routes are not supported");
                case 3 -> {
                    ProtoReader wc = a.message();
                    while (wc.next()) {
                        if (wc.field() == 1) {
                            ProtoReader c = wc.message();
                            String n = "";
                            int w = 0;
                            while (c.next()) {
                                if (c.field() == 1) n = c.string(); else if (c.field() == 2) w = (int) uint32Wrapper(c.bytes()); else c.skip();
                            }
                            if (n.isEmpty()) {
                                throw new Invalid("a weighted cluster has no name");
                            }
                            weighted.add(new WeightedCluster(n, w));
                            total += w;
                        } else {
                            wc.skip();
                        }
                    }
                }
                case 8 -> timeout = duration(a.bytes());
                default -> a.skip();
            }
        }
        if (cluster != null) {
            weighted = List.of(new WeightedCluster(cluster, 1));
            total = 1;
        } else if (weighted.isEmpty()) {
            throw new Invalid("a route in " + vhost + " names no cluster");
        } else if (total <= 0) {
            throw new Invalid("the weighted clusters of a route in " + vhost + " have no weight");
        }
        return new Route(path, List.copyOf(headers), List.copyOf(weighted), total, timeout);
    }

    private static PathMatch pathMatch(int field, ProtoReader m, PathMatch previous) throws Invalid {
        if (previous != null) {
            throw new Invalid("a route match has more than one of prefix, path and safe_regex");
        }
        if (field == 1) {
            return new PathMatch(m.string(), null, null, true);
        }
        if (field == 2) {
            return new PathMatch(null, m.string(), null, true);
        }
        ProtoReader re = m.message();
        String regex = null;
        while (re.next()) {
            if (re.field() == 2) regex = re.string(); else re.skip();
        }
        return new PathMatch(null, null, compile(regex), true);
    }

    private static Pattern compile(String regex) throws Invalid {
        if (regex == null) {
            throw new Invalid("a safe_regex has no regex");
        }
        try {
            return Pattern.compile(regex);
        } catch (PatternSyntaxException e) {
            throw new Invalid("bad regex " + regex + ": " + e.getDescription());
        }
    }

    private static HeaderMatch headerMatch(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "", value = null;
        HeaderKind kind = null;
        Pattern regex = null;
        long start = 0, end = 0;
        boolean present = true, invert = false, ignoreCase = false;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string().toLowerCase(Locale.ROOT);
                case 4 -> { kind = HeaderKind.EXACT; value = r.string(); }
                case 6 -> {
                    kind = HeaderKind.RANGE;
                    ProtoReader range = r.message();
                    while (range.next()) {
                        if (range.field() == 1) start = range.varint(); else if (range.field() == 2) end = range.varint(); else range.skip();
                    }
                }
                case 7 -> { kind = HeaderKind.PRESENT; present = r.bool(); }
                case 8 -> invert = r.bool();
                case 9 -> { kind = HeaderKind.PREFIX; value = r.string(); }
                case 10 -> { kind = HeaderKind.SUFFIX; value = r.string(); }
                case 11 -> {
                    kind = HeaderKind.REGEX;
                    ProtoReader re = r.message();
                    String s = null;
                    while (re.next()) {
                        if (re.field() == 2) s = re.string(); else re.skip();
                    }
                    regex = compile(s);
                }
                case 12 -> { kind = HeaderKind.CONTAINS; value = r.string(); }
                case 13 -> {   // string_match: a StringMatcher
                    ProtoReader sm = r.message();
                    while (sm.next()) {
                        switch (sm.field()) {
                            case 1 -> { kind = HeaderKind.EXACT; value = sm.string(); }
                            case 2 -> { kind = HeaderKind.PREFIX; value = sm.string(); }
                            case 3 -> { kind = HeaderKind.SUFFIX; value = sm.string(); }
                            case 7 -> { kind = HeaderKind.CONTAINS; value = sm.string(); }
                            case 6 -> ignoreCase = sm.bool();
                            case 5 -> {
                                kind = HeaderKind.REGEX;
                                ProtoReader re = sm.message();
                                String s = null;
                                while (re.next()) {
                                    if (re.field() == 2) s = re.string(); else re.skip();
                                }
                                regex = compile(s);
                            }
                            default -> sm.skip();
                        }
                    }
                }
                default -> r.skip();
            }
        }
        if (name.isEmpty() || kind == null) {
            throw new Invalid("a header matcher needs a name and a match kind");
        }
        return new HeaderMatch(name, kind, value, regex, start, end, present, invert, ignoreCase);
    }

    // ---- clusters ----

    static Cluster cluster(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "", edsName = null;
        int type = 0, lbPolicy = 0;
        byte[] loadAssignment = null;
        boolean aggregate = false;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> type = (int) r.varint();
                case 3 -> {
                    ProtoReader eds = r.message();
                    while (eds.next()) {
                        if (eds.field() == 2) edsName = eds.string(); else eds.skip();
                    }
                }
                case 6 -> lbPolicy = (int) r.varint();
                case 33 -> loadAssignment = r.bytes();
                case 38 -> { aggregate = true; r.skip(); }
                default -> r.skip();
            }
        }
        if (aggregate) {
            throw new Invalid("cluster " + name + " is an aggregate cluster, which is not supported");
        }
        if (lbPolicy != 0) {
            throw new Invalid("cluster " + name + " uses lb_policy " + lbPolicy + "; only ROUND_ROBIN is supported");
        }
        switch (type) {
            case 3 -> {   // EDS
                return new Cluster(name, edsName == null || edsName.isEmpty() ? name : edsName, null);
            }
            case 0 -> {   // STATIC
                if (loadAssignment == null) {
                    throw new Invalid("static cluster " + name + " has no load_assignment");
                }
                return new Cluster(name, null, clusterLoadAssignment(loadAssignment));
            }
            default -> throw new Invalid("cluster " + name + " has discovery type " + type + "; only EDS and STATIC are supported");
        }
    }

    // ---- endpoints ----

    static ClusterLoadAssignment clusterLoadAssignment(byte[] bytes) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        String name = "";
        List<byte[]> localities = new ArrayList<>();
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> localities.add(r.bytes());
                default -> r.skip();
            }
        }
        // only the highest-priority (lowest number) group is used; the others are what a failover would use
        int bestPriority = Integer.MAX_VALUE;
        List<Endpoint> best = new ArrayList<>();
        for (byte[] loc : localities) {
            ProtoReader l = new ProtoReader(loc);
            int priority = 0;
            long localityWeight = 1;
            List<byte[]> lb = new ArrayList<>();
            while (l.next()) {
                switch (l.field()) {
                    case 2 -> lb.add(l.bytes());
                    case 3 -> localityWeight = uint32Wrapper(l.bytes());
                    case 5 -> priority = (int) l.varint();
                    default -> l.skip();
                }
            }
            if (localityWeight == 0) {
                continue;   // a locality with weight 0 gets no traffic
            }
            if (priority > bestPriority) {
                continue;
            }
            if (priority < bestPriority) {
                bestPriority = priority;
                best.clear();
            }
            for (byte[] e : lb) {
                Endpoint ep = lbEndpoint(e, localityWeight);
                if (ep != null) {
                    best.add(ep);
                }
            }
        }
        return new ClusterLoadAssignment(name, List.copyOf(best));
    }

    /** Null for an endpoint that must not receive traffic (unhealthy, draining, timed out). */
    private static Endpoint lbEndpoint(byte[] bytes, long localityWeight) throws Invalid {
        ProtoReader r = new ProtoReader(bytes);
        byte[] endpoint = null;
        int health = 0;
        long weight = 1;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> endpoint = r.bytes();
                case 2 -> health = (int) r.varint();
                case 4 -> weight = uint32Wrapper(r.bytes());
                default -> r.skip();
            }
        }
        if (endpoint == null) {
            throw new Invalid("an lb_endpoint has no endpoint");
        }
        if (health == 2 || health == 3 || health == 4) {   // UNHEALTHY, DRAINING, TIMEOUT
            return null;
        }
        byte[] address = null;
        ProtoReader e = new ProtoReader(endpoint);
        while (e.next()) {
            if (e.field() == 1) address = e.bytes(); else e.skip();
        }
        if (address == null) {
            throw new Invalid("an endpoint has no address");
        }
        byte[] socket = null;
        ProtoReader a = new ProtoReader(address);
        while (a.next()) {
            if (a.field() == 1) socket = a.bytes(); else a.skip();
        }
        if (socket == null) {
            throw new Invalid("an endpoint address is not a socket_address");
        }
        String host = "";
        int port = -1;
        ProtoReader s = new ProtoReader(socket);
        while (s.next()) {
            switch (s.field()) {
                case 2 -> host = s.string();
                case 3 -> port = (int) s.varint();
                case 4 -> throw new Invalid("named ports are not supported");
                default -> s.skip();
            }
        }
        if (host.isEmpty() || port < 0 || port > 65535) {
            throw new Invalid("an endpoint needs an address and a port, got " + host + ":" + port);
        }
        InetAddress ip;
        try {
            ip = InetAddress.getByName(host);   // endpoints are IP literals; this does not resolve names for them
        } catch (java.net.UnknownHostException ex) {
            throw new Invalid("endpoint address " + host + " is not an IP address");
        }
        long effective = Math.min(Integer.MAX_VALUE, Math.max(0, weight) * Math.max(1, localityWeight));
        return effective == 0 ? null : new Endpoint(new InetSocketAddress(ip, port), (int) effective);
    }

    // ---- protobuf wrappers ----

    private static long uint32Wrapper(byte[] bytes) {
        ProtoReader r = new ProtoReader(bytes);
        while (r.next()) {
            if (r.field() == 1) return r.varint(); else r.skip();
        }
        return 0;
    }

    private static boolean boolWrapper(byte[] bytes) {
        return uint32Wrapper(bytes) != 0;
    }

    private static Duration duration(byte[] bytes) {
        ProtoReader r = new ProtoReader(bytes);
        long seconds = 0, nanos = 0;
        while (r.next()) {
            if (r.field() == 1) seconds = r.varint(); else if (r.field() == 2) nanos = r.varint(); else r.skip();
        }
        return Duration.ofSeconds(seconds, nanos);
    }
}
