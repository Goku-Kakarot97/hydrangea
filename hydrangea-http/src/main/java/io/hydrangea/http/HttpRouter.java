package io.hydrangea.http;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * Maps method + path pattern to routes and exposes path variables and request parameters.
 * <pre>{@code
 * var router = new HttpRouter()
 *     .get("/users/{id}", r -> HttpResponse.ok("user " + r.pathInt("id")))
 *     .get("/users/{userId}/posts/{postId}", r -> HttpResponse.ok(r.pathVariable("postId") + " limit=" + r.paramInt("limit", 10)))
 *     .get("/files/{path...}", r -> HttpResponse.ok(r.pathVariable("path")))      // catch-all, must be last
 *     .post("/login", r -> HttpResponse.ok(r.requiredParam("user")))                // query string or form body
 *     .getAsync("/slow", r -> service.fetch().thenApply(HttpResponse::ok));
 * HttpServer.bind(group, address, router);
 * }</pre>
 * <p>
 * Matching rules:
 * <ul>
 *   <li>Patterns start with {@code /}. Segments are literals, {@code {name}} (exactly one non-empty segment) or a
 *       final {@code {name...}} (the rest of the path, possibly with slashes).</li>
 *   <li>Lookup walks a prefix tree, so its cost depends on path depth, not on how many routes are registered.</li>
 *   <li>The most specific pattern wins regardless of registration order: at the first differing segment a literal
 *       beats a variable, which beats a catch-all (so {@code /users/me} wins over {@code /users/{id}}).</li>
 *   <li>One trailing slash is ignored ({@code /users/} = {@code /users}). Segments are percent-decoded after
 *       splitting, so {@code %2F} stays inside its segment.</li>
 *   <li>Path matches but method does not → <b>405</b> with an {@code Allow} header; no path matches → 404
 *       (customise with {@link #notFound}). {@code HEAD} runs the {@code GET} route.</li>
 *   <li>A {@link HttpStatusException} thrown by a route becomes that status; any other exception propagates and the
 *       server answers 500.</li>
 * </ul>
 * Register all routes before binding the server; routes are not meant to change while serving (registration is
 * safe, but a request may see either the old or the new table).
 * Routes run on the event loop: never block in them, use the {@code *Async} variants for slow work.
 */
public final class HttpRouter implements HttpHandler {

    /** A route that answers immediately. */
    @FunctionalInterface
    public interface Route {
        HttpResponse handle(RouteRequest request) throws Exception;
    }

    /** A route that completes later (any thread). */
    @FunctionalInterface
    public interface AsyncRoute {
        CompletionStage<HttpResponse> handle(RouteRequest request) throws Exception;
    }

    private sealed interface Segment permits Literal, Variable, CatchAll {
        int rank();
    }

    private record Literal(String text) implements Segment {
        public int rank() { return 2; }
    }

    private record Variable(String name) implements Segment {
        public int rank() { return 1; }
    }

    private record CatchAll(String name) implements Segment {
        public int rank() { return 0; }
    }

    /** A route that receives the request body as a stream instead of a buffered array. */
    @FunctionalInterface
    public interface StreamRoute {
        CompletionStage<HttpResponse> handle(RouteRequest request, RequestBodyStream body) throws Exception;
    }

    private record Entry(String method, String pattern, List<Segment> segments, AsyncRoute route, StreamRoute stream) {
    }

    /**
     * Prefix tree over path segments. Lookup cost depends on the path depth, not on the number of routes.
     * Immutable once published: every registration builds a fresh tree (startup-time cost only).
     */
    private static final class Node {
        final Map<String, Node> literals = new HashMap<>();
        Node variable;                                          // child for any {var} segment
        final Map<String, Entry> exact = new HashMap<>();       // routes ending here, by method
        final Map<String, Entry> catchAll = new HashMap<>();    // routes with a final {rest...} at this depth, by method
    }

    private volatile Node root = new Node();
    private final List<Entry> registrations = new ArrayList<>();
    private volatile AsyncRoute notFound = r -> CompletableFuture.completedFuture(HttpResponse.notFound());

    // ---- registration ----

    public HttpRouter get(String pattern, Route route) { return route("GET", pattern, route); }
    public HttpRouter post(String pattern, Route route) { return route("POST", pattern, route); }
    public HttpRouter put(String pattern, Route route) { return route("PUT", pattern, route); }
    public HttpRouter patch(String pattern, Route route) { return route("PATCH", pattern, route); }
    public HttpRouter delete(String pattern, Route route) { return route("DELETE", pattern, route); }

    public HttpRouter getAsync(String pattern, AsyncRoute route) { return routeAsync("GET", pattern, route); }
    public HttpRouter postAsync(String pattern, AsyncRoute route) { return routeAsync("POST", pattern, route); }
    public HttpRouter putAsync(String pattern, AsyncRoute route) { return routeAsync("PUT", pattern, route); }
    public HttpRouter patchAsync(String pattern, AsyncRoute route) { return routeAsync("PATCH", pattern, route); }
    public HttpRouter deleteAsync(String pattern, AsyncRoute route) { return routeAsync("DELETE", pattern, route); }

    public HttpRouter route(String method, String pattern, Route route) {
        return routeAsync(method, pattern, r -> CompletableFuture.completedFuture(route.handle(r)));
    }

    /** @throws IllegalArgumentException for a malformed pattern or a duplicate method + pattern */
    public synchronized HttpRouter routeAsync(String method, String pattern, AsyncRoute route) {
        String m = method.toUpperCase(Locale.ROOT);
        Entry entry = new Entry(m, pattern, parsePattern(pattern), route, null);
        Node fresh = new Node();
        for (Entry e : registrations) {
            insert(fresh, e);
        }
        insert(fresh, entry); // throws on a duplicate, before anything is published
        registrations.add(entry);
        root = fresh;
        return this;
    }

    private static void insert(Node root, Entry e) {
        Node n = root;
        Map<String, Entry> target = null;
        for (Segment seg : e.segments) {
            if (seg instanceof Literal l) {
                n = n.literals.computeIfAbsent(l.text, k -> new Node());
            } else if (seg instanceof Variable) {
                if (n.variable == null) {
                    n.variable = new Node();
                }
                n = n.variable;
            } else {
                target = n.catchAll;
            }
        }
        if (target == null) {
            target = n.exact;
        }
        Entry existing = target.putIfAbsent(e.method, e);
        if (existing != null) {
            throw new IllegalArgumentException("duplicate route " + e.method + " " + e.pattern + " (conflicts with " + existing.pattern + ")");
        }
    }

    // ---- streaming request bodies ----

    public HttpRouter postStream(String pattern, StreamRoute route) { return routeStream("POST", pattern, route); }
    public HttpRouter putStream(String pattern, StreamRoute route) { return routeStream("PUT", pattern, route); }
    public HttpRouter patchStream(String pattern, StreamRoute route) { return routeStream("PATCH", pattern, route); }

    /**
     * Registers a route that gets the request body as it arrives (uploads, large imports). Requests without a body
     * reach it too, with an already completed stream. Unlike buffered routes it is not subject to
     * {@link HttpConfig#maxBodyBytes()}: enforce your own limit and answer early (413) when it is exceeded.
     */
    public synchronized HttpRouter routeStream(String method, String pattern, StreamRoute route) {
        String m = method.toUpperCase(Locale.ROOT);
        Entry entry = new Entry(m, pattern, parsePattern(pattern), null, route);
        Node fresh = new Node();
        for (Entry e : registrations) {
            insert(fresh, e);
        }
        insert(fresh, entry);
        registrations.add(entry);
        root = fresh;
        return this;
    }

    @Override
    public boolean wantsStreamingBody(HttpRequest head) {
        try {
            String path = head.path();
            if (!path.startsWith("/")) {
                return false;
            }
            String[] decoded = decodeSegments(path);
            Entry e = search(root, 0, new Lookup(decoded, head.method()));
            return e != null && e.stream != null;
        } catch (HttpStatusException malformed) {
            return false; // the buffered path will answer 400
        }
    }

    @Override
    public CompletionStage<HttpResponse> handleStreaming(HttpRequest head, RequestBodyStream body) throws Exception {
        return dispatch(head, body);
    }

    /** Replaces the default plain-text 404. */
    public HttpRouter notFound(Route route) {
        this.notFound = r -> CompletableFuture.completedFuture(route.handle(r));
        return this;
    }

    // ---- dispatch ----

    @Override
    public CompletionStage<HttpResponse> handle(HttpRequest request) throws Exception {
        return dispatch(request, null);
    }

    private String[] decodeSegments(String path) {
        String[] raw = splitPath(path);
        String[] decoded = new String[raw.length];
        for (int i = 0; i < raw.length; i++) {
            decoded[i] = Url.decode(raw[i], false);
        }
        return decoded;
    }

    /** @param body the live request body for streaming dispatch, or null (then stream routes see an empty, completed body) */
    private CompletionStage<HttpResponse> dispatch(HttpRequest request, RequestBodyStream body) throws Exception {
        String path = request.path();
        if (!path.startsWith("/")) {
            return done(HttpResponse.status(400).text("Bad Request"));
        }
        try {
            String[] decoded = decodeSegments(path);
            Lookup lookup = new Lookup(decoded, request.method());
            Entry e = search(root, 0, lookup);
            if (e != null) {
                RouteRequest rr = new RouteRequest(request, variables(e, decoded));
                if (e.stream != null) {
                    return guard(e.stream.handle(rr, body != null ? body : EmptyBody.INSTANCE));
                }
                return guard(e.route.handle(rr));
            }
            if (!lookup.allowed.isEmpty()) {
                if (lookup.allowed.contains("GET")) {
                    lookup.allowed.add("HEAD");
                }
                return done(HttpResponse.status(405).header("Allow", String.join(", ", lookup.allowed)).text("Method Not Allowed"));
            }
            return guard(notFound.handle(new RouteRequest(request, Map.of())));
        } catch (HttpStatusException e) {
            return done(toResponse(e));
        }
    }

    /** Per-request search state. */
    private static final class Lookup {
        final String[] segments;
        final String method;
        final boolean head;
        final TreeSet<String> allowed = new TreeSet<>(); // methods of routes whose path matched but method did not

        Lookup(String[] segments, String method) {
            this.segments = segments;
            this.method = method;
            this.head = method.equals("HEAD");
        }
    }

    /**
     * Depth-first, trying literal child, then variable child, then catch-all at each level. This yields exactly
     * "the most specific pattern wins", with backtracking when a more specific branch has no route for the method.
     */
    private static Entry search(Node n, int i, Lookup lk) {
        if (i == lk.segments.length) {
            return pick(n.exact, lk);
        }
        Node literal = n.literals.get(lk.segments[i]);
        if (literal != null) {
            Entry e = search(literal, i + 1, lk);
            if (e != null) {
                return e;
            }
        }
        if (n.variable != null && !lk.segments[i].isEmpty()) {
            Entry e = search(n.variable, i + 1, lk);
            if (e != null) {
                return e;
            }
        }
        return n.catchAll.isEmpty() ? null : pick(n.catchAll, lk);
    }

    private static Entry pick(Map<String, Entry> byMethod, Lookup lk) {
        if (byMethod.isEmpty()) {
            return null;
        }
        Entry e = byMethod.get(lk.method);
        if (e == null && lk.head) {
            e = byMethod.get("GET");
        }
        if (e == null) {
            lk.allowed.addAll(byMethod.keySet());
        }
        return e;
    }

    private static Map<String, String> variables(Entry e, String[] decoded) {
        Map<String, String> vars = null;
        for (int i = 0; i < e.segments.size(); i++) {
            Segment s = e.segments.get(i);
            if (s instanceof Variable v) {
                vars = vars == null ? new LinkedHashMap<>() : vars;
                vars.put(v.name, decoded[i]);
            } else if (s instanceof CatchAll c) {
                vars = vars == null ? new LinkedHashMap<>() : vars;
                vars.put(c.name, String.join("/", Arrays.copyOfRange(decoded, i, decoded.length)));
            }
        }
        return vars == null ? Map.of() : vars;
    }

    /** What a stream route sees for a request that carries no body. */
    private static final class EmptyBody implements RequestBodyStream {
        static final EmptyBody INSTANCE = new EmptyBody();

        @Override
        public void listen(BodyListener listener) {
            listener.onComplete();
        }

        @Override
        public long contentLength() {
            return 0;
        }
    }

    private static CompletionStage<HttpResponse> guard(CompletionStage<HttpResponse> stage) {
        return stage.handle((resp, err) -> {
            if (err == null) {
                return resp;
            }
            Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
            if (cause instanceof HttpStatusException s) {
                return toResponse(s);
            }
            throw cause instanceof RuntimeException re ? re : new CompletionException(cause); // server logs it and answers 500
        });
    }

    private static HttpResponse toResponse(HttpStatusException e) {
        return HttpResponse.status(e.status()).text(e.getMessage() == null ? "" : e.getMessage());
    }

    private static CompletionStage<HttpResponse> done(HttpResponse r) {
        return CompletableFuture.completedFuture(r);
    }

    // ---- pattern parsing & matching ----

    private static List<Segment> parsePattern(String pattern) {
        if (!pattern.startsWith("/")) {
            throw new IllegalArgumentException("pattern must start with '/': " + pattern);
        }
        List<Segment> out = new ArrayList<>();
        Set<String> names = new HashSet<>();
        String body = pattern.length() > 1 && pattern.endsWith("/") ? pattern.substring(0, pattern.length() - 1) : pattern;
        if (body.equals("/")) {
            return out; // root
        }
        String[] parts = body.substring(1).split("/", -1);
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i];
            if (p.isEmpty()) {
                throw new IllegalArgumentException("empty segment in pattern: " + pattern);
            }
            if (p.startsWith("{") && p.endsWith("}")) {
                boolean catchAll = p.endsWith("...}");
                String name = p.substring(1, p.length() - (catchAll ? 4 : 1));
                if (!name.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                    throw new IllegalArgumentException("bad variable name '" + name + "' in pattern: " + pattern);
                }
                if (!names.add(name)) {
                    throw new IllegalArgumentException("duplicate variable {" + name + "} in pattern: " + pattern);
                }
                if (catchAll && i != parts.length - 1) {
                    throw new IllegalArgumentException("catch-all {" + name + "...} must be the last segment: " + pattern);
                }
                out.add(catchAll ? new CatchAll(name) : new Variable(name));
            } else {
                if (p.indexOf('{') >= 0 || p.indexOf('}') >= 0) {
                    throw new IllegalArgumentException("braces are only allowed around a whole segment: " + pattern);
                }
                out.add(new Literal(p));
            }
        }
        return out;
    }

    private static String[] splitPath(String path) {
        String p = path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return p.equals("/") ? new String[0] : p.substring(1).split("/", -1);
    }
}
