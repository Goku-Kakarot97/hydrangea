package io.hydrangea.grpc;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpHandler;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RequestBodyStream;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * A gRPC server on Hydrangea's HTTP/2: unary, server-streaming, client-streaming and bidirectional calls, deadlines, cancellation, gzip,
 * metadata and the status model. It is an {@link HttpHandler}, so it can answer ordinary HTTP requests (to a fallback handler) on the same port.
 * <pre>{@code
 * GrpcServer grpc = GrpcServer.builder()
 *     .unary(SAY_HELLO, (req, ctx) -> CompletableFuture.completedFuture(new HelloReply("hello " + req.name())))
 *     .build();
 * Server server = grpc.bind(group, new InetSocketAddress(50051), null);   // cleartext HTTP/2; pass an SSLContext for TLS
 * }</pre>
 * Handlers run on the connection's event loop and must not block; give the builder an {@link Builder#executor executor} (virtual threads, say)
 * to run them elsewhere. Messages of one call are always delivered in order.
 */
public final class GrpcServer implements HttpHandler {

    /** Answers one request with one response (completed later, from any thread, if need be). Fail it with a {@link StatusException} to set the status. */
    @FunctionalInterface
    public interface UnaryHandler<Req, Resp> {
        CompletionStage<Resp> call(Req request, CallContext ctx) throws Exception;
    }

    /** Answers one request with a stream: write to {@code responses}, then complete or fail it. */
    @FunctionalInterface
    public interface ServerStreamingHandler<Req, Resp> {
        void call(Req request, CallContext ctx, StreamObserver<Resp> responses) throws Exception;
    }

    /**
     * Takes a stream of requests: returns the observer that receives them; answers through {@code responses} (exactly one message for a
     * client-streaming method, any number for a bidirectional one).
     */
    @FunctionalInterface
    public interface StreamingHandler<Req, Resp> {
        StreamObserver<Req> call(CallContext ctx, StreamObserver<Resp> responses) throws Exception;
    }

    record Registered(MethodDescriptor<?, ?> method, Object handler) {
    }

    private final Map<String, Registered> methods;
    private final int maxInboundMessageBytes;
    private final boolean compressResponses;
    private final Executor executor;
    private final HttpHandler fallback;
    private final java.util.List<ServerInterceptor> interceptors;
    private final String corsOrigin;
    private volatile Channelz.ServerEntry channelz;
    private final BinaryLog binaryLog;
    private final java.util.List<String> corsHeaders;

    /** The distinct service names that have methods registered, e.g. {@code pkg.Echo}. */
    java.util.List<String> serviceNames() {
        java.util.Set<String> names = new java.util.TreeSet<>();
        for (String path : methods.keySet()) {
            names.add(path.substring(1, path.indexOf('/', 1)));
        }
        return java.util.List.copyOf(names);
    }

    private GrpcServer(Builder b) {
        this.corsOrigin = b.corsOrigin;
        this.binaryLog = b.binaryLog;
        this.corsHeaders = java.util.List.copyOf(b.corsHeaders);
        this.interceptors = java.util.List.copyOf(b.interceptors);
        this.methods = Map.copyOf(b.methods);
        this.maxInboundMessageBytes = b.maxInboundMessageBytes;
        this.compressResponses = b.compressResponses;
        this.executor = b.executor;
        this.fallback = b.fallback;
        for (java.util.function.Consumer<GrpcServer> l : b.builtListeners) {
            l.accept(this);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<String, Registered> methods = new HashMap<>();
        private int maxInboundMessageBytes = 4 * 1024 * 1024;
        private boolean compressResponses;
        private Executor executor;
        private HttpHandler fallback;
        private final java.util.List<ServerInterceptor> interceptors = new java.util.ArrayList<>();
        private String corsOrigin;
        private BinaryLog binaryLog;

        /** Records the calls this server handles in the binary log format; see {@link BinaryLog}. */
        public Builder binaryLog(BinaryLog log) {
            this.binaryLog = log;
            return this;
        }
        private final java.util.List<java.util.function.Consumer<GrpcServer>> builtListeners = new java.util.ArrayList<>();

        /** For services that need to know the finished server (reflection lists its services). */
        void addBuiltListener(java.util.function.Consumer<GrpcServer> l) {
            builtListeners.add(l);
        }
        private final java.util.List<String> corsHeaders = new java.util.ArrayList<>();

        /**
         * Lets browser pages from {@code origin} ({@code "*"} for any) call gRPC-Web methods: answers CORS preflights and adds the response
         * headers. Without it only same-origin pages can. {@code extraHeaders} are custom request headers (metadata) the pages may send.
         */
        public Builder allowCors(String origin, String... extraHeaders) {
            this.corsOrigin = origin;
            this.corsHeaders.addAll(java.util.List.of(extraHeaders));
            return this;
        }

        /** Adds an interceptor for all calls; the first one added runs first. */
        public Builder intercept(ServerInterceptor interceptor) {
            interceptors.add(java.util.Objects.requireNonNull(interceptor));
            return this;
        }

        public <Req, Resp> Builder unary(MethodDescriptor<Req, Resp> m, UnaryHandler<Req, Resp> handler) {
            return add(m, MethodDescriptor.MethodType.UNARY, handler);
        }

        public <Req, Resp> Builder serverStreaming(MethodDescriptor<Req, Resp> m, ServerStreamingHandler<Req, Resp> handler) {
            return add(m, MethodDescriptor.MethodType.SERVER_STREAMING, handler);
        }

        public <Req, Resp> Builder clientStreaming(MethodDescriptor<Req, Resp> m, StreamingHandler<Req, Resp> handler) {
            return add(m, MethodDescriptor.MethodType.CLIENT_STREAMING, handler);
        }

        public <Req, Resp> Builder bidiStreaming(MethodDescriptor<Req, Resp> m, StreamingHandler<Req, Resp> handler) {
            return add(m, MethodDescriptor.MethodType.BIDI_STREAMING, handler);
        }

        private Builder add(MethodDescriptor<?, ?> m, MethodDescriptor.MethodType expected, Object handler) {
            if (m.type() != expected) {
                throw new IllegalArgumentException(m.fullName() + " is declared " + m.type() + " but registered as " + expected);
            }
            if (methods.put(m.path(), new Registered(m, handler)) != null) {
                throw new IllegalArgumentException("method registered twice: " + m.fullName());
            }
            return this;
        }

        /** Registers a service built elsewhere ({@link HealthService}). */
        public Builder addService(BindableService service) {
            service.bindTo(this);
            return this;
        }

        /** Largest request message accepted (after decompression); default 4 MiB. Bigger ones fail the call with RESOURCE_EXHAUSTED. */
        public Builder maxInboundMessageBytes(int bytes) {
            this.maxInboundMessageBytes = bytes;
            return this;
        }

        /** Compress response messages with gzip for clients that accept it; default off. */
        public Builder compressResponses(boolean on) {
            this.compressResponses = on;
            return this;
        }

        /** Runs handler code (not the I/O) on this executor, one call's callbacks at a time and in order; default: on the event loop. */
        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        /** Where requests that are not gRPC go; without one they get 404 (or 415 for a wrong content type on a gRPC path). */
        public Builder fallback(HttpHandler handler) {
            this.fallback = handler;
            return this;
        }

        public GrpcServer build() {
            return new GrpcServer(this);
        }
    }

    /** A service that knows how to register its methods. */
    public interface BindableService {
        void bindTo(Builder builder);
    }

    // ---- listening ----

    /** Listens with HTTP/2 enabled: cleartext (prior knowledge, what gRPC clients use) or TLS with ALPN {@code h2}. */
    public Server bind(EventLoopGroup group, SocketAddress address, SSLContext tls) throws IOException {
        return bind(group, address, tls, new HttpConfig(16 * 1024, 1024 * 1024).withKeepAliveTimeout(Duration.ofMinutes(10)));
    }

    public Server bind(EventLoopGroup group, SocketAddress address, SSLContext tls, HttpConfig config) throws IOException {
        HttpConfig withH2 = config.http2() != null ? config : config.withHttp2(Http2Config.grpcDefaults());
        Server server = HttpServer.bind(group, address, Options.defaults(), tls, withH2, this);
        channelz = Channelz.global().registerServer(server.localAddress(), () -> server.localAddress() != null);
        return server;
    }

    // ---- HttpHandler ----

    static boolean isGrpc(HttpRequest r) {
        String ct = r.headers().get("content-type");
        if (ct == null) {
            return false;
        }
        String t = ct.toLowerCase(Locale.ROOT);
        return t.equals("application/grpc") || t.startsWith("application/grpc+") || t.startsWith("application/grpc;");
    }

    @Override
    public boolean wantsStream(HttpRequest head) {
        if (head.version().equals("HTTP/2") && head.method().equals("POST") && isGrpc(head)) {
            return true;
        }
        return fallback != null && fallback.wantsStream(head);
    }

    @Override
    public boolean wantsStreamingBody(HttpRequest head) {
        return fallback != null && !isGrpc(head) && !GrpcWeb.isWeb(head) && !GrpcWeb.isWebText(head) && fallback.wantsStreamingBody(head);
    }

    @Override
    public CompletionStage<HttpResponse> handleStreaming(HttpRequest head, RequestBodyStream body) throws Exception {
        return fallback.handleStreaming(head, body);
    }

    @Override
    public CompletionStage<HttpResponse> handle(HttpRequest request) throws Exception {
        if (corsOrigin != null && request.method().equals("OPTIONS") && request.headers().get("access-control-request-method") != null
                && methods.containsKey(request.path())) {
            HttpResponse pre = HttpResponse.status(204)
                    .header("access-control-allow-methods", "POST, OPTIONS")
                    .header("access-control-allow-headers", "content-type,x-grpc-web,x-user-agent,grpc-timeout,grpc-accept-encoding,grpc-encoding,authorization,"
                            + String.join(",", corsHeaders))
                    .header("access-control-max-age", "86400");
            cors(request, pre);
            return java.util.concurrent.CompletableFuture.completedFuture(pre);
        }
        if (GrpcWeb.isWeb(request)) {
            if (!request.method().equals("POST")) {
                return java.util.concurrent.CompletableFuture.completedFuture(HttpResponse.status(405).header("Allow", "POST").text("gRPC calls are POST"));
            }
            Registered reg = methods.get(request.path());
            if (reg != null && reg.method().type().clientStreams()) {
                return java.util.concurrent.CompletableFuture.completedFuture(webTrailersOnly(request,
                        Status.of(Status.Code.UNIMPLEMENTED).withDescription("gRPC-Web cannot call client-streaming or bidirectional methods")));
            }
            return java.util.concurrent.CompletableFuture.completedFuture(startCall(request, true));
        }
        if (!isGrpc(request)) {
            if (fallback != null) {
                return fallback.handle(request);
            }
            boolean grpcPath = methods.containsKey(request.path());
            return java.util.concurrent.CompletableFuture.completedFuture(grpcPath
                    ? HttpResponse.status(415).header("Accept-Post", "application/grpc").text("gRPC calls need Content-Type: application/grpc")
                    : HttpResponse.notFound());
        }
        if (!request.version().equals("HTTP/2")) {
            return java.util.concurrent.CompletableFuture.completedFuture(HttpResponse.status(505).text("gRPC needs HTTP/2"));
        }
        return java.util.concurrent.CompletableFuture.completedFuture(startCall(request, false));
    }

    private HttpResponse fail(HttpRequest request, boolean web, Status status) {
        return web ? webTrailersOnly(request, status) : trailersOnly(status);
    }

    private HttpResponse startCall(HttpRequest request, boolean web) {
        byte[] webBody = null;
        if (web) {
            try {
                webBody = GrpcWeb.isWebText(request) ? GrpcWeb.decodeText(request.body()) : request.body();
            } catch (IllegalArgumentException e) {
                return webTrailersOnly(request, Status.of(Status.Code.INTERNAL).withDescription("malformed base64 request body"));
            }
        }
        if (!request.method().equals("POST")) {
            return HttpResponse.status(405).header("Allow", "POST").text("gRPC calls are POST");
        }
        Registered reg = methods.get(request.path());
        if (reg == null) {
            return fail(request, web, Status.of(Status.Code.UNIMPLEMENTED).withDescription("Method not found: " + request.path()));
        }
        String encoding = request.headers().get("grpc-encoding");
        boolean gzipIn = false;
        if (encoding != null && !encoding.equalsIgnoreCase("identity")) {
            if (!encoding.equalsIgnoreCase("gzip")) {
                return fail(request, web, Status.of(Status.Code.UNIMPLEMENTED).withDescription("Can't decode grpc-encoding: " + encoding))
                        .header("grpc-accept-encoding", "identity,gzip");
            }
            gzipIn = true;
        }
        Duration timeout = null;
        String t = request.headers().get("grpc-timeout");
        if (t != null) {
            timeout = GrpcFraming.decodeTimeout(t);
            if (timeout == null) {
                return fail(request, web, Status.of(Status.Code.INTERNAL).withDescription("malformed grpc-timeout: " + t));
            }
        }
        boolean clientAcceptsGzip = false;
        String accept = request.headers().get("grpc-accept-encoding");
        if (accept != null) {
            for (String e : accept.split(",")) {
                clientAcceptsGzip |= e.trim().equalsIgnoreCase("gzip");
            }
        }
        boolean gzipOut = compressResponses && clientAcceptsGzip;
        final boolean gzipRequest = gzipIn;
        final byte[] decodedBody = webBody;
        final Duration deadline = timeout;
        HttpResponse head = HttpResponse.status(200)
                .header("content-type", web ? GrpcWeb.responseContentType(request) : "application/grpc")
                .header("grpc-accept-encoding", "identity,gzip");
        if (gzipOut) {
            head.header("grpc-encoding", "gzip");
        }
        if (!web) {
            head.deferHead();   // handlers may still add initial headers; a call that fails at once becomes a single Trailers-Only block
        }
        if (web) {
            head.header("access-control-expose-headers", "grpc-status,grpc-message,grpc-encoding,grpc-accept-encoding");
            if (request.version().equals("HTTP/1.1")) {
                head.header("transfer-encoding", "chunked");
            }
            cors(request, head);
        }
        return head.stream(() -> new ServerCall<>(reg, request, deadline, gzipRequest, gzipOut, maxInboundMessageBytes, executor, interceptors, channelz, binaryLog, decodedBody, web && request.version().equals("HTTP/1.1"), web && GrpcWeb.isWebText(request)));
    }

    /** For gRPC-Web the same, with the content type the request used and the CORS headers. */
    private HttpResponse webTrailersOnly(HttpRequest request, Status status) {
        HttpResponse r = trailersOnly(status).header("access-control-expose-headers", "grpc-status,grpc-message");
        r.headers().set("content-type", GrpcWeb.responseContentType(request));
        cors(request, r);
        return r;
    }

    /** Adds the CORS response header for browsers, if an origin policy is configured and the request's origin is allowed. */
    private void cors(HttpRequest request, HttpResponse response) {
        if (corsOrigin == null) {
            return;
        }
        String origin = request.headers().get("origin");
        if (origin == null) {
            return;
        }
        if (corsOrigin.equals("*")) {
            response.header("access-control-allow-origin", "*");
        } else if (corsOrigin.equalsIgnoreCase(origin)) {
            response.header("access-control-allow-origin", origin).header("vary", "Origin");
        }
    }

    /** A response that carries only a status (HEADERS with END_STREAM), for calls that fail before they start. */
    static HttpResponse trailersOnly(Status status) {
        HttpResponse r = HttpResponse.status(200).header("content-type", "application/grpc").header("grpc-status", Integer.toString(status.code().value()));
        if (status.description() != null && !status.description().isEmpty()) {
            r.header("grpc-message", GrpcFraming.encodeMessage(status.description()));
        }
        return r;
    }
}
