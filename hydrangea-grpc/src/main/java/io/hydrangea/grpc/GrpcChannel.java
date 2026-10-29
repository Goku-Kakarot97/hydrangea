package io.hydrangea.grpc;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Scheduled;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.BodySink;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Headers;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.UnprocessedException;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.RequestBody;
import io.hydrangea.http.ResponseHead;
import io.hydrangea.http.StreamHandle;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * A gRPC client: calls go over multiplexed HTTP/2 connections to one server, cleartext by prior knowledge ({@code http://host:port}, what gRPC
 * servers expect) or TLS with ALPN ({@code https://host:port}). Non-blocking from end to end.
 * <pre>{@code
 * try (GrpcChannel channel = GrpcChannel.forTarget("http://localhost:50051").build()) {
 *     Msg reply = channel.unary(UNARY, new Msg("hi"), CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(2))).join();
 * }
 * }</pre>
 * A failed call carries its {@link Status}: transport failures are UNAVAILABLE, a missed deadline is DEADLINE_EXCEEDED, a cancelled call
 * CANCELLED, and an HTTP error from something that is not a gRPC server is mapped as the protocol specifies (404 becomes UNIMPLEMENTED...).
 * Calls are never retried: that is for the application to decide, as only it knows whether a call is safe to repeat.
 */
public final class GrpcChannel implements Channel, AutoCloseable {
    /** How often a call is replayed because the server provably did not process it (GOAWAY above its mark, refused stream). */
    private static final int MAX_TRANSPARENT_RETRIES = 2;


    private final HttpClient http;
    private final java.util.concurrent.ConcurrentHashMap<String, ClientRequest> requestTemplates = new java.util.concurrent.ConcurrentHashMap<>();
    private final EventLoopGroup ownedGroup;
    private final String base;
    private final int maxInboundMessageBytes;
    private final String userAgent;
    private final String host;
    private final int port;
    private final NameResolver resolver;
    private final LoadBalancing balancing;
    private final Duration resolveInterval;
    private final CallCredentials credentials;
    private final String authority;
    private final RetryPolicy retry;
    private final HedgingPolicy hedging;
    private final ServiceConfig serviceConfig;
    private final RetryThrottling.Bucket throttle;
    private final int retryBufferBytes;
    private final Channel entry;
    private final java.util.concurrent.atomic.AtomicInteger rotation = new java.util.concurrent.atomic.AtomicInteger();
    private volatile List<Backend> backends = List.of();
    private volatile boolean closed;
    private final Channelz.ChannelEntry channelz;
    private final BinaryLog binaryLog;
    private final XdsClient xds;
    private final java.util.concurrent.ConcurrentHashMap<InetSocketAddress, Backend> xdsBackends = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile Scheduled refreshTimer;
    private final Duration healthInterval;
    private final String healthService;
    private volatile Scheduled healthTimer;
    private final java.util.concurrent.ExecutorService resolverThread = java.util.concurrent.Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "hydrangea-grpc-resolver");
        t.setDaemon(true);
        return t;
    });

    private GrpcChannel(Builder b) {
        URI target = URI.create(b.target);
        boolean isXds = "xds".equalsIgnoreCase(target.getScheme());
        String scheme, hostName;
        int portNum;
        if (isXds) {
            // xds:///service-name: the name is looked up with the xDS management server, not DNS
            String name = target.getPath() == null ? "" : target.getPath();
            if (target.getAuthority() != null && !target.getAuthority().isEmpty()) {
                throw new IllegalArgumentException("xDS federation (an authority in the target) is not supported: " + b.target);
            }
            if (name.startsWith("/")) {
                name = name.substring(1);
            }
            if (name.isEmpty()) {
                throw new IllegalArgumentException("an xDS target is xds:///service-name: " + b.target);
            }
            scheme = b.sslContext != null ? "https" : "http";
            hostName = name;
            portNum = -1;
        } else {
            if (target.getHost() == null || (!"http".equalsIgnoreCase(target.getScheme()) && !"https".equalsIgnoreCase(target.getScheme()))) {
                throw new IllegalArgumentException("a gRPC target is http://host:port, https://host:port or xds:///service: " + b.target);
            }
            scheme = target.getScheme().toLowerCase(Locale.ROOT);
            hostName = target.getHost();
            portNum = target.getPort();
        }
        this.base = scheme + "://" + hostName + (portNum != -1 ? ":" + portNum : "");
        this.host = hostName;
        this.port = portNum != -1 ? portNum : ("https".equals(scheme) ? 443 : 80);
        this.resolver = b.resolver != null ? b.resolver : NameResolver.dns();
        this.serviceConfig = b.serviceConfig;
        this.balancing = !b.balancingSet && b.serviceConfig != null && b.serviceConfig.loadBalancing() != null ? b.serviceConfig.loadBalancing() : b.balancing;
        this.resolveInterval = b.resolveInterval;
        this.credentials = b.credentials;
        this.binaryLog = b.binaryLog;
        this.healthInterval = b.healthInterval;
        this.healthService = !b.healthServiceSet && b.serviceConfig != null && b.serviceConfig.healthCheckServiceName() != null ? b.serviceConfig.healthCheckServiceName() : b.healthService;
        this.authority = hostName + (portNum != -1 ? ":" + portNum : "");
        if (b.credentials != null && !"https".equals(scheme) && !b.credentialsOverPlaintext) {
            throw new IllegalArgumentException("call credentials would be sent in the clear to " + b.target
                    + "; use https, or allowCredentialsOverPlaintext() if the network is trusted");
        }
        this.retry = b.retry;
        this.hedging = b.hedging;
        RetryThrottling throttling = b.throttling != null ? b.throttling : b.serviceConfig == null ? null : b.serviceConfig.retryThrottling();
        this.throttle = throttling == null ? null : new RetryThrottling.Bucket(throttling);
        if (b.retry != null && b.hedging != null) {
            throw new IllegalArgumentException("a channel retries or hedges, not both");
        }
        this.retryBufferBytes = b.retryBufferBytes;
        this.maxInboundMessageBytes = b.maxInboundMessageBytes;
        this.userAgent = b.userAgent;
        EventLoopGroup group = b.group != null ? b.group : new EventLoopGroup(2);
        this.ownedGroup = b.group != null ? null : group;
        HttpClientConfig cfg = HttpClientConfig.defaults()
                .withRequestTimeout(null)      // a call lives as long as its deadline says, not as long as an HTTP request may
                .withIdleTimeout(null)
                .withMaxRedirects(0)
                .withMaxConnectionsPerHost(b.maxConnections)
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)
                        .withKeepAlive(b.keepAliveInterval, b.keepAliveTimeout));
        if (b.sslContext != null) {
            cfg = cfg.withSslContext(b.sslContext);
        }
        this.http = new HttpClient(group, cfg);
        Channel chain = this::startWithRetry;
        for (int i = b.interceptors.size() - 1; i >= 0; i--) {
            ClientInterceptor ic = b.interceptors.get(i);
            Channel next = chain;
            chain = new Channel() {
                @Override
                public <Req, Resp> ClientCall<Req, Resp> newCall(MethodDescriptor<Req, Resp> m, CallOptions o, ClientCall.Listener<Resp> l) {
                    return ic.intercept(m, o, l, next);
                }
            };
        }
        this.entry = chain;
        if (isXds) {
            this.xds = new XdsClient(b.xdsBootstrap != null ? b.xdsBootstrap : XdsBootstrap.fromEnvironment(), hostName);
            this.xds.onSnapshot(snapshot -> {   // forget the servers that are no longer in the configuration
                java.util.Set<InetSocketAddress> keep = XdsClient.addressesIn(snapshot);
                for (var e : xdsBackends.entrySet()) {
                    if (!keep.contains(e.getKey()) && xdsBackends.remove(e.getKey(), e.getValue())) {
                        Channelz.global().unregisterSubchannel(e.getValue().channelz);
                    }
                }
            });
        } else {
            this.xds = null;
        }
        channelz = Channelz.global().registerChannel(b.target);
        channelz.state = () -> {
            if (closed) {
                return Channelz.State.SHUTDOWN;
            }
            if (xds != null) {
                return xds.snapshot() != null ? Channelz.State.READY : xds.isReady() ? Channelz.State.TRANSIENT_FAILURE : Channelz.State.CONNECTING;
            }
            long now = System.nanoTime();
            for (Backend bk : backends) {
                if (bk.healthy(now)) {
                    return Channelz.State.READY;
                }
            }
            return Channelz.State.TRANSIENT_FAILURE;
        };
        channelz.subchannels = () -> {
            List<Channelz.SubchannelEntry> subs = new ArrayList<>();
            for (Backend bk : xds != null ? xdsBackends.values() : backends) {
                subs.add(bk.channelz);
            }
            return subs;
        };
        if (xds == null) {
            refreshAddresses();   // the first resolution is synchronous: a channel that can be built can usually be used at once
            scheduleRefresh(resolveInterval);
            if (healthInterval != null) {
                scheduleHealthChecks(Duration.ofMillis(0));
            }
        }
    }

    public static Builder forTarget(String target) {
        return new Builder(target);
    }

    public static final class Builder {
        private final String target;
        private SSLContext sslContext;
        private int maxInboundMessageBytes = 4 * 1024 * 1024;
        private String userAgent = "grpc-hydrangea/0.1";
        private int maxConnections = 4;
        private EventLoopGroup group;
        private NameResolver resolver;
        private LoadBalancing balancing = LoadBalancing.PICK_FIRST;
        private Duration resolveInterval = Duration.ofSeconds(30);
        private Duration healthInterval;
        private String healthService = "";
        private boolean healthServiceSet;
        private boolean balancingSet;
        private ServiceConfig serviceConfig;

        /**
         * Applies a service config (JSON, see {@link ServiceConfig}): per-method timeouts, retry/hedging policies and size limits, plus the
         * load-balancing policy and retry throttling. What you set on the builder yourself ({@link #loadBalancing}, {@link #retryThrottling},
         * {@link #healthCheckService}) wins over the config; a method's policy in the config replaces the channel-wide retry or hedging policy.
         * @throws IllegalArgumentException for text that is not a valid config
         */
        public Builder serviceConfig(String json) {
            this.serviceConfig = ServiceConfig.parse(json);
            return this;
        }


        /**
         * Asks every server address {@code grpc.health.v1.Health/Check} each {@code interval} and keeps calls away from the ones that say they
         * are not serving, before a call has to fail to find out. Servers without a health service count as healthy. Default off.
         */
        public Builder healthCheck(Duration interval) {
            this.healthInterval = interval;
            return this;
        }

        /** The service name asked about by {@link #healthCheck}; default "" (the server as a whole). */
        public Builder healthCheckService(String service) {
            this.healthService = service;
            this.healthServiceSet = true;
            return this;
        }

        private XdsBootstrap xdsBootstrap;

        /** The bootstrap for an {@code xds:///service} target; default: from {@code GRPC_XDS_BOOTSTRAP} / {@code GRPC_XDS_BOOTSTRAP_CONFIG}. */
        public Builder xdsBootstrap(String json) {
            this.xdsBootstrap = XdsBootstrap.parse(json);
            return this;
        }

        private BinaryLog binaryLog;

        /** Records the calls this channel makes (each retry attempt as its own call) in the binary log format; see {@link BinaryLog}. */
        public Builder binaryLog(BinaryLog log) {
            this.binaryLog = log;
            return this;
        }

        private CallCredentials credentials;
        private boolean credentialsOverPlaintext;
        private RetryPolicy retry;

        /** Credentials added to every call; see {@link CallCredentials}. */
        public Builder callCredentials(CallCredentials credentials) {
            this.credentials = credentials;
            return this;
        }

        /** Allows {@link #callCredentials} on a cleartext {@code http://} target (a service mesh or a loopback test). */
        public Builder allowCredentialsOverPlaintext() {
            this.credentialsOverPlaintext = true;
            return this;
        }
        private HedgingPolicy hedging;
        private RetryThrottling throttling;

        /** Sends slow calls again in parallel and takes the first answer; default off. See {@link HedgingPolicy}. Not combinable with a retry policy. */
        public Builder hedgingPolicy(HedgingPolicy policy) {
            this.hedging = policy;
            return this;
        }

        /** Limits retries and hedges while the server keeps failing; default off. See {@link RetryThrottling}. */
        public Builder retryThrottling(RetryThrottling throttling) {
            this.throttling = throttling;
            return this;
        }
        private int retryBufferBytes = 256 * 1024;
        private final List<ClientInterceptor> interceptors = new ArrayList<>();

        /** Adds an interceptor; the first one added is the outermost. */
        public Builder intercept(ClientInterceptor interceptor) {
            interceptors.add(java.util.Objects.requireNonNull(interceptor));
            return this;
        }

        /** How the target's host becomes server addresses; default {@link NameResolver#dns()}. The host still names the server for TLS and {@code :authority}. */
        public Builder nameResolver(NameResolver resolver) {
            this.resolver = resolver;
            return this;
        }

        /** How calls are spread over the resolved addresses; default {@link LoadBalancing#PICK_FIRST}. */
        public Builder loadBalancing(LoadBalancing policy) {
            this.balancing = policy;
            this.balancingSet = true;
            return this;
        }

        /** How often the target is resolved again; default 30 s. Also bounds how soon a new or removed server is noticed. */
        public Builder resolveInterval(Duration interval) {
            this.resolveInterval = interval;
            return this;
        }

        /** Retries failed calls by itself; default off. See {@link RetryPolicy} for what can be retried. */
        public Builder retryPolicy(RetryPolicy policy) {
            this.retry = policy;
            return this;
        }

        /** How much request data a call may have sent and still be retryable; default 256 KiB. Past it the call can no longer be replayed. */
        public Builder retryBufferBytes(int bytes) {
            this.retryBufferBytes = bytes;
            return this;
        }

        /**
         * Sends an HTTP/2 PING after {@code interval} without any data from the server and drops the connection if it does not answer
         * within {@code timeout}. Default off.
         */
        public Builder keepAlive(Duration interval, Duration timeout) {
            this.keepAliveInterval = interval;
            this.keepAliveTimeout = timeout;
            return this;
        }

        private Duration keepAliveInterval;
        private Duration keepAliveTimeout;

        private Builder(String target) {
            this.target = target;
        }

        /** For https targets; default: the JVM's. */
        public Builder sslContext(SSLContext ctx) {
            this.sslContext = ctx;
            return this;
        }

        /** Largest response message accepted (after decompression); default 4 MiB. */
        public Builder maxInboundMessageBytes(int bytes) {
            this.maxInboundMessageBytes = bytes;
            return this;
        }

        public Builder userAgent(String ua) {
            this.userAgent = ua;
            return this;
        }

        /** Connections opened when calls outnumber what one connection's streams allow; default 4. */
        public Builder maxConnections(int n) {
            this.maxConnections = n;
            return this;
        }

        /** Shares event loops with other components; the channel then does not close them. */
        public Builder eventLoopGroup(EventLoopGroup group) {
            this.group = group;
            return this;
        }

        public GrpcChannel build() {
            return new GrpcChannel(this);
        }
    }

    @Override
    public void close() {
        closed = true;
        if (xds != null) {
            xds.close();
        }
        Channelz.global().unregisterChannel(channelz);
        Scheduled t = refreshTimer;
        if (t != null) {
            t.cancel();
        }
        Scheduled ht = healthTimer;
        if (ht != null) {
            ht.cancel();
        }
        resolverThread.shutdownNow();
        http.close();
        if (ownedGroup != null) {
            ownedGroup.close();
        }
    }

    // ---- the four kinds of call ----

    /** One request, one response. The future fails with a {@link StatusException}; cancelling it cancels the call. */
    public <Req, Resp> CompletableFuture<Resp> unary(MethodDescriptor<Req, Resp> method, Req request, CallOptions options) {
        CompletableFuture<Resp> result = new CompletableFuture<>();
        Object[] first = new Object[1];
        int[] count = {0};
        ClientCall<Req, Resp> call = newCall(method, options, new ClientCall.Listener<>() {
            @Override
            public void onMessage(Resp message) {
                if (count[0]++ == 0) {
                    first[0] = message;
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public void onClose(Status status, Metadata trailers) {
                if (!status.isOk()) {
                    result.completeExceptionally(new StatusException(status, trailers));
                } else if (count[0] == 1) {
                    result.complete((Resp) first[0]);
                } else {
                    result.completeExceptionally(new StatusException(Status.of(Status.Code.INTERNAL)
                            .withDescription("expected one response message, got " + count[0]), trailers));
                }
            }
        });
        result.whenComplete((r, e) -> {
            if (result.isCancelled()) {
                call.cancel("cancelled by the caller");
            }
        });
        call.send(request);
        call.halfClose();
        return result;
    }

    /** One request, a stream of responses: {@code responses.onNext} for each, then onCompleted or onError. Returns the call, to cancel it. */
    public <Req, Resp> ClientCall<Req, Resp> serverStreaming(MethodDescriptor<Req, Resp> method, Req request, CallOptions options,
                                                              StreamObserver<Resp> responses) {
        ClientCall<Req, Resp> call = newCall(method, options, adapt(responses));
        call.send(request);
        call.halfClose();
        return call;
    }

    /** A stream of requests, one response (delivered to {@code response}). Send with {@code call.send}, finish with {@code call.halfClose}. */
    public <Req, Resp> ClientCall<Req, Resp> clientStreaming(MethodDescriptor<Req, Resp> method, CallOptions options, StreamObserver<Resp> response) {
        return newCall(method, options, adapt(response));
    }

    /** Streams both ways. */
    public <Req, Resp> ClientCall<Req, Resp> bidiStreaming(MethodDescriptor<Req, Resp> method, CallOptions options, StreamObserver<Resp> responses) {
        return newCall(method, options, adapt(responses));
    }

    private static <Resp> ClientCall.Listener<Resp> adapt(StreamObserver<Resp> observer) {
        return new ClientCall.Listener<>() {
            @Override
            public void onMessage(Resp message) {
                observer.onNext(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                if (status.isOk()) {
                    observer.onCompleted();
                } else {
                    observer.onError(new StatusException(status, trailers));
                }
            }
        };
    }

    /** Starts a call: the request headers go out now, messages follow as you {@link ClientCall#send} them. Goes through the interceptors. */
    @Override
    public <Req, Resp> ClientCall<Req, Resp> newCall(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener) {
        return entry.newCall(method, options, listener);
    }

    private <Req, Resp> ClientCall<Req, Resp> startWithRetry(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener) {
        channelz.calls.start();
        final ClientCall.Listener<Resp> reported = listener;
        listener = new ClientCall.Listener<>() {
            @Override
            public void onHeaders(Metadata headers) {
                reported.onHeaders(headers);
            }

            @Override
            public void onMessage(Resp message) {
                reported.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                channelz.calls.end(status.isOk());
                reported.onClose(status, trailers);
            }
        };
        RetryPolicy retryPolicy = retry;
        HedgingPolicy hedgingPolicy = hedging;
        ServiceConfig.MethodConfig mc = serviceConfig == null ? null : serviceConfig.forMethod(method.serviceName(), method.fullName().substring(method.fullName().indexOf('/') + 1));
        if (mc != null) {
            if (mc.timeout() != null && (options.deadline() == null || mc.timeout().compareTo(options.deadline()) < 0)) {
                options = options.withDeadline(mc.timeout());
            }
            if (mc.maxRequestMessageBytes() != null && options.maxRequestBytes() == null) {
                options = options.withMaxRequestBytes(mc.maxRequestMessageBytes());
            }
            if (mc.maxResponseMessageBytes() != null && options.maxResponseBytes() == null) {
                options = options.withMaxResponseBytes(mc.maxResponseMessageBytes());
            }
            if (mc.retry() != null || mc.hedging() != null) {   // the method's own policy replaces the channel's
                retryPolicy = mc.retry();
                hedgingPolicy = mc.hedging();
            }
        }
        if (hedgingPolicy != null) {
            HedgingCall<Req, Resp> call = new HedgingCall<>(method, options, listener, hedgingPolicy);
            call.begin();
            return call;
        }
        // without a policy a call is still replayed once when the server provably did not process it (GOAWAY, refused stream)
        RetryingCall<Req, Resp> call = new RetryingCall<>(method, options, listener, retryPolicy);
        call.begin();
        return call;
    }

    private <Req, Resp> Call<Req, Resp> launchAttempt(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener) {
        Call<Req, Resp> call = new Call<>(method, options, listener);
        call.start();
        return call;
    }

    // ---- name resolution and load balancing ----

    /** One server address and what the channel has seen of its health. */
    private static final class Backend {
        final InetSocketAddress address;
        volatile long ejectedUntil;      // System.nanoTime() value; 0 = healthy
        int failures;
        final Channelz.SubchannelEntry channelz;

        Backend(InetSocketAddress address) {
            this.address = address;
            this.channelz = Channelz.global().registerSubchannel(String.valueOf(address), address);
            this.channelz.state = () -> healthy(System.nanoTime()) ? Channelz.State.READY : Channelz.State.TRANSIENT_FAILURE;
        }

        boolean healthy(long now) {
            return ejectedUntil == 0 || now - ejectedUntil >= 0;
        }

        synchronized void failed() {
            failures = Math.min(failures + 1, 6);
            long backoff = Math.min(500_000_000L << failures, 30_000_000_000L);   // 1 s, 2 s... 30 s
            ejectedUntil = System.nanoTime() + backoff;
            if (ejectedUntil == 0) {
                ejectedUntil = 1;
            }
        }

        /** Marks this address unusable for {@code nanos} (a health check said so). */
        synchronized void ejectFor(long nanos) {
            ejectedUntil = System.nanoTime() + nanos;
            if (ejectedUntil == 0) {
                ejectedUntil = 1;
            }
        }

        synchronized void succeeded() {
            failures = 0;
            ejectedUntil = 0;
        }
    }

    private void refreshAddresses() {
        try {
            List<InetSocketAddress> found = resolver.resolve(host, port);
            List<Backend> old = backends;
            List<Backend> next = new ArrayList<>();
            for (InetSocketAddress a : found) {
                Backend keep = null;
                for (Backend b : old) {
                    if (b.address.equals(a)) {
                        keep = b;
                        break;
                    }
                }
                next.add(keep != null ? keep : new Backend(a));
            }
            backends = List.copyOf(next);
            for (Backend gone : old) {
                if (!next.contains(gone)) {
                    Channelz.global().unregisterSubchannel(gone.channelz);
                }
            }
        } catch (Exception e) {
            // keep the addresses we have; try again at the next interval (sooner if there are none)
        }
    }

    private void scheduleRefresh(Duration after) {
        if (closed) {
            return;
        }
        refreshTimer = http.eventLoop().schedule(() -> {
            try {
                resolverThread.execute(() -> {
                    refreshAddresses();
                    scheduleRefresh(backends.isEmpty() ? Duration.ofSeconds(1) : resolveInterval);
                });
            } catch (java.util.concurrent.RejectedExecutionException closedMeanwhile) {
                // closing
            }
        }, after.toNanos(), TimeUnit.NANOSECONDS);
    }

    // ---- active health checking ----

    private void scheduleHealthChecks(Duration after) {
        if (closed) {
            return;
        }
        healthTimer = http.eventLoop().schedule(() -> {
            for (Backend b : backends) {
                probe(b);
            }
            scheduleHealthChecks(healthInterval);
        }, after.toNanos(), TimeUnit.NANOSECONDS);
    }

    /** One health check of one address, outside the interceptors and retries: it is about that address, not about the channel. */
    private void probe(Backend b) {
        Call<String, HealthService.ServingStatus> call = new Call<>(HealthService.CHECK,
                CallOptions.DEFAULT.withDeadline(healthInterval.compareTo(Duration.ofSeconds(5)) < 0 ? healthInterval : Duration.ofSeconds(5)),
                new ClientCall.Listener<>() {
                    HealthService.ServingStatus answer;

                    @Override
                    public void onMessage(HealthService.ServingStatus status) {
                        answer = status;
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        if (status.isOk() && answer == HealthService.ServingStatus.SERVING) {
                            b.succeeded();
                        } else if (status.isOk()) {
                            b.ejectFor(healthInterval.toNanos() * 2);   // it answered, and said no
                        } else if (status.code() == Status.Code.UNIMPLEMENTED) {
                            b.succeeded();                               // no health service there: nothing to check, assume it works
                        } else {
                            b.failed();
                        }
                    }
                }, b);
        call.start();
        call.send(healthService);
        call.halfClose();
    }

    private Backend xdsBackend(InetSocketAddress address) {
        return xdsBackends.computeIfAbsent(address, Backend::new);
    }

    private Backend pick() {
        List<Backend> bs = backends;
        int n = bs.size();
        if (n == 0) {
            return null;
        }
        long now = System.nanoTime();
        int start = balancing == LoadBalancing.ROUND_ROBIN ? Math.floorMod(rotation.getAndIncrement(), n) : 0;
        Backend soonest = null;
        for (int i = 0; i < n; i++) {
            Backend b = bs.get((start + i) % n);
            if (b.healthy(now)) {
                return b;
            }
            if (soonest == null || b.ejectedUntil - soonest.ejectedUntil < 0) {
                soonest = b;
            }
        }
        return soonest;   // all are ejected: better to try the one that comes back first than to fail every call
    }

    // ---- one call ----

    private final class Call<Req, Resp> implements ClientCall<Req, Resp> {
        private final MethodDescriptor<Req, Resp> method;
        private final CallOptions options;
        private final Listener<Resp> listener;
        private final boolean gzipOut;
        private Backend backend;
        private volatile BinaryLog.CallLog blog;
        private volatile boolean locallyCancelled;
        private volatile boolean counted;   // this attempt was started on a backend, so it must be ended there
        private boolean sawHeaders;

        private final Object lock = new Object();    // serialises listener callbacks and guards the state below
        private boolean closed;
        private boolean halfClosed;
        private boolean halfClosePending;
        private final List<byte[]> early = new ArrayList<>();   // messages sent before the transport attached its sink
        private volatile BodySink sink;
        private volatile StreamHandle handle;
        private volatile BodyControl control;
        private Scheduled deadlineTimer;
        private Duration deadlineSpan;

        // the response, as it arrives
        private GrpcFraming.Decoder decoder;
        private Status headStatus;
        private Metadata headTrailers;
        private Status protocolStatus;       // the response is not a valid gRPC response: this is the call's outcome
        private Status trailerStatus;
        private Metadata trailerMetadata;

        private final Backend forced;   // health probes go to one given address; null = ask the load balancer

        Call(MethodDescriptor<Req, Resp> method, CallOptions options, Listener<Resp> listener) {
            this(method, options, listener, null);
        }

        Call(MethodDescriptor<Req, Resp> method, CallOptions options, Listener<Resp> listener, Backend forced) {
            this.forced = forced;
            this.method = method;
            this.options = options;
            this.listener = listener;
            this.gzipOut = "gzip".equals(options.compression());
        }

        void start() {
            // ClientRequest is immutable: every call returns the request to go on with
            // the URL and the headers every call carries are parsed and validated once per method
            ClientRequest r = requestTemplates.computeIfAbsent(method.fullName(), name -> ClientRequest.post(base + "/" + name)
                    .header("content-type", "application/grpc")
                    .header("te", "trailers")
                    .header("user-agent", userAgent)
                    .header("grpc-accept-encoding", "gzip"));
            if (gzipOut) {
                r = r.header("grpc-encoding", "gzip");
            }
            if (options.deadline() != null) {
                deadlineSpan = options.deadline();
                r = r.header("grpc-timeout", GrpcFraming.encodeTimeout(deadlineSpan));
            }
            Headers extra = new Headers();
            options.metadata().toHeaders(extra);
            for (var e : extra.entries()) {
                r = r.header(e.getKey(), e.getValue());
            }
            r = r.body(new RequestBody() {
                @Override
                public long contentLength() {
                    return -1;
                }

                @Override
                public void start(BodySink s) {
                    attach(s);
                }
            });
            if (deadlineSpan != null) {
                deadlineTimer = http.eventLoop().schedule(this::deadlineExceeded, deadlineSpan.toNanos(), TimeUnit.NANOSECONDS);
            }
            if (credentials == null) {
                go(r);
                return;
            }
            final ClientRequest base = r;
            CompletionStage<Metadata> creds;
            try {
                creds = credentials.metadata(method.path(), authority);
            } catch (RuntimeException e) {
                creds = CompletableFuture.failedFuture(e);
            }
            creds.whenComplete((md, err) -> {
                if (err != null || md == null) {
                    Throwable cause = err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err;
                    Status st = cause instanceof StatusException se ? se.status()
                            : Status.of(Status.Code.UNAUTHENTICATED).withDescription("could not get call credentials").withCause(cause);
                    finish(st, null);
                    return;
                }
                Headers more = new Headers();
                md.toHeaders(more);
                ClientRequest withCreds = base;
                for (var e : more.entries()) {
                    withCreds = withCreds.header(e.getKey(), e.getValue());
                }
                go(withCreds);
            });
        }

        /** Picks a server and sends the request, unless the call has ended meanwhile (cancelled, or past its deadline while credentials loaded). */
        private void go(ClientRequest r) {
            synchronized (lock) {
                if (closed) {
                    return;
                }
            }
            if (xds != null && forced == null) {
                if (!xds.isReady()) {   // routing is not known yet: wait for it (the call's deadline keeps running)
                    final ClientRequest waiting = r;
                    xds.whenReady(Duration.ofSeconds(15)).whenComplete((v, err) -> {
                        if (err != null) {
                            Throwable cause = err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err;
                            finish(cause instanceof StatusException se ? se.status() : Status.of(Status.Code.UNAVAILABLE).withDescription(String.valueOf(cause.getMessage())), null);
                        } else {
                            go(waiting);
                        }
                    });
                    return;
                }
                XdsClient.Selection sel;
                try {
                    sel = xds.select(method.path(), options.metadata());
                } catch (StatusException e) {
                    finish(e.status(), null);
                    return;
                }
                XdsDecoder.Endpoint ep = sel.cluster().pick(addr -> xdsBackend(addr).healthy(System.nanoTime()));
                if (ep == null) {
                    finish(Status.of(Status.Code.UNAVAILABLE).withDescription("cluster " + sel.cluster().name + " has no endpoints"), null);
                    return;
                }
                backend = xdsBackend(ep.address());
                if (sel.timeout() != null && deadlineSpan == null) {   // the route's timeout is the default deadline
                    deadlineSpan = sel.timeout();
                    r = r.header("grpc-timeout", GrpcFraming.encodeTimeout(deadlineSpan));
                    deadlineTimer = http.eventLoop().schedule(this::deadlineExceeded, deadlineSpan.toNanos(), TimeUnit.NANOSECONDS);
                }
            } else {
                backend = forced != null ? forced : pick();
            }
            if (backend == null) {
                finish(Status.of(Status.Code.UNAVAILABLE).withDescription("no addresses for " + host), null);
                return;
            }
            r = r.connectTo(backend.address);
            if (binaryLog != null) {
                blog = binaryLog.start(BinaryLog.Side.CLIENT, method.path());
                if (blog != null) {
                    blog.clientHeader(options.metadata(), method.path(), authority, deadlineSpan, backend.address);
                }
            }
            counted = true;
            backend.channelz.calls.start();
            backend.channelz.socket.streams.start();
            backend.channelz.socket.lastLocalStreamMillis = System.currentTimeMillis();
            StreamHandle h = http.stream(r, new Receiver());
            handle = h;
            boolean over;
            synchronized (lock) {
                over = closed;
            }
            if (over) {
                h.cancel();   // ended while we were starting it
            }
        }

        private void attach(BodySink s) {
            List<byte[]> pending;
            boolean finish;
            synchronized (lock) {
                sink = s;
                pending = new ArrayList<>(early);
                early.clear();
                finish = halfClosePending;
            }
            for (byte[] frame : pending) {
                logRequest(frame);
                s.writeOwned(ByteBuffer.wrap(frame));
            }
            if (finish) {
                if (blog != null) {
                    blog.clientHalfClose();
                }
                s.finish();
            }
        }

        private void logRequest(byte[] framed) {
            if (blog != null) {
                try {
                    blog.clientMessage(GrpcFraming.payloadOf(framed));
                } catch (java.io.IOException e) {
                    // a frame we made ourselves: cannot happen
                }
            }
        }

        // ---- ClientCall ----

        @Override
        public CompletableFuture<Void> send(Req message) {
            byte[] framed;
            try {
                framed = frameRequest(method, options, message, gzipOut);
            } catch (Exception e) {
                finish(requestFailure(e), null);
                abort();
                return CompletableFuture.failedFuture(e);
            }
            return sendFramed(framed);
        }

        CompletableFuture<Void> sendFramed(byte[] framed) {
            BodySink s;
            synchronized (lock) {
                if (closed || halfClosed) {
                    return CompletableFuture.failedFuture(new IllegalStateException(closed ? "the call is over" : "the call is half-closed"));
                }
                s = sink;
                if (s == null) {
                    early.add(framed);
                    return CompletableFuture.completedFuture(null);
                }
            }
            if (counted) {
                backend.channelz.socket.messageSent();
            }
            logRequest(framed);
            return s.writeOwned(ByteBuffer.wrap(framed));
        }

        @Override
        public void halfClose() {
            BodySink s;
            synchronized (lock) {
                if (halfClosed || closed) {
                    return;
                }
                halfClosed = true;
                s = sink;
                if (s == null) {
                    halfClosePending = true;
                    return;
                }
            }
            if (blog != null) {
                blog.clientHalfClose();
            }
            s.finish();
        }

        @Override
        public void cancel(String message) {
            locallyCancelled = true;
            finish(Status.of(Status.Code.CANCELLED).withDescription(message), null);
            abort();
        }

        @Override
        public boolean isReady() {
            BodySink s = sink;
            return s == null || s.isWritable();
        }

        @Override
        public CompletableFuture<Void> whenReady() {
            BodySink s = sink;
            return s == null ? CompletableFuture.completedFuture(null) : s.whenWritable();
        }

        @Override
        public void pause() {
            BodyControl c = control;
            if (c != null) {
                c.pause();
            }
        }

        @Override
        public void resume() {
            BodyControl c = control;
            if (c != null) {
                c.resume();
            }
        }

        // ---- ending ----

        private void deadlineExceeded() {
            locallyCancelled = true;
            finish(Status.of(Status.Code.DEADLINE_EXCEEDED).withDescription("deadline exceeded after " + deadlineSpan.toMillis() + " ms"), null);
            abort();
        }

        /** Resets the stream (the server is told the client is gone). */
        private void abort() {
            StreamHandle h = handle;
            if (h != null) {
                h.cancel();
            }
        }

        /** Delivers the final status, once. */
        private void finish(Status status, Metadata trailers) {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                closed = true;
                early.clear();
                if (deadlineTimer != null) {
                    deadlineTimer.cancel();
                }
                if (blog != null) {
                    if (locallyCancelled) {
                        blog.cancel();
                    } else {
                        blog.serverTrailer(status, trailers, null);
                    }
                }
                if (counted) {
                    backend.channelz.calls.end(status.isOk());
                    backend.channelz.socket.streams.end(status.isOk());
                }
                try {
                    listener.onClose(status, trailers != null ? trailers : new Metadata());
                } catch (Throwable ignored) {
                    // a failing listener has nowhere to report
                }
            }
        }

        // ---- the response ----

        private final class Receiver implements BodyListener {
            @Override
            public void onHead(ResponseHead head, BodyControl c) {
                control = c;
                sawHeaders = true;
                if (backend != null) {
                    backend.succeeded();
                }
                Headers h = head.headers();
                String grpcStatus = h.get("grpc-status");
                if (grpcStatus != null) { // "Trailers-Only": the status came with the headers, there will be no body
                    headStatus = statusFrom(h);
                    headTrailers = Metadata.fromHeaders(h);
                    if (blog != null) {
                        blog.serverHeader(new Metadata(), null);
                    }
                    return;
                }
                String ct = h.get("content-type");
                if (head.status() != 200) {
                    protocolStatus = statusFromHttp(head.status());
                    return;
                }
                if (ct == null || !ct.toLowerCase(Locale.ROOT).startsWith("application/grpc")) {
                    protocolStatus = Status.of(Status.Code.UNKNOWN).withDescription("unexpected content-type: " + ct);
                    return;
                }
                String enc = h.get("grpc-encoding");
                boolean gzipIn = false;
                if (enc != null && !enc.equalsIgnoreCase("identity")) {
                    if (enc.equalsIgnoreCase("gzip")) {
                        gzipIn = true;
                    } else {
                        protocolStatus = Status.of(Status.Code.INTERNAL).withDescription("can't decode grpc-encoding: " + enc);
                        return;
                    }
                }
                decoder = new GrpcFraming.Decoder(options.maxResponseBytes() != null ? options.maxResponseBytes() : maxInboundMessageBytes, gzipIn);
                Metadata headers = Metadata.fromHeaders(h);
                if (blog != null) {
                    blog.serverHeader(headers, null);
                }
                synchronized (lock) {
                    if (!closed) {
                        try {
                            listener.onHeaders(headers);
                        } catch (Throwable ignored) {
                            // not the transport's problem
                        }
                    }
                }
            }

            @Override
            public void onData(ByteBuffer data, BodyControl c) {
                if (decoder == null || protocolStatus != null) {
                    return;
                }
                try {
                    decoder.feed(data, payload -> {
                        Resp message;
                        if (counted) {
                            backend.channelz.socket.messageReceived();
                        }
                        if (blog != null) {
                            blog.serverMessage(payload);
                        }
                        try {
                            message = method.responseMarshaller().decode(payload);
                        } catch (Exception e) {
                            throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("failed to parse the response message").withCause(e));
                        }
                        synchronized (lock) {
                            if (!closed) {
                                try {
                                    listener.onMessage(message);
                                } catch (Throwable t) {
                                    throw new StatusException(Status.of(Status.Code.CANCELLED).withDescription("the listener failed").withCause(t));
                                }
                            }
                        }
                    });
                } catch (StatusException e) {
                    finish(e.status(), null);
                    abort();
                }
            }

            @Override
            public void onTrailers(Headers trailers) {
                if (trailers.get("grpc-status") != null) {
                    trailerStatus = statusFrom(trailers);
                    trailerMetadata = Metadata.fromHeaders(trailers);
                }
            }

            @Override
            public void onComplete() {
                if (trailerStatus != null) {
                    complete(trailerStatus, trailerMetadata);
                } else if (headStatus != null) {
                    complete(headStatus, headTrailers);
                } else if (protocolStatus != null) {
                    finish(protocolStatus, null);
                } else {
                    finish(Status.of(Status.Code.INTERNAL).withDescription("the server ended the stream without a grpc-status"), null);
                }
            }

            private void complete(Status status, Metadata trailers) {
                if (status.isOk() && decoder != null) {
                    try {
                        decoder.finish();
                    } catch (StatusException e) {
                        finish(e.status(), trailers);
                        return;
                    }
                }
                finish(status, trailers);
            }

            @Override
            public void onError(Throwable error) {
                if (error instanceof CancellationException) {
                    finish(Status.of(Status.Code.CANCELLED).withDescription("cancelled"), null);
                } else {
                    if (backend != null && !sawHeaders) {
                        backend.failed();   // never got an answer from it: steer the next calls elsewhere for a while
                    }
                    finish(Status.of(Status.Code.UNAVAILABLE).withDescription(String.valueOf(error.getMessage())).withCause(error), null);
                }
            }
        }
    }

    // ---- hedging ----

    /** One logical call sent as several parallel attempts; the first to answer wins and the rest are cancelled. */
    private final class HedgingCall<Req, Resp> implements ClientCall<Req, Resp> {
        private final MethodDescriptor<Req, Resp> method;
        private final CallOptions options;
        private final ClientCall.Listener<Resp> listener;
        private final boolean gzipOut;
        private final HedgingPolicy hedging;
        private final long deadlineAt;

        private final Object lock = new Object();
        private final List<byte[]> buffered = new ArrayList<>();
        private int bufferedBytes;
        private boolean overflow;                 // too much was sent to replay it: no more attempts can be started
        private final List<Attempt> attempts = new ArrayList<>();
        private Attempt winner;
        private int launched;
        private boolean halfClosed;
        private boolean ended;
        private Scheduled timer;
        private Status lastFailure;
        private Metadata lastTrailers;

        private final class Attempt implements ClientCall.Listener<Resp> {
            Call<Req, Resp> call;
            boolean done;
            Metadata pendingHeaders;

            @Override
            public void onHeaders(Metadata headers) {
                synchronized (lock) {
                    pendingHeaders = headers;
                }
            }

            @Override
            public void onMessage(Resp message) {
                Metadata h;
                List<Call<Req, Resp>> losers = List.of();
                synchronized (lock) {
                    if (ended || (winner != null && winner != this)) {
                        return;
                    }
                    if (winner == null) {
                        winner = this;
                        losers = takeOthers(this);
                    }
                    h = pendingHeaders;
                    pendingHeaders = null;
                }
                cancelAll(losers);
                if (h != null) {
                    listener.onHeaders(h);
                }
                listener.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                boolean launchNext = false;
                boolean finish = false;
                Metadata h = null;
                List<Call<Req, Resp>> losers = List.of();
                synchronized (lock) {
                    done = true;
                    if (ended || (winner != null && winner != this)) {
                        return;
                    }
                    if (throttle != null) {
                        if (status.isOk()) {
                            throttle.success();
                        } else if (hedging.nonFatalCodes().contains(status.code())) {
                            throttle.failure();
                        }
                    }
                    if (winner == this || status.isOk()) {
                        winner = this;
                        losers = takeOthers(this);
                        h = pendingHeaders;
                        pendingHeaders = null;
                        finish = true;
                    } else if (hedging.nonFatalCodes().contains(status.code())) {
                        lastFailure = status;
                        lastTrailers = trailers;
                        boolean canLaunch = launched < hedging.maxAttempts() && !overflow && (throttle == null || throttle.allowsRetry());
                        if (canLaunch) {
                            launchNext = true;
                        } else if (allDone()) {
                            finish = true;
                        }
                    } else {
                        winner = this;       // a fatal status ends the call, whatever the other attempts are doing
                        losers = takeOthers(this);
                        finish = true;
                    }
                }
                cancelAll(losers);
                if (h != null) {
                    listener.onHeaders(h);
                }
                if (launchNext) {
                    launch();
                } else if (finish) {
                    end(status, trailers);
                }
            }
        }

        HedgingCall(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener, HedgingPolicy hedging) {
            this.hedging = hedging;
            this.method = method;
            this.options = options;
            this.listener = listener;
            this.gzipOut = "gzip".equals(options.compression());
            this.deadlineAt = options.deadline() == null ? 0 : Math.max(1, System.nanoTime() + options.deadline().toNanos());
        }

        void begin() {
            launch();
        }

        private boolean allDone() {
            for (Attempt a : attempts) {
                if (!a.done) {
                    return false;
                }
            }
            return true;
        }

        /** Marks the other running attempts done and returns their calls; the caller cancels them AFTER releasing {@code lock}
         *  (a call's own lock is held while it reports to us, so cancelling under ours could deadlock). */
        private List<Call<Req, Resp>> takeOthers(Attempt keep) {
            List<Call<Req, Resp>> out = new ArrayList<>();
            for (Attempt a : attempts) {
                if (a != keep && !a.done && a.call != null) {
                    a.done = true;
                    out.add(a.call);
                }
            }
            return out;
        }

        private static <Q, R> void cancelAll(List<Call<Q, R>> calls) {
            for (Call<Q, R> c : calls) {
                c.cancel("another attempt answered first");
            }
        }

        private void launch() {
            CallOptions o = options;
            if (deadlineAt != 0) {
                long left = deadlineAt - System.nanoTime();
                if (left <= 0) {
                    end(Status.of(Status.Code.DEADLINE_EXCEEDED).withDescription("deadline exceeded"), null);
                    return;
                }
                o = o.withDeadline(Duration.ofNanos(left));
            }
            Attempt a = new Attempt();
            List<byte[]> replay;
            boolean finish;
            synchronized (lock) {
                if (ended || winner != null) {
                    return;
                }
                launched++;
                attempts.add(a);
                replay = new ArrayList<>(buffered);
                finish = halfClosed;
                if (timer != null) {
                    timer.cancel();
                    timer = null;
                }
                if (launched < hedging.maxAttempts()) {
                    timer = http.eventLoop().schedule(this::hedgeDelayPassed, hedging.hedgingDelay().toNanos(), TimeUnit.NANOSECONDS);
                }
            }
            a.call = launchAttempt(method, o, a);
            for (byte[] frame : replay) {
                a.call.sendFramed(frame);
            }
            if (finish) {
                a.call.halfClose();
            }
        }

        private void hedgeDelayPassed() {
            synchronized (lock) {
                if (ended || winner != null || overflow || (throttle != null && !throttle.allowsRetry())) {
                    return;
                }
            }
            launch();
        }

        private void end(Status status, Metadata trailers) {
            List<Call<Req, Resp>> losers;
            synchronized (lock) {
                if (ended) {
                    return;
                }
                ended = true;
                buffered.clear();
                if (timer != null) {
                    timer.cancel();
                }
                losers = takeOthers(winner);
            }
            cancelAll(losers);
            try {
                listener.onClose(status, trailers != null ? trailers : new Metadata());
            } catch (Throwable ignored) {
                // nowhere to report
            }
        }

        @Override
        public CompletableFuture<Void> send(Req message) {
            byte[] framed;
            try {
                framed = frameRequest(method, options, message, gzipOut);
            } catch (Exception e) {
                end(requestFailure(e), null);
                return CompletableFuture.failedFuture(e);
            }
            List<Call<Req, Resp>> targets = new ArrayList<>();
            synchronized (lock) {
                if (ended || halfClosed) {
                    return CompletableFuture.failedFuture(new IllegalStateException(ended ? "the call is over" : "the call is half-closed"));
                }
                if (winner == null) {
                    bufferedBytes += framed.length;
                    if (bufferedBytes > retryBufferBytes) {
                        overflow = true;
                        buffered.clear();
                    } else if (!overflow) {
                        buffered.add(framed);
                    }
                }
                for (Attempt a : attempts) {
                    if (!a.done && a.call != null && (winner == null || winner == a)) {
                        targets.add(a.call);
                    }
                }
            }
            CompletableFuture<Void> last = CompletableFuture.completedFuture(null);
            for (Call<Req, Resp> c : targets) {
                last = c.sendFramed(framed);
            }
            return last;
        }

        @Override
        public void halfClose() {
            List<Call<Req, Resp>> targets = new ArrayList<>();
            synchronized (lock) {
                if (halfClosed || ended) {
                    return;
                }
                halfClosed = true;
                for (Attempt a : attempts) {
                    if (!a.done && a.call != null) {
                        targets.add(a.call);
                    }
                }
            }
            targets.forEach(Call::halfClose);
        }

        @Override
        public void cancel(String message) {
            end(Status.of(Status.Code.CANCELLED).withDescription(message), null);
        }

        private Call<Req, Resp> lead() {
            synchronized (lock) {
                if (winner != null) {
                    return winner.call;
                }
                return attempts.isEmpty() ? null : attempts.get(attempts.size() - 1).call;
            }
        }

        @Override
        public boolean isReady() {
            Call<Req, Resp> c = lead();
            return c == null || c.isReady();
        }

        @Override
        public CompletableFuture<Void> whenReady() {
            Call<Req, Resp> c = lead();
            return c == null ? CompletableFuture.completedFuture(null) : c.whenReady();
        }

        @Override
        public void pause() {
            Call<Req, Resp> c = lead();
            if (c != null) {
                c.pause();
            }
        }

        @Override
        public void resume() {
            Call<Req, Resp> c = lead();
            if (c != null) {
                c.resume();
            }
        }
    }

    // ---- retries ----

    /**
     * One logical call made of up to {@code maxAttempts} attempts. Request messages are kept (as framed bytes, within the retry buffer) so a
     * later attempt can replay them; once the response starts (headers or a message), or the buffer overflows, the call is "committed" to the
     * attempt it is on and is never retried.
     */
    private final class RetryingCall<Req, Resp> implements ClientCall<Req, Resp> {
        private final MethodDescriptor<Req, Resp> method;
        private final CallOptions options;
        private final ClientCall.Listener<Resp> listener;
        private final boolean gzipOut;
        private final RetryPolicy retry;     // this method's policy (the channel's, or one from the service config)
        private final long deadlineAt;       // System.nanoTime() value, 0 = none

        private final Object lock = new Object();
        private final List<byte[]> buffered = new ArrayList<>();
        private int bufferedBytes;
        private Call<Req, Resp> attempt;
        private int attempts;
        private boolean committed;
        private boolean halfClosed;
        private boolean ended;
        private Scheduled backoffTimer;
        private long nextBackoffNanos;
        private final boolean policyless;    // no retry policy: only transparent retries
        private int transparentRetries;   // replays of a call the server provably never processed; they do not count against the policy

        RetryingCall(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener, RetryPolicy retry) {
            this.policyless = retry == null;
            this.retry = retry != null ? retry : RetryPolicy.defaults();
            this.method = method;
            this.options = options;
            this.listener = listener;
            this.gzipOut = "gzip".equals(options.compression());
            this.deadlineAt = options.deadline() == null ? 0 : Math.max(1, System.nanoTime() + options.deadline().toNanos());
            this.nextBackoffNanos = this.retry.initialBackoff().toNanos();
        }

        void begin() {
            startAttempt();
        }

        private void startAttempt() {
            CallOptions o = options;
            if (deadlineAt != 0) {
                long left = deadlineAt - System.nanoTime();
                if (left <= 0) {
                    end(Status.of(Status.Code.DEADLINE_EXCEEDED).withDescription("deadline exceeded before attempt " + (attempts + 1)), null);
                    return;
                }
                o = o.withDeadline(Duration.ofNanos(left));
            }
            List<byte[]> replay;
            boolean finish;
            Call<Req, Resp> a;
            synchronized (lock) {
                if (ended) {
                    return;
                }
                attempts++;
                int number = attempts;
                a = launchAttempt(method, o, new AttemptListener(number));
                attempt = a;
                replay = new ArrayList<>(buffered);
                finish = halfClosed;
            }
            for (byte[] frame : replay) {
                a.sendFramed(frame);
            }
            if (finish) {
                a.halfClose();
            }
        }

        private final class AttemptListener implements ClientCall.Listener<Resp> {
            private final int number;

            AttemptListener(int number) {
                this.number = number;
            }

            private boolean current() {
                synchronized (lock) {
                    return !ended && number == attempts;
                }
            }

            // Response headers alone do not commit the call: a server may send them before it has done anything (Hydrangea's does), and a status
            // that follows is then still a failure of the attempt. They are held back until a message or the end, so a listener never sees headers
            // from an attempt that is then retried.
            private Metadata pendingHeaders;

            @Override
            public void onHeaders(Metadata headers) {
                synchronized (lock) {
                    if (ended || number != attempts) {
                        return;
                    }
                    pendingHeaders = headers;
                }
            }

            private void flushHeaders() {
                Metadata h;
                synchronized (lock) {
                    h = pendingHeaders;
                    pendingHeaders = null;
                }
                if (h != null) {
                    listener.onHeaders(h);
                }
            }

            @Override
            public void onMessage(Resp message) {
                if (!current()) {
                    return;
                }
                commit();
                flushHeaders();
                listener.onMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                boolean retryNow;
                long wait = 0;
                synchronized (lock) {
                    if (ended || number != attempts) {
                        return;
                    }
                    if (throttle != null && !policyless) {
                        if (status.isOk()) {
                            throttle.success();
                        } else if (retry.retryableCodes().contains(status.code())) {
                            throttle.failure();
                        }
                    }
                    boolean transparent = !committed && !status.isOk() && status.cause() instanceof UnprocessedException
                            && transparentRetries < MAX_TRANSPARENT_RETRIES;
                    retryNow = transparent || (!policyless && !committed && !status.isOk() && retry.retryableCodes().contains(status.code())
                            && attempts - transparentRetries < retry.maxAttempts() && (throttle == null || throttle.allowsRetry()));
                    if (transparent) {
                        transparentRetries++;
                    } else if (retryNow) {
                        long cap = Math.min(nextBackoffNanos, retry.maxBackoff().toNanos());
                        wait = (long) (java.util.concurrent.ThreadLocalRandom.current().nextDouble() * cap);
                        nextBackoffNanos = (long) Math.min(cap * retry.multiplier(), retry.maxBackoff().toNanos());
                        if (deadlineAt != 0 && System.nanoTime() + wait >= deadlineAt) {
                            retryNow = false;   // the wait would eat the rest of the deadline: fail now with the real cause
                        }
                    }
                    if (retryNow) {
                        attempt = null;
                    }
                }
                if (!retryNow) {
                    flushHeaders();
                    end(status, trailers);
                    return;
                }
                synchronized (lock) {
                    pendingHeaders = null;
                }
                synchronized (lock) {
                    backoffTimer = http.eventLoop().schedule(RetryingCall.this::startAttempt, wait, TimeUnit.NANOSECONDS);
                }
            }
        }

        private void commit() {
            synchronized (lock) {
                committed = true;
                buffered.clear();
                bufferedBytes = 0;
            }
        }

        private void end(Status status, Metadata trailers) {
            synchronized (lock) {
                if (ended) {
                    return;
                }
                ended = true;
                buffered.clear();
                if (backoffTimer != null) {
                    backoffTimer.cancel();
                }
            }
            try {
                listener.onClose(status, trailers != null ? trailers : new Metadata());
            } catch (Throwable ignored) {
                // nowhere to report
            }
        }

        @Override
        public CompletableFuture<Void> send(Req message) {
            byte[] framed;
            try {
                framed = frameRequest(method, options, message, gzipOut);
            } catch (Exception e) {
                end(requestFailure(e), null);
                cancelAttempt("serialization failed");
                return CompletableFuture.failedFuture(e);
            }
            Call<Req, Resp> a;
            synchronized (lock) {
                if (ended || halfClosed) {
                    return CompletableFuture.failedFuture(new IllegalStateException(ended ? "the call is over" : "the call is half-closed"));
                }
                if (!committed) {
                    bufferedBytes += framed.length;
                    if (bufferedBytes > retryBufferBytes) {
                        committed = true;   // too much to replay: from here on this attempt is the call
                        buffered.clear();
                        bufferedBytes = 0;
                    } else {
                        buffered.add(framed);
                    }
                }
                a = attempt;
            }
            return a == null ? CompletableFuture.completedFuture(null) : a.sendFramed(framed);   // null: between attempts, replayed later
        }

        @Override
        public void halfClose() {
            Call<Req, Resp> a;
            synchronized (lock) {
                if (halfClosed || ended) {
                    return;
                }
                halfClosed = true;
                a = attempt;
            }
            if (a != null) {
                a.halfClose();
            }
        }

        @Override
        public void cancel(String message) {
            cancelAttempt(message);
            end(Status.of(Status.Code.CANCELLED).withDescription(message), null);
        }

        private void cancelAttempt(String message) {
            Call<Req, Resp> a;
            synchronized (lock) {
                a = attempt;
            }
            if (a != null) {
                a.cancel(message);
            }
        }

        @Override
        public boolean isReady() {
            Call<Req, Resp> a;
            synchronized (lock) {
                a = attempt;
            }
            return a == null || a.isReady();
        }

        @Override
        public CompletableFuture<Void> whenReady() {
            Call<Req, Resp> a;
            synchronized (lock) {
                a = attempt;
            }
            return a == null ? CompletableFuture.completedFuture(null) : a.whenReady();
        }

        @Override
        public void pause() {
            Call<Req, Resp> a;
            synchronized (lock) {
                a = attempt;
            }
            if (a != null) {
                a.pause();
            }
        }

        @Override
        public void resume() {
            Call<Req, Resp> a;
            synchronized (lock) {
                a = attempt;
            }
            if (a != null) {
                a.resume();
            }
        }
    }

    /** Serialises and frames one request message, enforcing the call's size limit. */
    private static <Req> byte[] frameRequest(MethodDescriptor<Req, ?> method, CallOptions options, Req message, boolean gzip) throws Exception {
        byte[] payload = method.requestMarshaller().encode(message);
        Integer max = options.maxRequestBytes();
        if (max != null && payload.length > max) {
            throw new StatusException(Status.of(Status.Code.RESOURCE_EXHAUSTED)
                    .withDescription("request message of " + payload.length + " bytes exceeds the limit of " + max));
        }
        return GrpcFraming.frame(payload, gzip);
    }

    private static Status requestFailure(Exception e) {
        return e instanceof StatusException se ? se.status()
                : Status.of(Status.Code.INTERNAL).withDescription("failed to serialize the request message").withCause(e);
    }

    private static Status statusFrom(Headers h) {
        int code;
        try {
            code = Integer.parseInt(h.get("grpc-status").trim());
        } catch (NumberFormatException e) {
            return Status.of(Status.Code.UNKNOWN).withDescription("invalid grpc-status: " + h.get("grpc-status"));
        }
        String message = h.get("grpc-message");
        return Status.of(Status.Code.fromValue(code)).withDescription(message == null ? null : GrpcFraming.decodeMessage(message));
    }

    /** The status for an HTTP error from something that did not speak gRPC (the table in the protocol specification). */
    static Status statusFromHttp(int http) {
        Status.Code code = switch (http) {
            case 400 -> Status.Code.INTERNAL;
            case 401 -> Status.Code.UNAUTHENTICATED;
            case 403 -> Status.Code.PERMISSION_DENIED;
            case 404 -> Status.Code.UNIMPLEMENTED;
            case 429, 502, 503, 504 -> Status.Code.UNAVAILABLE;
            default -> Status.Code.UNKNOWN;
        };
        return Status.of(code).withDescription("HTTP status " + http);
    }
}
