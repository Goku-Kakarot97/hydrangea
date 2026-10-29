package io.hydrangea.grpc;

import io.hydrangea.Latency;
import io.hydrangea.Metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;

/**
 * Metrics for gRPC calls, as interceptors you add when you want them (so a server or channel that does not pay nothing):
 * <pre>{@code
 * GrpcServer.builder().intercept(GrpcMetrics.server(group.metrics()))
 * GrpcChannel.forTarget(url).intercept(GrpcMetrics.client(group.metrics()))
 * }</pre>
 * Published as {@code grpc.server.*} / {@code grpc.client.*}, each with the tag {@code method} (the method's full name: there are as many as you
 * defined, no more):
 * <ul>
 * <li>{@code calls} with the tag {@code code} (the status code the call ended with, OK to UNAUTHENTICATED)</li>
 * <li>{@code active}: calls started and not ended</li>
 * <li>{@code messages} with the tag {@code direction}=sent|received</li>
 * <li>{@code duration}: from the call starting to its status (count, sum and quantiles)</li>
 * </ul>
 * A client call is one logical call: retries and hedging happen below an interceptor and are not counted separately.
 */
public final class GrpcMetrics {

    private GrpcMetrics() {
    }

    private static final Status.Code[] CODES = Status.Code.values();

    private static final class PerMethod {
        final Metrics.Count active;
        final Metrics.Count sent;
        final Metrics.Count received;
        final Latency duration;
        final Metrics.Count[] calls = new Metrics.Count[CODES.length];
        private final Metrics metrics;
        private final String prefix;
        private final String method;

        PerMethod(Metrics m, String prefix, String method) {
            this.metrics = m;
            this.prefix = prefix;
            this.method = method;
            active = m.gauge(prefix + ".active", null, "method", method);
            sent = m.counter(prefix + ".messages", null, "method", method, "direction", "sent");
            received = m.counter(prefix + ".messages", null, "method", method, "direction", "received");
            duration = m.latency(prefix + ".duration", "method", method);
        }

        void ended(Status status, long start) {
            active.decrement();
            int i = status.code().ordinal();
            Metrics.Count c = calls[i];
            if (c == null) {   // the same object whoever asks, so two threads racing here do no harm
                c = metrics.counter(prefix + ".calls", null, "method", method, "code", status.code().name());
                calls[i] = c;
            }
            c.increment();
            duration.end(start);
        }
    }

    private static PerMethod of(ConcurrentHashMap<String, PerMethod> map, Metrics m, String prefix, MethodDescriptor<?, ?> method) {
        PerMethod p = map.get(method.fullName());
        if (p == null) {
            p = map.computeIfAbsent(method.fullName(), n -> new PerMethod(m, prefix, n));
        }
        return p;
    }

    public static ServerInterceptor server(Metrics metrics) {
        ConcurrentHashMap<String, PerMethod> methods = new ConcurrentHashMap<>();
        return (method, ctx) -> {
            PerMethod p = of(methods, metrics, "grpc.server", method);
            p.active.increment();
            long start = p.duration.begin();
            return new ServerInterceptor.Listener() {
                @Override
                public void onRequest(Object message) {
                    p.received.increment();
                }

                @Override
                public void onResponse(Object message) {
                    p.sent.increment();
                }

                @Override
                public void onClose(Status status) {
                    p.ended(status, start);
                }
            };
        };
    }

    public static ClientInterceptor client(Metrics metrics) {
        ConcurrentHashMap<String, PerMethod> methods = new ConcurrentHashMap<>();
        return new ClientInterceptor() {
            @Override
            public <Req, Resp> ClientCall<Req, Resp> intercept(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener, Channel next) {
                PerMethod p = of(methods, metrics, "grpc.client", method);
                p.active.increment();
                long start = p.duration.begin();
                ClientCall<Req, Resp> call = next.newCall(method, options, new ClientCall.Listener<Resp>() {
                    @Override
                    public void onHeaders(Metadata headers) {
                        listener.onHeaders(headers);
                    }

                    @Override
                    public void onMessage(Resp message) {
                        p.received.increment();
                        listener.onMessage(message);
                    }

                    @Override
                    public void onClose(Status status, Metadata trailers) {
                        p.ended(status, start);
                        listener.onClose(status, trailers);
                    }
                });
                return new ClientCall<Req, Resp>() {
                    @Override
                    public CompletableFuture<Void> send(Req message) {
                        p.sent.increment();
                        return call.send(message);
                    }

                    @Override
                    public void halfClose() {
                        call.halfClose();
                    }

                    @Override
                    public void cancel(String message) {
                        call.cancel(message);
                    }

                    @Override
                    public boolean isReady() {
                        return call.isReady();
                    }

                    @Override
                    public CompletableFuture<Void> whenReady() {
                        return call.whenReady();
                    }

                    @Override
                    public void pause() {
                        call.pause();
                    }

                    @Override
                    public void resume() {
                        call.resume();
                    }
                };
            }
        };
    }
}
