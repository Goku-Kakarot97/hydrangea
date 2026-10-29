package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The standard health checking service ({@code grpc.health.v1.Health}), which Kubernetes {@code grpc} probes, load balancers and
 * {@code grpc_health_probe} speak. {@code Check} answers once; {@code Watch} streams the status now and on every change.
 * The service name "" stands for the server as a whole.
 * <pre>{@code
 * HealthService health = new HealthService();
 * GrpcServer grpc = GrpcServer.builder().addService(health).build();
 * health.setStatus("my.package.MyService", HealthService.ServingStatus.SERVING);
 * }</pre>
 */
public final class HealthService implements GrpcServer.BindableService {

    public enum ServingStatus {
        UNKNOWN(0), SERVING(1), NOT_SERVING(2), SERVICE_UNKNOWN(3);

        final int value;

        ServingStatus(int value) {
            this.value = value;
        }

        static ServingStatus of(int v) {
            for (ServingStatus s : values()) {
                if (s.value == v) {
                    return s;
                }
            }
            return UNKNOWN;
        }
    }

    public static final Marshaller<String> REQUEST = Marshaller.of(
            service -> service.isEmpty() ? new byte[0] : new ProtoWriter().string(1, service).toByteArray(),
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                String service = "";
                while (r.next()) {
                    if (r.field() == 1) service = r.string(); else r.skip();
                }
                return service;
            });

    public static final Marshaller<ServingStatus> RESPONSE = Marshaller.of(
            status -> status.value == 0 ? new byte[0] : new ProtoWriter().varint(1, status.value).toByteArray(),
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                int status = 0;
                while (r.next()) {
                    if (r.field() == 1) status = (int) r.varint(); else r.skip();
                }
                return ServingStatus.of(status);
            });

    public static final MethodDescriptor<String, ServingStatus> CHECK =
            MethodDescriptor.of("grpc.health.v1.Health", "Check", MethodDescriptor.MethodType.UNARY, REQUEST, RESPONSE);
    public static final MethodDescriptor<String, ServingStatus> WATCH =
            MethodDescriptor.of("grpc.health.v1.Health", "Watch", MethodDescriptor.MethodType.SERVER_STREAMING, REQUEST, RESPONSE);

    private final Map<String, ServingStatus> statuses = new ConcurrentHashMap<>();
    private final Map<String, List<StreamObserver<ServingStatus>>> watchers = new ConcurrentHashMap<>();

    public HealthService() {
        statuses.put("", ServingStatus.SERVING);
    }

    /** Sets a service's status and tells everyone watching it. */
    public void setStatus(String service, ServingStatus status) {
        statuses.put(service, status);
        List<StreamObserver<ServingStatus>> ws = watchers.get(service);
        if (ws != null) {
            synchronized (ws) {
                for (StreamObserver<ServingStatus> w : new ArrayList<>(ws)) {
                    w.onNext(status);
                }
            }
        }
    }

    /** Marks everything NOT_SERVING (and keeps it so): call when shutting down so load balancers stop sending traffic first. */
    public void shutdown() {
        for (String service : new ArrayList<>(statuses.keySet())) {
            setStatus(service, ServingStatus.NOT_SERVING);
        }
    }

    public ServingStatus status(String service) {
        return statuses.getOrDefault(service, ServingStatus.SERVICE_UNKNOWN);
    }

    @Override
    public void bindTo(GrpcServer.Builder builder) {
        builder.unary(CHECK, (service, ctx) -> {
            ServingStatus s = statuses.get(service);
            if (s == null) {
                return CompletableFuture.failedFuture(Status.of(Status.Code.NOT_FOUND).withDescription("unknown service: " + service).asException());
            }
            return CompletableFuture.completedFuture(s);
        });
        builder.serverStreaming(WATCH, (service, ctx, responses) -> {
            List<StreamObserver<ServingStatus>> ws = watchers.computeIfAbsent(service, k -> new ArrayList<>());
            synchronized (ws) {
                ws.add(responses);
                responses.onNext(statuses.getOrDefault(service, ServingStatus.SERVICE_UNKNOWN)); // the current state first, as the protocol says
            }
            ctx.onCancel(() -> {
                synchronized (ws) {
                    ws.remove(responses);
                }
            });
        });
    }
}
