package io.hydrangea.otlp;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.Marshaller;
import io.hydrangea.grpc.Metadata;
import io.hydrangea.grpc.MethodDescriptor;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.log.Logger;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Where an encoded {@code ExportTraceServiceRequest} goes. {@link #grpc} and {@link #http} are the two OTLP transports a collector
 * listens on (4317 and 4318 by default); {@link #logging} and {@link #none} are for looking and for switching off. The sink calls
 * {@link #export} from its own thread, one batch at a time, and waits for the future.
 */
public interface Exporter extends AutoCloseable {

    /** Sends one batch; completes when the server accepted it, fails otherwise (the batch is then dropped, not retried). */
    CompletableFuture<Void> export(byte[] request);

    @Override
    default void close() {
    }

    /** OTLP/gRPC: {@code TraceService/Export} on {@code endpoint} ({@code http://host:4317} or {@code https://...}); {@code headers} go in the metadata. */
    static Exporter grpc(String endpoint, Map<String, String> headers, Duration timeout, EventLoopGroup group) {
        return new Grpc(endpoint, headers, timeout, group);
    }

    /** OTLP/HTTP with a protobuf body: {@code POST} to {@code url} (the full {@code .../v1/traces} URL). */
    static Exporter http(String url, Map<String, String> headers, Duration timeout, EventLoopGroup group) {
        return new Http(url, headers, timeout, group);
    }

    /** Writes each batch as one log line (the size and the first span), for trying things out without a collector. */
    static Exporter logging() {
        Logger log = Logger.get(Exporter.class);
        return request -> {
            log.info("otlp batch of {} bytes", request.length);
            return CompletableFuture.completedFuture(null);
        };
    }

    /** Drops everything. */
    static Exporter none() {
        return request -> CompletableFuture.completedFuture(null);
    }

    /** The OTLP gRPC service: the request and the response are opaque bytes here. */
    MethodDescriptor<byte[], byte[]> EXPORT = MethodDescriptor.of("opentelemetry.proto.collector.trace.v1.TraceService", "Export",
            MethodDescriptor.MethodType.UNARY, Marshaller.bytes(), Marshaller.bytes());

    /**
     * Reads {@code ExportTraceServiceResponse { ExportTracePartialSuccess partial_success = 1; }} with
     * {@code ExportTracePartialSuccess { int64 rejected_spans = 1; string error_message = 2; }}: how many spans the server refused, or
     * {@code null} when it took them all.
     */
    static String partialSuccess(byte[] response) {
        long rejected = 0;
        String message = "";
        ProtoReader r = new ProtoReader(response);
        while (r.next()) {
            if (r.field() == 1) {
                ProtoReader p = r.message();
                while (p.next()) {
                    switch (p.field()) {
                        case 1 -> rejected = p.varint();
                        case 2 -> message = p.string();
                        default -> p.skip();
                    }
                }
            } else {
                r.skip();
            }
        }
        return rejected == 0 && message.isEmpty() ? null : rejected + " span(s) rejected: " + message;
    }

    /** {@link #partialSuccess} for a body that may not be protobuf at all (a proxy's HTML page on a 2xx): {@code null} then. */
    private static String partialSuccessOrNull(byte[] body) {
        try {
            return partialSuccess(body);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    final class Grpc implements Exporter {
        private static final Logger log = Logger.get(Grpc.class);
        private final GrpcChannel channel;
        private final CallOptions options;

        Grpc(String endpoint, Map<String, String> headers, Duration timeout, EventLoopGroup group) {
            GrpcChannel.Builder b = GrpcChannel.forTarget(endpoint).userAgent("hydrangea-otlp/1.0").maxConnections(1);
            if (group != null) {
                b.eventLoopGroup(group);
            }
            this.channel = b.build();
            Metadata metadata = new Metadata();
            headers.forEach(metadata::put);
            this.options = CallOptions.DEFAULT.withDeadline(timeout).withMetadata(metadata);
        }

        @Override
        public CompletableFuture<Void> export(byte[] request) {
            return channel.unary(EXPORT, request, options).thenApply(response -> {
                String partial = partialSuccess(response);
                if (partial != null) {
                    log.warn("otlp collector accepted the batch partially: {}", partial);
                }
                return null;
            });
        }

        @Override
        public void close() {
            channel.close();
        }
    }

    final class Http implements Exporter {
        private static final Logger log = Logger.get(Http.class);
        private final HttpClient client;
        private final String url;
        private final Map<String, String> headers;
        private final Duration timeout;

        Http(String url, Map<String, String> headers, Duration timeout, EventLoopGroup group) {
            this.client = group == null ? new HttpClient() : new HttpClient(group, HttpClientConfig.defaults());
            this.url = url;
            this.headers = headers;
            this.timeout = timeout;
        }

        @Override
        public CompletableFuture<Void> export(byte[] request) {
            ClientRequest req = ClientRequest.post(url).header("Content-Type", "application/x-protobuf").body(request);
            for (Map.Entry<String, String> h : headers.entrySet()) {
                req = req.header(h.getKey(), h.getValue());
            }
            return client.send(req).orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS).thenApply(response -> {
                if (response.status() / 100 != 2) {
                    throw new IllegalStateException("HTTP " + response.status() + " from " + url);
                }
                String partial = response.body().length == 0 ? null : partialSuccessOrNull(response.body());
                if (partial != null) {
                    log.warn("otlp collector accepted the batch partially: {}", partial);
                }
                return null;
            });
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
