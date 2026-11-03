package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.log.Log;
import io.hydrangea.log.LogFormat;
import io.hydrangea.log.Logger;
import io.hydrangea.otlp.OtlpSink;
import io.hydrangea.trace.Span;
import io.hydrangea.trace.Tracing;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The framework's spans sent to an OpenTelemetry collector over OTLP by {@code hydrangea-otlp} (no OpenTelemetry library on the
 * classpath), configured the way a container is: by the standard environment variables.
 * <pre>
 * OTEL_SERVICE_NAME=order-service                         # the name Jaeger shows (default: unknown_service)
 * OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317   # where spans go (default: http://localhost:4317)
 * OTEL_EXPORTER_OTLP_PROTOCOL=grpc                         # or http/protobuf (then the endpoint's port is 4318)
 * OTEL_TRACES_EXPORTER=otlp                                # or logging to only count the batches, none to send nothing
 * OTEL_TRACES_SAMPLER=parentbased_traceidratio             # with OTEL_TRACES_SAMPLER_ARG=0.1 to keep one new trace in ten (default: all)
 * </pre>
 * Two routes on one server: {@code GET /orders/{id}} calls {@code GET /inventory/{id}} on the same server through the HTTP client, so one
 * request gives a trace of four spans (server, client, server, and an internal step), and its log lines carry the trace id.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.TracingExample 8080        # serve (target/classes: every module merged, after mvn install)
 * curl -i localhost:8080/orders/42                                        # X-Trace-Id in the answer
 * OTEL_TRACES_EXPORTER=logging java -cp target/classes io.hydrangea.examples.TracingExample demo    # one request, no collector, exit
 * </pre>
 * Kubernetes: {@code k8s/tracing/} has a collector, a Jaeger and a Deployment of this class.
 */
public final class TracingExample {

    private static final Logger log = Logger.get(TracingExample.class);

    public static void main(String[] args) throws Exception {
        boolean demo = args.length > 0 && args[0].equals("demo");
        int port = args.length > 0 && !demo ? Integer.parseInt(args[0]) : 8080;

        // 1. the sink from OTEL_* variables: a queue, one thread that batches, the framework's gRPC client to the collector.
        //    Ending a span queues it; every 5 s (or 512 spans) a batch goes out; its shutdown hook sends the last ones when the JVM exits.
        OtlpSink.Builder otlp = OtlpSink.fromEnvironment();
        if (System.getenv("OTEL_SERVICE_NAME") == null) {
            otlp.serviceName("hydrangea-example");
        }
        OtlpSink sink = otlp.build();
        Tracing.bindTo(sink);
        Log.configure().format(LogFormat.json()).console().apply();          // JSON lines with trace_id and span_id, for a log collector

        try (EventLoopGroup group = new EventLoopGroup(2); HttpClient client = new HttpClient(group, HttpClientConfig.defaults())) {
            String self = "http://127.0.0.1:" + port;
            HttpRouter router = new HttpRouter()
                    .get("/health", r -> HttpResponse.ok("ok"))
                    .get("/inventory/{sku}", r -> {
                        log.info("checking stock of {}", r.pathVariable("sku"));
                        try (Span step = Tracing.start("warehouse lookup", Span.Kind.INTERNAL); Span.Scope ignored = step.makeCurrent()) {
                            step.attribute("sku", r.pathVariable("sku"));
                            return HttpResponse.ok("{\"sku\":\"" + r.pathVariable("sku") + "\",\"stock\":7}");
                        }
                    })
                    .getAsync("/orders/{id}", r -> {
                        log.info("placing order {}", r.pathInt("id"));
                        // the client call made here is a child of this request's span and carries traceparent to /inventory
                        CompletableFuture<HttpResponse> answer = client.send(ClientRequest.get(self + "/inventory/sku-" + r.pathInt("id")))
                                .thenApply(inv -> {
                                    try (Span.Scope ignored = r.traced()) {   // on another thread now: reopen the request's span and log context for this line
                                        log.info("inventory answered {}", inv.status());
                                    }
                                    return HttpResponse.ok("{\"order\":" + r.pathInt("id") + ",\"inventory\":" + inv.bodyAsString() + "}");
                                });
                        return answer;
                    });
            Server server = HttpServer.bind(group, new InetSocketAddress(demo ? "127.0.0.1" : "0.0.0.0", port), Options.defaults(), null,
                    new HttpConfig(16 * 1024, 1024 * 1024), router);
            if (demo) {
                var resp = client.send(ClientRequest.get(self + "/orders/42")).get(10, TimeUnit.SECONDS);
                System.out.println("GET /orders/42 -> " + resp.status() + " " + resp.bodyAsString() + "  X-Trace-Id: " + resp.header("X-Trace-Id"));
                server.close();
            } else {
                log.info("serving on {}; try: curl -i localhost:{}/orders/42", port, port);
                Thread.currentThread().join();
            }
        } finally {
            sink.forceFlush().get(10, TimeUnit.SECONDS);   // what is still queued goes out now; the sink's hook closes it at exit
            log.info("spans exported: {}, dropped: {}, failed batches: {}", sink.exported(), sink.dropped(), sink.failedBatches());
            Log.flush();
        }
    }
}
