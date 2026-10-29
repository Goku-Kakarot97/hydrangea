package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.grpc.CallContext;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.HealthService;
import io.hydrangea.grpc.ReflectionService;
import io.hydrangea.grpc.Marshaller;
import io.hydrangea.grpc.MethodDescriptor;
import io.hydrangea.grpc.MethodDescriptor.MethodType;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StreamObserver;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test fixture (to learn from, see {@link GrpcHelloExample}): a gRPC service with all four kinds of call, written without protobuf-java. Its protocol (proto3):
 * <pre>
 * package hydrangea.test;
 * message EchoRequest { string text = 1; int32 n = 2; bytes blob = 3; }
 * message EchoReply   { string text = 1; int32 n = 2; bytes blob = 3; }
 * service Echo {
 *   rpc Unary        (EchoRequest)        returns (EchoReply);
 *   rpc ServerStream (EchoRequest)        returns (stream EchoReply);
 *   rpc ClientStream (stream EchoRequest) returns (EchoReply);
 *   rpc Bidi         (stream EchoRequest) returns (stream EchoReply);
 * }
 * </pre>
 * The text of a request steers the server, so tests and interop checks can provoke every situation:
 * <ul>
 *   <li>{@code FAIL:<code>:<message>} fails with that status; {@code THROW} throws an exception (the client must see UNKNOWN, not the message);</li>
 *   <li>{@code SLOW:<ms>} answers after that long; {@code META} answers with the request header {@code x-test} and adds the trailer {@code x-trailer};</li>
 *   <li>{@code BIG:<bytes>} answers with a blob of that size; {@code ENDLESS} (server stream) sends a message every 10 ms until the call is cancelled;</li>
 *   <li>{@code FAILAFTER:<k>} (server stream) sends k messages, then fails with INTERNAL; {@code CANCELLED?} answers how many calls were cancelled so far.</li>
 * </ul>
 */
public final class GrpcEchoExample {

    private GrpcEchoExample() {
    }

    public record Msg(String text, int n, byte[] blob) {
        public Msg(String text) {
            this(text, 0, new byte[0]);
        }

        public Msg(String text, int n) {
            this(text, n, new byte[0]);
        }
    }

    public static final Marshaller<Msg> MSG = Marshaller.of(
            m -> {
                ProtoWriter w = new ProtoWriter();
                if (!m.text().isEmpty()) w.string(1, m.text());
                if (m.n() != 0) w.varint(2, m.n());
                if (m.blob().length > 0) w.bytes(3, m.blob());
                return w.toByteArray();
            },
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                String text = "";
                int n = 0;
                byte[] blob = new byte[0];
                while (r.next()) {
                    switch (r.field()) {
                        case 1 -> text = r.string();
                        case 2 -> n = (int) r.varint();
                        case 3 -> blob = r.bytes();
                        default -> r.skip();
                    }
                }
                return new Msg(text, n, blob);
            });

    public static final MethodDescriptor<Msg, Msg> UNARY = MethodDescriptor.of("hydrangea.test.Echo", "Unary", MethodType.UNARY, MSG, MSG);
    public static final MethodDescriptor<Msg, Msg> SERVER_STREAM = MethodDescriptor.of("hydrangea.test.Echo", "ServerStream", MethodType.SERVER_STREAMING, MSG, MSG);
    public static final MethodDescriptor<Msg, Msg> CLIENT_STREAM = MethodDescriptor.of("hydrangea.test.Echo", "ClientStream", MethodType.CLIENT_STREAMING, MSG, MSG);
    public static final MethodDescriptor<Msg, Msg> BIDI = MethodDescriptor.of("hydrangea.test.Echo", "Bidi", MethodType.BIDI_STREAMING, MSG, MSG);

    /** Calls the server has seen cancelled (client reset, lost connection or deadline); for tests. */
    public static final AtomicInteger CANCELLED = new AtomicInteger();

    public static GrpcServer.Builder builder() {
        return GrpcServer.builder()
                .unary(UNARY, (req, ctx) -> unary(req, ctx))
                .serverStreaming(SERVER_STREAM, GrpcEchoExample::serverStream)
                .clientStreaming(CLIENT_STREAM, (ctx, out) -> {
                    List<String> texts = new ArrayList<>();
                    return new StreamObserver<Msg>() {
                        @Override
                        public void onNext(Msg m) {
                            texts.add(m.text());
                        }

                        @Override
                        public void onError(Throwable t) {
                            CANCELLED.incrementAndGet();
                        }

                        @Override
                        public void onCompleted() {
                            out.onNext(new Msg(String.join(",", texts), texts.size()));
                            out.onCompleted();
                        }
                    };
                })
                .bidiStreaming(BIDI, (ctx, out) -> new StreamObserver<Msg>() {
                    @Override
                    public void onNext(Msg m) {
                        out.onNext(new Msg("echo:" + m.text(), m.n(), m.blob()));
                    }

                    @Override
                    public void onError(Throwable t) {
                        CANCELLED.incrementAndGet();
                    }

                    @Override
                    public void onCompleted() {
                        out.onCompleted();
                    }
                });
    }

    private static java.util.concurrent.CompletionStage<Msg> unary(Msg req, CallContext ctx) {
        String t = req.text();
        if (t.startsWith("FAIL:")) {
            String[] p = t.split(":", 3);
            return CompletableFuture.failedFuture(Status.of(Status.Code.fromValue(Integer.parseInt(p[1]))).withDescription(p.length > 2 ? p[2] : "").asException());
        }
        if (t.equals("CANCELLED?")) {
            return CompletableFuture.completedFuture(new Msg(Integer.toString(CANCELLED.get())));
        }
        if (t.equals("THROW")) {
            throw new IllegalStateException("secret internal detail");
        }
        if (t.startsWith("SLOW:")) {
            CompletableFuture<Msg> f = new CompletableFuture<>();
            ctx.onCancel(() -> {
                CANCELLED.incrementAndGet();
                f.cancel(false);
            });
            CompletableFuture.delayedExecutor(Long.parseLong(t.substring(5)), TimeUnit.MILLISECONDS).execute(() -> f.complete(new Msg("slow done")));
            return f;
        }
        if (t.equals("META")) {
            ctx.trailers().put("x-trailer", "yes");
            return CompletableFuture.completedFuture(new Msg(String.valueOf(ctx.requestMetadata().get("x-test"))));
        }
        if (t.startsWith("BIG:")) {
            return CompletableFuture.completedFuture(new Msg("big", 0, new byte[Integer.parseInt(t.substring(4))]));
        }
        return CompletableFuture.completedFuture(new Msg("echo:" + t, req.n(), req.blob()));
    }

    private static void serverStream(Msg req, CallContext ctx, StreamObserver<Msg> out) {
        String t = req.text();
        if (t.equals("ENDLESS")) {
            var timer = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread th = new Thread(r, "endless");
                th.setDaemon(true);
                return th;
            });
            int[] i = {0};
            timer.scheduleAtFixedRate(() -> {
                if (!ctx.isCancelled() && ctx.isReady()) {
                    out.onNext(new Msg("tick", i[0]++));
                }
            }, 10, 10, TimeUnit.MILLISECONDS);
            ctx.onCancel(() -> {
                CANCELLED.incrementAndGet();
                timer.shutdownNow();
            });
            return;
        }
        if (t.startsWith("FAILAFTER:")) {
            int k = Integer.parseInt(t.substring(10));
            for (int i = 0; i < k; i++) {
                out.onNext(new Msg("m", i));
            }
            out.onError(Status.of(Status.Code.INTERNAL).withDescription("failed after " + k).asException());
            return;
        }
        int n = Math.max(0, req.n());
        for (int i = 0; i < n; i++) {
            out.onNext(new Msg(t + "-" + i, i, req.blob()));
        }
        out.onCompleted();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 50051;
        HealthService health = new HealthService();
        ReflectionService reflection = new ReflectionService();
        if (args.length > 1) {   // a FileDescriptorSet (protoc --descriptor_set_out): lets grpcurl describe the messages too
            reflection.addDescriptorSet(java.nio.file.Files.readAllBytes(java.nio.file.Path.of(args[1])));
        }
        GrpcServer grpc = builder().addService(health).addService(reflection).allowCors("https://app.example").build();
        EventLoopGroup group = new EventLoopGroup(2);
        Server server = grpc.bind(group, new InetSocketAddress("0.0.0.0", port), null);
        health.setStatus("hydrangea.test.Echo", HealthService.ServingStatus.SERVING);
        System.out.println("READY " + ((InetSocketAddress) server.localAddress()).getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            health.shutdown();
            server.shutdownGracefully(Duration.ofSeconds(5)).join();
            group.close();
        }));
        Thread.currentThread().join();
    }
}
