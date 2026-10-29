package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.Marshaller;
import io.hydrangea.grpc.MethodDescriptor;
import io.hydrangea.grpc.MethodDescriptor.MethodType;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;

/**
 * The smallest gRPC service: one unary and one server-streaming method. Its protocol (proto3):
 * <pre>
 * package my;
 * message Name  { string name = 1; }
 * message Greet { string text = 1; }
 * service Greeter {
 *   rpc Hello      (Name) returns (Greet);
 *   rpc HelloMany  (Name) returns (stream Greet);
 * }
 * </pre>
 * To add a method: (1) a message class and its {@link Marshaller} if it needs a new message, (2) a {@link MethodDescriptor},
 * (3) one line in the builder. Try it: {@code grpcurl -plaintext -proto examples-proto/greeter.proto -d '{"name":"An"}' localhost:50052 my.Greeter/Hello}
 * Client: {@link GrpcHelloClientExample}. Postman: New > gRPC, import examples-proto/greeter.proto, URL localhost:50052 (no TLS).
 */
public final class GrpcHelloExample {

    private GrpcHelloExample() {
    }

    // 1. messages: a field number + a way to write and read each field
    public record Name(String name) {
    }

    public record Greet(String text) {
    }

    static final Marshaller<Name> NAME = Marshaller.of(
            m -> new ProtoWriter().string(1, m.name()).toByteArray(),
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                String name = "";
                while (r.next()) {
                    if (r.field() == 1) name = r.string();
                    else r.skip();
                }
                return new Name(name);
            });

    static final Marshaller<Greet> GREET = Marshaller.of(
            m -> new ProtoWriter().string(1, m.text()).toByteArray(),
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                String text = "";
                while (r.next()) {
                    if (r.field() == 1) text = r.string();
                    else r.skip();
                }
                return new Greet(text);
            });

    // 2. one descriptor per method: service, method, kind of call, request and response marshallers
    static final MethodDescriptor<Name, Greet> HELLO = MethodDescriptor.of("my.Greeter", "Hello", MethodType.UNARY, NAME, GREET);
    static final MethodDescriptor<Name, Greet> HELLO_MANY = MethodDescriptor.of("my.Greeter", "HelloMany", MethodType.SERVER_STREAMING, NAME, GREET);

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 50052;

        // 3. one line per method: the handler
        GrpcServer grpc = GrpcServer.builder()
                .unary(HELLO, (req, ctx) -> CompletableFuture.completedFuture(new Greet("hello " + req.name())))
                .serverStreaming(HELLO_MANY, (req, ctx, out) -> {
                    for (int i = 1; i <= 3; i++) {
                        out.onNext(new Greet("hello " + req.name() + " #" + i));
                    }
                    out.onCompleted();
                })
                .build();

        EventLoopGroup group = new EventLoopGroup(1);
        grpc.bind(group, new InetSocketAddress("0.0.0.0", port), null);
        System.out.println("gRPC on " + port + ": my.Greeter/Hello, my.Greeter/HelloMany");
        Thread.currentThread().join();
    }
}
