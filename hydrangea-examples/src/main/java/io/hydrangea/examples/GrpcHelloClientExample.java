package io.hydrangea.examples;

import io.hydrangea.examples.GrpcHelloExample.Greet;
import io.hydrangea.examples.GrpcHelloExample.Name;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.StreamObserver;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Calls {@link GrpcHelloExample}. Start that first, then:
 * {@code java -cp target/classes io.hydrangea.examples.GrpcHelloClientExample [http://localhost:50052] [name]}
 */
public final class GrpcHelloClientExample {

    private GrpcHelloClientExample() {
    }

    public static void main(String[] args) throws Exception {
        String target = args.length > 0 ? args[0] : "http://localhost:50052";
        String who = args.length > 1 ? args[1] : "An";

        // one channel per server, shared by all calls; it connects lazily and reconnects by itself
        try (GrpcChannel channel = GrpcChannel.forTarget(target).build()) {

            // unary: a future of the reply; the deadline is part of the call options
            Greet reply = channel.unary(GrpcHelloExample.HELLO, new Name(who), CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(5)))
                    .get(10, TimeUnit.SECONDS);
            System.out.println("Hello     -> " + reply.text());

            // server streaming: an observer hears every reply, then completion or the error
            CompletableFuture<Void> done = new CompletableFuture<>();
            channel.serverStreaming(GrpcHelloExample.HELLO_MANY, new Name(who), CallOptions.DEFAULT, new StreamObserver<Greet>() {
                @Override
                public void onNext(Greet g) {
                    System.out.println("HelloMany -> " + g.text());
                }

                @Override
                public void onError(Throwable t) {
                    done.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    done.complete(null);
                }
            });
            done.get(10, TimeUnit.SECONDS);
        }
    }
}
