import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;

/** Makes a few calls through a Hydrangea server and client that both write the binary log to the file in args[0]. */
public class BinaryLogProbe {
    public static void main(String[] a) throws Exception {
        BinaryLog log = BinaryLog.toFile(Path.of(a[0]), "*{h;m:16}");
        EventLoopGroup group = new EventLoopGroup(2);
        Server server = GrpcEchoExample.builder().binaryLog(log).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).binaryLog(log).build();
        ch.unary(GrpcEchoExample.UNARY, new Msg("hello world, this is longer than sixteen bytes"), CallOptions.DEFAULT.withMetadata(new Metadata().put("x-test", "v1"))).get();
        try { ch.unary(GrpcEchoExample.UNARY, new Msg("FAIL:5:gone"), CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(5))).get(); } catch (Exception expected) { }
        java.util.concurrent.CompletableFuture<Void> done = new java.util.concurrent.CompletableFuture<>();
        ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("row", 3), CallOptions.DEFAULT, new StreamObserver<>() {
            public void onNext(Msg m) { }
            public void onError(Throwable t) { done.completeExceptionally(t); }
            public void onCompleted() { done.complete(null); }
        });
        done.get();
        Thread.sleep(500);
        ch.close();
        group.close();
        log.close();
        System.out.println("WROTE " + a[0] + " dropped=" + log.dropped());
        System.exit(0);
    }
}
