import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;

import java.net.InetSocketAddress;

/** A Hydrangea process with a server (channelz on) and a client channel talking to it, to be inspected by grpc-java's channelz stubs. */
public class ChannelzProbe {
    public static void main(String[] a) throws Exception {
        EventLoopGroup group = new EventLoopGroup(2);
        Server server = GrpcEchoExample.builder().addService(new ChannelzService()).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        GrpcChannel ch = GrpcChannel.forTarget("http://localhost:" + port).build();
        for (int i = 0; i < 5; i++) ch.unary(GrpcEchoExample.UNARY, new Msg("ok"), CallOptions.DEFAULT).get();
        for (int i = 0; i < 2; i++) {
            try { ch.unary(GrpcEchoExample.UNARY, new Msg("FAIL:5:nope"), CallOptions.DEFAULT).get(); } catch (Exception expected) { }
        }
        System.out.println("READY " + port);
        Thread.currentThread().join();
    }
}
