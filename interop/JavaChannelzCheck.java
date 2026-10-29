import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.channelz.v1.*;

/** Reads a Hydrangea process's channelz with grpc-java's official generated stubs and prints what it finds. Usage: JavaChannelzCheck port */
public class JavaChannelzCheck {
    static int failures;
    static void check(String what, boolean ok, Object detail) { System.out.println((ok ? "  ok   " : "  FAIL ") + what + (ok ? "" : "  -> " + detail)); if (!ok) failures++; }

    public static void main(String[] a) throws Exception {
        ManagedChannel ch = NettyChannelBuilder.forAddress("127.0.0.1", Integer.parseInt(a[0])).usePlaintext().build();
        ChannelzGrpc.ChannelzBlockingStub stub = ChannelzGrpc.newBlockingStub(ch);

        GetServersResponse servers = stub.getServers(GetServersRequest.newBuilder().build());
        check("one server", servers.getServerCount() == 1 && servers.getEnd(), servers);
        Server srv = servers.getServer(0);
        // 5 ok + 2 failed Unary calls from the probe, plus this check's own GetServers call (still running when the data is read)
        check("server counted the probe's calls (7 + this call)", srv.getData().getCallsStarted() >= 8, srv.getData());
        check("2 failed calls counted", srv.getData().getCallsFailed() == 2, srv.getData());
        check("5 succeeded calls counted", srv.getData().getCallsSucceeded() >= 5, srv.getData());
        check("last call timestamp set", srv.getData().getLastCallStartedTimestamp().getSeconds() > 1_700_000_000L, srv.getData());
        check("listen socket listed", srv.getListenSocketCount() == 1, srv);

        GetServerSocketsResponse socks = stub.getServerSockets(GetServerSocketsRequest.newBuilder().setServerId(srv.getRef().getServerId()).build());
        check("the server has served at least one connection", socks.getSocketRefCount() >= 1, socks);
        GetSocketResponse sock = stub.getSocket(GetSocketRequest.newBuilder().setSocketId(socks.getSocketRef(0).getSocketId()).build());
        check("socket has addresses", sock.getSocket().getRemote().hasTcpipAddress() && sock.getSocket().getLocal().hasTcpipAddress(), sock);
        check("socket counted streams and messages", sock.getSocket().getData().getStreamsStarted() >= 7 && sock.getSocket().getData().getMessagesReceived() >= 7
                && sock.getSocket().getData().getMessagesSent() >= 5, sock.getSocket().getData());
        GetSocketResponse listen = stub.getSocket(GetSocketRequest.newBuilder().setSocketId(srv.getListenSocket(0).getSocketId()).build());
        check("listen socket has a local address", listen.getSocket().getLocal().hasTcpipAddress(), listen);

        GetTopChannelsResponse top = stub.getTopChannels(GetTopChannelsRequest.newBuilder().build());
        check("one client channel", top.getChannelCount() == 1 && top.getEnd(), top);
        Channel c = top.getChannel(0);
        check("channel target", c.getData().getTarget().startsWith("http://localhost:"), c.getData());
        check("channel is READY", c.getData().getState().getState() == ChannelConnectivityState.State.READY, c.getData());
        check("channel counted 7 calls, 2 failed", c.getData().getCallsStarted() == 7 && c.getData().getCallsFailed() == 2 && c.getData().getCallsSucceeded() == 5, c.getData());
        check("channel has a subchannel", c.getSubchannelRefCount() >= 1, c);
        GetSubchannelResponse sub = stub.getSubchannel(GetSubchannelRequest.newBuilder().setSubchannelId(c.getSubchannelRef(0).getSubchannelId()).build());
        check("subchannel counted the attempts", sub.getSubchannel().getData().getCallsStarted() == 7, sub.getSubchannel().getData());
        check("subchannel has a socket", sub.getSubchannel().getSocketRefCount() == 1, sub);
        GetChannelResponse same = stub.getChannel(GetChannelRequest.newBuilder().setChannelId(c.getRef().getChannelId()).build());
        check("GetChannel agrees with GetTopChannels", same.getChannel().getRef().equals(c.getRef()), same);

        try { stub.getChannel(GetChannelRequest.newBuilder().setChannelId(999999).build()); check("unknown channel is an error", false, "no error"); }
        catch (io.grpc.StatusRuntimeException e) { check("unknown channel is NOT_FOUND", e.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND, e.getStatus()); }

        System.out.println(failures == 0 ? "ALL OK" : failures + " FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }
}
