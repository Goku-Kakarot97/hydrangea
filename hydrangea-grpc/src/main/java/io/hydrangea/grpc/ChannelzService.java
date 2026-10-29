package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnixDomainSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Channelz ({@code grpc.channelz.v1.Channelz}): a standard way to look inside a running process through gRPC itself. It reports the gRPC
 * servers and client channels of this process (see {@link Channelz}): calls started, succeeded and failed, connection state, the server
 * addresses a channel uses, and the connections a server has served. {@code grpcdebug} and the channelz UIs read it.
 * <pre>{@code
 * GrpcServer.builder().addService(new ChannelzService())
 * }</pre>
 * Do not expose it to untrusted clients: it lists peer addresses. Traces and flow-control windows are not reported.
 */
public final class ChannelzService implements GrpcServer.BindableService {

    private static final String SERVICE = "grpc.channelz.v1.Channelz";
    private static final Marshaller<byte[]> RAW = Marshaller.of(b -> b, b -> b);
    private final Channelz registry;

    public ChannelzService() {
        this(Channelz.global());
    }

    ChannelzService(Channelz registry) {
        this.registry = registry;
    }

    private interface Method {
        byte[] call(byte[] request) throws StatusException;
    }

    private static void register(GrpcServer.Builder b, String name, Method m) {
        b.unary(MethodDescriptor.of(SERVICE, name, MethodDescriptor.MethodType.UNARY, RAW, RAW), (req, ctx) -> {
            try {
                return CompletableFuture.completedFuture(m.call(req));
            } catch (StatusException e) {
                return CompletableFuture.failedFuture(e);
            }
        });
    }

    @Override
    public void bindTo(GrpcServer.Builder b) {
        register(b, "GetTopChannels", this::topChannels);
        register(b, "GetServers", this::servers);
        register(b, "GetServer", this::server);
        register(b, "GetServerSockets", this::serverSockets);
        register(b, "GetChannel", this::channel);
        register(b, "GetSubchannel", this::subchannel);
        register(b, "GetSocket", this::socket);
    }

    // ---- requests ----

    private static final int DEFAULT_PAGE = 100;

    private byte[] topChannels(byte[] req) {
        long start = 0, max = 0;
        ProtoReader r = new ProtoReader(req);
        while (r.next()) {
            if (r.field() == 1) start = r.varint(); else if (r.field() == 2) max = r.varint(); else r.skip();
        }
        int limit = max <= 0 ? DEFAULT_PAGE : (int) Math.min(max, 1000);
        ProtoWriter out = new ProtoWriter();
        int n = 0;
        boolean end = true;
        for (Channelz.ChannelEntry c : registry.channels.tailMap(start, true).values()) {
            if (n == limit) {
                end = false;
                break;
            }
            out.message(1, channelMessage(c));
            n++;
        }
        return out.bool(2, end).toByteArray();
    }

    private byte[] servers(byte[] req) {
        long start = 0, max = 0;
        ProtoReader r = new ProtoReader(req);
        while (r.next()) {
            if (r.field() == 1) start = r.varint(); else if (r.field() == 2) max = r.varint(); else r.skip();
        }
        int limit = max <= 0 ? DEFAULT_PAGE : (int) Math.min(max, 1000);
        ProtoWriter out = new ProtoWriter();
        int n = 0;
        boolean end = true;
        for (Channelz.ServerEntry e : registry.liveServers()) {
            if (e.id < start) {
                continue;
            }
            if (n == limit) {
                end = false;
                break;
            }
            out.message(1, serverMessage(e));
            n++;
        }
        return out.bool(2, end).toByteArray();
    }

    private byte[] server(byte[] req) throws StatusException {
        long id = firstVarint(req, 1);
        for (Channelz.ServerEntry e : registry.liveServers()) {
            if (e.id == id) {
                return new ProtoWriter().message(1, serverMessage(e)).toByteArray();
            }
        }
        throw notFound("server", id);
    }

    private byte[] serverSockets(byte[] req) throws StatusException {
        long serverId = 0, start = 0, max = 0;
        ProtoReader r = new ProtoReader(req);
        while (r.next()) {
            switch (r.field()) {
                case 1 -> serverId = r.varint();
                case 2 -> start = r.varint();
                case 3 -> max = r.varint();
                default -> r.skip();
            }
        }
        Channelz.ServerEntry entry = null;
        for (Channelz.ServerEntry e : registry.liveServers()) {
            if (e.id == serverId) entry = e;
        }
        if (entry == null) {
            throw notFound("server", serverId);
        }
        List<Channelz.SocketEntry> sockets = new ArrayList<>(entry.sockets.values());
        sockets.sort(java.util.Comparator.comparingLong(s -> s.id));
        int limit = max <= 0 ? DEFAULT_PAGE : (int) Math.min(max, 1000);
        ProtoWriter out = new ProtoWriter();
        int n = 0;
        boolean end = true;
        for (Channelz.SocketEntry s : sockets) {
            if (s.id < start) {
                continue;
            }
            if (n == limit) {
                end = false;
                break;
            }
            out.message(1, socketRef(s.id, String.valueOf(s.remote)));
            n++;
        }
        return out.bool(2, end).toByteArray();
    }

    private byte[] channel(byte[] req) throws StatusException {
        long id = firstVarint(req, 1);
        Channelz.ChannelEntry c = registry.channels.get(id);
        if (c == null) {
            throw notFound("channel", id);
        }
        return new ProtoWriter().message(1, channelMessage(c)).toByteArray();
    }

    private byte[] subchannel(byte[] req) throws StatusException {
        long id = firstVarint(req, 1);
        Channelz.SubchannelEntry s = registry.subchannels.get(id);
        if (s == null) {
            throw notFound("subchannel", id);
        }
        ProtoWriter sub = new ProtoWriter().message(1, subchannelRef(s.id, s.name)).message(2, data(s.state.get(), s.name, s.calls))
                .message(5, socketRef(s.socket.id, String.valueOf(s.socket.remote)));
        return new ProtoWriter().message(1, sub.toByteArray()).toByteArray();
    }

    private byte[] socket(byte[] req) throws StatusException {
        long id = firstVarint(req, 1);
        Channelz.SocketEntry s = registry.sockets.get(id);
        if (s == null) {
            throw notFound("socket", id);
        }
        ProtoWriter sock = new ProtoWriter().message(1, socketRef(s.id, String.valueOf(s.remote != null ? s.remote : s.local))).message(2, socketData(s));
        if (s.local != null) {
            sock.message(3, address(s.local));
        }
        if (s.remote != null) {
            sock.message(4, address(s.remote));
        }
        return new ProtoWriter().message(1, sock.toByteArray()).toByteArray();
    }

    // ---- messages ----

    private static long firstVarint(byte[] req, int field) {
        ProtoReader r = new ProtoReader(req);
        while (r.next()) {
            if (r.field() == field) return r.varint(); else r.skip();
        }
        return 0;
    }

    private static StatusException notFound(String what, long id) {
        return new StatusException(Status.of(Status.Code.NOT_FOUND).withDescription("no " + what + " with id " + id));
    }

    /** The four kinds of reference differ in their field numbers: ChannelRef 1/2, SocketRef 3/4, ServerRef 5/6, SubchannelRef 7/8 (id/name). */
    private static byte[] ref(int idField, long id, String name) {
        ProtoWriter w = new ProtoWriter().varint(idField, id);
        if (name != null && !name.isEmpty()) {
            w.string(idField + 1, name);
        }
        return w.toByteArray();
    }

    private static byte[] channelRef(long id, String name) {
        return ref(1, id, name);
    }

    private static byte[] socketRef(long id, String name) {
        return ref(3, id, name);
    }

    private static byte[] serverRef(long id, String name) {
        return ref(5, id, name);
    }

    private static byte[] subchannelRef(long id, String name) {
        return ref(7, id, name);
    }

    private static byte[] timestamp(long millis) {
        return new ProtoWriter().varint(1, millis / 1000).varint(2, (millis % 1000) * 1_000_000L).toByteArray();
    }

    private static byte[] data(Channelz.State state, String target, Channelz.Counters c) {
        ProtoWriter w = new ProtoWriter().message(1, new ProtoWriter().varint(1, state.ordinal()).toByteArray()).string(2, target)
                .varint(4, c.started.get()).varint(5, c.succeeded.get()).varint(6, c.failed.get());
        if (c.lastStartedMillis > 0) {
            w.message(7, timestamp(c.lastStartedMillis));
        }
        return w.toByteArray();
    }

    private byte[] channelMessage(Channelz.ChannelEntry c) {
        ProtoWriter w = new ProtoWriter().message(1, channelRef(c.id, c.target)).message(2, data(c.state.get(), c.target, c.calls));
        for (Channelz.SubchannelEntry s : c.subchannels.get()) {
            w.message(4, subchannelRef(s.id, s.name));
        }
        return w.toByteArray();
    }

    private byte[] serverMessage(Channelz.ServerEntry e) {
        ProtoWriter d = new ProtoWriter().varint(2, e.calls.started.get()).varint(3, e.calls.succeeded.get()).varint(4, e.calls.failed.get());
        if (e.calls.lastStartedMillis > 0) {
            d.message(5, timestamp(e.calls.lastStartedMillis));
        }
        return new ProtoWriter().message(1, serverRef(e.id, String.valueOf(e.listen))).message(2, d.toByteArray())
                .message(3, socketRef(e.listenSocketId, String.valueOf(e.listen))).toByteArray();
    }

    private static byte[] socketData(Channelz.SocketEntry s) {
        ProtoWriter w = new ProtoWriter().varint(1, s.streams.started.get()).varint(2, s.streams.succeeded.get()).varint(3, s.streams.failed.get())
                .varint(4, s.messagesSent.get()).varint(5, s.messagesReceived.get());
        if (s.lastLocalStreamMillis > 0) w.message(7, timestamp(s.lastLocalStreamMillis));
        if (s.lastRemoteStreamMillis > 0) w.message(8, timestamp(s.lastRemoteStreamMillis));
        if (s.lastMessageSentMillis > 0) w.message(9, timestamp(s.lastMessageSentMillis));
        if (s.lastMessageReceivedMillis > 0) w.message(10, timestamp(s.lastMessageReceivedMillis));
        return w.toByteArray();
    }

    private static byte[] address(SocketAddress a) {
        if (a instanceof InetSocketAddress i && i.getAddress() != null) {
            return new ProtoWriter().message(1, new ProtoWriter().bytes(1, i.getAddress().getAddress()).varint(2, i.getPort()).toByteArray()).toByteArray();
        }
        if (a instanceof UnixDomainSocketAddress u) {
            return new ProtoWriter().message(2, new ProtoWriter().string(1, u.getPath().toString()).toByteArray()).toByteArray();
        }
        return new ProtoWriter().message(3, new ProtoWriter().string(1, String.valueOf(a)).toByteArray()).toByteArray();
    }
}
