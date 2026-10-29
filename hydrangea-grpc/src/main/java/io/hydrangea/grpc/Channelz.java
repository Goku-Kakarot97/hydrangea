package io.hydrangea.grpc;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The bookkeeping behind {@link ChannelzService}: every {@link GrpcServer} that is bound and every {@link GrpcChannel} that is open report
 * their calls here, and the service reads it back. Process-wide, like in other gRPC implementations; entries disappear when their channel is
 * closed or their server stops.
 * <p>"Sockets" are what this layer can see: for a server, each remote address that carried gRPC calls recently (a TCP connection has a
 * unique remote address and port), dropped after a quiet period; for a client, one per server address the channel talks to.
 */
public final class Channelz {

    private static final Channelz GLOBAL = new Channelz();

    public static Channelz global() {
        return GLOBAL;
    }

    private final AtomicLong ids = new AtomicLong(1);
    final ConcurrentSkipListMap<Long, ChannelEntry> channels = new ConcurrentSkipListMap<>();
    final ConcurrentSkipListMap<Long, ServerEntry> servers = new ConcurrentSkipListMap<>();
    final ConcurrentHashMap<Long, SubchannelEntry> subchannels = new ConcurrentHashMap<>();
    final ConcurrentHashMap<Long, SocketEntry> sockets = new ConcurrentHashMap<>();

    /** Counts of calls, as channelz reports them. */
    static final class Counters {
        final AtomicLong started = new AtomicLong(), succeeded = new AtomicLong(), failed = new AtomicLong();
        volatile long lastStartedMillis;

        void start() {
            started.incrementAndGet();
            lastStartedMillis = System.currentTimeMillis();
        }

        void end(boolean ok) {
            (ok ? succeeded : failed).incrementAndGet();
        }
    }

    enum State { UNKNOWN, IDLE, CONNECTING, READY, TRANSIENT_FAILURE, SHUTDOWN }

    static final class SocketEntry {
        final long id;
        final SocketAddress local, remote;
        final Counters streams = new Counters();
        final AtomicLong messagesSent = new AtomicLong(), messagesReceived = new AtomicLong();
        final AtomicInteger activeStreams = new AtomicInteger();
        volatile long lastMessageSentMillis, lastMessageReceivedMillis, lastRemoteStreamMillis, lastLocalStreamMillis, lastActiveNanos = System.nanoTime();

        SocketEntry(long id, SocketAddress local, SocketAddress remote) {
            this.id = id;
            this.local = local;
            this.remote = remote;
        }

        void messageSent() {
            messagesSent.incrementAndGet();
            lastMessageSentMillis = System.currentTimeMillis();
            lastActiveNanos = System.nanoTime();
        }

        void messageReceived() {
            messagesReceived.incrementAndGet();
            lastMessageReceivedMillis = System.currentTimeMillis();
            lastActiveNanos = System.nanoTime();
        }
    }

    static final class SubchannelEntry {
        final long id;
        final String name;
        final Counters calls = new Counters();
        final SocketEntry socket;
        volatile Supplier<State> state = () -> State.READY;

        SubchannelEntry(long id, String name, SocketEntry socket) {
            this.id = id;
            this.name = name;
            this.socket = socket;
        }
    }

    static final class ChannelEntry {
        final long id;
        final String target;
        final Counters calls = new Counters();
        volatile Supplier<State> state = () -> State.UNKNOWN;
        volatile Supplier<List<SubchannelEntry>> subchannels = List::of;

        ChannelEntry(long id, String target) {
            this.id = id;
            this.target = target;
        }
    }

    static final class ServerEntry {
        final long id;
        final SocketAddress listen;
        final Counters calls = new Counters();
        final Map<String, SocketEntry> sockets = new ConcurrentHashMap<>();
        final Channelz owner;
        final Supplier<Boolean> alive;
        final long listenSocketId;

        ServerEntry(Channelz owner, long id, long listenSocketId, SocketAddress listen, Supplier<Boolean> alive) {
            this.owner = owner;
            this.id = id;
            this.listenSocketId = listenSocketId;
            this.listen = listen;
            this.alive = alive;
        }

        /** The socket of the connection a call arrived on (created on first use). Quiet ones are dropped now and then. */
        SocketEntry socket(SocketAddress remote, SocketAddress local) {
            if (sockets.size() > 256) {
                prune();
            }
            return sockets.computeIfAbsent(String.valueOf(remote), k -> {
                SocketEntry s = new SocketEntry(owner.ids.getAndIncrement(), local, remote);
                owner.sockets.put(s.id, s);
                return s;
            });
        }

        private void prune() {
            long cutoff = System.nanoTime() - 5L * 60 * 1_000_000_000L;
            sockets.entrySet().removeIf(e -> {
                SocketEntry s = e.getValue();
                boolean quiet = s.activeStreams.get() == 0 && s.lastActiveNanos - cutoff < 0;
                if (quiet) {
                    owner.sockets.remove(s.id);
                }
                return quiet;
            });
        }
    }

    // ---- registration ----

    ServerEntry registerServer(SocketAddress listen, Supplier<Boolean> alive) {
        long id = ids.getAndIncrement();
        long listenId = ids.getAndIncrement();
        sockets.put(listenId, new SocketEntry(listenId, listen, null));
        ServerEntry e = new ServerEntry(this, id, listenId, listen, alive);
        servers.put(id, e);
        return e;
    }

    ChannelEntry registerChannel(String target) {
        ChannelEntry e = new ChannelEntry(ids.getAndIncrement(), target);
        channels.put(e.id, e);
        return e;
    }

    void unregisterChannel(ChannelEntry e) {
        channels.remove(e.id);
        for (SubchannelEntry s : e.subchannels.get()) {
            unregisterSubchannel(s);
        }
    }

    SubchannelEntry registerSubchannel(String name, SocketAddress remote) {
        SocketEntry socket = new SocketEntry(ids.getAndIncrement(), null, remote);
        sockets.put(socket.id, socket);
        SubchannelEntry s = new SubchannelEntry(ids.getAndIncrement(), name, socket);
        subchannels.put(s.id, s);
        return s;
    }

    void unregisterSubchannel(SubchannelEntry s) {
        subchannels.remove(s.id);
        sockets.remove(s.socket.id);
    }

    /** Servers whose listener is gone are removed when anyone looks. */
    List<ServerEntry> liveServers() {
        List<ServerEntry> out = new ArrayList<>();
        for (ServerEntry e : servers.values()) {
            if (e.alive.get()) {
                out.add(e);
            } else {
                servers.remove(e.id);
                for (SocketEntry s : e.sockets.values()) {
                    sockets.remove(s.id);
                }
            }
        }
        return out;
    }
}
