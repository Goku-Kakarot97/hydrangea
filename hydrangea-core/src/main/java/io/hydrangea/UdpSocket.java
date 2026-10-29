package io.hydrangea;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.MembershipKey;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A non-blocking UDP socket driven by one {@link EventLoop}.
 * <p>
 * {@link #send} may be called from any thread. If the kernel buffer is full, datagrams are queued up to
 * {@code maxQueued}; beyond that they are <b>dropped</b> (and counted in {@link Metrics}), because UDP is lossy by
 * design and unbounded queues only turn overload into OOM.
 */
public final class UdpSocket implements AutoCloseable {

    private static final Logger LOG = System.getLogger(UdpSocket.class.getName());
    private static final int MAX_READS_PER_EVENT = 32;

    /** Receives datagrams on the socket's loop thread. {@code data} is only valid during the call. */
    @FunctionalInterface
    public interface Listener {
        void onDatagram(UdpSocket socket, SocketAddress sender, ByteBuffer data) throws Exception;

        default void onError(UdpSocket socket, Throwable error) {
            LOG.log(Level.WARNING, "udp error on " + socket.localAddress(), error);
        }
    }

    /**
     * Socket creation settings.
     *
     * @param maxQueued    datagrams queued when the kernel buffer is full before new ones are dropped
     * @param reuseAddress SO_REUSEADDR before bind; needed to let several processes receive the same multicast port
     * @param family       {@code StandardProtocolFamily.INET} for IPv4 multicast (a dual-stack socket cannot join
     *                     IPv4 groups on every OS), or null for the platform default
     */
    public record Config(int maxQueued, boolean reuseAddress, ProtocolFamily family) {
        public static Config defaults() {
            return new Config(1024, false, null);
        }
    }

    private record Packet(ByteBuffer data, SocketAddress target) {
    }

    private final EventLoop loop;
    private final DatagramChannel channel;
    private final Listener listener;
    private final int maxQueued;
    private final Metrics metrics;
    private final ArrayDeque<Packet> queue = new ArrayDeque<>(); // loop thread only
    private SelectionKey key;

    private UdpSocket(EventLoop loop, DatagramChannel channel, Listener listener, int maxQueued) {
        this.loop = loop;
        this.channel = channel;
        this.listener = listener;
        this.maxQueued = maxQueued;
        this.metrics = loop.metrics();
    }

    public static UdpSocket bind(EventLoopGroup group, SocketAddress address, Listener listener) throws IOException {
        return bind(group, address, Config.defaults(), listener);
    }

    /** Binds synchronously (bind errors throw here) and starts receiving. */
    public static UdpSocket bind(EventLoopGroup group, SocketAddress address, Config config, Listener listener)
            throws IOException {
        DatagramChannel ch = config.family() == null ? DatagramChannel.open() : DatagramChannel.open(config.family());
        try {
            ch.configureBlocking(false);
            if (config.reuseAddress()) {
                ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            }
            ch.bind(address);
            UdpSocket socket = new UdpSocket(group.next(), ch, listener, config.maxQueued());
            var done = new CompletableFuture<Void>();
            socket.loop.execute(() -> {
                try {
                    socket.key = socket.loop.register(ch, SelectionKey.OP_READ, socket.new Handle());
                    done.complete(null);
                } catch (Throwable t) {
                    done.completeExceptionally(t);
                }
            });
            try {
                done.join();
            } catch (CompletionException e) {
                throw e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
            }
            return socket;
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    /** The loop that drives this socket: timers belonging to the socket's users should run there. */
    public EventLoop eventLoop() {
        return loop;
    }

    public SocketAddress localAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Sets a socket option, e.g. {@code IP_MULTICAST_IF}, {@code IP_MULTICAST_TTL}, {@code IP_MULTICAST_LOOP},
     * {@code SO_RCVBUF}. Thread-safe.
     */
    public <T> UdpSocket setOption(SocketOption<T> option, T value) throws IOException {
        channel.setOption(option, value);
        return this;
    }

    /** Joins an IP multicast group on {@code iface}. Call {@link MembershipKey#drop()} to leave. */
    public MembershipKey joinGroup(InetAddress group, NetworkInterface iface) throws IOException {
        return channel.join(group, iface);
    }

    /** Joins only datagrams from {@code source} (source-specific multicast). */
    public MembershipKey joinGroup(InetAddress group, NetworkInterface iface, InetAddress source) throws IOException {
        return channel.join(group, iface, source);
    }

    public boolean isOpen() {
        return channel.isOpen();
    }

    /** Sends one datagram; ownership of {@code data} passes to the socket. Never blocks, may drop under overload. */
    public void send(ByteBuffer data, SocketAddress target) {
        Packet p = new Packet(data, target);
        if (loop.inEventLoop()) {
            enqueue(p);
        } else {
            try {
                loop.execute(() -> enqueue(p));
            } catch (RuntimeException rejected) {
                metrics.datagramsDropped.increment();
            }
        }
    }

    @Override
    public void close() {
        try {
            loop.closeAndRelease(channel); // returns once the port is really released
        } catch (IOException ignored) {
            // nothing more to do
        }
    }

    private void enqueue(Packet p) {
        if (!channel.isOpen() || queue.size() >= maxQueued) {
            metrics.datagramsDropped.increment();
            return;
        }
        queue.add(p);
        flush();
    }

    private void flush() {
        try {
            Packet p;
            while ((p = queue.peek()) != null) {
                if (channel.send(p.data, p.target) == 0) {
                    break; // kernel buffer full; wait for OP_WRITE
                }
                queue.poll();
                metrics.datagramsSent.increment();
            }
        } catch (IOException e) {
            queue.poll(); // unsendable packet (e.g. unreachable / too big): drop it, keep the socket
            metrics.datagramsDropped.increment();
            listener.onError(this, e);
            if (!queue.isEmpty() && channel.isOpen()) {
                flush();
                return;
            }
        }
        if (key != null && key.isValid()) {
            int ops = key.interestOps();
            int wanted = queue.isEmpty() ? ops & ~SelectionKey.OP_WRITE : ops | SelectionKey.OP_WRITE;
            if (wanted != ops) {
                key.interestOps(wanted);
            }
        }
    }

    private final class Handle implements SelectionHandle {
        @Override
        public void ready(int readyOps) {
            if ((readyOps & SelectionKey.OP_WRITE) != 0) {
                flush();
            }
            if ((readyOps & SelectionKey.OP_READ) == 0) {
                return;
            }
            ByteBuffer buf = loop.readBuffer();
            try {
                for (int i = 0; i < MAX_READS_PER_EVENT && channel.isOpen(); i++) {
                    buf.clear();
                    SocketAddress sender = channel.receive(buf);
                    if (sender == null) {
                        return;
                    }
                    buf.flip();
                    metrics.datagramsReceived.increment();
                    try {
                        listener.onDatagram(UdpSocket.this, sender, buf);
                    } catch (Throwable t) {
                        listener.onError(UdpSocket.this, t); // a bad datagram must not kill the socket
                    }
                }
            } catch (IOException e) {
                listener.onError(UdpSocket.this, e);
            }
        }

        @Override
        public void rebind(SelectionKey newKey) {
            key = newKey;
        }

        @Override
        public void terminate() {
            close();
        }
    }
}
