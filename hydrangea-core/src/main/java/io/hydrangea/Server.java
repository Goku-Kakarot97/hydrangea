package io.hydrangea;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.function.Supplier;

/**
 * Accepts connections on one loop and hands each to a loop of the worker group. Binds TCP
 * ({@link java.net.InetSocketAddress}) or Unix domain ({@link UnixDomainSocketAddress}) addresses.
 * For a Unix address the socket file is removed again on {@link #close()}; a stale file from a crashed
 * process makes bind fail and must be deleted by the operator.
 */
public final class Server implements AutoCloseable {

    private static final Logger LOG = System.getLogger(Server.class.getName());
    private static final int MAX_ACCEPTS_PER_EVENT = 64;

    private final EventLoopGroup workers;
    private final Supplier<? extends Handler> handlerFactory;
    private final Options options;
    private final ServerSocketChannel channel;
    private final EventLoop acceptLoop;
    private Path unixPath; // set when we created a socket file
    private final Set<SocketConnection> active = ConcurrentHashMap.newKeySet();
    private final ConcurrentHashMap<InetAddress, Integer> perAddress = new ConcurrentHashMap<>(); // updated only with merge/compute: atomic
    private final Object shutdownLock = new Object();
    private CompletableFuture<Void> shutdownFuture;

    private Server(EventLoopGroup workers, Supplier<? extends Handler> factory, Options options,
                   ServerSocketChannel channel) {
        this.workers = workers;
        this.handlerFactory = factory;
        this.options = options;
        this.channel = channel;
        this.acceptLoop = workers.next();
    }

    /**
     * Binds and starts accepting. Returns only after the socket is bound and registered, so a bind failure
     * surfaces here as an {@link IOException}.
     *
     * @param factory called once per accepted connection to create its handler
     */
    public static Server bind(EventLoopGroup group, SocketAddress address, Options options,
                              Supplier<? extends Handler> factory) throws IOException {
        boolean unix = address instanceof UnixDomainSocketAddress;
        ServerSocketChannel ch = unix ? ServerSocketChannel.open(StandardProtocolFamily.UNIX) : ServerSocketChannel.open();
        try {
            ch.configureBlocking(false);
            if (ch.supportedOptions().contains(StandardSocketOptions.SO_REUSEADDR)) {
                ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            }
            ch.bind(address, options.backlog());
            Server server = new Server(group, factory, options, ch);
            if (ch.getLocalAddress() instanceof UnixDomainSocketAddress u) {
                server.unixPath = u.getPath();
            }
            server.registerAcceptor();
            return server;
        } catch (IOException | RuntimeException e) {
            ch.close();
            throw e;
        }
    }

    public SocketAddress localAddress() {
        try {
            return channel.getLocalAddress();
        } catch (IOException e) {
            return null;
        }
    }

    /** Connections accepted by this server and not yet closed. */
    public int activeConnections() {
        return active.size();
    }

    /**
     * Shuts down in an orderly way, for deployments and restarts:
     * <ol>
     *   <li>the listening socket is closed at once (new clients are refused; with several instances behind a balancer
     *       they move to the others);</li>
     *   <li>every open connection is asked to wind down ({@link Handler#onDrain}): HTTP finishes the request in progress
     *       and answers it with {@code Connection: close}, idle keep-alive connections are closed immediately,
     *       WebSockets get a Close frame with status 1001, SSE streams end (browsers reconnect elsewhere);</li>
     *   <li>whatever is still open after {@code timeout} is closed by force.</li>
     * </ol>
     * The returned future completes when no connection is left. Calling it again returns the same future. The event
     * loops keep running; shut the {@link EventLoopGroup} down afterwards.
     */
    public CompletableFuture<Void> shutdownGracefully(Duration timeout) {
        synchronized (shutdownLock) {
            if (shutdownFuture != null) {
                return shutdownFuture;
            }
            shutdownFuture = new CompletableFuture<>();
        }
        close(); // returns once the port is free: nothing is accepted after this point, so the set below is complete
        List<SocketConnection> open = new ArrayList<>(active);
        if (open.isEmpty()) {
            shutdownFuture.complete(null);
            return shutdownFuture;
        }
        open.forEach(SocketConnection::drain);
        acceptLoop.schedule(() -> active.forEach(SocketConnection::close), timeout.toNanos(), TimeUnit.NANOSECONDS);
        checkShutdownDone(); // all of them may have closed already
        return shutdownFuture;
    }

    private void checkShutdownDone() {
        CompletableFuture<Void> f = shutdownFuture;
        if (f != null && active.isEmpty()) {
            f.complete(null);
        }
    }

    /** Stops accepting. Established connections are left alone. */
    @Override
    public void close() {
        try {
            acceptLoop.closeAndRelease(channel); // after this returns the port is free and nothing is listening
        } catch (IOException ignored) {
            // nothing more to do
        }
        Path p = unixPath;
        if (p != null) {
            try {
                Files.deleteIfExists(p);
            } catch (IOException ignored) {
                // best effort
            }
        }
    }

    private void registerAcceptor() throws IOException {
        var done = new CompletableFuture<Void>();
        acceptLoop.execute(() -> {
            try {
                acceptLoop.register(channel, SelectionKey.OP_ACCEPT, new Acceptor());
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
    }

    private final class Acceptor implements SelectionHandle {
        @Override
        public void ready(int readyOps) {
            for (int i = 0; i < MAX_ACCEPTS_PER_EVENT; i++) {
                SocketChannel accepted = null;
                try {
                    accepted = channel.accept();
                    if (accepted == null) {
                        return;
                    }
                    if (!admit(accepted)) {
                        accepted.close();
                        continue;
                    }
                    Handler handler = handlerFactory.get();
                    SocketConnection connection = new SocketConnection(workers.next(), accepted, handler, options);
                    track(connection, accepted);
                    connection.start(null);
                } catch (Throwable t) {
                    LOG.log(Level.WARNING, "failed to accept connection", t);
                    if (accepted != null) {
                        try {
                            accepted.close();
                        } catch (IOException ignored) {
                            // already failing
                        }
                    }
                    if (!channel.isOpen()) {
                        return;
                    }
                }
            }
        }

        @Override
        public void terminate() {
            close();
        }
    }

    /** Applies {@link Options#maxConnections()} and {@link Options#maxConnectionsPerAddress()}; false = turn this one away. */
    private boolean admit(SocketChannel accepted) {
        int max = options.maxConnections();
        if (max > 0 && active.size() >= max) {
            acceptLoop.metrics().recordRejected();
            return false;
        }
        int perIp = options.maxConnectionsPerAddress();
        if (perIp > 0) {
            InetAddress address = remoteAddress(accepted);
            if (address != null && perAddress.getOrDefault(address, 0) >= perIp) {
                acceptLoop.metrics().recordRejected();
                return false;
            }
        }
        return true;
    }

    /** Registers a connection for shutdown and per-address accounting; undone when it closes. */
    private void track(SocketConnection connection, SocketChannel accepted) {
        active.add(connection);
        InetAddress address = options.maxConnectionsPerAddress() > 0 ? remoteAddress(accepted) : null;
        if (address != null) {
            perAddress.merge(address, 1, Integer::sum);
        }
        connection.closeFuture().thenRun(() -> {
            active.remove(connection);
            if (address != null) {
                perAddress.compute(address, (a, n) -> n == null || n <= 1 ? null : n - 1);
            }
            checkShutdownDone();
        });
    }

    private static InetAddress remoteAddress(SocketChannel ch) {
        try {
            return ch.getRemoteAddress() instanceof InetSocketAddress isa ? isa.getAddress() : null;
        } catch (IOException e) {
            return null;
        }
    }
}
