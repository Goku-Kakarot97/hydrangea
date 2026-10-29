package io.hydrangea;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Opens outbound TCP connections. */
public final class Client {

    private Client() {
    }

    /**
     * Connects asynchronously. The returned future completes once the connection is established (and its
     * {@link Handler#onActive} has been scheduled), or fails with the connect error / timeout.
     */
    public static CompletableFuture<Connection> connect(EventLoopGroup group, SocketAddress address,
                                                        Options options, Handler handler) {
        return connect(group.next(), address, options, handler);
    }

    /**
     * As {@link #connect(EventLoopGroup, SocketAddress, Options, Handler)} on a loop of your choosing. A client that reconnects uses it to
     * keep all its connections, old and new, on the one loop, so its state needs no locking.
     */
    public static CompletableFuture<Connection> connect(EventLoop loop, SocketAddress address,
                                                        Options options, Handler handler) {
        CompletableFuture<Connection> result = new CompletableFuture<>();
        SocketChannel ch;
        try {
            ch = address instanceof UnixDomainSocketAddress
                    ? SocketChannel.open(StandardProtocolFamily.UNIX) : SocketChannel.open();
            ch.configureBlocking(false);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        loop.execute(() -> new Pending(loop, ch, address, options, handler, result).begin());
        return result;
    }

    private static final class Pending implements SelectionHandle {
        private final EventLoop loop;
        private final SocketChannel ch;
        private final SocketAddress address;
        private final Options options;
        private final Handler handler;
        private final CompletableFuture<Connection> result;
        private Scheduled timeout;
        private SelectionKey key;

        Pending(EventLoop loop, SocketChannel ch, SocketAddress address, Options options, Handler handler,
                CompletableFuture<Connection> result) {
            this.loop = loop;
            this.ch = ch;
            this.address = address;
            this.options = options;
            this.handler = handler;
            this.result = result;
        }

        void begin() {
            try {
                if (ch.connect(address)) {
                    succeed(null);
                    return;
                }
                key = loop.register(ch, SelectionKey.OP_CONNECT, this);
                if (options.connectTimeout() != null) {
                    timeout = loop.schedule(
                            () -> fail(new SocketTimeoutException("connect to " + address + " timed out")),
                            options.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
                }
            } catch (IOException | RuntimeException e) {
                fail(e);
            }
        }

        @Override
        public void ready(int readyOps) {
            try {
                if (ch.finishConnect()) {
                    succeed(key);
                }
            } catch (IOException e) {
                fail(e);
            }
        }

        private void succeed(SelectionKey existing) {
            if (timeout != null) {
                timeout.cancel();
            }
            if (result.isDone()) { // lost the race with the timeout / cancellation
                fail(null);
                return;
            }
            SocketConnection c = new SocketConnection(loop, ch, handler, options);
            result.complete(c);
            c.start(existing);
        }

        private void fail(Throwable cause) {
            if (timeout != null) {
                timeout.cancel();
            }
            if (key != null) {
                key.cancel();
            }
            try {
                ch.close();
            } catch (IOException ignored) {
                // already failing
            }
            if (cause != null) {
                result.completeExceptionally(cause);
            }
        }

        @Override
        public void rebind(SelectionKey newKey) {
            key = newKey;
        }

        @Override
        public void terminate() {
            fail(new IOException("event loop shut down"));
        }
    }
}
