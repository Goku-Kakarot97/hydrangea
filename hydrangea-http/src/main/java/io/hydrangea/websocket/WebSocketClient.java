package io.hydrangea.websocket;

import io.hydrangea.Client;
import io.hydrangea.Connection;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.tls.TlsStage;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Opens outbound WebSocket connections ({@code ws://} and {@code wss://}).
 * <pre>{@code
 * WebSocket ws = WebSocketClient.connect(group, "wss://example.org/chat", listener).join();
 * ws.sendText("hello");
 * }</pre>
 * The future completes after the opening handshake succeeded (and just before {@link WebSocketListener#onOpen}); it
 * fails with {@link WebSocketHandshakeException} when the server refuses (e.g. 401/403/404), or another
 * {@link IOException} for network, TLS or timeout problems.
 */
public final class WebSocketClient {

    private static final Set<String> RESERVED = Set.of("host", "upgrade", "connection", "sec-websocket-key",
            "sec-websocket-version", "sec-websocket-protocol", "sec-websocket-extensions", "sec-websocket-accept");
    /** DNS lookups block; keep them off the event loops. */
    private static final ExecutorService DNS = Executors.newVirtualThreadPerTaskExecutor();

    private WebSocketClient() {
    }

    public static CompletableFuture<WebSocket> connect(EventLoopGroup group, String uri, WebSocketListener listener) {
        return connect(group, uri, listener, WebSocketConfig.defaults());
    }

    public static CompletableFuture<WebSocket> connect(EventLoopGroup group, String uri, WebSocketListener listener,
                                                       WebSocketConfig config) {
        URI u;
        boolean tls;
        try {
            u = URI.create(uri);
            String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase(Locale.ROOT);
            if (!scheme.equals("ws") && !scheme.equals("wss")) {
                throw new IllegalArgumentException("URL must start with ws:// or wss://: " + uri);
            }
            if (u.getHost() == null || u.getUserInfo() != null || u.getRawFragment() != null) {
                throw new IllegalArgumentException("URL needs a host and no credentials or fragment: " + uri);
            }
            tls = scheme.equals("wss");
            config.headers().forEach((n, v) -> {
                if (RESERVED.contains(n.toLowerCase(Locale.ROOT)) || n.isEmpty() || n.chars().anyMatch(c -> c <= 0x20 || c >= 0x7f || c == ':')
                        || v.chars().anyMatch(c -> (c < 0x20 && c != '\t') || c == 0x7f)) {
                    throw new IllegalArgumentException("illegal or reserved handshake header: " + n);
                }
            });
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        int port = u.getPort() != -1 ? u.getPort() : (tls ? 443 : 80);
        String host = u.getHost();
        String hostHeader = host + (port != (tls ? 443 : 80) ? ":" + port : "");
        String path = u.getRawPath() == null || u.getRawPath().isEmpty() ? "/" : u.getRawPath();
        String target = path + (u.getRawQuery() != null ? "?" + u.getRawQuery() : "");

        CompletableFuture<WebSocket> result = new CompletableFuture<>();
        AtomicReference<Connection> connection = new AtomicReference<>();
        var timeout = new AtomicReference<io.hydrangea.Scheduled>();
        if (config.handshakeTimeout() != null) {
            timeout.set(group.next().schedule(() -> {
                if (result.completeExceptionally(new SocketTimeoutException("WebSocket handshake timed out"))) {
                    Connection c = connection.get();
                    if (c != null) {
                        c.close();
                    }
                }
            }, config.handshakeTimeout().toNanos(), TimeUnit.NANOSECONDS));
        }
        result.whenComplete((ws, err) -> {
            io.hydrangea.Scheduled t = timeout.get();
            if (t != null) {
                t.cancel();
            }
            if (err != null || result.isCancelled()) {
                Connection c = connection.get();
                if (c != null) {
                    c.close();
                }
            }
        });

        DNS.execute(() -> {
            try {
                SSLContext ssl = tls ? (config.sslContext() != null ? config.sslContext() : SSLContext.getDefault()) : null;
                InetAddress address = InetAddress.getByName(host.startsWith("[") ? host.substring(1, host.length() - 1) : host);
                WebSocketCore core = new WebSocketCore(true, listener, config, null);
                var handshake = new WebSocketStage.Handshake(hostHeader, target, config, result, core);
                var stage = new WebSocketStage(core, handshake);
                var handler = tls ? Pipeline.of(TlsStage.client(ssl, host, port), stage) : Pipeline.of(stage);
                Client.connect(group, new InetSocketAddress(address, port), Options.defaults(), handler).whenComplete((c, err) -> {
                    if (err != null) {
                        result.completeExceptionally(err instanceof CompletionException && err.getCause() != null ? err.getCause() : err);
                    } else {
                        connection.set(c);
                        if (result.isDone()) {
                            c.close(); // timed out or cancelled while connecting
                        }
                    }
                });
            } catch (Exception e) {
                result.completeExceptionally(e instanceof IOException ? e : new IOException(e));
            }
        });
        return result;
    }
}
