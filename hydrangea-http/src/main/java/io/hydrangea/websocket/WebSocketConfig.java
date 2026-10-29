package io.hydrangea.websocket;

import io.hydrangea.http.HttpRequest;

import javax.net.ssl.SSLContext;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Settings for the server ({@link WebSocketUpgrade}) and client ({@link WebSocketClient}) side.
 *
 * @param maxMessageBytes max size of an incoming frame and of a reassembled message; larger → close 1009. Does not apply
 *                        to messages a listener takes in parts ({@link WebSocketListener#streaming()})
 * @param maxStreamedMessageBytes limit for a message delivered in parts, or -1 for none (the listener sees every byte
 *                        and may enforce its own limit)
 * @param compression     the permessage-deflate extension (RFC 7692); null = never negotiate it (the default). Server: accept it
 *                        when a client offers it. Client: offer it. See {@link PerMessageDeflate} for costs and risks.
 * @param pingInterval    send a ping this often and drop the peer if nothing at all was received for twice as long;
 *                        null disables. Also keeps the connection alive against {@code Options.idleTimeout}.
 * @param closeTimeout    how long to wait for the peer's answer to our Close frame before dropping the TCP connection
 * @param subprotocols    server: supported subprotocols in order of preference; client: subprotocols to offer
 * @param originPolicy    server only: decides whether a handshake request may proceed (browsers always send Origin; this
 *                        is the defence against cross-site WebSocket hijacking). Default: no Origin header, or the same host.
 * @param handshakeTimeout client only
 * @param sslContext      client only, for wss://; null = JVM default
 * @param headers         client only: extra handshake headers such as Authorization or Origin
 */
public record WebSocketConfig(
        int maxMessageBytes,
        long maxStreamedMessageBytes,
        PerMessageDeflate compression,
        Duration pingInterval,
        Duration closeTimeout,
        List<String> subprotocols,
        Predicate<HttpRequest> originPolicy,
        Duration handshakeTimeout,
        SSLContext sslContext,
        Map<String, String> headers) {

    public WebSocketConfig {
        if (maxMessageBytes < 1) {
            throw new IllegalArgumentException("maxMessageBytes must be >= 1");
        }
        subprotocols = List.copyOf(subprotocols);
        headers = Map.copyOf(headers);
    }

    public static WebSocketConfig defaults() {
        return new WebSocketConfig(1024 * 1024, -1, null, null, Duration.ofSeconds(5), List.of(), WebSocketConfig::sameHostOrAbsent,
                Duration.ofSeconds(10), null, Map.of());
    }

    /** Accepts a request without Origin (non-browser clients) or whose Origin names the same host as the Host header. */
    public static boolean sameHostOrAbsent(HttpRequest request) {
        String origin = request.headers().get("origin");
        if (origin == null) {
            return true;
        }
        String host = request.headers().get("host");
        try {
            String authority = URI.create(origin).getAuthority();
            return authority != null && host != null && authority.equalsIgnoreCase(host);
        } catch (IllegalArgumentException e) {
            return false; // includes the literal Origin "null"
        }
    }

    public WebSocketConfig withMaxMessageBytes(int n) {
        return new WebSocketConfig(n, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, subprotocols, originPolicy, handshakeTimeout, sslContext, headers);
    }

    public WebSocketConfig withMaxStreamedMessageBytes(long n) {
        return new WebSocketConfig(maxMessageBytes, n, compression, pingInterval, closeTimeout, subprotocols, originPolicy, handshakeTimeout, sslContext, headers);
    }

    /** Enables permessage-deflate negotiation. @param c null disables it again */
    public WebSocketConfig withCompression(PerMessageDeflate c) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, c, pingInterval, closeTimeout, subprotocols, originPolicy, handshakeTimeout, sslContext, headers);
    }

    public WebSocketConfig withPingInterval(Duration d) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, d, closeTimeout, subprotocols, originPolicy, handshakeTimeout, sslContext, headers);
    }

    public WebSocketConfig withCloseTimeout(Duration d) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, d, subprotocols, originPolicy, handshakeTimeout, sslContext, headers);
    }

    public WebSocketConfig withSubprotocols(String... protocols) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, List.of(protocols), originPolicy, handshakeTimeout, sslContext, headers);
    }

    public WebSocketConfig withOriginPolicy(Predicate<HttpRequest> p) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, subprotocols, p, handshakeTimeout, sslContext, headers);
    }

    /** Accept any Origin. Only do this if the endpoint does not rely on cookies or other ambient credentials. */
    public WebSocketConfig allowAnyOrigin() {
        return withOriginPolicy(r -> true);
    }

    public WebSocketConfig withHandshakeTimeout(Duration d) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, subprotocols, originPolicy, d, sslContext, headers);
    }

    public WebSocketConfig withSslContext(SSLContext c) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, subprotocols, originPolicy, handshakeTimeout, c, headers);
    }

    public WebSocketConfig withHeaders(Map<String, String> h) {
        return new WebSocketConfig(maxMessageBytes, maxStreamedMessageBytes, compression, pingInterval, closeTimeout, subprotocols, originPolicy, handshakeTimeout, sslContext, h);
    }
}
