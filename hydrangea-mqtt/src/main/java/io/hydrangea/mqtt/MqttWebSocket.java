package io.hydrangea.mqtt;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * MQTT over WebSocket ({@code ws://} and {@code wss://}): the bytes of the MQTT connection travel in binary WebSocket messages, as brokers expect
 * on their WebSocket listener (subprotocol {@code mqtt}, which this transport asks for). Use it as the transport of a client:
 * <pre>{@code
 * MqttConfig config = MqttConfig.of("broker.example.org", 443)
 *         .withTransport(MqttWebSocket.of("wss://broker.example.org/mqtt"));
 * MqttClient client = MqttClient.connect(group, config).get();
 * }</pre>
 * Everything else works as over TCP (MQTT 3.1.1 and 5, reconnecting, back-pressure through {@code whenWritable()}). The host and port of the
 * {@link MqttConfig} are not used for connecting: the URI says where to go. TLS comes from the URI scheme and
 * {@link WebSocketConfig#withSslContext}; {@link MqttConfig#sslContext()} is ignored. An MQTT packet may be split across WebSocket messages,
 * or several packets share one; text messages are a protocol error.
 */
public final class MqttWebSocket implements MqttTransport {

    private final String uri;
    private final WebSocketConfig webSocket;

    private MqttWebSocket(String uri, WebSocketConfig webSocket) {
        this.uri = uri;
        this.webSocket = webSocket;
    }

    public static MqttWebSocket of(String uri) {
        return new MqttWebSocket(uri, WebSocketConfig.defaults());
    }

    /** For TLS settings, extra handshake headers (Authorization, Origin), or timeouts. The subprotocol {@code mqtt} is added when none is set. */
    public MqttWebSocket withWebSocketConfig(WebSocketConfig config) {
        return new MqttWebSocket(uri, config);
    }

    @Override
    public void connect(EventLoopGroup group, EventLoop home, MqttConfig config, Events events) {
        WebSocketConfig c = webSocket.subprotocols().isEmpty() ? webSocket.withSubprotocols("mqtt") : webSocket;
        if (config.connectTimeout() != null && c.handshakeTimeout() == null) {
            c = c.withHandshakeTimeout(config.connectTimeout());
        }
        WebSocketClient.connect(group, uri, new Listener(events), c).whenComplete((ws, err) -> {
            if (err != null) {
                events.closed(err instanceof CompletionException && err.getCause() != null ? err.getCause() : err);
            }
        });
    }

    private static final class Listener implements WebSocketListener {
        private final MqttTransport.Events events;
        private volatile Throwable cause;

        Listener(MqttTransport.Events events) {
            this.events = events;
        }

        @Override
        public void onOpen(WebSocket ws) {
            events.connected(new WsLink(ws));
        }

        @Override
        public boolean streaming() {
            return true;   // an MQTT packet is cut by the client, not by the message boundary; no WebSocket message size limit applies
        }

        @Override
        public void onBinaryPart(WebSocket ws, ByteBuffer part, boolean last) {
            events.data(part);
        }

        @Override
        public void onTextPart(WebSocket ws, String part, boolean last) {
            cause = new MqttException("the broker sent a text WebSocket message; MQTT uses binary ones", false);
            ws.close(1003, "MQTT uses binary messages");
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            if (cause == null) {
                cause = error;
            }
        }

        @Override
        public void onClose(WebSocket ws, int code, String reason) {
            Throwable c = cause;
            events.closed(c != null ? c : new IOException("the WebSocket closed (" + code + (reason == null || reason.isEmpty() ? "" : " " + reason) + ")"));
        }
    }

    private record WsLink(WebSocket ws) implements Link {
        private static final int FRAGMENT = 256 * 1024;

        @Override
        public void write(ByteBuffer bytes) {
            // Copies. The futures only report a closed socket, which the close event reports as well. A big packet goes out as one fragmented message:
            // sendBinary refuses messages over the WebSocket maxMessageBytes, while the pieces of a fragmented one have no limit.
            if (bytes.remaining() <= FRAGMENT) {
                ws.sendBinary(bytes);
                return;
            }
            while (bytes.hasRemaining()) {
                int n = Math.min(FRAGMENT, bytes.remaining());
                ByteBuffer piece = bytes.slice(bytes.position(), n);
                bytes.position(bytes.position() + n);
                ws.sendBinaryPart(piece, !bytes.hasRemaining());
            }
        }

        @Override
        public void close() {
            ws.close(1000, "");
        }

        @Override
        public void closeAfterFlush() {
            ws.close(1000, "");
        }

        @Override
        public boolean isWritable() {
            return ws.isWritable();
        }

        @Override
        public CompletableFuture<Void> whenWritable() {
            return ws.whenWritable();
        }
    }
}
