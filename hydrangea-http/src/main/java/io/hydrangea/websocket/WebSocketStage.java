package io.hydrangea.websocket;

import io.hydrangea.codec.Accumulator;
import io.hydrangea.http.Headers;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;

/**
 * Pipeline stage that speaks WebSocket. On the server it is created after the 101 response and handed the
 * connection by {@code HttpServerStage}. On the client it first performs the opening handshake (request out,
 * 101 response in), then behaves identically. Last stage of its pipeline.
 */
final class WebSocketStage implements Stage {

    private static final int MAX_HANDSHAKE_BYTES = 16 * 1024;

    private final WebSocketCore core;
    private final Handshake handshake; // client only; null once complete or on the server

    WebSocketStage(WebSocketCore core, Handshake handshake) {
        this.core = core;
        this.handshake = handshake;
    }

    @Override
    public void onActive(Context ctx) throws Exception {
        core.attach(ctx);
        if (handshake == null) {
            core.opened(); // server: the 101 is already on its way
        } else {
            handshake.start(ctx);
        }
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        ByteBuffer data = (ByteBuffer) msg;
        if (handshake != null && !handshake.done()) {
            ByteBuffer rest = handshake.feed(data, ctx);
            if (rest == null) {
                return; // response head still incomplete
            }
            core.opened();
            if (rest.hasRemaining()) {
                core.feed(rest); // frames that arrived in the same read as the 101
            }
            return;
        }
        core.feed(data);
    }

    @Override
    public void onWritabilityChanged(Context ctx) {
        core.writabilityChanged();
    }

    @Override
    public void onDrain(Context ctx) {
        core.drain();
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        if (handshake != null && !handshake.done()) {
            handshake.fail(cause != null ? cause : new IOException("connection closed during the WebSocket handshake"));
        }
        core.connectionClosed(cause);
    }

    /** Client side of the opening handshake (RFC 6455 §4.1). */
    static final class Handshake {
        private final String host;      // for the Host header
        private final String target;    // request-target
        private final WebSocketConfig config;
        private final CompletableFuture<WebSocket> result;
        private final WebSocketCore core;
        private final String key;
        private final Accumulator acc = new Accumulator();
        private boolean done;

        Handshake(String host, String target, WebSocketConfig config, CompletableFuture<WebSocket> result, WebSocketCore core) {
            this.host = host;
            this.target = target;
            this.config = config;
            this.result = result;
            this.core = core;
            byte[] nonce = new byte[16];
            new java.security.SecureRandom().nextBytes(nonce);
            this.key = Base64.getEncoder().encodeToString(nonce);
        }

        boolean done() {
            return done;
        }

        void start(Context ctx) throws Exception {
            StringBuilder sb = new StringBuilder(256);
            sb.append("GET ").append(target).append(" HTTP/1.1\r\n")
                    .append("Host: ").append(host).append("\r\n")
                    .append("Upgrade: websocket\r\n")
                    .append("Connection: Upgrade\r\n")
                    .append("Sec-WebSocket-Key: ").append(key).append("\r\n")
                    .append("Sec-WebSocket-Version: 13\r\n");
            if (!config.subprotocols().isEmpty()) {
                sb.append("Sec-WebSocket-Protocol: ").append(String.join(", ", config.subprotocols())).append("\r\n");
            }
            if (config.compression() != null) {
                sb.append("Sec-WebSocket-Extensions: ").append(Extensions.clientOffer(config.compression())).append("\r\n");
            }
            config.headers().forEach((n, v) -> sb.append(n).append(": ").append(v).append("\r\n"));
            sb.append("\r\n");
            ctx.write(ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.ISO_8859_1)));
        }

        /** @return bytes following the response head once the handshake succeeded; null if more input is needed */
        ByteBuffer feed(ByteBuffer data, Context ctx) throws IOException {
            acc.append(data);
            ByteBuffer in = acc.buffer();
            int end = indexOfCrlfCrlf(in);
            if (end < 0) {
                if (in.remaining() > MAX_HANDSHAKE_BYTES) {
                    throw new IOException("handshake response too large");
                }
                return null;
            }
            byte[] raw = new byte[end - in.position()];
            in.get(raw);
            in.position(in.position() + 4);
            String[] lines = new String(raw, StandardCharsets.ISO_8859_1).split("\r\n", -1);
            Headers h = new Headers();
            for (int i = 1; i < lines.length; i++) {
                int colon = lines[i].indexOf(':');
                if (colon <= 0 || lines[i].charAt(0) == ' ' || lines[i].charAt(0) == '\t') {
                    throw new IOException("malformed header in handshake response");
                }
                h.add(lines[i].substring(0, colon), lines[i].substring(colon + 1).trim());
            }
            int status = parseStatus(lines[0]);
            if (status != 101) {
                throw new WebSocketHandshakeException("server answered " + lines[0] + " instead of switching protocols", status, h);
            }
            if (!h.hasToken("upgrade", "websocket") || !h.hasToken("connection", "upgrade")) {
                throw new WebSocketHandshakeException("response lacks Upgrade: websocket / Connection: Upgrade", 101, h);
            }
            if (!WebSocketUpgrade.acceptKey(key).equals(h.get("sec-websocket-accept"))) {
                throw new WebSocketHandshakeException("Sec-WebSocket-Accept does not match our key", 101, h);
            }
            Extensions.Agreed deflate;
            try {
                deflate = Extensions.acceptServerResponse(h.getAll("sec-websocket-extensions"), config.compression());
            } catch (WebSocketHandshakeException e) {
                throw new WebSocketHandshakeException(e.getMessage(), 101, h);
            }
            String proto = h.get("sec-websocket-protocol");
            if (proto != null && !config.subprotocols().contains(proto)) {
                throw new WebSocketHandshakeException("server selected a subprotocol we did not offer: " + proto, 101, h);
            }
            done = true;
            core.setSubprotocol(proto);
            core.setDeflate(deflate);
            ByteBuffer rest = ByteBuffer.allocate(in.remaining()).put(in).flip();
            acc.release();
            result.complete(core);
            return rest;
        }

        void fail(Throwable t) {
            done = true;
            acc.release();
            result.completeExceptionally(t);
        }

        private static int parseStatus(String line) throws IOException {
            if (!line.startsWith("HTTP/1.1 ") || line.length() < 12) {
                throw new IOException("malformed status line in handshake response");
            }
            try {
                return Integer.parseInt(line.substring(9, 12));
            } catch (NumberFormatException e) {
                throw new IOException("malformed status code in handshake response");
            }
        }

        private static int indexOfCrlfCrlf(ByteBuffer in) {
            for (int i = in.position(); i <= in.limit() - 4; i++) {
                if (in.get(i) == '\r' && in.get(i + 1) == '\n' && in.get(i + 2) == '\r' && in.get(i + 3) == '\n') {
                    return i;
                }
            }
            return -1;
        }
    }
}
