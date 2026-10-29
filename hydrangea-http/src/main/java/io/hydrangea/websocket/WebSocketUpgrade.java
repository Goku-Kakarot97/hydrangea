package io.hydrangea.websocket;

import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.function.Supplier;

/**
 * Server side of the opening handshake, used from a normal HTTP route:
 * <pre>{@code
 * router.get("/chat", r -> WebSocketUpgrade.accept(r.request(), config, () -> new ChatListener()));
 * }</pre>
 * Returns a 101 response that switches the connection to WebSocket, or an error response (400/403/405/426) when the
 * request is not a valid, permitted upgrade. Over HTTP/2 (RFC 8441, an extended CONNECT with {@code :protocol = websocket}) the
 * answer is a 200 and the stream carries the frames. The supplier is called once per accepted connection.
 */
public final class WebSocketUpgrade {

    /** Fixed GUID from RFC 6455 §1.3. */
    private static final String GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    private WebSocketUpgrade() {
    }

    public static HttpResponse accept(HttpRequest request, Supplier<? extends WebSocketListener> listener) {
        return accept(request, WebSocketConfig.defaults(), listener);
    }

    public static HttpResponse accept(HttpRequest request, WebSocketConfig config, Supplier<? extends WebSocketListener> listener) {
        if (!request.method().equals("GET")) {
            return HttpResponse.status(405).header("Allow", "GET").text("WebSocket handshake requires GET");
        }
        boolean h2 = request.version().equals("HTTP/2"); // RFC 8441: an extended CONNECT that the HTTP/2 layer presents as a GET with ":protocol"
        if (h2) {
            if (!"websocket".equalsIgnoreCase(request.headers().get(":protocol"))) {
                return HttpResponse.status(400).text("Not a WebSocket request");
            }
        } else {
            if (!request.version().equals("HTTP/1.1")) {
                return HttpResponse.status(400).text("WebSocket handshake requires HTTP/1.1");
            }
            if (!request.headers().hasToken("upgrade", "websocket") || !request.headers().hasToken("connection", "upgrade")) {
                return HttpResponse.status(400).text("Not a WebSocket upgrade request");
            }
        }
        if (request.body().length > 0) {
            return HttpResponse.status(400).text("Handshake request must not have a body");
        }
        List<String> versions = request.headers().getAll("sec-websocket-version");
        if (versions.size() != 1 || !versions.get(0).trim().equals("13")) {
            return HttpResponse.status(426).header("Sec-WebSocket-Version", "13").text("Unsupported WebSocket version");
        }
        List<String> keys = request.headers().getAll("sec-websocket-key");
        if (!h2 && (keys.size() != 1 || !validKey(keys.get(0).trim()))) { // HTTP/2 has no key: the stream itself is the handshake
            return HttpResponse.status(400).text("Missing or invalid Sec-WebSocket-Key");
        }
        if (!config.originPolicy().test(request)) {
            return HttpResponse.status(403).text("Origin not allowed");
        }

        String chosen = null;
        List<String> offered = request.headers().getAll("sec-websocket-protocol").stream()
                .flatMap(v -> java.util.Arrays.stream(v.split(","))).map(String::trim).filter(v -> !v.isEmpty()).toList();
        for (String supported : config.subprotocols()) { // our order of preference wins
            if (offered.contains(supported)) {
                chosen = supported;
                break;
            }
        }
        final String subprotocol = chosen;
        HttpResponse response = h2
                ? HttpResponse.status(200)
                : HttpResponse.status(101)
                        .header("Upgrade", "websocket")
                        .header("Connection", "Upgrade")
                        .header("Sec-WebSocket-Accept", acceptKey(keys.get(0).trim()));
        if (subprotocol != null) {
            response.header("Sec-WebSocket-Protocol", subprotocol);
        }
        // permessage-deflate: only if configured, and only if the client made an offer we can honour
        Extensions.ServerResult ext = Extensions.negotiateServer(request.headers().getAll("sec-websocket-extensions"), config.compression());
        if (ext != null) {
            response.header("Sec-WebSocket-Extensions", ext.responseHeader());
        }
        final Extensions.Agreed deflate = ext == null ? null : ext.agreed();
        if (h2) { // a 200 that keeps the stream open for frames in both directions
            return response.stream(() -> new WebSocketStage(new WebSocketCore(false, listener.get(), config, subprotocol, deflate), null));
        }
        return response.upgrade(() -> new WebSocketStage(new WebSocketCore(false, listener.get(), config, subprotocol, deflate), null));
    }

    /** base64(SHA-1(key + GUID)) */
    static String acceptKey(String key) {
        try {
            byte[] sha1 = MessageDigest.getInstance("SHA-1").digest((key + GUID).getBytes(StandardCharsets.ISO_8859_1)); // codeql[java/potentially-weak-cryptographic-algorithm] Sec-WebSocket-Accept is defined as SHA-1 by RFC 6455; it proves the handshake was understood, it protects nothing
            return Base64.getEncoder().encodeToString(sha1);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean validKey(String key) {
        try {
            return Base64.getDecoder().decode(key).length == 16;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
