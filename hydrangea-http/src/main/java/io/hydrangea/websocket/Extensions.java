package io.hydrangea.websocket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Parsing and negotiation of the {@code Sec-WebSocket-Extensions} header for {@code permessage-deflate} (RFC 7692 §5, §7).
 */
final class Extensions {

    static final String NAME = "permessage-deflate";

    private Extensions() {
    }

    /** One extension offer: a name and its parameters (value null when the parameter has none). */
    record Offer(String name, Map<String, String> params, boolean wellFormed) {
    }

    /** What both sides agreed on, from the point of view of the endpoint holding it. */
    record Agreed(boolean compressorResets, boolean decompressorResets, int level, int thresholdBytes) {
    }

    record ServerResult(String responseHeader, Agreed agreed) {
    }

    /**
     * Parses header values like {@code permessage-deflate; client_max_window_bits, foo; x=1}. Offers with a malformed
     * syntax or a repeated parameter come back with {@code wellFormed = false} (a server must decline those).
     */
    static List<Offer> parse(List<String> headerValues) {
        List<Offer> offers = new ArrayList<>();
        for (String value : headerValues) {
            for (String element : splitOutsideQuotes(value, ',')) {
                if (element.isBlank()) {
                    continue;
                }
                List<String> parts = splitOutsideQuotes(element, ';');
                String name = parts.get(0).trim().toLowerCase(Locale.ROOT);
                Map<String, String> params = new LinkedHashMap<>();
                boolean ok = isToken(name);
                for (int i = 1; i < parts.size(); i++) {
                    String p = parts.get(i).trim();
                    if (p.isEmpty()) {
                        ok = false;
                        continue;
                    }
                    int eq = p.indexOf('=');
                    String key = (eq < 0 ? p : p.substring(0, eq)).trim().toLowerCase(Locale.ROOT);
                    String val = null;
                    if (eq >= 0) {
                        val = p.substring(eq + 1).trim();
                        if (val.length() >= 2 && val.startsWith("\"") && val.endsWith("\"")) {
                            val = val.substring(1, val.length() - 1);
                        } else if (!isToken(val)) {
                            ok = false;
                        }
                    }
                    if (!isToken(key) || params.containsKey(key)) {
                        ok = false; // bad syntax or the same parameter twice
                    }
                    params.put(key, val);
                }
                offers.add(new Offer(name, params, ok));
            }
        }
        return offers;
    }

    /**
     * Server side: picks the first acceptable {@code permessage-deflate} offer. Returns null (no extension) if
     * compression is not configured or no offer can be honoured.
     */
    static ServerResult negotiateServer(List<String> headerValues, PerMessageDeflate cfg) {
        if (cfg == null) {
            return null;
        }
        for (Offer o : parse(headerValues)) {
            if (!o.name().equals(NAME) || !o.wellFormed()) {
                continue;
            }
            boolean serverNoCtx = cfg.serverNoContextTakeover();
            boolean clientNoCtx = cfg.clientNoContextTakeover();
            String serverBits = null;
            boolean acceptable = true;
            for (var p : o.params().entrySet()) {
                switch (p.getKey()) {
                    case "server_no_context_takeover" -> {
                        acceptable &= p.getValue() == null;
                        serverNoCtx = true;
                    }
                    case "client_no_context_takeover" -> {
                        acceptable &= p.getValue() == null;
                        clientNoCtx = true;
                    }
                    case "server_max_window_bits" -> {
                        Integer bits = bits(p.getValue());
                        // the JDK deflater cannot shrink its window, so only 15 can be honoured
                        acceptable &= bits != null && bits == 15;
                        serverBits = "15";
                    }
                    case "client_max_window_bits" -> {
                        // a hint about what the client will use to compress; our inflater copes with any window up to 15
                        acceptable &= p.getValue() == null || bits(p.getValue()) != null;
                    }
                    default -> acceptable = false; // unknown parameter: the offer must be declined
                }
            }
            if (!acceptable) {
                continue;
            }
            StringBuilder resp = new StringBuilder(NAME);
            if (serverNoCtx) {
                resp.append("; server_no_context_takeover");
            }
            if (clientNoCtx) {
                resp.append("; client_no_context_takeover");
            }
            if (serverBits != null) {
                resp.append("; server_max_window_bits=").append(serverBits);
            }
            return new ServerResult(resp.toString(), new Agreed(serverNoCtx, clientNoCtx, cfg.level(), cfg.thresholdBytes()));
        }
        return null;
    }

    /** The client's offer header value for its configuration. */
    static String clientOffer(PerMessageDeflate cfg) {
        StringBuilder sb = new StringBuilder(NAME);
        if (cfg.clientNoContextTakeover()) {
            sb.append("; client_no_context_takeover");
        }
        if (cfg.serverNoContextTakeover()) {
            sb.append("; server_no_context_takeover");
        }
        return sb.toString();
    }

    /**
     * Client side: validates the server's answer. Returns null if the response carries no extension.
     *
     * @throws WebSocketHandshakeException if the server selected something we did not offer or used parameters illegally
     */
    static Agreed acceptServerResponse(List<String> headerValues, PerMessageDeflate offered) throws WebSocketHandshakeException {
        if (headerValues.isEmpty()) {
            return null;
        }
        List<Offer> chosen = parse(headerValues);
        if (offered == null) {
            throw refused("server selected an extension we did not offer: " + headerValues);
        }
        if (chosen.size() != 1 || !chosen.get(0).name().equals(NAME) || !chosen.get(0).wellFormed()) {
            throw refused("unexpected Sec-WebSocket-Extensions in the response: " + headerValues);
        }
        boolean serverNoCtx = false;
        boolean clientNoCtx = false;
        for (var p : chosen.get(0).params().entrySet()) {
            switch (p.getKey()) {
                case "server_no_context_takeover" -> {
                    if (p.getValue() != null) throw refused("server_no_context_takeover takes no value");
                    serverNoCtx = true;
                }
                case "client_no_context_takeover" -> {
                    if (p.getValue() != null) throw refused("client_no_context_takeover takes no value");
                    clientNoCtx = true;
                }
                case "server_max_window_bits" -> {
                    if (bits(p.getValue()) == null) throw refused("invalid server_max_window_bits");
                    // a smaller server window only helps us: our inflater accepts any window up to 32 KB
                }
                case "client_max_window_bits" ->
                        throw refused("the server demanded client_max_window_bits, which we did not offer and cannot honour");
                default -> throw refused("unknown permessage-deflate parameter: " + p.getKey());
            }
        }
        // The client compresses what the extension calls "client" data: it forgets its window per message if it asked for
        // that or the server told it to. It decompresses the "server" data, which needs a fresh window per message only if
        // the server says it compresses that way.
        return new Agreed(clientNoCtx || offered.clientNoContextTakeover(), serverNoCtx, offered.level(), offered.thresholdBytes());
    }

    private static WebSocketHandshakeException refused(String why) {
        return new WebSocketHandshakeException("permessage-deflate negotiation failed: " + why, 101, null);
    }

    private static Integer bits(String v) {
        if (v == null) {
            return null;
        }
        try {
            int n = Integer.parseInt(v);
            return n >= 8 && n <= 15 && v.equals(Integer.toString(n)) ? n : null; // no leading zeros or signs
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static boolean isToken(String s) {
        if (s.isEmpty()) {
            return false;
        }
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!(c > 0x20 && c < 0x7f) || "()<>@,;:\\\"/[]?={}".indexOf(c) >= 0) {
                return false;
            }
        }
        return true;
    }

    private static List<String> splitOutsideQuotes(String s, char sep) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            }
            if (c == sep && !quoted) {
                out.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        out.add(cur.toString());
        return out;
    }
}
