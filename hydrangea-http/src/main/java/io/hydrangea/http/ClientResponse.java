package io.hydrangea.http;

import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A complete response (body fully buffered, bounded by {@link HttpClientConfig#maxResponseBytes()}).
 *
 * @param uri the URI that produced this response (the last one if redirects were followed)
 */
public record ClientResponse(int status, String reason, Headers headers, byte[] body, URI uri) {

    public String header(String name) {
        return headers.get(name);
    }

    public boolean isSuccessful() {
        return status >= 200 && status < 300;
    }

    /** Decodes the body using the {@code charset} of Content-Type, defaulting to UTF-8. */
    public String bodyAsString() {
        return new String(body, charset());
    }

    private Charset charset() {
        String type = headers.get("content-type");
        if (type != null) {
            for (String part : type.split(";")) {
                String p = part.trim();
                if (p.regionMatches(true, 0, "charset=", 0, 8)) {
                    try {
                        return Charset.forName(p.substring(8).replace("\"", "").trim());
                    } catch (IllegalArgumentException ignored) {
                        break; // unknown charset: fall back
                    }
                }
            }
        }
        return StandardCharsets.UTF_8;
    }
}
