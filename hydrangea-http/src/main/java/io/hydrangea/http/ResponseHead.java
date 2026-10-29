package io.hydrangea.http;

import java.net.URI;

/** Status line and headers of a streamed response. {@code uri} is the final URL if redirects were followed. */
public record ResponseHead(int status, String reason, Headers headers, URI uri) {

    public String header(String name) {
        return headers.get(name);
    }

    public boolean isSuccessful() {
        return status >= 200 && status < 300;
    }
}
