package io.hydrangea.websocket;

import io.hydrangea.http.Headers;

import java.io.IOException;

/** The server refused or botched the upgrade. {@link #status()} is the HTTP status it answered with (0 if unknown). */
public final class WebSocketHandshakeException extends IOException {

    private final int status;
    private final Headers headers;

    public WebSocketHandshakeException(String message, int status, Headers headers) {
        super(message);
        this.status = status;
        this.headers = headers;
    }

    public int status() {
        return status;
    }

    public Headers headers() {
        return headers;
    }
}
