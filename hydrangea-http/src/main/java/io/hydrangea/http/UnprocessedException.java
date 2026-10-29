package io.hydrangea.http;

import java.io.IOException;

/**
 * The request was certainly not processed by the server (a stream above its GOAWAY mark, a refused stream, a connection that cannot take
 * new streams): RFC 9113 sections 6.8 and 8.7 make it safe to send again, whatever the method, as long as the body can be replayed.
 */
public final class UnprocessedException extends IOException {
    UnprocessedException(String message) {
        super(message);
    }
}
