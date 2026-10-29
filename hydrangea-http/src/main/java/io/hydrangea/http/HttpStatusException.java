package io.hydrangea.http;

/**
 * Throw from a route to answer with a specific status, e.g. {@code throw new HttpStatusException(404, "no user 7")}.
 * {@link HttpRouter} turns it into a plain-text response; the message is sent to the client, so keep secrets out of it.
 */
public final class HttpStatusException extends RuntimeException {

    private final int status;

    public HttpStatusException(int status, String message) {
        super(message, null, false, false);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
