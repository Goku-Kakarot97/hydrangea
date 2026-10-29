package io.hydrangea.redis;

/**
 * A failure of a command or of the connection. For an error reply of the server, {@link #code()} is its first word ({@code WRONGTYPE}, {@code NOSCRIPT}, ...)
 * and {@link #fromServer()} is true. {@link #retriable()} says whether trying the same command again can help: true when the connection was lost, in which case
 * the command <em>may or may not</em> have been executed (so retry only what is idempotent), or when the client refused the command because it is not online.
 */
public final class RedisException extends RuntimeException {

    private final String code;
    private final boolean retriable;
    private final boolean fromServer;

    public RedisException(String message, boolean retriable) {
        this(null, message, retriable, false, null);
    }

    public RedisException(String message, boolean retriable, Throwable cause) {
        this(null, message, retriable, false, cause);
    }

    RedisException(String code, String message, boolean retriable, boolean fromServer, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.retriable = retriable;
        this.fromServer = fromServer;
    }

    static RedisException of(RedisError e) {
        return new RedisException(e.code(), e.message(), false, true, null);
    }

    /** The error code of a server error, or null for a failure of the client. */
    public String code() {
        return code;
    }

    public boolean retriable() {
        return retriable;
    }

    public boolean fromServer() {
        return fromServer;
    }
}
