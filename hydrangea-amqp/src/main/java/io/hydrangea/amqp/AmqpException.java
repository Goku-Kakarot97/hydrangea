package io.hydrangea.amqp;

/**
 * An AMQP error: the broker closed the channel or connection (with its reply code, e.g. 404 NOT_FOUND, 406 PRECONDITION_FAILED),
 * or the peer broke the protocol. {@link #isConnectionError()} tells whether the whole connection is gone or only one channel.
 */
public final class AmqpException extends RuntimeException {

    public static final int REPLY_SUCCESS = 200;
    public static final int FRAME_ERROR = 501;
    public static final int SYNTAX_ERROR = 502;
    public static final int COMMAND_INVALID = 503;
    public static final int CHANNEL_ERROR = 504;
    public static final int UNEXPECTED_FRAME = 505;
    public static final int RESOURCE_ERROR = 506;
    public static final int NOT_ALLOWED = 530;
    public static final int NOT_IMPLEMENTED = 540;
    public static final int INTERNAL_ERROR = 541;

    private final int replyCode;
    private final boolean connectionError;

    public AmqpException(int replyCode, String message, boolean connectionError) {
        super(replyCode + " " + message, null, false, false);
        this.replyCode = replyCode;
        this.connectionError = connectionError;
    }

    AmqpException(String message, Throwable cause) {
        super(message, cause);
        this.replyCode = INTERNAL_ERROR;
        this.connectionError = true;
    }

    public int replyCode() {
        return replyCode;
    }

    /** True when the connection is closed, false when only the channel is. */
    public boolean isConnectionError() {
        return connectionError;
    }
}
