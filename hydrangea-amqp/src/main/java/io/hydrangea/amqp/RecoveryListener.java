package io.hydrangea.amqp;

/**
 * Notified about the life of a {@link RecoveringConnection}. The callbacks run on the recovery thread (never on the event loop),
 * so they may block briefly, but a slow one delays the recovery itself.
 */
public interface RecoveryListener {

    /** The connection to the broker was lost; recovery starts. */
    default void connectionLost(Throwable cause) {
    }

    /** About to retry (attempt 1 is the first retry). */
    default void attemptStarting(int attempt) {
    }

    /** Connected again and everything declared before was declared again. */
    default void recovered() {
    }

    /**
     * A declaration could not be made again because the broker refused it (for example 406: the queue now exists with other settings). Retrying
     * cannot cure that, so it is dropped from what is recovered, and recovery goes on with the rest; a consumer of that queue may then fail
     * with the broker's error when its channel is recovered. {@code what} names the exchange, queue or binding.
     */
    default void declarationFailed(String what, Throwable cause) {
    }

    /** Recovery stopped for good (the policy's attempt limit, or a refusal that retrying cannot cure, such as bad credentials). */
    default void gaveUp(Throwable cause) {
    }
}
