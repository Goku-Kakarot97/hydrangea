package io.hydrangea.log;

/** A handle on a handler registered with {@link Log#on}; cancel it to stop receiving events. */
@FunctionalInterface
public interface Registration extends AutoCloseable {

    void cancel();

    @Override
    default void close() {
        cancel();
    }
}
