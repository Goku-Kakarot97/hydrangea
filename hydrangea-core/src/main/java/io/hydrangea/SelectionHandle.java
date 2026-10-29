package io.hydrangea;

import java.nio.channels.SelectionKey;

/** Attached to a {@link SelectionKey}; the loop dispatches readiness to it. Loop-thread only. */
interface SelectionHandle {

    void ready(int readyOps);

    /** The loop is shutting down: release the underlying resource now. */
    void terminate();

    /** The loop moved the channel to a new selector (spin-bug workaround); keep {@code newKey} instead of the old key. */
    default void rebind(SelectionKey newKey) {
    }
}
