package io.hydrangea;

/** Handle to a task scheduled on an {@link EventLoop}. */
public interface Scheduled {
    /** Prevents the task from running if it has not run yet. Safe to call from any thread, any number of times. */
    void cancel();
}
