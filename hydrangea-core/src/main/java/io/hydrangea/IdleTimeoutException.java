package io.hydrangea;

import java.io.IOException;

/** Passed to {@link Handler#onClose} when a connection exceeded {@link Options#idleTimeout()}. */
public final class IdleTimeoutException extends IOException {
    public IdleTimeoutException(String message) {
        super(message);
    }
}
