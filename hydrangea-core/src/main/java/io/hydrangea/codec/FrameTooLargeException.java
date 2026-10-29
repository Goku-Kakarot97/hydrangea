package io.hydrangea.codec;

import java.io.IOException;

/** A peer sent a frame (or an unterminated prefix) beyond the configured limit. */
public final class FrameTooLargeException extends IOException {
    public FrameTooLargeException(String message) {
        super(message);
    }
}
