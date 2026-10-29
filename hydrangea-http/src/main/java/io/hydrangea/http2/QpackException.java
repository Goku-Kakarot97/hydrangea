package io.hydrangea.http2;

/** A field section that cannot be decoded (QPACK_DECOMPRESSION_FAILED); with a static-only table the connection's compression state is unaffected, but HTTP/3 treats it as a connection error. */
public final class QpackException extends Exception {
    public QpackException(String message) {
        super(message);
    }
}
