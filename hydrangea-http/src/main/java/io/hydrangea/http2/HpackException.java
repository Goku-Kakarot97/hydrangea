package io.hydrangea.http2;

/** A header block that cannot be decoded. The connection's HPACK state is then undefined, so this is always a connection error (COMPRESSION_ERROR). */
public final class HpackException extends Exception {
    public HpackException(String message) {
        super(message);
    }
}
