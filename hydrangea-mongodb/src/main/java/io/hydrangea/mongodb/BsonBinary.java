package io.hydrangea.mongodb;

/**
 * BSON binary data (0x05) of a subtype other than the ones with a Java type of their own: generic ({@code byte[]}) and UUID (subtype 4, {@link java.util.UUID}).
 * Legacy UUIDs (subtype 3), MD5 (5), encrypted (6), column (7), user-defined (0x80+) come out as one of these, and go in as one.
 */
public record BsonBinary(int subtype, byte[] data) {

    public BsonBinary {
        if (subtype < 0 || subtype > 255 || data == null) {
            throw new IllegalArgumentException("subtype 0..255 and data are required");
        }
    }
}
