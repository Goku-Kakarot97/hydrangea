package io.hydrangea.http;

import java.nio.ByteBuffer;

/** HTTP/2 frame encoding shared by the client and server stages. */
final class HttpFrames {

    private HttpFrames() {
    }

    /** The 9-byte frame header followed by {@code payload}, ready to write. */
    static ByteBuffer frameBytes(int type, int flags, int sid, byte[] payload) {
        int n = payload.length;
        ByteBuffer b = ByteBuffer.allocate(9 + n);
        b.put((byte) (n >>> 16)).put((byte) (n >>> 8)).put((byte) n).put((byte) type).put((byte) flags).putInt(sid).put(payload).flip();
        return b;
    }
}
