package io.hydrangea.websocket;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Strict UTF-8 decoding of a text message that arrives in pieces. A character split between two pieces is carried
 * over; an invalid sequence is reported at once (so a bad byte ends the connection before the rest of the message is
 * read, RFC 6455 §8.1) and an incomplete sequence at the very end is invalid too.
 */
final class Utf8StreamDecoder {

    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
    private final CharBuffer scratch = CharBuffer.allocate(8192);
    private byte[] carry = new byte[0];

    /**
     * @param last true for the final piece of the message
     * @return the text decoded from this piece (possibly empty while a character is incomplete), or null if the bytes
     *         cannot be valid UTF-8
     */
    String accept(byte[] chunk, boolean last) {
        byte[] all = new byte[carry.length + chunk.length];
        System.arraycopy(carry, 0, all, 0, carry.length);
        System.arraycopy(chunk, 0, all, carry.length, chunk.length);
        ByteBuffer in = ByteBuffer.wrap(all);
        StringBuilder out = new StringBuilder(all.length);
        boolean ok = true;
        while (true) {
            scratch.clear();
            CoderResult r = decoder.decode(in, scratch, last);
            scratch.flip();
            out.append(scratch);
            if (r.isError()) {
                ok = false;
                break;
            }
            if (!r.isOverflow()) {
                break; // underflow: all decodable bytes are consumed
            }
        }
        if (ok && last) {
            scratch.clear();
            CoderResult f = decoder.flush(scratch);
            scratch.flip();
            out.append(scratch);
            ok = !f.isError();
        }
        if (!ok || last) {
            decoder.reset();
            carry = new byte[0];
        } else {
            carry = new byte[in.remaining()]; // at most 3 bytes of an unfinished character
            in.get(carry);
        }
        return ok ? out.toString() : null;
    }
}
