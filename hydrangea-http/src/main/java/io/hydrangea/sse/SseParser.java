package io.hydrangea.sse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Incremental parser for the {@code text/event-stream} format, following the WHATWG HTML standard §9.2.6.
 * Bytes can arrive in any pieces: a line, a CR LF pair or a multi-byte character may be split anywhere.
 * <ul>
 *   <li>UTF-8 with replacement characters for malformed input; a leading byte order mark is ignored.</li>
 *   <li>Lines end with CRLF, LF or CR. {@code :comment} lines are ignored. A field is {@code name:value} with one
 *       optional space after the colon; a line without colon is a field with an empty value.</li>
 *   <li>{@code data} lines are joined with "\n"; an event with no data lines is dropped; the {@code id} (unless it
 *       contains NUL) outlives the event; {@code retry} counts only if it is all ASCII digits; unknown fields are ignored.</li>
 *   <li>An event is dispatched by the blank line that ends it. Bytes left at the end of the stream (an unfinished
 *       event) are discarded, as the standard requires.</li>
 * </ul>
 * A single line or event larger than {@code maxEventBytes} is a hard error (a server that never ends a line must not
 * make the client buffer without limit).
 */
final class SseParser {

    interface Sink {
        void event(String type, String data, String lastEventId);

        void retry(long millis);
    }

    private final Sink sink;
    private final int maxEventChars;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
    private final CharBuffer chars = CharBuffer.allocate(4096);
    private byte[] carry = new byte[0];          // trailing bytes of an unfinished UTF-8 sequence
    private boolean atStart = true;              // still waiting to see whether a BOM comes first
    private boolean lastWasCr;

    private final StringBuilder line = new StringBuilder();
    private final StringBuilder data = new StringBuilder();
    private String eventType = "";
    private String lastEventId;                  // the "last event ID buffer"; survives dispatches

    SseParser(Sink sink, int maxEventBytes) {
        this.sink = sink;
        this.maxEventChars = maxEventBytes;
    }

    String lastEventId() {
        return lastEventId;
    }

    /** Seeds the id (a reconnecting client continues from the previous connection's last id). */
    void setLastEventId(String id) {
        this.lastEventId = id;
    }

    void feed(ByteBuffer bytes) throws IOException {
        byte[] all = new byte[carry.length + bytes.remaining()];
        System.arraycopy(carry, 0, all, 0, carry.length);
        bytes.get(all, carry.length, bytes.remaining());
        ByteBuffer in = ByteBuffer.wrap(all);
        while (true) {
            chars.clear();
            CoderResult r = decoder.decode(in, chars, false);
            chars.flip();
            consume(chars);
            if (!r.isOverflow()) {
                break; // underflow: whatever is left is an incomplete character
            }
        }
        carry = new byte[in.remaining()];
        in.get(carry);
    }

    private void consume(CharBuffer cb) throws IOException {
        while (cb.hasRemaining()) {
            char c = cb.get();
            if (atStart) {
                atStart = false;
                if (c == '﻿') {
                    continue;
                }
            }
            if (lastWasCr) {
                lastWasCr = false;
                if (c == '\n') {
                    continue; // the LF of a CRLF pair that was already handled at the CR
                }
            }
            if (c == '\r') {
                lastWasCr = true;
                endOfLine();
            } else if (c == '\n') {
                endOfLine();
            } else {
                line.append(c);
                if (line.length() + data.length() > maxEventChars) {
                    throw new IOException("event stream line or event exceeds " + maxEventChars + " characters");
                }
            }
        }
    }

    private void endOfLine() {
        if (line.length() == 0) {
            dispatch();
            return;
        }
        if (line.charAt(0) == ':') {
            line.setLength(0); // comment
            return;
        }
        int colon = line.indexOf(":");
        String field;
        String value;
        if (colon < 0) {
            field = line.toString();
            value = "";
        } else {
            field = line.substring(0, colon);
            int v = colon + 1;
            if (v < line.length() && line.charAt(v) == ' ') {
                v++;
            }
            value = line.substring(v);
        }
        line.setLength(0);
        switch (field) {
            case "event" -> eventType = value;
            case "data" -> data.append(value).append('\n');
            case "id" -> {
                if (value.indexOf('\0') < 0) {
                    lastEventId = value;
                }
            }
            case "retry" -> {
                if (!value.isEmpty() && value.length() <= 15 && value.chars().allMatch(ch -> ch >= '0' && ch <= '9')) {
                    sink.retry(Long.parseLong(value));
                }
            }
            default -> {
                // unknown fields are ignored
            }
        }
    }

    private void dispatch() {
        if (data.length() == 0) {
            eventType = "";
            return; // nothing to deliver; also resets a lone "event:" line
        }
        data.setLength(data.length() - 1); // drop the final "\n"
        String type = eventType.isEmpty() ? "message" : eventType;
        String text = data.toString();
        data.setLength(0);
        eventType = "";
        sink.event(type, text, lastEventId);
    }
}
