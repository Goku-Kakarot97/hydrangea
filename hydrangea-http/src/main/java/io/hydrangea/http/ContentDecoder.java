package io.hydrangea.http;

import java.io.IOException;
import java.util.Locale;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Incremental decoder for {@code Content-Encoding: gzip} (RFC 1952, concatenated members included, CRC and length verified) and
 * {@code deflate} (zlib-wrapped as RFC 9110 says, and the raw deflate that some servers send instead). Bytes go in as they arrive
 * and decoded bytes come out through a {@link Sink}; nothing blocks.
 * <p>
 * The output is bounded by {@code maxOutput}: a few kilobytes of compressed zeros decode to gigabytes, and the limit is what stops that.
 */
final class ContentDecoder {

    interface Sink {
        void accept(byte[] data, int off, int len) throws IOException;
    }

    private enum State { HEADER, EXTRA_LEN, EXTRA, NAME, COMMENT, HEADER_CRC, BODY, TRAILER, AFTER_MEMBER, SNIFF, RAW_OR_ZLIB, IGNORE }

    private final boolean gzip;
    private final long maxOutput;
    private final Inflater inflater;
    private final byte[] scratch = new byte[16 * 1024];
    private final byte[] small = new byte[16];     // header / trailer bytes being collected
    private int smallLen;
    private int need;
    private int flags;
    private int extraLeft;
    private State state;
    private Inflater inflaterZlib;                 // deflate: chosen once the first two bytes are known
    private final CRC32 crc = new CRC32();
    private long memberBytes;
    private long total;
    private boolean ended;                          // deflate: the stream's final block was seen
    private boolean sawInput;

    private ContentDecoder(boolean gzip, long maxOutput) {
        this.gzip = gzip;
        this.maxOutput = maxOutput;
        this.inflater = new Inflater(true);
        this.state = gzip ? State.HEADER : State.SNIFF;
        this.need = gzip ? 10 : 2;
    }

    /** A decoder for this {@code Content-Encoding} value, or null if the body should be passed on as it is (identity, unknown, several codings). */
    static ContentDecoder forEncoding(String contentEncoding, long maxOutput) {
        if (contentEncoding == null) {
            return null;
        }
        String e = contentEncoding.trim().toLowerCase(Locale.ROOT);
        return switch (e) {
            case "gzip", "x-gzip" -> new ContentDecoder(true, maxOutput);
            case "deflate" -> new ContentDecoder(false, maxOutput);
            default -> null;
        };
    }

    void write(byte[] in, int off, int len, Sink sink) throws IOException {
        int pos = off;
        int end = off + len;
        sawInput |= len > 0;
        try {
            while (pos < end) {
                switch (state) {
                    case HEADER, TRAILER, SNIFF -> pos = collect(in, pos, end, sink);
                    case EXTRA_LEN -> pos = collect(in, pos, end, sink);
                    case EXTRA -> {
                        int n = Math.min(extraLeft, end - pos);
                        pos += n;
                        extraLeft -= n;
                        if (extraLeft == 0) {
                            afterExtra();
                        }
                    }
                    case NAME, COMMENT -> {
                        while (pos < end && in[pos] != 0) {
                            pos++;
                        }
                        if (pos < end) {
                            pos++; // the terminating zero
                            if (state == State.NAME) {
                                afterName();
                            } else {
                                afterComment();
                            }
                        }
                    }
                    case HEADER_CRC -> pos = collect(in, pos, end, sink);
                    case BODY, RAW_OR_ZLIB -> pos = inflate(in, pos, end, sink);
                    case AFTER_MEMBER -> {
                        if (gzip && in[pos] == (byte) 0x1f) {
                            beginMember();
                        } else {
                            state = State.IGNORE; // anything after the last member that is not another member is not data
                        }
                    }
                    case IGNORE -> pos = end;
                }
            }
        } catch (DataFormatException e) {
            throw new IOException("corrupt " + (gzip ? "gzip" : "deflate") + " data: " + e.getMessage(), e);
        }
    }

    /** The body is over: the compressed stream must be complete. */
    void finish() throws IOException {
        boolean complete = !sawInput // an empty body labelled gzip is an empty body, not a truncated stream
                || (gzip ? (state == State.AFTER_MEMBER || state == State.IGNORE) : (ended || state == State.IGNORE));
        abort();
        if (!complete) {
            throw new IOException("truncated " + (gzip ? "gzip" : "deflate") + " body");
        }
    }

    /** Releases the native inflater early (also done by {@link #finish}); safe to call twice. */
    void abort() {
        inflater.end();
        if (inflaterZlib != null) {
            inflaterZlib.end();
        }
    }

    // ---- header / trailer ----

    /** Collects {@code need} bytes into {@link #small}; acts on them when complete. */
    private int collect(byte[] in, int pos, int end, Sink sink) throws IOException, DataFormatException {
        int n = Math.min(need - smallLen, end - pos);
        System.arraycopy(in, pos, small, smallLen, n);
        smallLen += n;
        pos += n;
        if (smallLen < need) {
            return pos;
        }
        smallLen = 0;
        switch (state) {
            case HEADER -> {
                if ((small[0] & 0xff) != 0x1f || (small[1] & 0xff) != 0x8b) {
                    throw new IOException("not gzip data (bad magic)");
                }
                if (small[2] != 8) {
                    throw new IOException("unsupported gzip compression method " + small[2]);
                }
                flags = small[3] & 0xff;
                if ((flags & 0xe0) != 0) {
                    throw new IOException("reserved gzip flags set");
                }
                afterFixedHeader();
            }
            case EXTRA_LEN -> {
                extraLeft = (small[0] & 0xff) | (small[1] & 0xff) << 8;
                if (extraLeft == 0) {
                    afterExtra();
                } else {
                    state = State.EXTRA;
                }
            }
            case HEADER_CRC -> startBody();
            case TRAILER -> {
                long expectedCrc = (small[0] & 0xffL) | (small[1] & 0xffL) << 8 | (small[2] & 0xffL) << 16 | (small[3] & 0xffL) << 24;
                long expectedSize = (small[4] & 0xffL) | (small[5] & 0xffL) << 8 | (small[6] & 0xffL) << 16 | (small[7] & 0xffL) << 24;
                if (expectedCrc != crc.getValue()) {
                    throw new IOException("gzip CRC mismatch");
                }
                if (expectedSize != (memberBytes & 0xffffffffL)) {
                    throw new IOException("gzip length mismatch");
                }
                state = State.AFTER_MEMBER;
            }
            case SNIFF -> {
                int cmf = small[0] & 0xff;
                int flg = small[1] & 0xff;
                boolean zlib = (cmf & 0x0f) == 8 && ((cmf << 8) | flg) % 31 == 0;
                if (zlib) {
                    inflaterZlib = new Inflater(false);
                }
                // the two bytes consumed belong to the stream: feed them to the chosen inflater
                state = State.RAW_OR_ZLIB;
                Inflater inf = zlib ? inflaterZlib : inflater;
                inf.setInput(small.clone(), 0, 2);
                drain(inf, sink);
            }
            default -> throw new IllegalStateException(state.toString());
        }
        return pos;
    }

    private void afterFixedHeader() {
        if ((flags & 4) != 0) {
            state = State.EXTRA_LEN;
            need = 2;
        } else {
            afterExtra();
        }
    }

    private void afterExtra() {
        if ((flags & 8) != 0) {
            state = State.NAME;
        } else {
            afterName();
        }
    }

    private void afterName() {
        if ((flags & 16) != 0) {
            state = State.COMMENT;
        } else {
            afterComment();
        }
    }

    private void afterComment() {
        if ((flags & 2) != 0) {
            state = State.HEADER_CRC;
            need = 2;
        } else {
            startBody();
        }
    }

    private void startBody() {
        state = State.BODY;
        inflater.reset();
        crc.reset();
        memberBytes = 0;
    }

    private void beginMember() {
        state = State.HEADER;
        need = 10;
        smallLen = 0;
    }

    // ---- the deflate body ----

    private int inflate(byte[] in, int pos, int end, Sink sink) throws IOException, DataFormatException {
        Inflater inf = (!gzip && inflaterZlib != null) ? inflaterZlib : inflater;
        if (gzip) {
            inf = inflater;
        }
        inf.setInput(in, pos, end - pos);
        drain(inf, sink);
        if (inf.finished()) {
            int left = inf.getRemaining();
            int consumed = (end - pos) - left;
            pos += consumed;
            if (gzip) {
                state = State.TRAILER;
                need = 8;
                smallLen = 0;
            } else {
                ended = true;
                state = State.IGNORE;
            }
            return pos;
        }
        if (inf.needsDictionary()) {
            throw new IOException("deflate stream needs a preset dictionary");
        }
        return end; // all input consumed, more is needed
    }

    private void drain(Inflater inf, Sink sink) throws IOException, DataFormatException {
        for (;;) {
            int n = inf.inflate(scratch);
            if (n == 0) {
                if (inf.finished() || inf.needsInput() || inf.needsDictionary()) {
                    if (inf.finished() && !gzip) {
                        ended = true;
                    }
                    return;
                }
                continue;
            }
            total += n;
            if (total > maxOutput) {
                throw new IOException("decompressed body exceeds the limit of " + maxOutput + " bytes");
            }
            if (gzip) {
                crc.update(scratch, 0, n);
                memberBytes += n;
            }
            sink.accept(scratch, 0, n);
        }
    }
}
