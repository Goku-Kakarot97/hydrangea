package io.hydrangea.kafka;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** The record batch format (magic 2): encoding of a batch to send and decoding of the batches a fetch returns. Compression: none and gzip. */
final class Records {

    private Records() {
    }

    static final int NONE = 0, GZIP = 1;
    private static final int HEADER = 61;   // bytes of a batch before the records

    /**
     * One record before it is encoded. Its size is worked out once, here, from what is in it: the producer asks for it when the record is accepted and again when
     * it is encoded, and it is the hottest arithmetic of the send path.
     */
    static final class Out {
        private final byte[] key;
        private final byte[] value;
        private final List<Header> headers;
        private final long timestamp;
        private final byte[][] headerKeys;   // the UTF-8 of the header keys; null without headers
        private final int rest;              // bytes of the record body except the attributes byte and the two varints that depend on the batch (timestamp delta, offset delta)

        Out(byte[] key, byte[] value, List<Header> headers, long timestamp) {
            this.key = key;
            this.value = value;
            this.headers = headers;
            this.timestamp = timestamp;
            int r = (key == null ? 1 : Wire.varlongSize(key.length) + key.length) + (value == null ? 1 : Wire.varlongSize(value.length) + value.length)
                    + Wire.varlongSize(headers.size());
            if (headers.isEmpty()) {
                headerKeys = null;
            } else {
                headerKeys = new byte[headers.size()][];
                for (int i = 0; i < headerKeys.length; i++) {
                    Header h = headers.get(i);
                    byte[] k = h.key().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    headerKeys[i] = k;
                    r += Wire.varlongSize(k.length) + k.length + (h.value() == null ? 1 : Wire.varlongSize(h.value().length) + h.value().length);
                }
            }
            this.rest = r;
        }

        byte[] key() {
            return key;
        }

        byte[] value() {
            return value;
        }

        List<Header> headers() {
            return headers;
        }

        long timestamp() {
            return timestamp;
        }

        /** Bytes this record takes in a batch (the two batch-dependent varints counted as one byte each). */
        int size() {
            int body = 3 + rest;
            return Wire.varlongSize(body) + body;
        }
    }

    static int sizeOf(Out r) {
        return r.size();
    }

    /** One batch holding the records; {@code baseOffset} 0 (the broker assigns the real ones). */
    static byte[] encode(List<Out> records, int compression) {
        return encode(records, compression, -1, (short) -1, -1);
    }

    /**
     * With a producer id, epoch and base sequence the broker can drop a batch it already has (idempotent producer); -1 for all three means none.
     * Without compression the batch is written once, into an array of exactly its size.
     */
    static byte[] encode(List<Out> records, int compression, long producerId, short epoch, int baseSequence) {
        int n = records.size();
        long first = records.get(0).timestamp;
        long max = first;
        int recordsLength = 0;
        for (int i = 0; i < n; i++) {
            Out r = records.get(i);
            max = Math.max(max, r.timestamp);
            int body = 1 + Wire.varlongSize(r.timestamp - first) + Wire.varlongSize(i) + r.rest;
            recordsLength += Wire.varlongSize(body) + body;
        }
        Wire.Writer w;
        if (compression == NONE) {
            w = new Wire.Writer(HEADER + recordsLength);
            header(w, compression, n, first, max, producerId, epoch, baseSequence);
            writeRecords(w, records, first);
        } else {
            Wire.Writer body = new Wire.Writer(recordsLength);
            writeRecords(body, records, first);
            byte[] payload = gzip(body.buf, body.len);
            w = new Wire.Writer(HEADER + payload.length);
            header(w, compression, n, first, max, producerId, epoch, baseSequence);
            w.raw(payload, 0, payload.length);
        }
        w.patchI32(8, w.len - 12);
        CRC32C crc = new CRC32C();
        crc.update(w.buf, 21, w.len - 21);
        w.patchI32(17, (int) crc.getValue());
        return w.len == w.buf.length ? w.buf : w.toArray();
    }

    private static void header(Wire.Writer w, int compression, int count, long first, long max, long producerId, short epoch, int baseSequence) {
        w.i64(0);                         // base offset
        w.i32(0);                         // batch length, patched
        w.i32(-1);                        // partition leader epoch
        w.i8(2);                          // magic
        w.i32(0);                         // crc, patched
        w.i16(compression);               // attributes
        w.i32(count - 1);                 // last offset delta
        w.i64(first);
        w.i64(max);
        w.i64(producerId);
        w.i16(epoch);
        w.i32(baseSequence);
        w.i32(count);
    }

    private static void writeRecords(Wire.Writer w, List<Out> records, long first) {
        for (int i = 0; i < records.size(); i++) {
            Out r = records.get(i);
            long tsDelta = r.timestamp - first;
            w.varint(1 + Wire.varlongSize(tsDelta) + Wire.varlongSize(i) + r.rest);
            w.i8(0);
            w.varlong(tsDelta);
            w.varint(i);
            if (r.key == null) w.varint(-1); else w.varint(r.key.length).raw(r.key, 0, r.key.length);
            if (r.value == null) w.varint(-1); else w.varint(r.value.length).raw(r.value, 0, r.value.length);
            w.varint(r.headers.size());
            for (int h = 0; h < r.headers.size(); h++) {
                byte[] k = r.headerKeys[h];
                byte[] v = r.headers.get(h).value();
                w.varint(k.length).raw(k, 0, k.length);
                if (v == null) w.varint(-1); else w.varint(v.length).raw(v, 0, v.length);
            }
        }
    }

    private static byte[] gzip(byte[] in, int length) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(length / 2 + 32);
            try (GZIPOutputStream g = new GZIPOutputStream(out)) {
                g.write(in, 0, length);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] gunzip(byte[] in, int off, int len, int limit) {
        try (GZIPInputStream g = new GZIPInputStream(new ByteArrayInputStream(in, off, len))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream(len * 3);
            byte[] chunk = new byte[8192];
            int n;
            while ((n = g.read(chunk)) > 0) {
                out.write(chunk, 0, n);
                if (out.size() > limit) {
                    throw new KafkaException("protocol error: decompressed batch over " + limit + " bytes", false);
                }
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new KafkaException("protocol error: bad gzip data: " + e, false, e);
        }
    }

    /**
     * The records of the batches in {@code data} (a fetch's record set), in order, each with its absolute offset. A batch cut off at the end (the broker may
     * return part of one when the size limit falls inside it) is ignored; a batch that is whole but wrong (crc, magic, counts) is a protocol error.
     * Records before {@code from} (a batch can start earlier than the requested offset) are skipped.
     */
    static List<ConsumerRecord> decode(String topic, int partition, byte[] data, long from, int maxDecompressed) {
        List<ConsumerRecord> out = new ArrayList<>();
        decode(topic, partition, data, from, maxDecompressed, out);
        return out;
    }

    /** Same, appending to {@code out} (no list in between); returns the offset of the last record appended, or -1 if none. */
    static long decode(String topic, int partition, byte[] data, long from, int maxDecompressed, java.util.Collection<ConsumerRecord> out) {
        long last = -1;
        Wire.Reader r = new Wire.Reader(data);
        while (r.remaining() >= 12) {
            int start = r.pos;
            long base = r.i64();
            int length = r.i32();
            if (length < 49 || length > Integer.MAX_VALUE - 12) {
                throw new KafkaException("protocol error: batch length " + length, false);
            }
            if (length > r.remaining()) {
                break;   // a partial batch at the end
            }
            Wire.Reader b = r.slice(length);
            b.i32();                       // leader epoch
            int magic = b.i8();
            if (magic != 2) {
                throw new KafkaException("unsupported record format " + magic + " (this client reads magic 2, Kafka 0.11 and later)", false);
            }
            int crc = b.i32();
            CRC32C c = new CRC32C();
            c.update(b.array(), b.pos, b.end - b.pos);
            if ((int) c.getValue() != crc) {
                throw new KafkaException("protocol error: batch crc mismatch at offset " + base, KafkaException.CORRUPT_MESSAGE, false, null);
            }
            int attributes = b.i16();
            b.i32();                       // last offset delta
            long firstTs = b.i64();
            b.i64();                       // max timestamp
            b.i64();
            b.i16();
            b.i32();                       // producer id, epoch, base sequence
            int count = b.i32();
            if (count < 0 || count > b.remaining()) {
                throw new KafkaException("protocol error: batch of " + count + " records in " + b.remaining() + " bytes", false);
            }
            if ((attributes & 0x20) != 0) {
                continue;                  // a control batch (transaction marker): no user records
            }
            int codec = attributes & 7;
            Wire.Reader recs = b;
            if (codec == GZIP) {
                byte[] plain = gunzip(b.array(), b.pos, b.end - b.pos, maxDecompressed);
                recs = new Wire.Reader(plain);
            } else if (codec != NONE) {
                throw new KafkaException("unsupported compression codec " + codec + " (this client reads none and gzip)", false);
            }
            boolean logAppendTime = (attributes & 8) != 0;
            for (int i = 0; i < count; i++) {
                int size = recs.varint();
                Wire.Reader rec = recs.slice(size);
                rec.i8();
                long tsDelta = rec.varlong();
                int offsetDelta = rec.varint();
                int kl = rec.varint();
                byte[] key = kl < 0 ? null : rec.rawBytes(kl);
                int vl = rec.varint();
                byte[] value = vl < 0 ? null : rec.rawBytes(vl);
                int hc = rec.varint();
                if (hc < 0 || hc > rec.remaining()) {
                    throw new KafkaException("protocol error: " + hc + " headers", false);
                }
                List<Header> headers = hc == 0 ? List.of() : new ArrayList<>(hc);
                for (int h = 0; h < hc; h++) {
                    int hkl = rec.varint();
                    String hk = new String(rec.rawBytes(hkl), java.nio.charset.StandardCharsets.UTF_8);
                    int hvl = rec.varint();
                    headers.add(new Header(hk, hvl < 0 ? null : rec.rawBytes(hvl)));
                }
                long offset = base + offsetDelta;
                if (offset >= from) {
                    out.add(new ConsumerRecord(topic, partition, offset, firstTs + tsDelta, key, value, headers));
                    last = offset;
                }
            }
        }
        return last;
    }
}
