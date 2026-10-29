package io.hydrangea.http2;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * HPACK decoder (RFC 7541). One instance per connection: it owns the dynamic table, which both sides must keep in step, so every
 * header block of the connection has to go through it, in order, even one whose headers will be thrown away.
 * <p>
 * Defences: strings and the sum of a block's header sizes are bounded; an integer cannot run past 31 bits; the table never grows
 * beyond what this endpoint announced in SETTINGS_HEADER_TABLE_SIZE.
 */
public final class HpackDecoder {

    /** Receives the headers of one block, in order. */
    public interface Sink {
        void header(String name, String value);
    }

    private record Entry(String name, String value) {
        int size() {
            return name.length() + value.length() + 32; // header names and values are ISO-8859-1: one char per octet
        }
    }

    private final ArrayDeque<Entry> table = new ArrayDeque<>(); // first = newest = index 62
    private int tableSize;
    private int maxTableSize;       // current limit, changed by the peer's "dynamic table size update" within the allowed ceiling
    private int allowedMaxTableSize; // what we announced; the peer may not exceed it
    private final int maxStringLength;
    private final long maxListSize;
    private long listSize;           // sum of size() of the headers of the block being decoded (RFC 9113 section 6.5.2)
    private boolean tooLarge;        // listSize passed maxListSize: the rest of the block is parsed (the table must stay in step) but not delivered
    private boolean sawHeader;

    public HpackDecoder(int allowedMaxTableSize, int maxStringLength) {
        this(allowedMaxTableSize, maxStringLength, Long.MAX_VALUE);
    }

    /**
     * @param maxListSize bound on the header list of one block; past it the headers are no longer delivered to the sink
     *                    (a one-byte reference to a 4 KB table entry must not be able to inflate a small block into megabytes)
     */
    public HpackDecoder(int allowedMaxTableSize, int maxStringLength, long maxListSize) {
        this.allowedMaxTableSize = allowedMaxTableSize;
        this.maxTableSize = allowedMaxTableSize;
        this.maxStringLength = maxStringLength;
        this.maxListSize = maxListSize;
    }

    /** True when the last block's header list exceeded the limit given to the constructor. */
    public boolean lastBlockTooLarge() {
        return tooLarge;
    }

    /** Raises or lowers the ceiling after we announced a new SETTINGS_HEADER_TABLE_SIZE. */
    public void setAllowedMaxTableSize(int size) {
        this.allowedMaxTableSize = size;
    }

    /** Sum of name length + value length + 32 over the headers of the last decoded block. */
    public long lastListSize() {
        return listSize;
    }

    /** Decodes one complete header block. */
    public void decode(byte[] block, Sink sink) throws HpackException {
        listSize = 0;
        tooLarge = false;
        sawHeader = false;
        int[] pos = {0};
        while (pos[0] < block.length) {
            int b = block[pos[0]] & 0xff;
            if ((b & 0x80) != 0) {                       // 1xxxxxxx indexed header field
                int index = readInt(block, pos, 7);
                if (index == 0) {
                    throw new HpackException("index 0");
                }
                Entry e = lookup(index);
                emit(sink, e.name(), e.value());
            } else if ((b & 0x40) != 0) {                // 01xxxxxx literal with incremental indexing
                int nameIndex = readInt(block, pos, 6);
                String name = nameIndex == 0 ? readString(block, pos) : lookup(nameIndex).name();
                String value = readString(block, pos);
                emit(sink, name, value);
                insert(new Entry(name, value));
            } else if ((b & 0x20) != 0) {                // 001xxxxx dynamic table size update
                if (sawHeader) {
                    throw new HpackException("table size update after a header");
                }
                int size = readInt(block, pos, 5);
                if (size > allowedMaxTableSize) {
                    throw new HpackException("table size update " + size + " above the allowed " + allowedMaxTableSize);
                }
                maxTableSize = size;
                evictTo(maxTableSize);
            } else {                                      // 0000xxxx / 0001xxxx literal without indexing / never indexed
                int nameIndex = readInt(block, pos, 4);
                String name = nameIndex == 0 ? readString(block, pos) : lookup(nameIndex).name();
                String value = readString(block, pos);
                emit(sink, name, value);
            }
        }
    }

    private void emit(Sink sink, String name, String value) {
        sawHeader = true;
        listSize += name.length() + value.length() + 32L;
        if (listSize > maxListSize) {
            tooLarge = true;
        }
        if (!tooLarge) {
            sink.header(name, value);
        }
    }

    private Entry lookup(int index) throws HpackException {
        if (index <= HpackStatic.SIZE) {
            return new Entry(HpackStatic.name(index), HpackStatic.value(index));
        }
        int dyn = index - HpackStatic.SIZE - 1; // 0 = newest
        if (dyn >= table.size()) {
            throw new HpackException("index " + index + " beyond the table");
        }
        Iterator<Entry> it = table.iterator();
        Entry e = null;
        for (int i = 0; i <= dyn; i++) {
            e = it.next();
        }
        return e;
    }

    private void insert(Entry e) {
        int size = e.size();
        if (size > maxTableSize) { // an entry that cannot fit empties the table and is not added
            table.clear();
            tableSize = 0;
            return;
        }
        evictTo(maxTableSize - size);
        table.addFirst(e);
        tableSize += size;
    }

    private void evictTo(int limit) {
        while (tableSize > limit) {
            tableSize -= table.removeLast().size();
        }
    }

    // ---- primitives (RFC 7541 section 5) ----

    private static int readInt(byte[] b, int[] pos, int prefixBits) throws HpackException {
        if (pos[0] >= b.length) {
            throw new HpackException("truncated integer");
        }
        int mask = (1 << prefixBits) - 1;
        int value = b[pos[0]++] & mask;
        if (value < mask) {
            return value;
        }
        long result = mask;
        int shift = 0;
        while (true) {
            if (pos[0] >= b.length) {
                throw new HpackException("truncated integer");
            }
            int next = b[pos[0]++] & 0xff;
            result += (long) (next & 0x7f) << shift;
            if (result > Integer.MAX_VALUE) {
                throw new HpackException("integer too large");
            }
            if ((next & 0x80) == 0) {
                return (int) result;
            }
            shift += 7;
            if (shift > 28) {
                throw new HpackException("integer too long");
            }
        }
    }

    private String readString(byte[] b, int[] pos) throws HpackException {
        if (pos[0] >= b.length) {
            throw new HpackException("truncated string");
        }
        boolean huffman = (b[pos[0]] & 0x80) != 0;
        int len = readInt(b, pos, 7);
        if (len > b.length - pos[0]) {
            throw new HpackException("string longer than the block");
        }
        if (len > maxStringLength && !huffman) {
            throw new HpackException("string longer than " + maxStringLength);
        }
        String s;
        if (huffman) {
            s = new String(Huffman.decode(b, pos[0], len, maxStringLength), StandardCharsets.ISO_8859_1);
        } else {
            s = new String(b, pos[0], len, StandardCharsets.ISO_8859_1);
        }
        pos[0] += len;
        return s;
    }
}
