package io.hydrangea.sse;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SseParserTest {

    /** Parses {@code input} fed in the given piece sizes (cycled) and returns events + retries as strings. */
    private static List<String> parse(byte[] input, int... pieceSizes) throws IOException {
        List<String> out = new ArrayList<>();
        SseParser p = new SseParser(new SseParser.Sink() {
            public void event(String type, String data, String id) {
                out.add("event[" + type + "|" + data.replace("\n", "\\n") + "|" + id + "]");
            }

            public void retry(long millis) {
                out.add("retry[" + millis + "]");
            }
        }, 1 << 20);
        int pos = 0, i = 0;
        while (pos < input.length) {
            int n = Math.min(pieceSizes[i++ % pieceSizes.length], input.length - pos);
            p.feed(ByteBuffer.wrap(input, pos, n));
            pos += n;
        }
        return out;
    }

    private static List<String> parse(String s) throws IOException {
        return parse(s.getBytes(StandardCharsets.UTF_8), Integer.MAX_VALUE);
    }

    @Test
    void basicEvents() throws Exception {
        assertEquals(List.of("event[message|hello|null]"), parse("data: hello\n\n"));
        assertEquals(List.of("event[price|42|7]"), parse("event: price\nid: 7\ndata: 42\n\n"));
        assertEquals(List.of("event[message|a\\nb\\nc|null]"), parse("data: a\ndata: b\ndata: c\n\n"));
        assertEquals(List.of("event[message|a\\n\\nc|null]"), parse("data: a\ndata:\ndata: c\n\n"), "an empty data line is a newline");
        assertEquals(List.of("event[message|x|null]", "event[message|y|null]"), parse("data: x\n\ndata: y\n\n"));
    }

    @Test
    void lineEndingsCrLfAndCrAreAllLineEndings() throws Exception {
        assertEquals(List.of("event[message|a\\nb|1]"), parse("id: 1\r\ndata: a\r\ndata: b\r\n\r\n"));
        assertEquals(List.of("event[message|a\\nb|1]"), parse("id: 1\rdata: a\rdata: b\r\r"));
        assertEquals(List.of("event[message|a|null]", "event[message|b|null]"), parse("data: a\n\r\ndata: b\r\n\n"), "mixed endings");
    }

    @Test
    void fieldSyntax() throws Exception {
        assertEquals(List.of("event[message|x|null]"), parse("data:x\n\n"), "no space after the colon");
        assertEquals(List.of("event[message| x|null]"), parse("data:  x\n\n"), "only ONE leading space is stripped");
        assertEquals(List.of("event[message||null]"), parse("data\n\n"), "a field without colon has an empty value (and an event with only empty data still dispatches)");
        assertEquals(List.of("event[message|a:b|null]"), parse("data: a:b\n\n"), "only the first colon splits");
        assertEquals(List.of(), parse(": just a comment\n\n"));
        assertEquals(List.of("event[message|x|null]"), parse(": comment\ndata: x\n:another\n\n"));
        assertEquals(List.of("event[message|x|null]"), parse("unknown: field\ndata: x\n\n"), "unknown fields are ignored");
        assertEquals(List.of("event[message|x|null]"), parse("EVENT: nope\ndata: x\n\n"), "field names are case-sensitive");
    }

    @Test
    void eventsWithoutDataAreDropped() throws Exception {
        assertEquals(List.of(), parse("event: ping\n\n"));
        assertEquals(List.of(), parse("id: 5\n\n"));
        assertEquals(List.of("event[message|real|5]"), parse("event: ping\n\nid: 5\n\ndata: real\n\n"),
                "the dropped event's type does not leak into the next one, but its id does persist");
    }

    @Test
    void lastEventIdPersistsAcrossEventsAndIgnoresNul() throws Exception {
        assertEquals(List.of("event[message|a|1]", "event[message|b|1]", "event[message|c|3]"),
                parse("id: 1\ndata: a\n\ndata: b\n\nid: 3\ndata: c\n\n"));
        assertEquals(List.of("event[message|a|1]", "event[message|b|1]"), parse("id: 1\ndata: a\n\nid: 2\u0000x\ndata: b\n\n"), "an id with NUL is ignored");
        assertEquals(List.of("event[message|a|]"), parse("id\ndata: a\n\n"), "an empty id resets the last event id to empty");
    }

    @Test
    void retryOnlyAcceptsDigits() throws Exception {
        assertEquals(List.of("retry[1500]"), parse("retry: 1500\n\n"));
        assertEquals(List.of(), parse("retry: 15s\n\n"));
        assertEquals(List.of(), parse("retry: -5\n\n"));
        assertEquals(List.of(), parse("retry:\n\n"));
        assertEquals(List.of("retry[200]", "event[message|x|null]"), parse("retry: 200\ndata: x\n\n"));
    }

    @Test
    void byteOrderMarkOnlyAtTheStart() throws Exception {
        assertEquals(List.of("event[message|x|null]"), parse("\uFEFFdata: x\n\n"));
        assertEquals(List.of("event[message|\uFEFFx|null]"), parse("data: \uFEFFx\n\n"), "a BOM elsewhere is just a character");
    }

    @Test
    void anUnfinishedEventAtTheEndIsDiscarded() throws Exception {
        assertEquals(List.of("event[message|done|null]"), parse("data: done\n\ndata: half"));
        assertEquals(List.of("event[message|done|null]"), parse("data: done\n\ndata: half\n"), "needs the blank line");
    }

    @Test
    void invalidUtf8BecomesReplacementCharacters() throws Exception {
        byte[] bytes = concat("data: a".getBytes(), new byte[]{(byte) 0xff}, "b\n\n".getBytes());
        assertEquals(List.of("event[message|a\uFFFDb|null]"), parse(bytes, 100));
    }

    @Test
    void multiByteCharactersAreHandledWhateverTheSplit() throws Exception {
        String text = "data: Việt Nam ✓ 🎉 ñ\nevent: tình\nid: ñ1\n\n";
        List<String> expected = List.of("event[tình|Việt Nam ✓ 🎉 ñ|ñ1]");
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        for (int size = 1; size <= bytes.length; size++) {
            assertEquals(expected, parse(bytes, size), "piece size " + size);
        }
    }

    @Test
    void crlfSplitBetweenPieces() throws Exception {
        byte[] bytes = "data: a\r\ndata: b\r\n\r\ndata: c\r\n\r\n".getBytes();
        for (int size = 1; size <= bytes.length; size++) {
            assertEquals(List.of("event[message|a\\nb|null]", "event[message|c|null]"), parse(bytes, size), "piece size " + size);
        }
    }

    @Test
    void randomStreamsParseTheSameWhateverThePieceBoundaries() throws Exception {
        Random rnd = new Random(2024);
        String[] atoms = {"data: ", "data:", "event: ", "id: ", "retry: ", ": c", "x", "Việt", "✓", "🎉", " ", "42", "\u0000", "\uFEFF"};
        String[] eols = {"\n", "\r\n", "\r"};
        for (int round = 0; round < 300; round++) {
            StringBuilder sb = new StringBuilder();
            int lines = 1 + rnd.nextInt(40);
            for (int l = 0; l < lines; l++) {
                int parts = rnd.nextInt(4);
                for (int p = 0; p < parts; p++) sb.append(atoms[rnd.nextInt(atoms.length)]);
                sb.append(eols[rnd.nextInt(eols.length)]);
            }
            byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
            List<String> whole = parse(bytes, Integer.MAX_VALUE);
            for (int trial = 0; trial < 5; trial++) {
                int[] sizes = new int[1 + rnd.nextInt(5)];
                for (int i = 0; i < sizes.length; i++) sizes[i] = 1 + rnd.nextInt(9);
                assertEquals(whole, parse(bytes, sizes), "round " + round + " input " + sb.toString().replace("\r", "\\r").replace("\n", "\\n"));
            }
        }
    }

    @Test
    void oversizedLinesAndEventsAreRejected() {
        SseParser p = new SseParser(new SseParser.Sink() {
            public void event(String type, String data, String id) { }
            public void retry(long millis) { }
        }, 100);
        assertThrows(IOException.class, () -> p.feed(ByteBuffer.wrap(("data: " + "x".repeat(200)).getBytes())), "a line that never ends");
        SseParser q = new SseParser(new SseParser.Sink() {
            public void event(String type, String data, String id) { }
            public void retry(long millis) { }
        }, 100);
        assertThrows(IOException.class, () -> {
            for (int i = 0; i < 20; i++) q.feed(ByteBuffer.wrap("data: 0123456789\n".getBytes())); // many short lines, one huge event
        });
    }

    private static byte[] concat(byte[]... parts) {
        int n = 0;
        for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n];
        int pos = 0;
        for (byte[] p : parts) {
            System.arraycopy(p, 0, out, pos, p.length);
            pos += p.length;
        }
        return out;
    }
}
