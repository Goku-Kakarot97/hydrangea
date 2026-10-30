package io.hydrangea.elasticsearch;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    @Test
    void readsEveryKindOfValue() {
        Object v = Json.parse("{\"a\": [1, 2.5, -3, 1e3, true, false, null, \"s\"], \"b\": {\"c\": {}}, \"d\": []}");
        assertEquals(List.of(1L, 2.5, -3L, 1000.0, true, false), Json.list(Json.at(v, "a")).subList(0, 6));
        assertNull(Json.at(v, "a", 6));
        assertEquals("s", Json.at(v, "a", 7));
        assertEquals(Map.of(), Json.at(v, "b", "c"));
        assertEquals(List.of(), Json.at(v, "d"));
        assertNull(Json.at(v, "a", 99));
        assertNull(Json.at(v, "b", "c", "nothing"));
        assertNull(Json.at(v, "a", "not an index"));
    }

    @Test
    void integersStayExactAndHugeOnesBecomeDoubles() {
        assertEquals(9007199254740993L, Json.parse("9007199254740993"));                  // past 2^53: a double would round it
        assertEquals(Long.MAX_VALUE, Json.parse("9223372036854775807"));
        assertEquals(9.223372036854776E18, Json.parse("9223372036854775808"));           // past Long.MAX_VALUE
        assertEquals(-0.0, Json.parse("-0.0"));
        assertEquals(0L, Json.parse("0"));
        assertEquals(0L, Json.parse("-0"));
    }

    @Test
    void stringsAreUnescaped() {
        assertEquals("a\"b\\c/d\b\f\n\r\t\u00e9\ud83d\ude00", Json.parse("\"a\\\"b\\\\c\\/d\\b\\f\\n\\r\\t\\u00e9\\ud83d\\ude00\""));
        assertEquals("plain", Json.parse("\"plain\""));
        assertEquals("", Json.parse("\"\""));
    }

    @Test
    void refusesWhatIsNotJson() {
        for (String bad : List.of("", "{", "[1,]", "{\"a\":1,}", "01", "1.", ".5", "+1", "1e", "\"abc", "\"a\nb\"", "\"\\x\"", "\"\\u12\"", "tru", "nul", "{a:1}", "{\"a\" 1}", "[1] x", "NaN", "Infinity")) {
            assertThrows(IllegalArgumentException.class, () -> Json.parse(bad), bad);
        }
        assertThrows(IllegalArgumentException.class, () -> Json.parse("[".repeat(300) + "]".repeat(300)), "too deep");
        assertEquals(List.of(List.of()), Json.parse("[[]]"));
    }

    @Test
    void writesEveryKindOfValueAndRoundTrips() {
        Map<String, Object> m = Json.obj("s", "q\"\\\n\t\u0001\u2028", "i", 7, "l", 1L << 40, "d", 2.5, "whole", 3.0, "b", true, "n", null, "list", Json.arr(1, "two", null),
                "arr", new int[]{1, 2}, "nested", Json.obj("k", Json.obj()), "e", java.time.DayOfWeek.MONDAY, "t", java.time.LocalDate.of(2026, 1, 2));
        String text = Json.write(m);
        assertEquals("{\"s\":\"q\\\"\\\\\\n\\t\\u0001\\u2028\",\"i\":7,\"l\":1099511627776,\"d\":2.5,\"whole\":3,\"b\":true,\"n\":null,\"list\":[1,\"two\",null],\"arr\":[1,2],"
                + "\"nested\":{\"k\":{}},\"e\":\"MONDAY\",\"t\":\"2026-01-02\"}", text);
        Object back = Json.parse(text);
        assertEquals("q\"\\\n\t\u0001\u2028", Json.at(back, "s"));
        assertEquals(1L << 40, Json.at(back, "l"));
        assertEquals(3L, Json.at(back, "whole"));
        assertEquals(List.of(1L, "two"), Json.list(Json.at(back, "list")).subList(0, 2));
    }

    @Test
    void refusesToWriteWhatItCannotRepresent() {
        assertThrows(IllegalArgumentException.class, () -> Json.write(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> Json.write(new Object()));
        assertThrows(IllegalArgumentException.class, () -> Json.obj("odd"));
        java.util.List<Object> cycle = new java.util.ArrayList<>();
        cycle.add(cycle);
        assertThrows(IllegalArgumentException.class, () -> Json.write(cycle));
    }

    @Test
    void accessorsCoerce() {
        assertEquals(5L, Json.lng("5"));
        assertEquals(5L, Json.lng(5.9));
        assertEquals(0L, Json.lng(null));
        assertEquals("5", Json.str(5L));
        assertEquals("{\"a\":1}", Json.str(Json.obj("a", 1)));
        assertTrue(Json.bool("true"));
        assertFalse(Json.bool(null));
        assertEquals(Map.of(), Json.map("not a map"));
        assertEquals(List.of(), Json.list(null));
    }
}
