package io.hydrangea.postgres;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PgTypesTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void decodesByTypeOid() {
        assertEquals(true, PgTypes.decode(PgTypes.BOOL, b("t")));
        assertEquals(false, PgTypes.decode(PgTypes.BOOL, b("f")));
        assertEquals((short) -7, PgTypes.decode(PgTypes.INT2, b("-7")));
        assertEquals(42, PgTypes.decode(PgTypes.INT4, b("42")));
        assertEquals(Long.MIN_VALUE, PgTypes.decode(PgTypes.INT8, b("-9223372036854775808")));
        assertEquals(1.5f, PgTypes.decode(PgTypes.FLOAT4, b("1.5")));
        assertEquals(Double.POSITIVE_INFINITY, PgTypes.decode(PgTypes.FLOAT8, b("Infinity")));
        assertEquals(new BigDecimal("12.3400"), PgTypes.decode(PgTypes.NUMERIC, b("12.3400")));
        assertEquals("NaN", PgTypes.decode(PgTypes.NUMERIC, b("NaN")));
        assertArrayEquals(new byte[]{(byte) 0xde, (byte) 0xad, 0, 1}, (byte[]) PgTypes.decode(PgTypes.BYTEA, b("\\xdead0001")));
        assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), PgTypes.decode(PgTypes.UUID_OID, b("123e4567-e89b-12d3-a456-426614174000")));
        assertEquals(LocalDate.of(2020, 2, 29), PgTypes.decode(PgTypes.DATE, b("2020-02-29")));
        assertEquals("infinity", PgTypes.decode(PgTypes.DATE, b("infinity")));
        assertEquals(LocalTime.of(3, 4, 5, 123_000_000), PgTypes.decode(PgTypes.TIME, b("03:04:05.123")));
        assertEquals(LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_000), PgTypes.decode(PgTypes.TIMESTAMP, b("2020-01-02 03:04:05.123456")));
        assertEquals("héllo", PgTypes.decode(25, b("héllo")));
        assertEquals("{\"a\": 1}", PgTypes.decode(3802, b("{\"a\": 1}")));
    }

    @Test
    void timestamptzOffsetsOfHoursMinutesAndSeconds() {
        assertEquals(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC), PgTypes.parseTimestamptz("2020-01-02 03:04:05+00"));
        assertEquals(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)), PgTypes.parseTimestamptz("2020-01-02 03:04:05.5+05:30"));
        assertEquals(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHours(-8)), PgTypes.parseTimestamptz("2020-01-02 03:04:05-08"));
        assertEquals(OffsetDateTime.of(1900, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHoursMinutesSeconds(0, -53, -28)), PgTypes.parseTimestamptz("1900-01-02 03:04:05-00:53:28"));
    }

    @Test
    void encodesJavaValuesWithTheirTypes() {
        assertEquals(PgTypes.INT4, PgTypes.encode(5).oid());
        assertEquals(PgTypes.INT8, PgTypes.encode(5L).oid());
        assertEquals(PgTypes.INT2, PgTypes.encode((short) 5).oid());
        assertEquals(0, PgTypes.encode("x").oid());
        assertEquals("t", new String(PgTypes.encode(true).text()));
        assertEquals("\\xcafe", new String(PgTypes.encode(new byte[]{(byte) 0xca, (byte) 0xfe}).text()));
        assertEquals("12.50", new String(PgTypes.encode(new BigDecimal("12.50")).text()));
        assertEquals("2020-01-02", new String(PgTypes.encode(LocalDate.of(2020, 1, 2)).text()));
        assertEquals(PgTypes.TIMESTAMPTZ, PgTypes.encode(Instant.EPOCH).oid());
        assertEquals(null, PgTypes.encode(null).text());
        assertThrows(IllegalArgumentException.class, () -> PgTypes.encode(new Object()));
    }

    @Test
    void parsesIntegersStrictly() {
        assertEquals(0, PgTypes.parseLong(b("0")));
        assertEquals(-123, PgTypes.parseLong(b("-123")));
        assertEquals(Long.MAX_VALUE, PgTypes.parseLong(b("9223372036854775807")));
        assertThrows(NumberFormatException.class, () -> PgTypes.parseLong(b("")));
        assertThrows(NumberFormatException.class, () -> PgTypes.parseLong(b("-")));
        assertThrows(NumberFormatException.class, () -> PgTypes.parseLong(b("12a")));
        assertThrows(ArithmeticException.class, () -> PgTypes.parseLong(b("9223372036854775808")));
    }

    // ---- arrays ----

    @Test
    void decodesArrayLiterals() {
        assertEquals(List.of(1, 2, 3), PgTypes.decode(1007, b("{1,2,3}")));
        assertEquals(List.of(), PgTypes.decode(1007, b("{}")));
        assertEquals(java.util.Arrays.asList(1L, null, 3L), PgTypes.decode(1016, b("{1,NULL,3}")));
        assertEquals(List.of("a b", "c\"d", "e\\f", "NULL", "", "x,y", "{z}"), PgTypes.decode(1009, b("{\"a b\",\"c\\\"d\",\"e\\\\f\",\"NULL\",\"\",\"x,y\",\"{z}\"}")));
        assertEquals(java.util.Arrays.asList("a", null), PgTypes.decode(1009, b("{a, null}")), "unquoted NULL in any case is null, white space around an element is dropped");
        assertEquals(List.of(List.of(1, 2), List.of(3, 4)), PgTypes.decode(1007, b("{{1,2},{3,4}}")));
        assertEquals(List.of(true, false), PgTypes.decode(1000, b("{t,f}")));
        assertEquals(List.of(new BigDecimal("1.50"), new BigDecimal("2")), PgTypes.decode(1231, b("{1.50,2}")));
        assertEquals(List.of(UUID.fromString("123e4567-e89b-12d3-a456-426614174000")), PgTypes.decode(2951, b("{123e4567-e89b-12d3-a456-426614174000}")));
        assertEquals(List.of(LocalDate.of(2020, 1, 2)), PgTypes.decode(1182, b("{2020-01-02}")));
        assertEquals(List.of(LocalDateTime.of(2020, 1, 2, 3, 4, 5)), PgTypes.decode(1115, b("{\"2020-01-02 03:04:05\"}")));
        assertEquals(List.of(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.UTC)), PgTypes.decode(1185, b("{\"2020-01-02 03:04:05+00\"}")));
        assertArrayEquals(new byte[]{1, 2}, (byte[]) ((List<?>) PgTypes.decode(1001, b("{\"\\\\x0102\"}"))).get(0));
        assertEquals(List.of(7, 8), PgTypes.decode(1007, b("[0:1]={7,8}")), "an array that does not start at 1 says so before the braces");
        assertEquals(List.of("{\"a\": 1}"), PgTypes.decode(3807, b("{\"{\\\"a\\\": 1}\"}")));
        assertThrows(IllegalArgumentException.class, () -> PgTypes.decode(1007, b("{1,2")));
    }

    @Test
    void encodesCollectionsAndArraysWithTheirElementTypes() {
        PgTypes.Param ints = PgTypes.encode(List.of(1, 2, 3));
        assertEquals(1007, ints.oid());
        assertEquals("{1,2,3}", new String(ints.text()));
        assertEquals(1016, PgTypes.encode(new long[]{1, 2}).oid());
        assertEquals("{1,2}", new String(PgTypes.encode(new long[]{1, 2}).text()));
        assertEquals(1007, PgTypes.encode(new int[]{4}).oid());
        assertEquals(0, PgTypes.encode(new String[]{"x"}).oid(), "strings carry no type: the server infers it");
        assertEquals("{a,\"b c\",\"q\\\"uote\",\"back\\\\slash\",\"\",\"NULL\",NULL}", new String(PgTypes.encode(java.util.Arrays.asList("a", "b c", "q\"uote", "back\\slash", "", "NULL", null)).text()));
        assertEquals("{{1,2},{3,4}}", new String(PgTypes.encode(List.of(List.of(1, 2), List.of(3, 4))).text()));
        assertEquals(1007, PgTypes.encode(List.of(List.of(1, 2), List.of(3, 4))).oid());
        assertEquals("{}", new String(PgTypes.encode(List.of()).text()));
        assertEquals(0, PgTypes.encode(List.of()).oid());
        assertEquals("{t,f}", new String(PgTypes.encode(new boolean[]{true, false}).text()));
        assertEquals(1000, PgTypes.encode(new boolean[]{true}).oid());
        assertEquals(2951, PgTypes.encode(List.of(UUID.randomUUID())).oid());
        assertEquals(1182, PgTypes.encode(List.of(LocalDate.of(2020, 1, 2))).oid());
        assertEquals(1185, PgTypes.encode(List.of(Instant.EPOCH)).oid());
        assertEquals(1231, PgTypes.encode(List.of(new BigDecimal("1.5"))).oid());
        assertEquals(1001, PgTypes.encode(List.of(new byte[]{(byte) 0xca})).oid());
        assertEquals("{\"\\\\xca\"}", new String(PgTypes.encode(List.of(new byte[]{(byte) 0xca})).text()));
        assertEquals(17, PgTypes.encode(new byte[]{1}).oid(), "a byte[] is still bytea, not an array of numbers");
    }

    @Test
    void whatIsEncodedDecodesToTheSame() {
        List<String> tricky = List.of("plain", "with space", "comma,here", "{braces}", "quote\"", "back\\slash", "", "NULL", "null", "ünï", "tab\there", "new\nline");
        PgTypes.Param p = PgTypes.encode(tricky);
        assertEquals(tricky, PgTypes.decodeArray(25, new String(p.text(), StandardCharsets.UTF_8)));
    }

    // ---- the binary format ----

    private static Object roundTrip(Object value, int oid) {
        byte[] bin = PgTypes.encodeBinary(value, oid);
        return bin == null ? null : PgTypes.decodeBinary(oid, bin);
    }

    @Test
    void numericRoundTripsThroughTheBinaryForm() {
        String[] numbers = {"0", "0.00", "1", "-1", "0.5", "-0.5", "0.0001", "0.00000001", "10000", "100000000", "99999999.99999999", "12.3400", "1234567890123456789012345678901234567890.123456789012345678901234567890",
                "-0.000000000000000000000000000001", "1E+3", "100", "0.1", "9999", "10000.0001", "123456789"};
        for (String n : numbers) {
            BigDecimal d = new BigDecimal(n);
            Object back = roundTrip(d, PgTypes.NUMERIC);
            BigDecimal expected = d.scale() < 0 ? d.setScale(0) : d;      // a negative scale (1E+3) has no binary form: PostgreSQL's numeric has none either
            assertEquals(expected, back, n);
            assertEquals(expected.toPlainString(), PgTypes.textOfBinary(PgTypes.NUMERIC, PgTypes.encodeBinary(d, PgTypes.NUMERIC)), n);
        }
        assertEquals("NaN", PgTypes.decodeNumeric(new byte[]{0, 0, 0, 0, (byte) 0xC0, 0, 0, 0}));
    }

    @Test
    void scalarsRoundTripThroughTheBinaryForm() {
        assertEquals(true, roundTrip(true, PgTypes.BOOL));
        assertEquals(false, roundTrip(false, PgTypes.BOOL));
        assertEquals((short) -32768, roundTrip(-32768, PgTypes.INT2));
        assertEquals(Integer.MIN_VALUE, roundTrip(Integer.MIN_VALUE, PgTypes.INT4));
        assertEquals(Long.MAX_VALUE, roundTrip(Long.MAX_VALUE, PgTypes.INT8));
        assertEquals(7L, roundTrip(7, PgTypes.INT8), "an Integer for an int8 parameter");
        assertEquals(null, PgTypes.encodeBinary(40000, PgTypes.INT2), "out of range: left to the text form so the server says so");
        assertEquals(null, PgTypes.encodeBinary(5_000_000_000L, PgTypes.INT4));
        assertEquals(null, PgTypes.encodeBinary("12", PgTypes.INT4), "a String is not converted: the text form lets the server parse it");
        assertEquals(null, PgTypes.encodeBinary(null, PgTypes.INT4));
        UUID u = UUID.randomUUID();
        assertEquals(u, roundTrip(u, PgTypes.UUID_OID));
        assertArrayEquals(new byte[]{0, 1, (byte) 0xff}, (byte[]) roundTrip(new byte[]{0, 1, (byte) 0xff}, PgTypes.BYTEA));
        assertEquals("\\x0001ff", PgTypes.textOfBinary(PgTypes.BYTEA, new byte[]{0, 1, (byte) 0xff}));
        assertEquals("t", PgTypes.textOfBinary(PgTypes.BOOL, new byte[]{1}));
    }

    @Test
    void arraysRoundTripThroughTheBinaryForm() {
        assertEquals(List.of(1, 2, 3), roundTrip(List.of(1, 2, 3), 1007));
        assertEquals(java.util.Arrays.asList(1L, null, 3L), roundTrip(java.util.Arrays.asList(1L, null, 3L), 1016));
        assertEquals(List.of(List.of(1, 2), List.of(3, 4)), roundTrip(List.of(List.of(1, 2), List.of(3, 4)), 1007));
        assertEquals(List.of(), roundTrip(List.of(), 1007));
        assertEquals(List.of(true, false), roundTrip(new boolean[]{true, false}, 1000));
        assertEquals(List.of("a b", "c\"d", "", "NULL", "ünï"), roundTrip(List.of("a b", "c\"d", "", "NULL", "ünï"), 1009));
        assertEquals(java.util.Arrays.asList("x", null), roundTrip(java.util.Arrays.asList("x", null), 1009));
        assertEquals(List.of(new BigDecimal("1.50"), new BigDecimal("2")), roundTrip(List.of(new BigDecimal("1.50"), new BigDecimal("2")), 1231));
        UUID u = UUID.randomUUID();
        assertEquals(List.of(u), roundTrip(List.of(u), 2951));
        assertEquals(null, PgTypes.encodeBinary(List.of(List.of(1, 2), List.of(3)), 1007), "not rectangular: left to the server's text parser, which says why");
        assertEquals(null, PgTypes.encodeBinary(List.of("a"), 1007), "a String in an int[] is left to the text form");
        assertEquals(null, PgTypes.encodeBinary(List.of(1), 1009), "and a number in a text[] too");
        assertEquals("{1,NULL,3}", PgTypes.textOfBinary(1016, PgTypes.encodeBinary(java.util.Arrays.asList(1L, null, 3L), 1016)));
        assertEquals("{\"a b\",\"c\\\"d\",\"\",\"NULL\",NULL}", PgTypes.textOfBinary(1009, PgTypes.encodeBinary(java.util.Arrays.asList("a b", "c\"d", "", "NULL", null), 1009)));
        assertEquals("{{1,2},{3,4}}", PgTypes.textOfBinary(1007, PgTypes.encodeBinary(List.of(List.of(1, 2), List.of(3, 4)), 1007)));
        assertEquals("{}", PgTypes.textOfBinary(1007, PgTypes.encodeBinary(List.of(), 1007)));
    }

    @Test
    void dateAndTimeTypesAreSentInBinary() {
        byte[] d = PgTypes.encodeBinary(LocalDate.of(2000, 1, 2), PgTypes.DATE);
        assertEquals(1, java.nio.ByteBuffer.wrap(d).getInt(), "days since 2000-01-01");
        assertEquals(-1, java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(LocalDate.of(1999, 12, 31), PgTypes.DATE)).getInt());
        assertEquals(1_500_000L, java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(LocalTime.of(0, 0, 1, 500_000_000), PgTypes.TIME)).getLong(), "microseconds since midnight");
        assertEquals(86_400_000_000L + 5L, java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(LocalDateTime.of(2000, 1, 2, 0, 0, 0, 5000), PgTypes.TIMESTAMP)).getLong());
        assertEquals(3_600_000_000L, java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(Instant.parse("2000-01-01T01:00:00Z"), PgTypes.TIMESTAMPTZ)).getLong());
        assertEquals(java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(Instant.parse("2000-01-01T01:00:00Z"), PgTypes.TIMESTAMPTZ)).getLong(),
                java.nio.ByteBuffer.wrap(PgTypes.encodeBinary(OffsetDateTime.of(2000, 1, 1, 8, 0, 0, 0, ZoneOffset.ofHours(7)), PgTypes.TIMESTAMPTZ)).getLong(), "the instant, whatever the offset");
        assertEquals(null, PgTypes.encodeBinary(LocalDateTime.of(2000, 1, 1, 0, 0), PgTypes.TIMESTAMPTZ), "a LocalDateTime has no zone: the text form lets the server apply its own");
    }
}
