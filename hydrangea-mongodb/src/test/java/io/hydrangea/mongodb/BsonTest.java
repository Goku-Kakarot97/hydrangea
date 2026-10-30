package io.hydrangea.mongodb;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class BsonTest {

    @Test
    void theSpecExample() {
        // {"hello": "world"} from the BSON spec
        byte[] b = Bson.encode(Bson.doc("hello", "world"));
        assertArrayEquals(new byte[]{0x16, 0, 0, 0, 0x02, 'h', 'e', 'l', 'l', 'o', 0, 0x06, 0, 0, 0, 'w', 'o', 'r', 'l', 'd', 0, 0}, b);
        assertEquals(Map.of("hello", "world"), Bson.decode(b));
        // {"BSON": ["awesome", 5.05, 1986]}
        byte[] arr = Bson.encode(Bson.doc("BSON", List.of("awesome", 5.05, 1986)));
        assertEquals(49, arr.length);
        assertEquals(List.of("awesome", 5.05, 1986), Bson.decode(arr).get("BSON"));
    }

    @Test
    void everyTypeRoundTrips() {
        ObjectId id = ObjectId.generate();
        UUID uuid = UUID.randomUUID();
        Instant now = Instant.ofEpochMilli(System.currentTimeMillis());
        Map<String, Object> doc = Bson.doc(
                "_id", id, "d", 1.5, "s", "ü\u0000x", "doc", Bson.doc("a", 1), "arr", List.of(1, 2L, "three"), "bin", new byte[]{1, 2, 3}, "uuid", uuid,
                "other", new BsonBinary(0x80, new byte[]{9}), "b", true, "t", now, "n", null, "re", new BsonRegex("^a.*", "i"), "i", 42, "ts", new BsonTimestamp(1700000000, 7),
                "l", 1L << 40, "dec", new BigDecimal("12345.6789"), "min", Bson.MIN_KEY, "max", Bson.MAX_KEY, "empty", Bson.doc(), "nested", List.of(List.of(Bson.doc("x", List.of()))));
        Map<String, Object> back = Bson.decode(Bson.encode(doc));
        assertEquals(id, back.get("_id"));
        assertEquals(1.5, back.get("d"));
        assertEquals("ü\u0000x", back.get("s"), "a string may contain NUL (it is length-prefixed)");
        assertEquals(Map.of("a", 1), back.get("doc"));
        assertEquals(List.of(1, 2L, "three"), back.get("arr"));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) back.get("bin"));
        assertEquals(uuid, back.get("uuid"));
        assertEquals(0x80, ((BsonBinary) back.get("other")).subtype());
        assertEquals(true, back.get("b"));
        assertEquals(now, back.get("t"));
        assertTrue(back.containsKey("n"));
        assertNull(back.get("n"));
        assertEquals(new BsonRegex("^a.*", "i"), back.get("re"));
        assertEquals(42, back.get("i"));
        assertEquals(new BsonTimestamp(1700000000, 7), back.get("ts"));
        assertEquals(1L << 40, back.get("l"));
        assertEquals(0, new BigDecimal("12345.6789").compareTo((BigDecimal) back.get("dec")));
        assertEquals(Bson.MIN_KEY, back.get("min"));
        assertEquals(Bson.MAX_KEY, back.get("max"));
        assertEquals(Map.of(), back.get("empty"));
        assertEquals(List.of(List.of(Map.of("x", List.of()))), back.get("nested"));
        assertEquals(List.copyOf(doc.keySet()), List.copyOf(back.keySet()), "field order is kept");
    }

    @Test
    void javaTypesAreWrittenAsTheirBsonKind() {
        Map<String, Object> back = Bson.decode(Bson.encode(Bson.doc(
                "f", 2.5f, "sh", (short) 3, "by", (byte) 4, "ch", 'c', "e", java.time.DayOfWeek.MONDAY, "date", new java.util.Date(1000), "ld", java.time.LocalDate.of(2020, 1, 2),
                "ldt", java.time.LocalDateTime.of(2020, 1, 2, 3, 4, 5), "odt", java.time.OffsetDateTime.parse("2020-01-02T03:04:05+07:00"), "p", Pattern.compile("x", Pattern.CASE_INSENSITIVE | Pattern.MULTILINE),
                "bigSmall", BigInteger.valueOf(7), "bigBig", new BigInteger("123456789012345678901234567890"), "opt", java.util.Optional.of(1), "none", java.util.Optional.empty(),
                "ints", new int[]{1, 2}, "strs", new String[]{"a"}, "set", java.util.Set.of("only"))));
        assertEquals(2.5, back.get("f"));
        assertEquals(3, back.get("sh"));
        assertEquals(4, back.get("by"));
        assertEquals("c", back.get("ch"));
        assertEquals("MONDAY", back.get("e"));
        assertEquals(Instant.ofEpochMilli(1000), back.get("date"));
        assertEquals(Instant.parse("2020-01-02T00:00:00Z"), back.get("ld"));
        assertEquals(Instant.parse("2020-01-02T03:04:05Z"), back.get("ldt"));
        assertEquals(Instant.parse("2020-01-01T20:04:05Z"), back.get("odt"));
        assertEquals(new BsonRegex("x", "im"), back.get("p"));
        assertEquals(7L, back.get("bigSmall"));
        assertEquals(0, new BigDecimal("123456789012345678901234567890").compareTo((BigDecimal) back.get("bigBig")));
        assertEquals(1, back.get("opt"));
        assertNull(back.get("none"));
        assertEquals(List.of(1, 2), back.get("ints"));
        assertEquals(List.of("a"), back.get("strs"));
        assertEquals(List.of("only"), back.get("set"));
    }

    private static byte[] dec(long lo, long hi) {
        ByteBuffer b = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        b.putLong(lo).putLong(hi);
        return b.array();
    }

    private static byte[] bytesOf(Object v) {
        byte[] doc = Bson.encode(Bson.doc("v", v));
        return java.util.Arrays.copyOfRange(doc, 4 + 1 + 2, doc.length - 1);     // past the length, the type, "v\0"; before the final 0
    }

    @Test
    void decimal128MatchesTheKnownEncodings() {
        // from the BSON corpus: 1, 0.1, -0, 1E+3, 1E-6176 (smallest), 9.999999999999999999999999999999999E+6144 (largest)
        assertArrayEquals(dec(1L, 0x3040000000000000L), bytesOf(new BigDecimal("1")));
        assertArrayEquals(dec(1L, 0x303E000000000000L), bytesOf(new BigDecimal("0.1")));
        assertArrayEquals(dec(1000L, 0x3040000000000000L), bytesOf(new BigDecimal("1000")));
        assertArrayEquals(dec(100000L, 0x303C000000000000L), bytesOf(new BigDecimal("1E+3").setScale(2)), "1000.00: coefficient 100000, exponent -2 (the scale is kept)");
        assertArrayEquals(dec(1L, 0x0000000000000000L), bytesOf(new BigDecimal("1E-6176")));
        assertArrayEquals(dec(0x378D8E63FFFFFFFFL, 0x5FFFED09BEAD87C0L), bytesOf(new BigDecimal("9.999999999999999999999999999999999E+6144")));
        assertEquals(0, new BigDecimal("-0.00123").compareTo((BigDecimal) Bson.decode(Bson.encode(Bson.doc("v", new BigDecimal("-0.00123")))).get("v")));
        assertThrows(IllegalArgumentException.class, () -> Bson.encode(Bson.doc("v", new BigDecimal("1.2345678901234567890123456789012345"))), "35 digits");
        // reading the special values and the non-canonical large form
        assertEquals(Double.NaN, Bson.Reader.decimal128(0, 0x7C00000000000000L));
        assertEquals(Double.POSITIVE_INFINITY, Bson.Reader.decimal128(0, 0x7800000000000000L));
        assertEquals(Double.NEGATIVE_INFINITY, Bson.Reader.decimal128(0, 0xF800000000000000L));
        assertEquals(0, BigDecimal.ZERO.compareTo((BigDecimal) Bson.Reader.decimal128(0, 0x6000000000000000L)), "large-coefficient form reads as zero, as MongoDB does");
    }

    @Test
    void malformedBytesAreRefusedNotTrusted() {
        byte[] good = Bson.encode(Bson.doc("a", "b", "n", Bson.doc("x", 1)));
        for (int cut = 0; cut < good.length; cut++) {
            byte[] truncated = java.util.Arrays.copyOf(good, cut);
            assertThrows(IllegalArgumentException.class, () -> Bson.decode(truncated), "cut at " + cut);
        }
        byte[] hugeLength = good.clone();
        hugeLength[0] = (byte) 0xFF;
        hugeLength[1] = (byte) 0xFF;
        hugeLength[2] = (byte) 0xFF;
        hugeLength[3] = 0x7F;
        assertThrows(IllegalArgumentException.class, () -> Bson.decode(hugeLength));
        byte[] negativeStringLength = good.clone();
        negativeStringLength[7] = (byte) 0xFF;                     // the string's length: -1
        negativeStringLength[8] = (byte) 0xFF;
        negativeStringLength[9] = (byte) 0xFF;
        negativeStringLength[10] = (byte) 0xFF;
        assertThrows(IllegalArgumentException.class, () -> Bson.decode(negativeStringLength));
        byte[] unknownType = good.clone();
        unknownType[4] = 0x42;
        assertThrows(IllegalArgumentException.class, () -> Bson.decode(unknownType));
        assertThrows(IllegalArgumentException.class, () -> Bson.encode(Bson.doc("bad\0key", 1)), "NUL in a key");
        Map<String, Object> deep = Bson.doc("x", 1);
        for (int i = 0; i < 300; i++) {
            deep = Bson.doc("d", deep);
        }
        Map<String, Object> tooDeep = deep;
        assertThrows(IllegalArgumentException.class, () -> Bson.encode(tooDeep), "nested too deeply");
        assertThrows(IllegalArgumentException.class, () -> Bson.encode(Bson.doc("o", new Object())));
    }

    enum Status { ACTIVE, GONE }

    record Address(String city, @Bson.Name("zip_code") String zip) {
    }

    record Person(@Bson.Name("_id") ObjectId id, String name, int age, Status status, Instant since, Address address, List<String> tags, Map<String, Integer> scores, byte[] photo, UUID device, BigDecimal balance) {
    }

    @Test
    void recordsGoInAndOut() {
        Person p = new Person(ObjectId.generate(), "An", 30, Status.ACTIVE, Instant.ofEpochMilli(1700000000000L), new Address("Hanoi", "100000"), List.of("a", "b"), Map.of("math", 9), new byte[]{1}, UUID.randomUUID(), new BigDecimal("10.50"));
        Map<String, Object> doc = Bson.fromRecord(p);
        assertEquals(p.id(), doc.get("_id"));
        assertEquals("ACTIVE", doc.get("status"));
        assertEquals("100000", ((Map<?, ?>) doc.get("address")).get("zip_code"));
        Person back = Bson.toRecord(Person.class, Bson.decode(Bson.encode(p)));
        assertEquals(p.id(), back.id());
        assertEquals(p.name(), back.name());
        assertEquals(p.age(), back.age());
        assertEquals(p.status(), back.status());
        assertEquals(p.since(), back.since(), "an Instant component takes the decoded datetime as it is");
        assertEquals(p.address(), back.address());
        assertEquals(p.tags(), back.tags());
        assertEquals(p.scores(), back.scores());
        assertArrayEquals(p.photo(), back.photo());
        assertEquals(p.device(), back.device());
        assertEquals(0, p.balance().compareTo(back.balance()));
        Person sparse = Bson.toRecord(Person.class, Bson.doc("name", "x", "extra", 1));
        assertNull(sparse.id());
        assertEquals(0, sparse.age());
        assertNull(sparse.tags());
    }

    @Test
    void objectIds() {
        ObjectId a = ObjectId.generate(), b = ObjectId.generate();
        assertNotEquals(a, b);
        assertEquals(24, a.toHexString().length());
        assertEquals(a, ObjectId.of(a.toHexString()));
        assertEquals(a, ObjectId.of(a.toBytes()));
        assertTrue(a.compareTo(b) < 0, "ids made in a row are increasing");
        assertTrue(Math.abs(a.timestamp().getEpochSecond() - Instant.now().getEpochSecond()) < 5);
        assertTrue(ObjectId.isValid("507f1f77bcf86cd799439011"));
        assertFalse(ObjectId.isValid("507f1f77bcf86cd79943901z"));
        assertThrows(IllegalArgumentException.class, () -> ObjectId.of("short"));
    }
}
