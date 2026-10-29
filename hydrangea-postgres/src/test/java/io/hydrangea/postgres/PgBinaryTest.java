package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The binary format must change nothing a caller can see. Every query here runs on a client with binary transfer (three times: the first run is text, the later ones binary
 * for what the server described) and on a client without it, and the rows must be the same under every accessor, {@code getString} included.
 */
class PgBinaryTest {

    private EventLoopGroup group;
    private PgClient bin;
    private PgClient txt;
    private final Map<String, DoubleSupplier> meters = new HashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        Pg.require(Pg.PORT);
        group = new EventLoopGroup(2);
        group.metrics().bindTo(new io.hydrangea.MetricsSink() {
            public void counterFunction(String name, String unit, DoubleSupplier total, String... tags) {
                meters.put(name, total);
            }

            public void gauge(String name, String unit, DoubleSupplier value, String... tags) {
                meters.put(name, value);
            }

            public Counter counter(String name, String unit, String... tags) {
                return NOOP.counter(name, unit, tags);
            }

            public Timer timer(String name, String... tags) {
                return NOOP.timer(name, tags);
            }
        });
        bin = Pg.get(PgClient.connect(group, Pg.config()));
        txt = Pg.get(PgClient.connect(group, Pg.config().withBinaryTransfer(false)));
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private static Object norm(Object o) {
        if (o instanceof byte[] b) {
            return "bytes:" + Arrays.toString(b);
        }
        if (o instanceof List<?> l) {
            List<Object> out = new ArrayList<>();
            for (Object x : l) {
                out.add(norm(x));
            }
            return out;
        }
        return o;
    }

    private static void sameRows(String what, PgResult expected, PgResult actual) {
        assertEquals(expected.columns(), actual.columns(), what + ": columns");
        assertEquals(expected.size(), actual.size(), what + ": row count");
        for (int r = 0; r < expected.size(); r++) {
            PgRow e = expected.rows().get(r), a = actual.rows().get(r);
            for (int c = 0; c < e.size(); c++) {
                String where = what + " row " + r + " col " + c + " (" + expected.columns().get(c).name() + ")";
                assertEquals(e.isNull(c), a.isNull(c), where + ": null");
                assertEquals(norm(e.get(c)), norm(a.get(c)), where + ": get");
                assertEquals(e.getString(c), a.getString(c), where + ": getString");
            }
        }
    }

    /** Runs the query on the binary client three times and on the text client once; every run must equal the text one. Returns the third run of the binary client. */
    private PgResult same(String sql, Object... params) throws Exception {
        PgResult text = Pg.get(txt.query(sql, params));
        PgResult first = Pg.get(bin.query(sql, params));
        PgResult second = Pg.get(bin.query(sql, params));
        PgResult third = Pg.get(bin.query(sql, params));
        sameRows(sql + " (first run)", text, first);
        sameRows(sql + " (second run)", text, second);
        sameRows(sql + " (third run)", text, third);
        return third;
    }

    private static int binaryColumns(PgResult r) {
        int n = 0;
        for (int c = 0; c < r.columns().size(); c++) {
            if (r.first() != null && r.first().wasBinary(c)) {
                n++;
            }
        }
        return n;
    }

    // ---- results ----

    @Test
    void integersBooleansAndTheirNulls() throws Exception {
        PgResult r = same("SELECT $1::int2, $2::int4, $3::int8, $4::bool, $5::bool, $6::int4", (short) -32768, Integer.MAX_VALUE, Long.MIN_VALUE, true, false, null);
        assertEquals(6, binaryColumns(r), "every one of them came in binary");
        assertEquals(-32768, r.first().getInt(0));
        assertEquals(Integer.MAX_VALUE, r.first().getLong(1));
        assertEquals(Long.MIN_VALUE, r.first().getLong(2));
        assertTrue(r.first().getBoolean(3));
        assertFalse(r.first().getBoolean(4));
        assertTrue(r.first().isNull(5));
        assertEquals(42.0, same("SELECT 42::int4").first().getDouble(0));
        assertEquals(new BigDecimal(7), same("SELECT 7::int8").first().getBigDecimal(0));
        assertEquals("7", same("SELECT 7::int2").first().getString(0));
    }

    @Test
    void numericInAllItsShapes() throws Exception {
        PgResult r = same("SELECT 0::numeric, 0.00::numeric, 1::numeric, (-1)::numeric, 123456789012345678901234567890.123456789012345678901234567890::numeric, 1e10::numeric,"
                + " 0.0001::numeric, 0.00000001::numeric, 10000::numeric, 100000000::numeric, 99999999.99999999::numeric, (-0.5)::numeric, 'NaN'::numeric, 12.3400::numeric,"
                + " 1e-20::numeric, 1e20::numeric, 0.1::numeric, 9999.9999::numeric, 10000.0001::numeric, 1e1000::numeric");
        assertEquals(20, binaryColumns(r));
        assertEquals(new BigDecimal("12.3400"), r.first().getBigDecimal(13));
        assertEquals("NaN", r.first().getString(12));
        assertEquals("NaN", r.first().get(12));
        assertEquals(new BigDecimal("0.00"), r.first().get(1));
        Object infinite = same("SELECT 'Infinity'::numeric, '-Infinity'::numeric").first().get(0);   // PostgreSQL 14 and later
        assertEquals("Infinity", infinite);
    }

    @Test
    void byteaWithZerosLargeAndEmpty() throws Exception {
        byte[] big = new byte[300_000];
        new Random(5).nextBytes(big);
        PgResult r = same("SELECT $1::bytea, $2::bytea, $3::bytea", new byte[0], new byte[]{0, 1, 0, (byte) 0xff}, big);
        assertEquals(3, binaryColumns(r));
        assertArrayEquals(new byte[0], r.first().getBytes(0));
        assertArrayEquals(new byte[]{0, 1, 0, (byte) 0xff}, r.first().getBytes(1));
        assertArrayEquals(big, r.first().getBytes(2));
        assertEquals("\\x00010 0ff".replace(" ", ""), r.first().getString(1));
    }

    @Test
    void uuids() throws Exception {
        UUID u = UUID.randomUUID();
        PgResult r = same("SELECT $1::uuid, '00000000-0000-0000-0000-000000000000'::uuid", u);
        assertEquals(2, binaryColumns(r));
        assertEquals(u, r.first().getUuid(0));
        assertEquals(u.toString(), r.first().getString(0));
    }

    @Test
    void arraysOfEverySupportedKind() throws Exception {
        PgResult r = same("SELECT $1::int[], $2::bigint[], $3::bool[], $4::numeric[], $5::text[], $6::uuid[], $7::int[], $8::text[], $9::int[]",
                List.of(1, 2, 3), Arrays.asList(1L, null, Long.MAX_VALUE), List.of(true, false), List.of(new BigDecimal("1.50"), new BigDecimal("-2"), new BigDecimal("0.001")),
                Arrays.asList("a b", "c\"d", "e\\f", "", "NULL", "x,y", "{z}", "ünï", null), List.of(UUID.randomUUID(), UUID.randomUUID()), List.of(List.of(1, 2, 3), List.of(4, 5, 6)),
                List.of(List.of("a", "b"), List.of("c d", "NULL")), List.of());
        assertEquals(9, binaryColumns(r));
        assertEquals(List.of(1, 2, 3), r.first().getList(0));
        assertEquals(Arrays.asList("a b", "c\"d", "e\\f", "", "NULL", "x,y", "{z}", "ünï", null), r.first().getList(4));
        assertEquals(List.of(List.of(1, 2, 3), List.of(4, 5, 6)), r.first().getList(6));
        assertEquals(List.of(), r.first().getList(8));
        // literals the server writes with its own decorations
        PgResult lit = same("SELECT '[0:2]={1,2,3}'::int[], '{}'::int[], ARRAY[ARRAY['a','b'],ARRAY['c d','NULL']], '[2:3]={x,y}'::text[], ARRAY[NULL::int]");
        assertEquals(5, binaryColumns(lit));
        assertEquals("[0:2]={1,2,3}", lit.first().getString(0));
        assertEquals(List.of(1, 2, 3), lit.first().getList(0));
    }

    @Test
    void aRowMixingBinaryAndTextColumns() throws Exception {
        PgResult r = same("SELECT 1::int4 a, 1.5::float8 b, '2020-01-02 03:04:05+00'::timestamptz c, '{\"x\": 1}'::jsonb d, 'text'::text e, ARRAY[1.5,2.5]::float8[] f, 7::int8 g,"
                + " '2020-01-02'::date h, interval '1 day 2 hours' i, 1.25::float4 j, 'abc'::varchar k");
        assertEquals(2, binaryColumns(r), "the integers only: the rest stays in text, where its form is the server's");
        assertTrue(r.first().wasBinary(0) && r.first().wasBinary(6));
        assertFalse(r.first().wasBinary(1) || r.first().wasBinary(2) || r.first().wasBinary(3) || r.first().wasBinary(5));
        assertEquals(1.5, r.first().getDouble(1));
    }

    // ---- parameters ----

    @Test
    void everySupportedParameterTypeGivesTheSameAnswerAsTheTextForm() throws Exception {
        same("SELECT $1::date, $2::time, $3::timestamp, $4::timestamptz, $5::float4, $6::float8, $7::numeric, $8::text[], $9::date[], $10::timestamptz[], $11::float8[]",
                LocalDate.of(2024, 2, 29), LocalTime.of(23, 59, 59, 123_456_000), LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999_999_000), Instant.parse("2000-01-01T00:00:00.123456Z"),
                1.25f, -2.5e300, new BigDecimal("12345.678900"), List.of("p", "q r"), List.of(LocalDate.of(1900, 1, 1), LocalDate.of(9999, 12, 31)),
                List.of(OffsetDateTime.of(2020, 1, 2, 3, 4, 5, 0, ZoneOffset.ofHours(7)).toInstant()), List.of(1.5, -2.25));
        // sub-microsecond parts are rounded to the nearest microsecond, as the server does with the text form
        same("SELECT $1::timestamptz, $2::time, $3::timestamp", Instant.parse("2020-01-02T03:04:05.123456789Z"), LocalTime.of(1, 2, 3, 123_456_400), LocalDateTime.of(2020, 1, 2, 3, 4, 5, 999_999_700));
        // values that cannot be sent in binary for the type the server gave fall back to text, and the server answers as it always did
        same("SELECT $1::int + 1", "41");
        same("SELECT $1::bigint", 5);
        same("SELECT $1::numeric", 5L);
    }

    @Test
    void anOutOfRangeParameterFailsTheSameWayInBothForms() throws Exception {
        for (PgClient c : new PgClient[]{txt, bin, bin, bin}) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT $1::int2", 40000).get(10, TimeUnit.SECONDS));
            assertEquals("22003", ((PgException) e.getCause()).sqlState());
        }
        for (PgClient c : new PgClient[]{txt, bin, bin}) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.query("SELECT $1::int4", 5_000_000_000L).get(10, TimeUnit.SECONDS));
            assertTrue(((PgException) e.getCause()).sqlState() != null);
        }
    }

    @Test
    void largeArraysAndALargeNumberOfRows() throws Exception {
        List<Integer> ints = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            ints.add(i);
            names.add("name " + i);
        }
        PgResult r = same("SELECT $1::int[], $2::text[]", ints, names);
        assertEquals(ints, r.first().getList(0));
        assertEquals(names, r.first().getList(1));
        PgResult rows = same("SELECT g::int4, g::int8, (g * 1.5)::numeric, 'row ' || g, g % 2 = 0 FROM generate_series(1, 20000) g");
        assertEquals(20000, rows.size());
        assertEquals(4, binaryColumns(rows));
    }

    @Test
    void streamsBatchesAndNullsInBinary() throws Exception {
        Pg.notOnRemote();                      // two sessions look at the table: a real one
        String t = Pg.table("bn");
        Pg.get(txt.execute("CREATE TABLE " + t + " (id int, big bigint, n numeric, u uuid, b bytea, flag bool, tags text[], nums int[])"));
        try {
            streamsBatchesAndNullsInBinary(t);
        } finally {
            Pg.get(txt.execute("DROP TABLE " + t));
        }
    }

    private void streamsBatchesAndNullsInBinary(String t) throws Exception {
        // the batch and the stream use the same statement, so after the first set they run in binary
        List<Object[]> sets = new ArrayList<>();
        Random rnd = new Random(2);
        for (int i = 0; i < 300; i++) {
            byte[] blob = new byte[rnd.nextInt(50)];
            rnd.nextBytes(blob);
            sets.add(new Object[]{i, i * 10_000_000_000L, new BigDecimal(i).divide(new BigDecimal(7), 10, java.math.RoundingMode.HALF_UP), UUID.nameUUIDFromBytes(new byte[]{(byte) i}),
                    blob, i % 3 == 0 ? null : i % 2 == 0, List.of("t" + i, "x y"), i % 5 == 0 ? null : List.of(i, i + 1)});
        }
        long[] counts = Pg.get(bin.executeBatch("INSERT INTO " + t + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", sets));
        assertEquals(300, counts.length);
        PgResult text = Pg.get(txt.query("SELECT * FROM " + t + " ORDER BY id"));
        assertEquals(300, text.size());
        for (int i = 0; i < 300; i++) {
            assertEquals(i, text.rows().get(i).getInt("id"));
            assertEquals(i * 10_000_000_000L, text.rows().get(i).getLong("big"));
            assertArrayEquals((byte[]) sets.get(i)[4], text.rows().get(i).getBytes("b"));
            assertEquals(sets.get(i)[5] == null, text.rows().get(i).isNull("flag"));
            assertEquals(sets.get(i)[2], text.rows().get(i).getBigDecimal("n"));
        }
        // read the same rows as a stream in binary, and compare
        Pg.get(bin.query("SELECT * FROM " + t + " ORDER BY id"));          // describes the statement
        List<PgRow> streamed = new ArrayList<>();
        PgRowStream stream = Pg.get(bin.stream(70, "SELECT * FROM " + t + " ORDER BY id"));
        Pg.get(stream.forEach(streamed::add));
        Pg.get(bin.stream(70, "SELECT * FROM " + t + " ORDER BY id")).forEach(streamed::add).get(10, TimeUnit.SECONDS);
        assertEquals(600, streamed.size());
        for (int i = 0; i < 300; i++) {
            PgRow a = text.rows().get(i), b = streamed.get(300 + i);
            for (int c = 0; c < a.size(); c++) {
                assertEquals(norm(a.get(c)), norm(b.get(c)), "streamed row " + i + " col " + c);
                assertEquals(a.getString(c), b.getString(c), "streamed row " + i + " col " + c + " as text");
            }
        }
        assertTrue(streamed.get(300).wasBinary(0), "the second stream ran in binary");
    }

    @Test
    void aCallerMayReuseItsListsArraysAndBytesTheMomentTheCallReturns() throws Exception {
        // the same list, int[] and byte[] are changed after every call, while the queries are still queued behind each other: each query must carry what they held when it was made
        List<Integer> list = new ArrayList<>();
        int[] ints = new int[3];
        byte[] bytes = new byte[4];
        List<CompletableFuture<PgResult>> futures = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < 3; round++) {                 // the first round runs in text, the later ones in binary
            for (int i = 0; i < 1500; i++) {
                list.clear();
                list.add(i);
                list.add(i + 1);
                Arrays.fill(ints, i);
                Arrays.fill(bytes, (byte) i);
                futures.add(bin.query("SELECT $1::int[], $2::int[], $3::bytea", list, ints, bytes));
                expected.add(List.of(i, i + 1) + "|" + Arrays.toString(new int[]{i, i, i}) + "|" + Arrays.toString(new byte[]{(byte) i, (byte) i, (byte) i, (byte) i}));
                list.set(0, -1);                                  // and right after the call, spoil them
                ints[0] = -1;
                bytes[0] = -1;
            }
        }
        for (int i = 0; i < futures.size(); i++) {
            PgRow r = Pg.get(futures.get(i)).first();
            String got = r.getList(0) + "|" + Arrays.toString(r.getList(1, Integer.class).stream().mapToInt(Integer::intValue).toArray()) + "|" + Arrays.toString(r.getBytes(2));
            assertEquals(expected.get(i), got, "query " + i);
        }
    }

    // ---- counters and the switch ----

    @Test
    void theCountersSeeBinaryAndTheSwitchTurnsItOff() throws Exception {
        for (int i = 0; i < 5; i++) {
            Pg.get(bin.query("SELECT $1::int4, $2::bytea, $3::int[]", i, new byte[]{1}, List.of(1, 2)));
        }
        assertTrue(meters.get("pg.binary.parameters").getAsDouble() >= 12, "3 parameters on each of the 4 runs after the first");
        assertTrue(meters.get("pg.binary.columns").getAsDouble() >= 12);
        double params = meters.get("pg.binary.parameters").getAsDouble(), columns = meters.get("pg.binary.columns").getAsDouble();
        for (int i = 0; i < 5; i++) {
            Pg.get(txt.query("SELECT $1::int4, $2::bytea, $3::int[]", i, new byte[]{1}, List.of(1, 2)));
        }
        assertEquals(params, meters.get("pg.binary.parameters").getAsDouble(), "the client without binary transfer sent none");
        assertEquals(columns, meters.get("pg.binary.columns").getAsDouble());
        assertFalse(Pg.get(txt.query("SELECT 1::int4")).first().wasBinary(0));
        Pg.get(txt.query("SELECT 1::int4"));
        assertFalse(Pg.get(txt.query("SELECT 1::int4")).first().wasBinary(0));
    }

    @Test
    void aStatementCacheOfZeroIsAlwaysText() throws Exception {
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config().withStatementCacheSize(0)))) {
            for (int i = 0; i < 3; i++) {
                PgResult r = Pg.get(c.query("SELECT $1::int4, $2::bytea", i, new byte[]{1, 2}));
                assertFalse(r.first().wasBinary(0));
                assertEquals(i, r.first().getInt(0));
                assertArrayEquals(new byte[]{1, 2}, r.first().getBytes(1));
            }
        }
    }

    @Test
    void aTableThatChangesTypeUnderACachedBinaryStatementIsHandled() throws Exception {
        Pg.notOnRemote();
        String t = Pg.table("bs");
        try (PgClient other = Pg.get(PgClient.connect(group, Pg.config()))) {
            Pg.get(other.execute("CREATE TABLE " + t + " (id int, v int)"));
            try {
                Pg.get(other.query("INSERT INTO " + t + " VALUES ($1, $2)", 1, 10));
                for (int i = 0; i < 3; i++) {
                    assertEquals(10, Pg.get(bin.query("SELECT * FROM " + t + " WHERE id = $1", 1)).first().getInt("v"));
                }
                Pg.get(other.execute("ALTER TABLE " + t + " ALTER COLUMN v TYPE bigint"));
                PgResult after = Pg.get(bin.query("SELECT * FROM " + t + " WHERE id = $1", 1));
                assertEquals(10L, after.first().getLong("v"), "the cached statement was refused for its old result type and prepared again, now with the new one");
                assertEquals(10L, Pg.get(bin.query("SELECT * FROM " + t + " WHERE id = $1", 1)).first().getLong("v"));
            } finally {
                Pg.get(other.execute("DROP TABLE " + t));
            }
        }
    }
}
