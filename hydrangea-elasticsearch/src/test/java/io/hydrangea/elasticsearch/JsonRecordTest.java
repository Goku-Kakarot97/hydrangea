package io.hydrangea.elasticsearch;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** Records in and out of JSON. */
class JsonRecordTest {

    enum Color { RED, GREEN }

    record Address(String city, @Json.Name("zip_code") String zip) {
    }

    record Product(String name, int price, double weight, boolean inStock, Color color, Address address, List<String> tags, Set<Integer> sizes,
                   Map<String, Double> ratings, List<Address> warehouses, Optional<String> note, @Json.Name("created_at") Instant createdAt, LocalDate since,
                   BigDecimal exact, UUID uid, byte[] blob, int[] counts, Object anything, long views) {
    }

    private static final Product SAMPLE = new Product("lamp", 20, 1.5, true, Color.RED, new Address("Hanoi", "100000"), List.of("a", "b"), Set.of(38, 40),
            Map.of("q", 4.5), List.of(new Address("HCM", "700000")), Optional.of("hi"), Instant.parse("2026-01-02T03:04:05Z"), LocalDate.of(2020, 5, 6),
            new BigDecimal("12.3400"), UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), new byte[]{1, 2, 3}, new int[]{7, 8}, Map.of("k", 1L), 1L << 40);

    @Test
    void roundTrip() {
        String text = Json.write(SAMPLE);
        assertTrue(text.contains("\"zip_code\":\"100000\""), text);
        assertTrue(text.contains("\"created_at\":\"2026-01-02T03:04:05Z\""), text);
        assertTrue(text.contains("\"blob\":\"AQID\""), text);
        assertTrue(text.contains("\"note\":\"hi\""), text);
        Product back = Json.to(Product.class, Json.parse(text));
        assertEquals(SAMPLE.name(), back.name());
        assertEquals(SAMPLE.price(), back.price());
        assertEquals(SAMPLE.weight(), back.weight());
        assertEquals(SAMPLE.inStock(), back.inStock());
        assertEquals(SAMPLE.color(), back.color());
        assertEquals(SAMPLE.address(), back.address());
        assertEquals(SAMPLE.tags(), back.tags());
        assertEquals(SAMPLE.sizes(), back.sizes());
        assertEquals(SAMPLE.ratings(), back.ratings());
        assertEquals(SAMPLE.warehouses(), back.warehouses());
        assertEquals(SAMPLE.note(), back.note());
        assertEquals(SAMPLE.createdAt(), back.createdAt());
        assertEquals(SAMPLE.since(), back.since());
        assertEquals(0, SAMPLE.exact().compareTo(back.exact()));
        assertEquals(SAMPLE.uid(), back.uid());
        assertArrayEquals(SAMPLE.blob(), back.blob());
        assertArrayEquals(SAMPLE.counts(), back.counts());
        assertEquals(Map.of("k", 1L), back.anything());
        assertEquals(SAMPLE.views(), back.views());
        assertEquals(Json.parse(text), Json.from(SAMPLE), "from() gives the parsed form");
    }

    @Test
    void missingFieldsAreNullOrZeroAndExtraFieldsAreIgnored() {
        Product p = Json.to(Product.class, Json.parse("{\"name\":\"x\",\"unknown\":{\"deep\":[1]},\"address\":{\"city\":\"a\",\"extra\":1}}"));
        assertEquals("x", p.name());
        assertEquals(0, p.price());
        assertEquals(0.0, p.weight());
        assertFalse(p.inStock());
        assertNull(p.color());
        assertEquals(new Address("a", null), p.address());
        assertNull(p.tags());
        assertEquals(Optional.empty(), p.note(), "an Optional component is never null");
        assertNull(p.createdAt());
        assertNull(Json.to(Product.class, null));
    }

    @Test
    void numbersAndDatesAreLenientWhereElasticsearchIs() {
        Product p = Json.to(Product.class, Json.parse("{\"price\":\"42\",\"weight\":3,\"inStock\":\"true\",\"color\":\"green\",\"created_at\":1735689600000,\"views\":12.0}"));
        assertEquals(42, p.price(), "a numeric string");
        assertEquals(3.0, p.weight(), "an integer into a double");
        assertTrue(p.inStock());
        assertEquals(Color.GREEN, p.color(), "enum names match case-insensitively as a fallback");
        assertEquals(Instant.parse("2025-01-01T00:00:00Z"), p.createdAt(), "epoch milliseconds, as Elasticsearch stores dates");
        assertEquals(12L, p.views());
    }

    @Test
    void wrongShapesFailWithThePath() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Json.to(Product.class, Json.parse("{\"warehouses\":[{\"city\":\"a\"},5]}")));
        assertEquals("at warehouses[1]: a Address needs an object, not a number (a number to Address)", e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> Json.to(Page.class, Json.parse("{\"items\":[{\"sizes\":[1,\"two\"]}]}")));
        assertTrue(e.getMessage().startsWith("at items[0].sizes[1]:"), e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> Json.to(Product.class, Json.parse("{\"price\":\"many\"}")));
        assertTrue(e.getMessage().contains("price"), e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> Json.to(Product.class, Json.parse("[1]")));
        assertTrue(e.getMessage().contains("needs an object"), e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> Json.to(Product.class, Json.parse("{\"tags\":\"one\"}")));
        assertTrue(e.getMessage().contains("tags"), e.getMessage());
        e = assertThrows(IllegalArgumentException.class, () -> Json.to(Product.class, Json.parse("{\"color\":\"blue\"}")));
        assertTrue(e.getMessage().contains("no constant blue"), e.getMessage());
        assertThrows(IllegalArgumentException.class, () -> Json.to(String.class, Json.parse("{\"a\":1}")), "a String component does not swallow an object");
        assertThrows(IllegalArgumentException.class, () -> Json.to(Thread.class, Json.parse("{}")), "not a record, not convertible");
    }

    record Page(long total, List<Product> items, Map<String, List<Integer>> extras) {
    }

    @Test
    void nestedGenericsAndTopLevelLists() {
        Page page = Json.to(Page.class, Json.parse("{\"total\":2,\"items\":[{\"name\":\"a\",\"price\":1},{\"name\":\"b\"}],\"extras\":{\"x\":[1,2]}}"));
        assertEquals(2, page.total());
        assertEquals("b", page.items().get(1).name());
        assertEquals(1, page.items().get(0).price());
        assertEquals(List.of(1, 2), page.extras().get("x"));
        List<Address> addresses = Json.toList(Address.class, Json.parse("[{\"city\":\"a\"},{\"city\":\"b\",\"zip_code\":\"1\"}]"));
        assertEquals(List.of(new Address("a", null), new Address("b", "1")), addresses);
        assertEquals(List.of(1, 2), Json.toList(Integer.class, Json.parse("[1,2]")));
        assertEquals(5, Json.to(int.class, 5L));
        assertEquals("5", Json.to(String.class, 5L));
    }

    record Strict(String name) {
        Strict {
            if (name == null) {
                throw new IllegalArgumentException("name is required");
            }
        }
    }

    @Test
    void aRecordsOwnValidationIsReported() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Json.to(Strict.class, Json.parse("{}")));
        assertTrue(e.getMessage().contains("name is required"), e.getMessage());
        assertEquals(new Strict("x"), Json.to(Strict.class, Json.parse("{\"name\":\"x\"}")));
    }
}
