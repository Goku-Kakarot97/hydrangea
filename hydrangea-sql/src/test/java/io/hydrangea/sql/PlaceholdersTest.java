package io.hydrangea.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class PlaceholdersTest {

    @Test
    void numbersThePlaceholdersInOrder() {
        assertEquals("SELECT $1, $2 FROM t WHERE a = $3", Placeholders.toDollar("SELECT ?, ? FROM t WHERE a = ?"));
        assertEquals("INSERT INTO t VALUES ($1, $2, $3)", Placeholders.toDollar("INSERT INTO t VALUES (?, ?, ?)"));
    }

    @Test
    void aStatementWithoutPlaceholdersIsReturnedAsIs() {
        String sql = "SELECT $1 FROM t";
        assertSame(sql, Placeholders.toDollar(sql));
    }

    @Test
    void stringsIdentifiersCommentsAndDollarQuotesAreLeftAlone() {
        assertEquals("SELECT '?', \"col?\", $1", Placeholders.toDollar("SELECT '?', \"col?\", ?"));
        assertEquals("SELECT 'it''s ?', $1", Placeholders.toDollar("SELECT 'it''s ?', ?"));
        assertEquals("SELECT E'a\\'? ', $1", Placeholders.toDollar("SELECT E'a\\'? ', ?"));
        assertEquals("SELECT $1 -- is it ?\n, $2", Placeholders.toDollar("SELECT ? -- is it ?\n, ?"));
        assertEquals("SELECT /* ? /* ? */ ? */ $1", Placeholders.toDollar("SELECT /* ? /* ? */ ? */ ?"));
        assertEquals("SELECT $$a ? b$$, $tag$ ? $tag$, $1", Placeholders.toDollar("SELECT $$a ? b$$, $tag$ ? $tag$, ?"));
        assertEquals("SELECT $1 WHERE j ? 'k'", Placeholders.toDollar("SELECT ? WHERE j ?? 'k'"));
        assertEquals("SELECT $1 WHERE j ?| $2", Placeholders.toDollar("SELECT ? WHERE j ??| ?"));
    }

    @Test
    void anUnterminatedStringDoesNotRunPastTheEnd() {
        assertEquals("SELECT 'abc ?", Placeholders.toDollar("SELECT 'abc ?"));
        assertEquals("SELECT $1 /* open ?", Placeholders.toDollar("SELECT ? /* open ?"));
    }
}
