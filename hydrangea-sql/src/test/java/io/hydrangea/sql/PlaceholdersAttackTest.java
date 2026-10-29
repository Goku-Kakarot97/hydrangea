package io.hydrangea.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SQL written to confuse the placeholder rewriting: if a {@code ?} that is not a placeholder were numbered, or a real one skipped, the parameters would shift by one and
 * a value would land in the wrong column (or a {@code WHERE} would compare against the wrong thing). The translation only ever moves {@code ?} to {@code $n}; parameters
 * never enter the text, so there is no injection through them, but the numbering must be exact.
 */
class PlaceholdersAttackTest {

    @Test
    void quotesCommentsAndDollarQuotesCannotHideOrInventPlaceholders() {
        assertEquals("SELECT $1 FROM t WHERE a = 'x''?' AND b = $2", Placeholders.toDollar("SELECT ? FROM t WHERE a = 'x''?' AND b = ?"));
        assertEquals("SELECT $1 FROM t WHERE a = E'\\'?' AND b = $2", Placeholders.toDollar("SELECT ? FROM t WHERE a = E'\\'?' AND b = ?"));
        assertEquals("SELECT $1 FROM \"ta?ble\" WHERE \"c\"\"?\" = $2", Placeholders.toDollar("SELECT ? FROM \"ta?ble\" WHERE \"c\"\"?\" = ?"));
        assertEquals("SELECT $1 /* ? */ , $2 -- ?\n, $3", Placeholders.toDollar("SELECT ? /* ? */ , ? -- ?\n, ?"));
        assertEquals("SELECT $1 /* /* ? */ ? */ , $2", Placeholders.toDollar("SELECT ? /* /* ? */ ? */ , ?"));
        assertEquals("SELECT $a$ ? '?' $a$, $1", Placeholders.toDollar("SELECT $a$ ? '?' $a$, ?"));
        assertEquals("SELECT $$?$$, $1, $tag$?$tag$, $2", Placeholders.toDollar("SELECT $$?$$, ?, $tag$?$tag$, ?"));
    }

    @Test
    void aDollarThatIsNotAQuoteIsLeftAlone() {
        assertEquals("SELECT $1 WHERE cost > 5$ AND x = $2", Placeholders.toDollar("SELECT ? WHERE cost > 5$ AND x = ?"));
        assertEquals("SELECT $1, $1 FROM t", Placeholders.toDollar("SELECT $1, ? FROM t"));     // mixed: an existing $1 and a ?: the ? is numbered from 1 as always (mixing is the caller's mistake, and PostgreSQL then reports a type or count error, not a silent shift)
        assertEquals("SELECT $_x$ ? $_x$, $1", Placeholders.toDollar("SELECT $_x$ ? $_x$, ?"));
    }

    @Test
    void anUnterminatedConstructSwallowsTheRestWithoutGoingPastTheEnd() {
        assertEquals("SELECT $1, 'open ?", Placeholders.toDollar("SELECT ?, 'open ?"));
        assertEquals("SELECT $1, \"open ?", Placeholders.toDollar("SELECT ?, \"open ?"));
        assertEquals("SELECT $1, $q$ open ?", Placeholders.toDollar("SELECT ?, $q$ open ?"));
        assertEquals("SELECT $1 -- open ?", Placeholders.toDollar("SELECT ? -- open ?"));
    }

    @Test
    void jsonOperatorsAreWrittenDoubled() {
        assertEquals("SELECT j ? $1, j ?| $2, j ?& $3", Placeholders.toDollar("SELECT j ?? ?, j ??| ?, j ??& ?"));
        assertEquals("SELECT $1 ?", Placeholders.toDollar("SELECT ? ??"));
    }
}
