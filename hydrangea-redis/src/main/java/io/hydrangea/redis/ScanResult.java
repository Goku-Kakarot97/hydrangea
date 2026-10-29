package io.hydrangea.redis;

import java.util.List;

/** One page of {@code SCAN} (or {@code SSCAN}, {@code HSCAN}...): the keys, and the cursor for the next page; {@code "0"} means the iteration is complete. */
public record ScanResult(String cursor, List<String> keys) {

    public boolean finished() {
        return "0".equals(cursor);
    }
}
