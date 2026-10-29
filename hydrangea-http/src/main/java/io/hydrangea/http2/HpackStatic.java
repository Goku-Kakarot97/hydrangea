package io.hydrangea.http2;

/** The static table of RFC 7541, Appendix A. Index 1 is the first entry. */
final class HpackStatic {

    static final String[][] ENTRIES = {
            {":authority", ""}, {":method", "GET"}, {":method", "POST"}, {":path", "/"}, {":path", "/index.html"},
            {":scheme", "http"}, {":scheme", "https"}, {":status", "200"}, {":status", "204"}, {":status", "206"},
            {":status", "304"}, {":status", "400"}, {":status", "404"}, {":status", "500"}, {"accept-charset", ""},
            {"accept-encoding", "gzip, deflate"}, {"accept-language", ""}, {"accept-ranges", ""}, {"accept", ""},
            {"access-control-allow-origin", ""}, {"age", ""}, {"allow", ""}, {"authorization", ""}, {"cache-control", ""},
            {"content-disposition", ""}, {"content-encoding", ""}, {"content-language", ""}, {"content-length", ""},
            {"content-location", ""}, {"content-range", ""}, {"content-type", ""}, {"cookie", ""}, {"date", ""}, {"etag", ""},
            {"expect", ""}, {"expires", ""}, {"from", ""}, {"host", ""}, {"if-match", ""}, {"if-modified-since", ""},
            {"if-none-match", ""}, {"if-range", ""}, {"if-unmodified-since", ""}, {"last-modified", ""}, {"link", ""},
            {"location", ""}, {"max-forwards", ""}, {"proxy-authenticate", ""}, {"proxy-authorization", ""}, {"range", ""},
            {"referer", ""}, {"refresh", ""}, {"retry-after", ""}, {"server", ""}, {"set-cookie", ""},
            {"strict-transport-security", ""}, {"transfer-encoding", ""}, {"user-agent", ""}, {"vary", ""}, {"via", ""},
            {"www-authenticate", ""},
    };

    static final int SIZE = ENTRIES.length; // 61

    private HpackStatic() {
    }

    static String name(int index) {
        return ENTRIES[index - 1][0];
    }

    static String value(int index) {
        return ENTRIES[index - 1][1];
    }

    private static final java.util.Map<String, Integer> FIRST_WITH_NAME = new java.util.HashMap<>();

    static {
        for (int i = SIZE - 1; i >= 0; i--) {   // backwards, so that the lowest index wins
            FIRST_WITH_NAME.put(ENTRIES[i][0], i + 1);
        }
    }

    /** Index of an entry with this exact name and value, or 0. */
    static int indexOf(String name, String value) {
        Integer first = FIRST_WITH_NAME.get(name);   // entries with one name are adjacent in the table
        if (first == null) {
            return 0;
        }
        for (int i = first - 1; i < SIZE && ENTRIES[i][0].equals(name); i++) {
            if (ENTRIES[i][1].equals(value)) {
                return i + 1;
            }
        }
        return 0;
    }

    /** Index of the first entry with this name, or 0. */
    static int nameIndex(String name) {
        Integer i = FIRST_WITH_NAME.get(name);
        return i == null ? 0 : i;
    }
}
