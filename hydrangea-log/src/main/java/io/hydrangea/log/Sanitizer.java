package io.hydrangea.log;

/**
 * Keeps one event on one line: in text output, a line break or another control character inside a message, a field or an exception
 * message is written as an escape ({@code \n}, {@code \r}, {@code \u001b} ...), so nothing a user typed can forge a second log
 * line or drive a terminal. Tab is kept. JSON output escapes by its own rules ({@link JsonFormat}).
 */
final class Sanitizer {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Sanitizer() {
    }

    static void append(StringBuilder sb, CharSequence s) {
        int len = s.length();
        int i = 0;
        while (i < len && clean(s.charAt(i))) {
            i++;
        }
        if (i == len) {
            sb.append(s);
            return;
        }
        sb.append(s, 0, i);
        for (; i < len; i++) {
            char c = s.charAt(i);
            if (clean(c)) {
                sb.append(c);
            } else {
                escape(sb, c);
            }
        }
    }

    private static boolean clean(char c) {
        return (c >= 0x20 && c != 0x7f && (c < 0x80 || c > 0x9f) && c != ' ' && c != ' ') || c == '\t';
    }

    private static void escape(StringBuilder sb, char c) {
        switch (c) {
            case '\n' -> sb.append("\\n");
            case '\r' -> sb.append("\\r");
            default -> sb.append("\\u").append(HEX[(c >> 12) & 0xf]).append(HEX[(c >> 8) & 0xf]).append(HEX[(c >> 4) & 0xf]).append(HEX[c & 0xf]);
        }
    }
}
