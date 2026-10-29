package io.hydrangea.grpc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A small strict JSON reader for the service config: objects become {@code Map}, arrays {@code List}, numbers {@code Double}. */
final class Json {
    private final String s;
    private int pos;

    private Json(String s) {
        this.s = s;
    }

    static Object parse(String text) {
        Json j = new Json(text);
        j.ws();
        Object v = j.value(0);
        j.ws();
        if (j.pos != text.length()) {
            throw j.error("unexpected trailing characters");
        }
        return v;
    }

    private IllegalArgumentException error(String what) {
        return new IllegalArgumentException("invalid JSON at " + pos + ": " + what);
    }

    private void ws() {
        while (pos < s.length() && (s.charAt(pos) == ' ' || s.charAt(pos) == '\t' || s.charAt(pos) == '\n' || s.charAt(pos) == '\r')) {
            pos++;
        }
    }

    private Object value(int depth) {
        if (depth > 64) {
            throw error("nested too deeply");
        }
        if (pos >= s.length()) {
            throw error("unexpected end");
        }
        char c = s.charAt(pos);
        switch (c) {
            case '{': {
                pos++;
                Map<String, Object> m = new LinkedHashMap<>();
                ws();
                if (peek('}')) {
                    pos++;
                    return m;
                }
                for (;;) {
                    ws();
                    if (!peek('"')) {
                        throw error("object key expected");
                    }
                    String k = string();
                    ws();
                    expect(':');
                    ws();
                    m.put(k, value(depth + 1));
                    ws();
                    if (peek(',')) {
                        pos++;
                    } else {
                        expect('}');
                        return m;
                    }
                }
            }
            case '[': {
                pos++;
                List<Object> l = new ArrayList<>();
                ws();
                if (peek(']')) {
                    pos++;
                    return l;
                }
                for (;;) {
                    ws();
                    l.add(value(depth + 1));
                    ws();
                    if (peek(',')) {
                        pos++;
                    } else {
                        expect(']');
                        return l;
                    }
                }
            }
            case '"':
                return string();
            case 't':
                return literal("true", Boolean.TRUE);
            case 'f':
                return literal("false", Boolean.FALSE);
            case 'n':
                return literal("null", null);
            default:
                return number();
        }
    }

    private boolean peek(char c) {
        return pos < s.length() && s.charAt(pos) == c;
    }

    private void expect(char c) {
        if (!peek(c)) {
            throw error("'" + c + "' expected");
        }
        pos++;
    }

    private Object literal(String word, Object v) {
        if (!s.startsWith(word, pos)) {
            throw error("unknown literal");
        }
        pos += word.length();
        return v;
    }

    private Double number() {
        int start = pos;
        while (pos < s.length() && "+-0123456789.eE".indexOf(s.charAt(pos)) >= 0) {
            pos++;
        }
        if (start == pos) {
            throw error("value expected");
        }
        try {
            return Double.parseDouble(s.substring(start, pos));
        } catch (NumberFormatException e) {
            throw error("bad number");
        }
    }

    private String string() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (pos < s.length()) {
            char c = s.charAt(pos++);
            if (c == '"') {
                return sb.toString();
            }
            if (c < 0x20) {
                throw error("control character in string");
            }
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (pos >= s.length()) {
                break;
            }
            char e = s.charAt(pos++);
            switch (e) {
                case '"', '\\', '/' -> sb.append(e);
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (pos + 4 > s.length()) {
                        throw error("bad \\u escape");
                    }
                    try {
                        sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                    } catch (NumberFormatException ex) {
                        throw error("bad \\u escape");
                    }
                    pos += 4;
                }
                default -> throw error("bad escape");
            }
        }
        throw error("unterminated string");
    }
}
