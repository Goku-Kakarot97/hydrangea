package io.hydrangea.log;

import java.util.Arrays;

/**
 * Fills {@code {}} placeholders (and, for the {@link System.Logger} bridge, {@code {0}} ones). Plain text substitution: nothing in a
 * message or an argument is ever interpreted as a lookup, a format directive or an expression, so {@code ${jndi:...}} is just those
 * characters.
 */
final class Messages {

    private Messages() {
    }

    /** Whether {@code pattern} has at least {@code n} unescaped {@code {}} placeholders. */
    static boolean hasPlaceholders(String pattern, int n) {
        int found = 0;
        int i = 0;
        int len = pattern.length();
        while (found < n) {
            int j = pattern.indexOf("{}", i);
            if (j < 0) {
                return false;
            }
            if (j == 0 || pattern.charAt(j - 1) != '\\') {
                found++;
            }
            i = j + 2;
        }
        return found >= n || n == 0;
    }

    static String render(String pattern, Object a) {
        return render(pattern, new Object[] {a}, 1);
    }

    static String render(String pattern, Object a, Object b) {
        return render(pattern, new Object[] {a, b}, 2);
    }

    /** {@code {}} placeholders replaced in order by the first {@code n} args; {@code \{}} is a literal {@code {}}; extra placeholders stay. */
    static String render(String pattern, Object[] args, int n) {
        if (pattern == null) {
            return "null";
        }
        if (n == 0) {
            return pattern;
        }
        int len = pattern.length();
        StringBuilder sb = new StringBuilder(len + 16 * n);
        int i = 0;
        int ai = 0;
        while (i < len) {
            int j = pattern.indexOf("{}", i);
            if (j < 0) {
                sb.append(pattern, i, len);
                break;
            }
            if (j > 0 && pattern.charAt(j - 1) == '\\') {
                sb.append(pattern, i, j - 1).append("{}");
                i = j + 2;
                continue;
            }
            sb.append(pattern, i, j);
            if (ai < n) {
                append(sb, args[ai++]);
            } else {
                sb.append("{}");
            }
            i = j + 2;
        }
        return sb.toString();
    }

    /** {@code {0}}, {@code {1}} ... replaced by the arguments ({@link System.Logger} style); unknown indexes stay. No {@link java.text.MessageFormat}. */
    static String renderIndexed(String pattern, Object[] args) {
        if (pattern == null) {
            return "null";
        }
        if (args == null || args.length == 0 || pattern.indexOf('{') < 0) {
            return pattern;
        }
        int len = pattern.length();
        StringBuilder sb = new StringBuilder(len + 16 * args.length);
        int i = 0;
        while (i < len) {
            char c = pattern.charAt(i);
            if (c == '{') {
                int close = pattern.indexOf('}', i + 1);
                if (close > i + 1 && close - i <= 4) {
                    int idx = parseIndex(pattern, i + 1, close);
                    if (idx >= 0 && idx < args.length) {
                        append(sb, args[idx]);
                        i = close + 1;
                        continue;
                    }
                }
            }
            sb.append(c);
            i++;
        }
        return sb.toString();
    }

    private static int parseIndex(String s, int from, int to) {
        int v = 0;
        for (int i = from; i < to; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            v = v * 10 + (c - '0');
        }
        return v;
    }

    /** Appends an argument's text: arrays element by element, everything else through {@code toString}, which may not throw its way out. */
    static void append(StringBuilder sb, Object o) {
        if (o == null) {
            sb.append("null");
        } else if (o instanceof String s) {
            sb.append(s);
        } else if (o instanceof Integer || o instanceof Long || o instanceof Boolean || o instanceof Character) {
            sb.append(o);
        } else if (o.getClass().isArray()) {
            sb.append(arrayToString(o));
        } else {
            try {
                sb.append(o.toString());
            } catch (RuntimeException e) {
                sb.append("[toString failed: ").append(o.getClass().getName()).append(": ").append(e.getClass().getName()).append(']');
            }
        }
    }

    private static String arrayToString(Object o) {
        if (o instanceof Object[] a) {
            return Arrays.deepToString(a);
        } else if (o instanceof int[] a) {
            return Arrays.toString(a);
        } else if (o instanceof long[] a) {
            return Arrays.toString(a);
        } else if (o instanceof byte[] a) {
            return Arrays.toString(a);
        } else if (o instanceof char[] a) {
            return Arrays.toString(a);
        } else if (o instanceof double[] a) {
            return Arrays.toString(a);
        } else if (o instanceof float[] a) {
            return Arrays.toString(a);
        } else if (o instanceof short[] a) {
            return Arrays.toString(a);
        } else if (o instanceof boolean[] a) {
            return Arrays.toString(a);
        }
        return o.toString();
    }
}
