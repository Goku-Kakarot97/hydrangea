package io.hydrangea.log;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormatTest {

    private static final long TS = 1_762_139_742_123L;   // 2025-11-03T03:15:42.123Z

    private static LogEvent event(String message, Throwable cause, Object[] fields, String[] context) {
        return new LogEvent(TS, Level.INFO, "io.hydrangea.Server", "main", 1, message, cause, fields, fields == null ? 0 : fields.length / 2, context);
    }

    private static String format(LogFormat f, LogEvent e) {
        StringBuilder sb = new StringBuilder();
        f.format(e, sb);
        return sb.toString();
    }

    @Test
    void lineFormat() {
        LineFormat f = new LineFormat(ZoneOffset.UTC, false);
        assertEquals("2025-11-03 03:15:42.123 INFO  [main] io.hydrangea.Server - listening\n", format(f, event("listening", null, null, null)));
        assertEquals("2025-11-03 10:15:42.123 INFO  [main] Server - x req=r1 k=1 s=\"a b\" q=\"say \\\"hi\\\"\" e=\"\" n=null\n",
                format(new LineFormat(ZoneId.of("Asia/Ho_Chi_Minh"), true),
                        event("x", null, new Object[] {"k", 1, "s", "a b", "q", "say \"hi\"", "e", "", "n", null}, new String[] {"req", "r1"})));
    }

    @Test
    void lineFormatWithStackTrace() {
        RuntimeException cause = new RuntimeException("root\ncause");
        IllegalStateException e = new IllegalStateException("outer", cause);
        e.addSuppressed(new RuntimeException("sup"));
        String out = format(new LineFormat(ZoneOffset.UTC, false), event("failed", e, null, null));
        List<String> lines = out.lines().toList();
        assertEquals("2025-11-03 03:15:42.123 INFO  [main] io.hydrangea.Server - failed", lines.get(0));
        assertEquals("java.lang.IllegalStateException: outer", lines.get(1));
        assertTrue(lines.get(2).startsWith("\tat io.hydrangea.log.FormatTest.lineFormatWithStackTrace("), lines.get(2));
        assertTrue(out.contains("\n\tSuppressed: java.lang.RuntimeException: sup\n\t\tat "));
        assertTrue(out.contains("\nCaused by: java.lang.RuntimeException: root\\ncause\n\tat "));
        assertTrue(out.contains(" more\n"), "common frames collapsed");
        assertTrue(out.endsWith("\n"));
        assertFalse(out.endsWith("\n\n"));
    }

    @Test
    void circularAndDeepCausesAreBounded() {
        RuntimeException a = new RuntimeException("a");
        RuntimeException b = new RuntimeException("b", a);
        a.initCause(b);
        String out = format(LogFormat.line(), event("loop", a, null, null));
        assertTrue(out.contains("[CIRCULAR REFERENCE: java.lang.RuntimeException: a]"), out);

        Throwable deep = new RuntimeException("0");
        for (int i = 1; i < 200; i++) {
            deep = new RuntimeException("" + i, deep);
        }
        out = format(LogFormat.line(), event("deep", deep, null, null));
        assertTrue(out.contains("chain truncated after 64 causes"), out);
        assertFalse(out.contains("java.lang.RuntimeException: 0\n"));
    }

    @Test
    void jsonFormat() {
        String out = format(LogFormat.json(), event("listening on \"port\"\n", null, new Object[] {"port", 8080, "ok", true, "ratio", 0.5, "name", "x\ty", "obj", null}, new String[] {"req", "r1"}));
        assertEquals("{\"ts\":\"2025-11-03T03:15:42.123Z\",\"level\":\"INFO\",\"logger\":\"io.hydrangea.Server\",\"thread\":\"main\","
                + "\"msg\":\"listening on \\\"port\\\"\\n\",\"req\":\"r1\",\"port\":8080,\"ok\":true,\"ratio\":0.5,\"name\":\"x\\ty\",\"obj\":null}\n", out);
        out = format(new JsonFormat(ZoneId.of("Asia/Ho_Chi_Minh")), event("x", new IllegalStateException("bad\u0001"), null, null));
        assertTrue(out.startsWith("{\"ts\":\"2025-11-03T10:15:42.123+07:00\","), out);
        assertTrue(out.contains(",\"error\":{\"type\":\"java.lang.IllegalStateException\",\"message\":\"bad\\u0001\",\"stack\":\"java.lang.IllegalStateException: bad\\\\u0001\\n\\tat "), out);
        assertTrue(out.endsWith("\"}}\n"), out);
        assertEquals("{\"ts\":\"2025-11-03T03:15:42.123Z\",\"level\":\"INFO\",\"logger\":\"io.hydrangea.Server\",\"thread\":\"main\",\"msg\":\"\\u2028 \\u001b[31m\",\"nan\":\"NaN\"}\n",
                format(LogFormat.json(), event("\u2028 \u001b[31m", null, new Object[] {"nan", Double.NaN}, null)));
    }

    @Test
    void controlCharactersCannotForgeALine() {
        String out = format(new LineFormat(ZoneOffset.UTC, false),
                event("user typed\r\n2025-11-03 03:15:42.123 ERROR [main] fake - injected \u001b[31mred", null, new Object[] {"k\n", "v\r"}, new String[] {"c", "\u0000"}));
        assertEquals(1, out.lines().count());
        assertEquals("2025-11-03 03:15:42.123 INFO  [main] io.hydrangea.Server - user typed\\r\\n2025-11-03 03:15:42.123 ERROR [main] fake - injected \\u001b[31mred c=\\u0000 k\\n=v\\r\n", out);
    }

    @Test
    void unnamedThreadsShowTheirId() {
        LogEvent e = new LogEvent(TS, Level.DEBUG, "l", "", 77, "m", null, null, 0, null);
        assertTrue(format(new LineFormat(ZoneOffset.UTC, false), e).contains(" DEBUG [thread-77] l - m"));
        assertTrue(format(LogFormat.json(), e).contains("\"thread\":\"thread-77\""));
    }

    @Test
    void timestampsAcrossSecondsAndZones() {
        Timestamps t = new Timestamps(ZoneOffset.UTC, false);
        StringBuilder sb = new StringBuilder();
        t.append(sb, 0);
        assertEquals("1970-01-01 00:00:00.000", sb.toString());
        sb.setLength(0);
        t.append(sb, 999);
        assertEquals("1970-01-01 00:00:00.999", sb.toString());
        sb.setLength(0);
        t.append(sb, 1000);
        assertEquals("1970-01-01 00:00:01.000", sb.toString());
        sb.setLength(0);
        t.append(sb, -1);
        assertEquals("1969-12-31 23:59:59.999", sb.toString());
        sb.setLength(0);
        new Timestamps(ZoneId.of("America/New_York"), true).append(sb, TS);
        assertEquals("2025-11-02T22:15:42.123-05:00", sb.toString());
    }

    @Test
    void utf8Encoding() {
        int[] n = new int[1];
        byte[] b = Utf8.encode("aé€😀\ud800x", new byte[2], n);
        byte[] expected = "aé€😀\ufffdx".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertEquals(expected.length, n[0]);
        for (int i = 0; i < n[0]; i++) {
            assertEquals(expected[i], b[i], "byte " + i);
        }
    }

    @Test
    void toStringThatThrowsDoesNotEscape() {
        Object bad = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("no");
            }
        };
        assertEquals("v=[toString failed: " + bad.getClass().getName() + ": java.lang.IllegalStateException]", Messages.render("v={}", bad));
        Throwable t = new RuntimeException() {
            @Override
            public String getMessage() {
                throw new IllegalStateException("no message");
            }
        };
        String out = format(LogFormat.line(), event("x", t, null, null));
        assertTrue(out.contains("[getMessage failed: java.lang.IllegalStateException]"), out);
    }

    @Test
    void indexedPlaceholders() {
        assertEquals("b a {2} {x} {}", Messages.renderIndexed("{1} {0} {2} {x} {}", new Object[] {"a", "b"}));
        assertEquals("none {0}", Messages.renderIndexed("none {0}", new Object[0]));
        assertEquals("none", Messages.renderIndexed("none", null));
    }
}
