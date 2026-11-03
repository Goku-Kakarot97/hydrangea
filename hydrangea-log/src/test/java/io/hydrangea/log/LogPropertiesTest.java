package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogPropertiesTest {

    @TempDir
    Path dir;

    @AfterEach
    void tearDown() {
        System.clearProperty("hydrangea.log.config");
        System.clearProperty("hydrangea.log.level");
        System.clearProperty("hydrangea.log.level.p.sys");
        Log.reset();
    }

    @Test
    void defaultsWhenNothingIsSet() {
        LogConfig c = LogProperties.parse(new Properties());
        assertEquals(Level.INFO, c.root);
        assertTrue(c.async);
        assertEquals(1 << 16, c.buffer);
        assertFalse(c.dropWhenFull);
        List<LogStage> stages = c.build(new java.util.ArrayList<>());
        assertEquals(1, stages.size());
        assertInstanceOf(StreamAppender.class, stages.get(0));
    }

    @Test
    void everyKeyIsRead() {
        Properties p = new Properties();
        p.setProperty("level", "warn");
        p.setProperty("level.io.hydrangea.http", "DEBUG");
        p.setProperty("level.p.a.b", "trace");
        p.setProperty("async", "false");
        p.setProperty("buffer", "100");
        p.setProperty("full", "drop");
        p.setProperty("format", "json");
        p.setProperty("zone", "Asia/Ho_Chi_Minh");
        p.setProperty("console", "err");
        p.setProperty("file", dir.resolve("a.log").toString());
        p.setProperty("file.maxBytes", "1000");
        p.setProperty("file.maxFiles", "2");
        LogConfig c = LogProperties.parse(p);
        assertEquals(Level.WARN, c.root);
        assertEquals(Level.DEBUG, c.levels.get("io.hydrangea.http"));
        assertEquals(Level.TRACE, c.levels.get("p.a.b"));
        assertFalse(c.async);
        assertEquals(100, c.buffer);
        assertTrue(c.dropWhenFull);
        assertInstanceOf(JsonFormat.class, c.format);
        List<LogStage> owned = new java.util.ArrayList<>();
        List<LogStage> stages = c.build(owned);
        assertEquals(2, stages.size());
        assertInstanceOf(StreamAppender.class, stages.get(0));
        assertInstanceOf(FileAppender.class, stages.get(1));
        assertEquals(2, owned.size());
        assertEquals(dir.resolve("a.log").toAbsolutePath(), ((FileAppender) stages.get(1)).file());
        owned.forEach(LogStage::close);
    }

    @Test
    void badValuesAreReportedAndDefaulted() {
        Properties p = new Properties();
        p.setProperty("level", "LOUD");
        p.setProperty("async", "maybe");
        p.setProperty("buffer", "one");
        p.setProperty("full", "explode");
        p.setProperty("format", "xml");
        p.setProperty("zone", "Mars/Olympus");
        p.setProperty("console", "printer");
        p.setProperty("appender.class", "com.evil.Loader");
        PrintStream err = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true));
        LogConfig c;
        try {
            c = LogProperties.parse(p);
        } finally {
            System.setErr(err);
        }
        String warnings = captured.toString();
        assertEquals(Level.INFO, c.root);
        assertTrue(c.async);
        assertEquals(1 << 16, c.buffer);
        assertFalse(c.dropWhenFull);
        assertInstanceOf(LineFormat.class, c.format);
        assertTrue(warnings.contains("unknown log level: LOUD"), warnings);
        assertTrue(warnings.contains("async must be true or false"), warnings);
        assertTrue(warnings.contains("buffer must be a number"), warnings);
        assertTrue(warnings.contains("full must be block or drop"), warnings);
        assertTrue(warnings.contains("format must be line or json"), warnings);
        assertTrue(warnings.contains("bad zone"), warnings);
        assertTrue(warnings.contains("console must be out, err or off"), warnings);
        assertTrue(warnings.contains("unknown key appender.class"), warnings);
    }

    @Test
    void fileNamedBySystemPropertyAndSystemPropertiesWin() throws Exception {
        Path file = dir.resolve("log.properties");
        Files.writeString(file, "level=ERROR\nlevel.p.sys=DEBUG\nconsole=off\n");
        System.setProperty("hydrangea.log.config", file.toString());
        System.setProperty("hydrangea.log.level", "WARN");
        LogConfig c = LogProperties.load();
        assertEquals(Level.WARN, c.root);
        assertEquals(Level.DEBUG, c.levels.get("p.sys"));
        assertTrue(c.chain.isEmpty());
    }
}
