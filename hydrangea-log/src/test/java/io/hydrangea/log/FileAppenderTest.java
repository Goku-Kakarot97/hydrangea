package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileAppenderTest {

    @TempDir
    Path dir;

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    @Test
    void writesAppendsAndFlushes() throws IOException {
        Path file = dir.resolve("sub/app.log");
        Files.writeString(dir.resolve("existing.log"), "old\n");
        Log.configure().async(false).level(Level.TRACE).file(file, 0, 0).file(dir.resolve("existing.log"), 0, 0).apply();
        Logger.get("f.write").info("hello {}", "file");
        List<String> lines = Files.readAllLines(file);
        assertEquals(1, lines.size());
        assertTrue(lines.get(0).endsWith(" INFO  [main] f.write - hello file"), lines.get(0));
        assertEquals(List.of("old"), Files.readAllLines(dir.resolve("existing.log")).subList(0, 1));
        assertEquals(2, Files.readAllLines(dir.resolve("existing.log")).size());
        Log.reset();                                    // replacing the configuration closes the files it created
        Log.configure().async(false).level(Level.TRACE).file(file, 0, 0).apply();
        Logger.get("f.write").info("again");
        assertEquals(2, Files.readAllLines(file).size());
    }

    @Test
    void rollsBySizeAndKeepsAtMostMaxFiles() throws IOException {
        Path file = dir.resolve("roll.log");
        FileAppender appender = Appender.file(file, (e, sb) -> sb.append(e.message()).append('\n'), 100, 3);
        Log.configure().async(false).level(Level.TRACE).stage(appender).apply();
        Logger log = Logger.get("f.roll");
        String line = "x".repeat(49);                  // 50 bytes with the newline: two per file
        for (int i = 0; i < 9; i++) {
            log.info(line);
        }
        appender.close();
        assertEquals(1, Files.readAllLines(file).size());
        assertEquals(2, Files.readAllLines(dir.resolve("roll.log.1")).size());
        assertEquals(2, Files.readAllLines(dir.resolve("roll.log.2")).size());
        assertEquals(2, Files.readAllLines(dir.resolve("roll.log.3")).size());
        assertFalse(Files.exists(dir.resolve("roll.log.4")));
        try (var listing = Files.list(dir)) {
            assertEquals(4, listing.count());
        }
    }

    @Test
    void rollingKeepsOrderNewestFirst() throws IOException {
        Path file = dir.resolve("order.log");
        FileAppender appender = Appender.file(file, (e, sb) -> sb.append(e.message()).append('\n'), 4, 2);
        Log.configure().async(false).level(Level.TRACE).stage(appender).apply();
        Logger log = Logger.get("f.order");
        log.info("aaa");
        log.info("bbb");
        log.info("ccc");
        appender.close();
        assertEquals(List.of("ccc"), Files.readAllLines(file));
        assertEquals(List.of("bbb"), Files.readAllLines(dir.resolve("order.log.1")));
        assertEquals(List.of("aaa"), Files.readAllLines(dir.resolve("order.log.2")));
    }

    @Test
    void zeroMaxFilesDiscardsTheRolledFile() throws IOException {
        Path file = dir.resolve("discard.log");
        FileAppender appender = Appender.file(file, (e, sb) -> sb.append(e.message()).append('\n'), 4, 0);
        Log.configure().async(false).level(Level.TRACE).stage(appender).apply();
        Logger.get("f.discard").info("aaa");
        Logger.get("f.discard").info("bbb");
        appender.close();
        assertEquals(List.of("bbb"), Files.readAllLines(file));
        try (var listing = Files.list(dir)) {
            assertEquals(1, listing.count());
        }
    }

    @Test
    void asyncBatchesReachTheFileAfterFlush() throws IOException {
        Path file = dir.resolve("async.log");
        Log.configure().async(true).level(Level.TRACE).format(LogFormat.json()).file(file).apply();
        Logger log = Logger.get("f.async");
        for (int i = 0; i < 5000; i++) {
            log.info("line {}", i);
        }
        assertTrue(Log.flush());
        List<String> lines = Files.readAllLines(file);
        assertEquals(5000, lines.size());
        assertTrue(lines.get(4999).endsWith("\"msg\":\"line 4999\"}"), lines.get(4999));
    }
}
