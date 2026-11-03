package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ListResourceBundle;
import java.util.ResourceBundle;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SystemLoggerBridgeTest {

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    @Test
    void theFinderIsRegisteredAsAService() {
        boolean found = false;
        for (System.LoggerFinder f : ServiceLoader.load(System.LoggerFinder.class)) {
            found |= f instanceof HydrangeaLoggerFinder;
        }
        assertTrue(found);
    }

    @Test
    void systemLoggerCallsLandInTheChain() {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).level("b.quiet", Level.OFF).stage(mem).apply();
        System.Logger log = System.getLogger("b.bridge");
        assertSame(HydrangeaLoggerFinder.Bridge.class, log.getClass(), "the JDK picked this backend (only one LoggerFinder on the class path)");
        assertEquals("b.bridge", log.getName());
        assertTrue(log.isLoggable(System.Logger.Level.TRACE));
        assertFalse(log.isLoggable(System.Logger.Level.OFF));
        RuntimeException e = new RuntimeException("x");
        log.log(System.Logger.Level.WARNING, "plain");
        log.log(System.Logger.Level.INFO, "indexed {1} {0}", "a", "b");
        log.log(System.Logger.Level.ERROR, "thrown", e);
        log.log(System.Logger.Level.DEBUG, () -> "supplied");
        log.log(System.Logger.Level.ALL, "all");
        System.getLogger("b.quiet").log(System.Logger.Level.ERROR, "never");

        var events = mem.events();
        assertEquals(5, events.size());
        assertEquals(Level.WARN, events.get(0).level());
        assertEquals("plain", events.get(0).message());
        assertEquals("indexed b a", events.get(1).message());
        assertEquals(Level.ERROR, events.get(2).level());
        assertSame(e, events.get(2).cause());
        assertEquals("supplied", events.get(3).message());
        assertEquals(Level.TRACE, events.get(4).level());
    }

    @Test
    void resourceBundlesAreHonoured() {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(mem).apply();
        ResourceBundle bundle = new ListResourceBundle() {
            @Override
            protected Object[][] getContents() {
                return new Object[][] {{"greet", "hello {0}"}};
            }
        };
        System.Logger log = System.getLogger("b.bundle");
        log.log(System.Logger.Level.INFO, bundle, "greet", "world");
        log.log(System.Logger.Level.INFO, bundle, "missing {0}", "key");
        assertEquals("hello world", mem.events().get(0).message());
        assertEquals("missing key", mem.events().get(1).message());
    }
}
