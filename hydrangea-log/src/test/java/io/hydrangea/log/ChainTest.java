package io.hydrangea.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainTest {

    @AfterEach
    void tearDown() {
        Log.reset();
    }

    @Test
    void stagesRunInOrderAndMayStopOrChangeTheEvent() {
        List<String> trail = new ArrayList<>();
        MemoryAppender mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE)
                .stage(LogStage.each(e -> trail.add("first:" + e.message())))
                .stage(LogStage.filter(e -> !e.message().contains("secret")))
                .stage(LogStage.map(e -> e.withMessage(e.message().replace("4111", "****"))))
                .stage(mem)
                .stage(LogStage.each(e -> trail.add("last:" + e.message())))
                .apply();
        Logger log = Logger.get("c.order");
        log.info("card 4111");
        log.info("a secret");
        log.info("plain");
        assertEquals(List.of("first:card 4111", "last:card ****", "first:a secret", "first:plain", "last:plain"), trail);
        assertEquals(List.of("card ****", "plain"), mem.events().stream().map(LogEvent::message).toList());
    }

    @Test
    void chainOfBuiltInStages() {
        MemoryAppender mem = Appender.memory();
        Log.configure().async(false).level(Level.TRACE)
                .chain(LogStage.minLevel(Level.INFO), LogStage.loggerPrefix("c.pre"), mem)
                .apply();
        Logger.get("c.pre").debug("no: level");
        Logger.get("c.prefix").info("no: logger");
        Logger.get("c.pre.X").info("yes");
        Logger.get("c.pre").warn("yes too");
        assertEquals(List.of("yes", "yes too"), mem.events().stream().map(LogEvent::message).toList());
    }

    @Test
    void registrationByLevelThresholdAndExact() {
        MemoryAppender main = Appender.memory();
        Log.configure().async(false).level(Level.TRACE).stage(main).apply();
        List<String> errors = new ArrayList<>();
        List<String> warnsOnly = new ArrayList<>();
        Registration r1 = Log.on(Level.WARN, (e, next) -> errors.add(e.level() + ":" + e.message()));
        Registration r2 = Log.onExactly(Level.WARN, LogStage.each(e -> warnsOnly.add(e.message())));
        Logger log = Logger.get("c.reg");
        log.debug("d");
        log.info("i");
        log.warn("w");
        log.error("e");
        assertEquals(List.of("WARN:w", "ERROR:e"), errors);
        assertEquals(List.of("w"), warnsOnly);
        assertEquals(4, main.size());

        r1.cancel();
        log.error("e2");
        assertEquals(2, errors.size());
        r2.close();
        log.warn("w2");
        assertEquals(1, warnsOnly.size());
        r1.cancel();                                   // twice is fine
        assertThrows(IllegalArgumentException.class, () -> Log.on(Level.OFF, main));
    }

    @Test
    void registeredHandlersSeeEventsTheMainChainDropped() {
        Log.configure().async(false).level(Level.TRACE).stage(LogStage.filter(e -> false)).apply();
        AtomicInteger seen = new AtomicInteger();
        Log.on(Level.TRACE, (e, next) -> seen.incrementAndGet());
        Logger.get("c.dropped").info("x");
        assertEquals(1, seen.get());
    }

    @Test
    void registrationsSurviveReconfiguration() {
        AtomicInteger seen = new AtomicInteger();
        Log.on(Level.INFO, (e, next) -> seen.incrementAndGet());
        Log.configure().async(false).level(Level.TRACE).stage(Appender.memory()).apply();
        Logger.get("c.survive").info("x");
        assertEquals(1, seen.get());
        Log.reset();
        Log.configure().async(false).stage(Appender.memory()).apply();
        Logger.get("c.survive").info("y");
        assertEquals(1, seen.get());
    }

    @Test
    void aFailingStageDoesNotStopTheOthersOrTheCaller() {
        MemoryAppender mem = Appender.memory();
        long errorsBefore = Log.stats().errors();
        Log.configure().async(false).level(Level.TRACE)
                .stage((e, next) -> {
                    throw new IllegalStateException("broken stage");
                })
                .stage(mem)
                .apply();
        Log.on(Level.INFO, (e, next) -> {
            throw new IllegalStateException("broken handler");
        });
        AtomicInteger after = new AtomicInteger();
        Log.on(Level.INFO, (e, next) -> after.incrementAndGet());
        Logger.get("c.fail").info("x");
        assertEquals(0, mem.size());                       // the main chain stopped at the broken stage
        assertEquals(1, after.get());                      // the handler after the broken one still ran
        assertEquals(errorsBefore + 2, Log.stats().errors());
    }

    @Test
    void loggingFromInsideAStageIsDeliveredInlineNotQueued() {
        MemoryAppender mem = Appender.memory();
        Logger inner = Logger.get("c.inner");
        Log.configure().async(true).level(Level.TRACE)
                .stage(LogStage.each(e -> {
                    if (!e.logger().equals("c.inner")) {
                        inner.info("from stage: " + e.message());
                    }
                }))
                .stage(mem)
                .apply();
        Logger.get("c.outer").info("hello");
        assertTrue(Log.flush());
        assertEquals(List.of("from stage: hello", "hello"), mem.events().stream().map(LogEvent::message).toList());
    }

    @Test
    void stagesOfAChain() {
        LogStage a = (e, n) -> { };
        LogStage b = (e, n) -> { };
        assertEquals(List.of(a, b), LogChain.stagesOf(LogChain.of(a, b)));
        assertEquals(List.of(), LogChain.stagesOf(LogChain.END));
    }
}
