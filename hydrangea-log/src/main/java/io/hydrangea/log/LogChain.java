package io.hydrangea.log;

import java.util.ArrayList;
import java.util.List;

/**
 * The rest of a chain of {@link LogStage}s. A stage receives the event and the chain after it, and decides whether to pass the event
 * on ({@code next.next(event)}), pass a modified copy, or stop. Built once from the stages, in order; nothing is allocated per event.
 *
 * <pre>{@code
 * LogChain chain = LogChain.of(LogStage.minLevel(Level.INFO), redactPasswords, Appender.console(), Appender.file(path));
 * }</pre>
 */
@FunctionalInterface
public interface LogChain {

    /** Hands the event to the next stage. */
    void next(LogEvent event);

    /** The chain with nothing left: {@code next} does nothing. */
    LogChain END = event -> { };

    static LogChain of(LogStage... stages) {
        return of(List.of(stages));
    }

    static LogChain of(List<? extends LogStage> stages) {
        LogChain c = END;
        for (int i = stages.size() - 1; i >= 0; i--) {
            c = new Node(stages.get(i), c);
        }
        return c;
    }

    /** The stages of a chain built with {@link #of}, in order; empty for any other chain. */
    static List<LogStage> stagesOf(LogChain chain) {
        List<LogStage> out = new ArrayList<>();
        while (chain instanceof Node n) {
            out.add(n.stage);
            chain = n.rest;
        }
        return out;
    }

    /** One link: a stage and what follows it. */
    final class Node implements LogChain {
        final LogStage stage;
        final LogChain rest;

        Node(LogStage stage, LogChain rest) {
            this.stage = stage;
            this.rest = rest;
        }

        @Override
        public void next(LogEvent event) {
            stage.handle(event, rest);
        }
    }
}
