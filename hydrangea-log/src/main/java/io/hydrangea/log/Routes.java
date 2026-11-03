package io.hydrangea.log;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Where an event of each level goes: the main chain, then the chains registered with {@link Log#on} for that level. Built once per
 * change, read per event. A stage that throws is reported and does not stop the others.
 */
final class Routes {

    record Subscription(int from, int to, LogChain chain) {
    }

    private static final LogChain[] NONE = new LogChain[0];

    final LogChain main;
    private final LogChain[][] subscribers;
    private final LogStage[] stages;

    Routes(LogChain main, List<Subscription> subscriptions) {
        this.main = main;
        subscribers = new LogChain[Level.VALUES.length][];
        for (int level = 0; level < subscribers.length; level++) {
            List<LogChain> chains = new ArrayList<>();
            for (Subscription s : subscriptions) {
                if (s.from <= level && level <= s.to) {
                    chains.add(s.chain);
                }
            }
            subscribers[level] = chains.isEmpty() ? NONE : chains.toArray(LogChain[]::new);
        }
        Set<LogStage> all = new LinkedHashSet<>(LogChain.stagesOf(main));
        for (Subscription s : subscriptions) {
            all.addAll(LogChain.stagesOf(s.chain));
        }
        stages = all.toArray(LogStage[]::new);
    }

    void deliver(LogEvent event) {
        try {
            main.next(event);
        } catch (Throwable t) {
            Log.failure("chain", t);
        }
        for (LogChain c : subscribers[event.level().ordinal()]) {
            try {
                c.next(event);
            } catch (Throwable t) {
                Log.failure("subscriber", t);
            }
        }
    }

    void flushAll() {
        for (LogStage s : stages) {
            try {
                s.flush();
            } catch (Throwable t) {
                Log.failure(s + " flush", t);
            }
        }
    }

    List<LogStage> stages() {
        return List.of(stages);
    }
}
