package io.hydrangea.pipeline;

import io.hydrangea.Connection;
import io.hydrangea.Handler;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A {@link Handler} that runs connection events through a chain of {@link Stage}s, e.g.
 * {@code Pipeline.of(tls, new LineStage(1024), appLogic)}. Create one per connection:
 * <pre>{@code Server.bind(group, addr, opts, () -> Pipeline.of(new LineStage(1024), new Chat()));}</pre>
 */
public final class Pipeline implements Handler {

    private final List<Stage> stages;
    private final Context[] contexts;
    private Connection connection;

    private Pipeline(List<Stage> stages) {
        if (stages.isEmpty()) {
            throw new IllegalArgumentException("a pipeline needs at least one stage");
        }
        this.stages = List.copyOf(stages);
        this.contexts = new Context[stages.size()];
        for (int i = 0; i < contexts.length; i++) {
            contexts[i] = new Context(this, i);
        }
    }

    public static Pipeline of(Stage... stages) {
        return new Pipeline(List.of(stages));
    }

    Connection connection() {
        return connection;
    }

    Stage stage(int i) {
        return i >= 0 && i < stages.size() ? stages.get(i) : null;
    }

    Context context(int i) {
        return contexts[i];
    }

    void writeFromTail(Object msg) throws Exception {
        int last = stages.size() - 1;
        stages.get(last).onWrite(contexts[last], msg);
    }

    @Override
    public void onActive(Connection c) throws Exception {
        this.connection = c;
        stages.get(0).onActive(contexts[0]);
    }

    @Override
    public void onRead(Connection c, ByteBuffer data) throws Exception {
        stages.get(0).onRead(contexts[0], data);
    }

    @Override
    public void onWritabilityChanged(Connection c) throws Exception {
        stages.get(0).onWritabilityChanged(contexts[0]);
    }

    @Override
    public void onDrain(Connection c) throws Exception {
        stages.get(0).onDrain(contexts[0]);
    }

    @Override
    public void onClose(Connection c, Throwable cause) {
        stages.get(0).onClose(contexts[0], cause);
    }
}
