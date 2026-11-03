package io.hydrangea.log;

import java.util.ArrayList;
import java.util.List;

/** Keeps the formatted lines and the events in memory, for tests and for showing recent output. */
public final class MemoryAppender extends Appender {

    private final List<String> lines = new ArrayList<>();
    private final List<LogEvent> events = new ArrayList<>();

    public MemoryAppender(LogFormat format) {
        super(format);
    }

    @Override
    protected void write(LogEvent event, CharSequence text) {
        synchronized (lines) {
            lines.add(text.toString());
            events.add(event);
        }
    }

    /** A copy of the lines so far, each ending with {@code \n}. */
    public List<String> lines() {
        synchronized (lines) {
            return new ArrayList<>(lines);
        }
    }

    public List<LogEvent> events() {
        synchronized (lines) {
            return new ArrayList<>(events);
        }
    }

    /** All lines concatenated. */
    public String text() {
        synchronized (lines) {
            return String.join("", lines);
        }
    }

    public int size() {
        synchronized (lines) {
            return lines.size();
        }
    }

    public void clear() {
        synchronized (lines) {
            lines.clear();
            events.clear();
        }
    }

    @Override
    public String toString() {
        return "MemoryAppender";
    }
}
