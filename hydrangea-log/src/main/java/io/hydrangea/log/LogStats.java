package io.hydrangea.log;

/**
 * Counters, from {@link Log#stats}.
 *
 * @param published events accepted from loggers (queued or delivered inline)
 * @param dropped   events discarded because the buffer was full (only with {@link LogConfig#dropWhenFull})
 * @param errors    failures inside stages and appenders (a write that failed, a stage that threw)
 * @param queued    events waiting in the buffer right now
 * @param capacity  the buffer's size (0 when synchronous)
 */
public record LogStats(long published, long dropped, long errors, int queued, int capacity, boolean async) {
}
