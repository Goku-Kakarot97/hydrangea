package io.hydrangea.sse;

import java.time.Duration;

/**
 * @param heartbeat       a comment line is sent after this much silence so proxies keep the connection open and dead
 *                        clients are noticed by the failing write; null disables
 * @param maxPendingBytes disconnect a client whose unsent data exceeds this (slow consumer protection)
 * @param retry           reconnection delay suggested to clients when the stream opens; null = browser default
 */
public record SseConfig(Duration heartbeat, int maxPendingBytes, Duration retry) {

    public SseConfig {
        if (maxPendingBytes < 1) {
            throw new IllegalArgumentException("maxPendingBytes must be >= 1");
        }
    }

    public static SseConfig defaults() {
        return new SseConfig(Duration.ofSeconds(15), 1024 * 1024, null);
    }

    public SseConfig withHeartbeat(Duration d) {
        return new SseConfig(d, maxPendingBytes, retry);
    }

    public SseConfig withMaxPendingBytes(int n) {
        return new SseConfig(heartbeat, n, retry);
    }

    public SseConfig withRetry(Duration d) {
        return new SseConfig(heartbeat, maxPendingBytes, d);
    }
}
