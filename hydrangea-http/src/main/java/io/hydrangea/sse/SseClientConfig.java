package io.hydrangea.sse;

import java.time.Duration;
import java.util.Map;

/**
 * @param defaultRetry   reconnection delay until the server says otherwise with a {@code retry:} field
 * @param maxRetry       cap for the exponential backoff applied after consecutive failures
 * @param reconnect      false = report the first disconnect as final
 * @param maxEventBytes  longest line / event accepted from the server
 * @param headers        extra request headers, e.g. {@code Authorization}
 */
public record SseClientConfig(Duration defaultRetry, Duration maxRetry, boolean reconnect, int maxEventBytes, Map<String, String> headers) {

    public SseClientConfig {
        headers = Map.copyOf(headers);
    }

    public static SseClientConfig defaults() {
        return new SseClientConfig(Duration.ofSeconds(3), Duration.ofSeconds(60), true, 1024 * 1024, Map.of());
    }

    public SseClientConfig withDefaultRetry(Duration d) {
        return new SseClientConfig(d, maxRetry, reconnect, maxEventBytes, headers);
    }

    public SseClientConfig withMaxRetry(Duration d) {
        return new SseClientConfig(defaultRetry, d, reconnect, maxEventBytes, headers);
    }

    public SseClientConfig withReconnect(boolean b) {
        return new SseClientConfig(defaultRetry, maxRetry, b, maxEventBytes, headers);
    }

    public SseClientConfig withHeaders(Map<String, String> h) {
        return new SseClientConfig(defaultRetry, maxRetry, reconnect, maxEventBytes, h);
    }
}
