package io.hydrangea.kafka;

import io.hydrangea.Backoff;

import javax.net.ssl.SSLContext;
import java.time.Duration;
import java.util.List;

/**
 * How to reach the cluster. {@code bootstrap} are "host:port" addresses of any brokers; the rest of the cluster is learned from them.
 * {@code sslContext} non-null turns on TLS; {@code saslUser} non-null logs in with SASL/PLAIN after connecting (use it with TLS outside of tests: PLAIN sends the password as is).
 */
public record KafkaConfig(List<String> bootstrap, String clientId, SSLContext sslContext, String saslUser, String saslPassword, Duration connectTimeout,
                          Duration requestTimeout, int maxResponseBytes, Backoff retryBackoff, int maxRetries) {

    public KafkaConfig {
        if (bootstrap == null || bootstrap.isEmpty()) {
            throw new IllegalArgumentException("no bootstrap server");
        }
        bootstrap = List.copyOf(bootstrap);
        if (maxResponseBytes < 1024) {
            throw new IllegalArgumentException("maxResponseBytes");
        }
    }

    public static KafkaConfig of(String... bootstrap) {
        return new KafkaConfig(List.of(bootstrap), "hydrangea", null, null, null, Duration.ofSeconds(10), Duration.ofSeconds(30), 256 * 1024 * 1024,
                new Backoff(Duration.ofMillis(100), Duration.ofSeconds(2), 2.0, 0.2, 0), 10);
    }

    public KafkaConfig withClientId(String id) {
        return new KafkaConfig(bootstrap, id, sslContext, saslUser, saslPassword, connectTimeout, requestTimeout, maxResponseBytes, retryBackoff, maxRetries);
    }

    public KafkaConfig withTls(SSLContext ctx) {
        return new KafkaConfig(bootstrap, clientId, ctx, saslUser, saslPassword, connectTimeout, requestTimeout, maxResponseBytes, retryBackoff, maxRetries);
    }

    public KafkaConfig withSaslPlain(String user, String password) {
        return new KafkaConfig(bootstrap, clientId, sslContext, user, password, connectTimeout, requestTimeout, maxResponseBytes, retryBackoff, maxRetries);
    }

    public KafkaConfig withTimeouts(Duration connect, Duration request) {
        return new KafkaConfig(bootstrap, clientId, sslContext, saslUser, saslPassword, connect, request, maxResponseBytes, retryBackoff, maxRetries);
    }

    /** The largest response accepted, in bytes: a length above it closes the connection before anything is buffered. */
    public KafkaConfig withMaxResponseBytes(int n) {
        return new KafkaConfig(bootstrap, clientId, sslContext, saslUser, saslPassword, connectTimeout, requestTimeout, n, retryBackoff, maxRetries);
    }

    public KafkaConfig withRetries(int max, Backoff backoff) {
        return new KafkaConfig(bootstrap, clientId, sslContext, saslUser, saslPassword, connectTimeout, requestTimeout, maxResponseBytes, backoff, max);
    }
}
