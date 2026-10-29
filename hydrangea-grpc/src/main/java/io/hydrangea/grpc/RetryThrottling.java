package io.hydrangea.grpc;

/**
 * Stops retries and hedges from piling onto a server that is already failing (gRFC A6). The channel keeps a bucket of tokens, starting full at
 * {@code maxTokens}: every failed attempt takes one, every success adds {@code tokenRatio}; while the bucket is at or below half full, calls
 * are no longer retried or hedged (the first attempt always goes out).
 *
 * @param maxTokens  bucket size, e.g. 10
 * @param tokenRatio refill per success, e.g. 0.1: roughly one retry is allowed per ten successes once the server is struggling
 */
public record RetryThrottling(int maxTokens, double tokenRatio) {

    public RetryThrottling {
        if (maxTokens < 1 || tokenRatio <= 0) {
            throw new IllegalArgumentException("maxTokens must be at least 1 and tokenRatio positive");
        }
    }

    /** The mutable bucket for one channel. */
    static final class Bucket {
        private final double max;
        private final double ratio;
        private double tokens;

        Bucket(RetryThrottling t) {
            this.max = t.maxTokens;
            this.ratio = t.tokenRatio;
            this.tokens = max;
        }

        synchronized void failure() {
            tokens = Math.max(0, tokens - 1);
        }

        synchronized void success() {
            tokens = Math.min(max, tokens + ratio);
        }

        synchronized boolean allowsRetry() {
            return tokens > max / 2;
        }
    }
}
