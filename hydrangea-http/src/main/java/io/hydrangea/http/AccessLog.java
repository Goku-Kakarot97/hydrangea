package io.hydrangea.http;

import java.net.SocketAddress;
import java.time.Duration;

/**
 * Receives one record per HTTP response the server sends. Called on the connection's event loop, so it must not block:
 * hand slow work (files, network) to another thread. An exception thrown here is swallowed.
 */
@FunctionalInterface
public interface AccessLog {

    void log(Entry entry);

    /**
     * @param remote        the client's address
     * @param method        request method, or null if the request line was never understood
     * @param target        request target as sent (possibly null as above)
     * @param status        status code sent
     * @param responseBytes size of the response body (0 for streamed responses and protocol switches)
     * @param duration      from the moment the request head was parsed until the response was handed to the connection
     * @param userAgent     the {@code User-Agent} header, or null
     * @param traceId       the response's {@code X-Trace-Id} (set by {@link HttpRouter}), or null
     */
    record Entry(SocketAddress remote, String method, String target, String version, int status, long responseBytes,
                 Duration duration, String userAgent, String traceId) {

        public Entry(SocketAddress remote, String method, String target, String version, int status, long responseBytes,
                     Duration duration, String userAgent) {
            this(remote, method, target, version, status, responseBytes, duration, userAgent, null);
        }
    }

    /**
     * One line per response through a {@link io.hydrangea.log.Logger} at INFO, in a common-log-like layout, with the request's
     * trace id (see {@link HttpRouter}) as the {@code trace_id} field when there is one.
     */
    static AccessLog toLogger(String loggerName) {
        io.hydrangea.log.Logger logger = io.hydrangea.log.Logger.get(loggerName);
        return e -> {
            io.hydrangea.log.LogEntry entry = logger.at(io.hydrangea.log.Level.INFO);
            if (e.traceId() != null) {
                entry.kv(HttpRouter.TRACE_KEY, e.traceId());
            }
            entry.log(String.format("%s \"%s %s %s\" %d %d %.1fms \"%s\"",
                    e.remote(), clean(e.method()), clean(e.target()), clean(e.version()), e.status(), e.responseBytes(),
                    e.duration().toNanos() / 1e6, clean(e.userAgent())));
        };
    }

    /** Client-controlled text must not be able to forge log lines or break the quoting. */
    private static String clean(String s) {
        if (s == null) {
            return "-";
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\\') {
                sb.append('\\').append(c);
            } else if (c < 0x20 || c == 0x7f) {
                sb.append('?');
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
