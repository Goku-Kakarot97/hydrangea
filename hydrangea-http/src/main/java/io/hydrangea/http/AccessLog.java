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
     */
    record Entry(SocketAddress remote, String method, String target, String version, int status, long responseBytes,
                 Duration duration, String userAgent) {
    }

    /**
     * One line per response through {@link System.Logger} at INFO, in a common-log-like layout.
     * <p>
     * Caveat: when {@code java.util.logging} is the backend, the JDK's own shutdown hook closes its handlers as soon as the JVM starts
     * shutting down, so lines for requests that finish <em>during</em> a graceful shutdown started from a shutdown hook can be lost.
     * Use a backend that does not do that (Logback, Log4j 2 and others keep logging until their own hooks run) or a custom {@link AccessLog}.
     */
    static AccessLog toLogger(String loggerName) {
        System.Logger logger = System.getLogger(loggerName);
        return e -> logger.log(System.Logger.Level.INFO, String.format("%s \"%s %s %s\" %d %d %.1fms \"%s\"",
                e.remote(), clean(e.method()), clean(e.target()), clean(e.version()), e.status(), e.responseBytes(),
                e.duration().toNanos() / 1e6, clean(e.userAgent())));
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
