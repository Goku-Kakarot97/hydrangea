package io.hydrangea.sse;

import io.hydrangea.Scheduled;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.ResponseHead;
import io.hydrangea.http.StreamHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Server-Sent Events client (the equivalent of a browser's {@code EventSource}) on top of
 * {@link HttpClient#stream}.
 * <pre>{@code
 * SseSubscription sub = SseClient.subscribe(http, "https://example.org/events", new SseClientListener() {
 *     public void onMessage(SseMessage m) { System.out.println(m.event() + ": " + m.data()); }
 * });
 * ...
 * sub.close();
 * }</pre>
 * Behaviour follows the WHATWG standard:
 * <ul>
 *   <li>Sends {@code Accept: text/event-stream} and {@code Cache-Control: no-cache}, and {@code Last-Event-ID} once an
 *       event with an id was received, so the server can resume.</li>
 *   <li>A {@code 200} with content type {@code text/event-stream} opens the stream. {@code 204} ends the subscription
 *       (the server's way of saying "stop reconnecting"). Any other status or content type is a final failure.</li>
 *   <li>When the stream ends or the network fails, it reconnects after the delay the server suggested with
 *       {@code retry:} (default {@link SseClientConfig#defaultRetry()}), backing off exponentially, up to
 *       {@link SseClientConfig#maxRetry()}, while connections keep failing before delivering an event.</li>
 * </ul>
 * Redirects are followed by the underlying client. A stream that stays silent longer than the
 * {@link io.hydrangea.http.HttpClientConfig#idleTimeout()} is treated as dead and reconnected, so use a server heartbeat.
 */
public final class SseClient {

    private SseClient() {
    }

    public static SseSubscription subscribe(HttpClient http, String url, SseClientListener listener) {
        return subscribe(http, url, SseClientConfig.defaults(), listener);
    }

    public static SseSubscription subscribe(HttpClient http, String url, SseClientConfig config, SseClientListener listener) {
        ClientRequest.get(url); // validate the URL now, on the caller's thread
        Subscription s = new Subscription(http, url, config, listener);
        s.connect();
        return s;
    }

    private static final class Subscription implements SseSubscription {
        private final HttpClient http;
        private final String url;
        private final SseClientConfig config;
        private final SseClientListener listener;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean closeNotified = new AtomicBoolean();

        private volatile String lastEventId;
        private volatile StreamHandle current;
        private volatile Scheduled reconnectTimer;
        private volatile long retryMillis;
        private int consecutiveFailures;   // loop thread: failures since the last event
        private final io.hydrangea.Metrics.Count mEvents;
        private final io.hydrangea.Metrics.Count mReconnects;

        Subscription(HttpClient http, String url, SseClientConfig config, SseClientListener listener) {
            this.http = http;
            this.url = url;
            this.config = config;
            this.listener = listener;
            this.retryMillis = config.defaultRetry().toMillis();
            io.hydrangea.Metrics metrics = http.eventLoop().metrics();
            this.mEvents = metrics.counter("sse.events", null, "side", "client", "direction", "received");
            this.mReconnects = metrics.counter("sse.reconnects", null);
        }

        void connect() {
            if (closed.get()) {
                return;
            }
            ClientRequest request = ClientRequest.get(url)
                    .header("Accept", "text/event-stream")
                    .header("Cache-Control", "no-cache");
            for (var h : config.headers().entrySet()) {
                request = request.header(h.getKey(), h.getValue());
            }
            String id = lastEventId;
            if (id != null) {
                request = request.header("Last-Event-ID", id);
            }
            current = http.stream(request, new Attempt());
            if (closed.get()) { // closed while connecting
                current.cancel();
            }
        }

        /** One connection attempt. */
        private final class Attempt implements BodyListener, SseParser.Sink {
            private final SseParser parser = new SseParser(this, config.maxEventBytes());
            private boolean opened;
            private boolean fatal;
            private boolean noContent;

            Attempt() {
                parser.setLastEventId(lastEventId);
            }

            @Override
            public void onHead(ResponseHead head, BodyControl control) throws Exception {
                if (head.status() == 204) {
                    noContent = true; // "no more events, do not reconnect"
                    fatal = true;
                    return;
                }
                String type = head.header("content-type");
                boolean sse = type != null && type.toLowerCase(Locale.ROOT).startsWith("text/event-stream");
                if (head.status() != 200 || !sse) {
                    fatal = true;
                    throw new IOException(head.status() != 200
                            ? "server answered " + head.status() + " " + head.reason() + " instead of an event stream"
                            : "unexpected content type '" + type + "', expected text/event-stream");
                }
                opened = true;
                listener.onOpen();
            }

            @Override
            public void onData(ByteBuffer data, BodyControl control) throws Exception {
                if (!noContent) {
                    parser.feed(data);
                }
            }

            @Override
            public void event(String type, String data, String id) {
                lastEventId = id;
                consecutiveFailures = 0;
                mEvents.increment();
                try {
                    listener.onMessage(new SseMessage(type, data, id));
                } catch (Throwable t) {
                    // a failing listener must not kill the subscription's parser state; surface it and carry on
                    safeError(t, true);
                }
            }

            @Override
            public void retry(long millis) {
                retryMillis = millis;
            }

            @Override
            public void onComplete() {
                if (noContent) {
                    finish();
                } else {
                    disconnected(new IOException("the server ended the event stream"));
                }
            }

            @Override
            public void onError(Throwable error) {
                if (closed.get()) {
                    finish();
                } else if (fatal) {
                    // the HTTP layer wraps what onHead threw; show the real reason
                    Throwable reason = error.getCause() != null && error.getMessage() != null
                            && error.getMessage().startsWith("stream listener failed") ? error.getCause() : error;
                    safeError(reason, false);
                    finish();
                } else {
                    disconnected(error);
                }
            }
        }

        private void disconnected(Throwable cause) {
            if (closed.get()) {
                finish();
                return;
            }
            if (!config.reconnect()) {
                safeError(cause, false);
                finish();
                return;
            }
            consecutiveFailures++;
            long base = Math.max(1, retryMillis);
            long delay = Math.min(config.maxRetry().toMillis(), base << Math.min(consecutiveFailures - 1, 20));
            delay = Math.max(delay, 1);
            safeError(cause, true);
            reconnectTimer = http.eventLoop().schedule(this::connect, delay, TimeUnit.MILLISECONDS);
            if (closed.get()) {
                reconnectTimer.cancel();
                finish();
            }
        }

        private void safeError(Throwable t, boolean willReconnect) {
            if (willReconnect) {
                mReconnects.increment();
            }
            try {
                listener.onError(t, willReconnect);
            } catch (Throwable ignored) {
                // nowhere to report
            }
        }

        /** Exactly once, whatever path ends the subscription. */
        private void finish() {
            closed.set(true);
            if (closeNotified.compareAndSet(false, true)) {
                try {
                    http.eventLoop().execute(() -> {
                        try {
                            listener.onClose();
                        } catch (Throwable ignored) {
                            // nowhere to report
                        }
                    });
                } catch (RuntimeException shutDown) {
                    try {
                        listener.onClose();
                    } catch (Throwable ignored) {
                        // nowhere to report
                    }
                }
            }
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                Scheduled t = reconnectTimer;
                if (t != null) {
                    t.cancel();
                }
                StreamHandle h = current;
                if (h != null) {
                    h.cancel(); // its onError sees closed == true and finishes
                }
                finish();
            }
        }

        @Override
        public boolean isClosed() {
            return closed.get();
        }

        @Override
        public String lastEventId() {
            return lastEventId;
        }
    }
}
