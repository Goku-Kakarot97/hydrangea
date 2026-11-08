package io.hydrangea.sse;

import io.hydrangea.http.HttpRequest;
import io.hydrangea.http.HttpResponse;

import java.util.function.Supplier;

/**
 * Starts a Server-Sent Events stream from a normal HTTP route:
 * <pre>{@code
 * router.get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
 *     public void onOpen(SseEmitter e) { subscribers.add(e); e.send(SseEvent.data("hello").event("greeting")); }
 *     public void onClose(SseEmitter e) { subscribers.remove(e); }
 * }));
 * }</pre>
 * The returned response may be customised before it is returned (e.g. {@code .header("Access-Control-Allow-Origin", ...)}
 * for cross-origin browsers). The listener supplier is called once per accepted client.
 * <p>
 * GET is what a browser's {@code EventSource} sends; POST is accepted too, for APIs that take a request body and answer with
 * a stream of events (a chat completion), read the body from {@code request.body()} before accepting. Other methods get 405.
 */
public final class SseResponse {

    private SseResponse() {
    }

    public static HttpResponse accept(HttpRequest request, Supplier<? extends SseListener> listener) {
        return accept(request, SseConfig.defaults(), listener);
    }

    public static HttpResponse accept(HttpRequest request, SseConfig config, Supplier<? extends SseListener> listener) {
        if (!request.method().equals("GET") && !request.method().equals("POST")) {
            return HttpResponse.status(405).header("Allow", "GET, POST").text("Method Not Allowed");
        }
        boolean chunked = request.version().equals("HTTP/1.1"); // HTTP/1.0 clients get a close-delimited body
        String lastEventId = request.headers().get("last-event-id");
        HttpResponse response = HttpResponse.status(200)
                .header("Content-Type", "text/event-stream; charset=utf-8")
                .header("Cache-Control", "no-cache")
                .header("X-Accel-Buffering", "no"); // nginx: do not buffer the stream
        if (chunked) {
            response.header("Transfer-Encoding", "chunked");
        }
        return response.stream(() -> new SseConnection(listener.get(), config, chunked, lastEventId));
    }
}
