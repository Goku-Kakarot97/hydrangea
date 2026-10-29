package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.sse.SseConfig;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseEvent;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Server-Sent Events: a clock ticking every second plus a broadcast channel.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.SseExample [port]
 *
 * open  http://localhost:8080/                                   # browser page using EventSource
 * curl -N http://localhost:8080/events                           # raw stream
 * curl -N -H 'Last-Event-ID: 5' http://localhost:8080/events     # a reconnecting client says where it was
 * curl -d 'hello everyone' http://localhost:8080/publish         # delivered to every connected client
 * </pre>
 */
public final class SseExample {

    private static final String PAGE = """
            <!doctype html><meta charset=utf-8><title>Hydrangea SSE</title>
            <body style="font:16px system-ui;max-width:40rem;margin:2rem auto">
            <h3>Server-Sent Events</h3><pre id=log style="height:18rem;overflow:auto;border:1px solid #888;padding:.5rem"></pre>
            <script>
              const log = document.getElementById('log'), add = t => { log.textContent += t + '\\n'; log.scrollTop = log.scrollHeight; };
              const es = new EventSource('/events');   // reconnects by itself and sends Last-Event-ID
              es.onopen = () => add('* connected');
              es.onerror = () => add('* connection lost, retrying...');
              es.addEventListener('tick', e => add('tick ' + e.data + ' (id ' + e.lastEventId + ')'));
              es.addEventListener('news', e => add('NEWS: ' + e.data));
            </script>""";

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        EventLoopGroup group = new EventLoopGroup();
        Set<SseEmitter> clients = ConcurrentHashMap.newKeySet();
        AtomicLong sequence = new AtomicLong();

        SseConfig config = SseConfig.defaults()
                .withHeartbeat(Duration.ofSeconds(15))       // comment lines keep proxies from closing a quiet stream
                .withMaxPendingBytes(256 * 1024)             // a client that stops reading is dropped, not buffered forever
                .withRetry(Duration.ofSeconds(2));           // reconnect delay suggested to the browser

        HttpRouter router = new HttpRouter()
                .get("/", r -> HttpResponse.status(200).header("Content-Type", "text/html; charset=utf-8")
                        .body(PAGE.getBytes(StandardCharsets.UTF_8)))
                .get("/events", r -> SseResponse.accept(r.request(), config, () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter client) {
                        clients.add(client);
                        String resumeFrom = client.lastEventId(); // non-null when the browser reconnected
                        client.send(SseEvent.data(resumeFrom == null ? "welcome" : "welcome back, last seen id " + resumeFrom).event("news"));
                    }

                    @Override
                    public void onClose(SseEmitter client) {
                        clients.remove(client);
                    }
                }))
                .post("/publish", r -> {
                    SseEvent news = SseEvent.data(r.bodyAsString()).event("news");
                    clients.forEach(c -> c.send(news));
                    return HttpResponse.ok("sent to " + clients.size() + " client(s)");
                });

        Server server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), null, HttpConfig.defaults(), router);

        // the clock: one event per second to everybody; the id lets a reconnecting client tell us where it was
        var clock = Executors.newSingleThreadScheduledExecutor();
        clock.scheduleAtFixedRate(() -> {
            SseEvent tick = SseEvent.data(LocalTime.now().withNano(0).toString()).event("tick").id(String.valueOf(sequence.incrementAndGet()));
            clients.forEach(c -> {
                if (c.isWritable()) {
                    c.send(tick);
                }
            });
        }, 1, 1, TimeUnit.SECONDS);

        System.out.println("open http://localhost:" + ((InetSocketAddress) server.localAddress()).getPort() + "/");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            clock.shutdownNow();
            clients.forEach(SseEmitter::close);
            server.close();
            group.close();
        }));
        Thread.currentThread().join();
    }
}
