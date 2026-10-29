package servers;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.sse.SseConfig;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseEvent;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark / soak target built on Hydrangea: the same routes as {@link NettyHttpServer} and {@link JdkHttpServer}, plus
 * WebSocket, SSE, streaming upload and a Prometheus endpoint. Usage: HydrangeaHttpServer port ioThreads [bindAddress]
 * <p>
 * GET /plain  GET /json  GET /users/{id}?q=  POST /echo  POST /sink (streamed)  GET /download/{bytes}
 * GET /ws (echo)  GET /sse?interval=ms (timestamp ticks to every subscriber)  GET /health  GET /metrics
 */
public final class HydrangeaHttpServer {

    static final byte[] JSON = ("{\"id\":12345,\"name\":\"hydrangea\",\"active\":true,\"tags\":[\"alpha\",\"beta\",\"gamma\"],"
            + "\"scores\":[1.5,2.25,3.125],\"address\":{\"city\":\"Ho Chi Minh\",\"zip\":\"700000\"}}").getBytes(StandardCharsets.UTF_8);

    /** Prints one "health ..." line every {@code seconds}: the numbers a leak shows up in (live heap after GC, threads, descriptors, connections). */
    private static void startHealthReports(Server server, EventLoopGroup group, int seconds) {
        var os = (com.sun.management.UnixOperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        var threads = java.lang.management.ManagementFactory.getThreadMXBean();
        long started = System.nanoTime();
        Thread t = new Thread(() -> {
            for (;;) {
                try {
                    Thread.sleep(seconds * 1000L);
                } catch (InterruptedException e) {
                    return;
                }
                long oldGen = 0; // what survived the last collection, summed over the heap pools: the honest "live data" number
                for (var pool : java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
                    if (pool.getType() == java.lang.management.MemoryType.HEAP && pool.getCollectionUsage() != null) {
                        oldGen += pool.getCollectionUsage().getUsed();
                    }
                }
                Runtime rt = Runtime.getRuntime();
                var m = group.metrics().snapshot();
                System.out.printf("health t=%d heapMB=%d oldGenAfterGcMB=%d threads=%d fds=%d active=%d opened=%d closed=%d rejected=%d http=%d timeouts=%d maxBlockMs=%d%n",
                        (System.nanoTime() - started) / 1_000_000_000L, (rt.totalMemory() - rt.freeMemory()) >> 20, oldGen >> 20, threads.getThreadCount(),
                        os.getOpenFileDescriptorCount(), server.activeConnections(), m.connectionsOpened(), m.connectionsClosed(), m.connectionsRejected(),
                        m.httpRequests(), m.httpTimeouts(), group.metrics().resetMaxBlockNanos() / 1_000_000);
                System.out.flush();
            }
        }, "health");
        t.setDaemon(true);
        t.start();
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);
        String bind = args.length > 2 ? args[2] : "127.0.0.1";
        EventLoopGroup group = new EventLoopGroup(threads);
        Set<SseEmitter> subscribers = ConcurrentHashMap.newKeySet();
        byte[] zeros = new byte[64 * 1024 * 1024];

        HttpRouter router = new HttpRouter()
                .get("/plain", r -> HttpResponse.status(200).header("Content-Type", "text/plain").body("Hello, World!".getBytes(StandardCharsets.UTF_8)))
                .get("/json", r -> HttpResponse.status(200).header("Content-Type", "application/json").body(JSON))
                .get("/users/{id}", r -> HttpResponse.status(200).header("Content-Type", "text/plain")
                        .body(("user " + r.pathVariable("id") + " q=" + r.param("q", "-")).getBytes(StandardCharsets.UTF_8)))
                .post("/echo", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(r.body()))
                .get("/download/{n}", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream")
                        .body(r.pathInt("n") == zeros.length ? zeros : java.util.Arrays.copyOf(zeros, r.pathInt("n"))))
                .postStream("/sink", (r, body) -> {
                    var done = new CompletableFuture<HttpResponse>();
                    long[] n = {0};
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer d, BodyControl c) {
                            n[0] += d.remaining();
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok(Long.toString(n[0])));
                        }
                    });
                    return done;
                })
                .get("/health", r -> HttpResponse.ok("ok"))
                .get("/metrics", r -> HttpResponse.status(200).header("Content-Type", "text/plain; version=0.0.4")
                        .body(group.metrics().snapshot().toPrometheus("hydrangea").getBytes(StandardCharsets.UTF_8)))
                .get("/ws", r -> WebSocketUpgrade.accept(r.request(), WebSocketConfig.defaults(), () -> new WebSocketListener() {
                    @Override
                    public void onText(WebSocket ws, String text) {
                        ws.sendText(text);
                    }

                    @Override
                    public void onBinary(WebSocket ws, ByteBuffer data) {
                        ws.sendBinary(data);
                    }
                }))
                .get("/sse", r -> SseResponse.accept(r.request(), SseConfig.defaults().withHeartbeat(Duration.ofSeconds(15)), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        subscribers.add(e);
                    }

                    @Override
                    public void onClose(SseEmitter e) {
                        subscribers.remove(e);
                    }
                }));

        // one ticker for everybody: each event carries the wall-clock time it was produced, so clients can measure delivery latency
        int tickMs = Integer.getInteger("sse.tick.ms", 100);
        Executors.newSingleThreadScheduledExecutor(rn -> {
            Thread t = new Thread(rn, "sse-ticker");
            t.setDaemon(true);
            return t;
        }).scheduleAtFixedRate(() -> {
            SseEvent tick = SseEvent.data(Long.toString(System.currentTimeMillis()));
            for (SseEmitter e : subscribers) {
                if (e.isWritable()) {
                    e.send(tick);
                }
            }
        }, tickMs, tickMs, TimeUnit.MILLISECONDS);

        javax.net.ssl.SSLContext tls = null;
        if (System.getProperty("tls.keystore") != null) {   // https (ALPN h2 / http/1.1 when -Dh2=true), PKCS12 keystore
            char[] pw = System.getProperty("tls.pass", "changeit").toCharArray();
            java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            try (var in = java.nio.file.Files.newInputStream(java.nio.file.Path.of(System.getProperty("tls.keystore")))) {
                store.load(in, pw);
            }
            javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(store, pw);
            tls = javax.net.ssl.SSLContext.getInstance("TLS");
            tls.init(kmf.getKeyManagers(), null, null);
        }
        Server server = HttpServer.bind(group, new InetSocketAddress(bind, port), Options.defaults().withBacklog(4096), tls,
                (Boolean.getBoolean("h2") ? HttpConfig.defaults().withHttp2(io.hydrangea.http.Http2Config.defaults()) : HttpConfig.defaults()).withKeepAliveTimeout(Duration.ofMinutes(5)), router);
        int healthSec = Integer.getInteger("health.interval.sec", 0);
        if (healthSec > 0) {
            startHealthReports(server, group, healthSec);
        }
        // SIGTERM (Kubernetes, systemd, Ctrl-C): stop accepting, let requests in progress finish, then stop the loops
        long graceSec = Long.getLong("shutdown.grace.sec", 20);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdownGracefully(Duration.ofSeconds(graceSec)).join();
            group.close();
        }));
        System.out.println("READY");
        System.out.flush();
        Thread.currentThread().join();
    }
}
