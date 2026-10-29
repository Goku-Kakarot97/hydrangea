package bench;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RequestBody;
import io.hydrangea.http.StreamHandle;
import io.hydrangea.sse.SseEmitter;
import io.hydrangea.sse.SseListener;
import io.hydrangea.sse.SseResponse;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Leak hunt for the HTTP/2 paths: Hydrangea's client against Hydrangea's server in one JVM, with traffic that ends in every way a stream
 * can end (answered, cancelled, timed out, reset by the client, aborted mid-upload, event stream dropped), then a count of the
 * stream objects still alive once the traffic stops and the connections are idle. Usage: Http2Stress seconds
 */
public final class Http2Stress {

    public static void main(String[] args) throws Exception {
        int seconds = Integer.parseInt(args[0]);
        EventLoopGroup serverGroup = new EventLoopGroup(2);
        EventLoopGroup clientGroup = new EventLoopGroup(2);
        Set<SseEmitter> emitters = ConcurrentHashMap.newKeySet();
        Set<CompletableFuture<HttpResponse>> slow = ConcurrentHashMap.newKeySet();
        AtomicLong sunk = new AtomicLong();
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        ticker.scheduleAtFixedRate(() -> {
            for (SseEmitter e : emitters) {
                if (e.isOpen() && e.isWritable()) e.send("tick");
            }
            for (var f : slow) {                          // answer part of the slow requests after a while
                if (ThreadLocalRandom.current().nextInt(4) == 0) {
                    f.complete(HttpResponse.ok("slow"));
                    slow.remove(f);
                }
            }
        }, 20, 20, TimeUnit.MILLISECONDS);

        HttpRouter router = new HttpRouter()
                .get("/hello", r -> HttpResponse.ok("hello"))
                .get("/big", r -> HttpResponse.status(200).body(new byte[300_000]))
                .post("/echo", r -> HttpResponse.status(200).body(r.body()))
                .getAsync("/slow", r -> {
                    CompletableFuture<HttpResponse> f = new CompletableFuture<>();
                    slow.add(f);
                    return f;
                })
                .get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                    @Override
                    public void onOpen(SseEmitter e) {
                        emitters.add(e);
                    }

                    @Override
                    public void onClose(SseEmitter e) {
                        emitters.remove(e);
                    }
                }))
                .postStream("/sink", (r, body) -> {
                    CompletableFuture<HttpResponse> done = new CompletableFuture<>();
                    body.listen(new BodyListener() {
                        @Override
                        public void onData(ByteBuffer d, BodyControl c) {
                            sunk.addAndGet(d.remaining());
                        }

                        @Override
                        public void onComplete() {
                            done.complete(HttpResponse.ok("done"));
                        }

                        @Override
                        public void onError(Throwable e) {
                            done.completeExceptionally(e);
                        }
                    });
                    return done;
                });
        Server server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null,
                HttpConfig.defaults().withHttp2(Http2Config.defaults()), router);
        int port = ((InetSocketAddress) server.localAddress()).getPort();
        String base = "http://127.0.0.1:" + port;
        HttpClient client = new HttpClient(clientGroup, HttpClientConfig.defaults().withRequestTimeout(Duration.ofSeconds(20))
                .withMaxConnectionsPerHost(3).withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)));

        AtomicLong ok = new AtomicLong(), failed = new AtomicLong(), cancelled = new AtomicLong(), timedOut = new AtomicLong();
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        Thread[] workers = new Thread[8];
        for (int w = 0; w < workers.length; w++) {
            int id = w;
            workers[w] = Thread.ofPlatform().name("stress-" + w).start(() -> {
                var rnd = ThreadLocalRandom.current();
                while (System.nanoTime() < end) {
                    try {
                        switch (rnd.nextInt(10)) {
                            case 0, 1, 2 -> { client.get(base + "/hello").get(10, TimeUnit.SECONDS); ok.incrementAndGet(); }
                            case 3 -> { client.get(base + "/big").get(10, TimeUnit.SECONDS); ok.incrementAndGet(); }
                            case 4 -> { client.send(ClientRequest.post(base + "/echo").body(new byte[rnd.nextInt(1, 200_000)])).get(10, TimeUnit.SECONDS); ok.incrementAndGet(); }
                            case 5 -> { // started and then given up on
                                var f = client.get(base + "/slow");
                                Thread.sleep(rnd.nextInt(30));
                                f.cancel(true);
                                cancelled.incrementAndGet();
                            }
                            case 6 -> { // a deadline that fires
                                try {
                                    client.send(ClientRequest.get(base + "/slow").timeout(Duration.ofMillis(rnd.nextInt(5, 60)))).get(10, TimeUnit.SECONDS);
                                    ok.incrementAndGet();
                                } catch (java.util.concurrent.ExecutionException e) {
                                    timedOut.incrementAndGet();
                                }
                            }
                            case 7 -> { // an event stream that the client drops
                                StreamHandle h = client.stream(ClientRequest.get(base + "/events"), new BodyListener() { });
                                Thread.sleep(rnd.nextInt(80));
                                h.cancel();
                                cancelled.incrementAndGet();
                            }
                            case 8 -> { // an upload that the producer abandons half way
                                RequestBody body = RequestBody.producer(5_000_000, false, sink -> {
                                    for (int i = 0; i < 20; i++) {
                                        sink.whenWritable().get(5, TimeUnit.SECONDS);
                                        sink.write(ByteBuffer.wrap(new byte[100_000]));
                                    }
                                    sink.fail(new java.io.IOException("giving up"));
                                });
                                try {
                                    client.send(ClientRequest.post(base + "/sink").body(body)).get(10, TimeUnit.SECONDS);
                                } catch (java.util.concurrent.ExecutionException e) {
                                    failed.incrementAndGet();
                                }
                            }
                            default -> { // a complete streamed upload
                                RequestBody body = RequestBody.producer(2_000_000, false, sink -> {
                                    for (int i = 0; i < 20; i++) {
                                        sink.whenWritable().get(5, TimeUnit.SECONDS);
                                        sink.write(ByteBuffer.wrap(new byte[100_000]));
                                    }
                                    sink.finish();
                                });
                                client.send(ClientRequest.post(base + "/sink").body(body)).get(20, TimeUnit.SECONDS);
                                ok.incrementAndGet();
                            }
                        }
                    } catch (java.util.concurrent.CancellationException expected) {
                        // counted where it happened
                    } catch (Exception e) {
                        failed.incrementAndGet();
                        if (failed.get() < 5) System.err.println("unexpected: " + e);
                    }
                }
            });
        }
        // progress
        for (int t = 10; t <= seconds; t += 10) {
            Thread.sleep(10_000);
            System.out.printf("t=%ds ok=%d cancelled=%d timedOut=%d failed=%d emitters=%d slowPending=%d serverConns=%d%n", t, ok.get(), cancelled.get(),
                    timedOut.get(), failed.get(), emitters.size(), slow.size(), server.activeConnections());
        }
        for (Thread t : workers) t.join();
        slow.forEach(f -> f.complete(HttpResponse.ok("late")));
        Thread.sleep(3000);  // let the last resets and closes drain
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(300);
        }
        System.out.printf("after the traffic: emitters still registered=%d, slow pending=%d, server connections=%d (client keeps a few idle ones)%n",
                emitters.size(), slow.size(), server.activeConnections());
        System.out.println("IDLE pid=" + ProcessHandle.current().pid());   // run-http2-stress.sh takes a class histogram now
        System.out.flush();
        Thread.sleep(25_000);
        client.close();
        Thread.sleep(1500);
        System.out.println("after the client closed: server connections=" + server.activeConnections());
        System.exit(0);
    }
}
