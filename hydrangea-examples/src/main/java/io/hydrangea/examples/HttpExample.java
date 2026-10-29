package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.AccessLog;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.HttpStatusException;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HTTP server showing path variables, request parameters, form bodies, async routes and error handling.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.HttpExample [port]
 *
 * curl http://localhost:8080/
 * curl 'http://localhost:8080/hello?name=Sinh'                                  # request param
 * curl http://localhost:8080/users/42                                           # path variable
 * curl -X PUT -d 'name=An&age=30' http://localhost:8080/users/42                 # form body params
 * curl http://localhost:8080/users/42/posts/7                                   # two path variables
 * curl 'http://localhost:8080/search?q=java&tag=nio&tag=net&limit=5&exact'   # repeated + typed params
 * curl http://localhost:8080/files/docs/readme.txt                              # catch-all path
 * curl http://localhost:8080/slow                                               # async, does not block the loop
 * curl http://localhost:8080/users/abc                                          # -> 400, id must be an integer
 * curl -X DELETE http://localhost:8080/users/42                                 # -> 405 with Allow header
 * </pre>
 */
public final class HttpExample {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        EventLoopGroup group = new EventLoopGroup();
        // Stand-in for a database / remote call. Never block inside a route: it runs on the event loop.
        ScheduledExecutorService background = Executors.newScheduledThreadPool(1);
        Map<Integer, String> users = new ConcurrentHashMap<>(Map.of(42, "Sinh"));

        HttpRouter router = new HttpRouter()
                .get("/", r -> HttpResponse.ok("""
                        Hydrangea HTTP example
                          GET  /hello?name=...
                          GET  /users/{id}          PUT /users/{id} (form: name=...)
                          GET  /users/{id}/posts/{postId}
                          GET  /search?q=...&tag=...&limit=...
                          GET  /files/{path...}
                          GET  /slow     GET /metrics
                        """))

                // request parameter with a default
                .get("/hello", r -> {
                    System.out.printf("Thread: " + Thread.currentThread().getName());
                    return HttpResponse.ok("Hello, " + r.param("name", "world") + "!");
                })

                // path variable; pathInt answers 400 by itself when it is not a number
                .get("/users/{id}", r -> {
                    int id = r.pathInt("id");
                    String name = users.get(id);
                    if (name == null) {
                        throw new HttpStatusException(404, "no user " + id); // becomes a 404 response
                    }
                    return HttpResponse.ok("user " + id + " = " + name);
                })

                // path variable + form body parameters (application/x-www-form-urlencoded)
                .put("/users/{id}", r -> {
                    int id = r.pathInt("id");
                    boolean created = users.put(id, r.requiredParam("name")) == null; // 400 if "name" is missing
                    return HttpResponse.status(created ? 201 : 200).text("saved user " + id + " age=" + r.paramInt("age", -1));
                })

                // several path variables
                .get("/users/{userId}/posts/{postId}", r ->
                        HttpResponse.ok("post " + r.pathLong("postId") + " of user " + r.pathVariable("userId")))

                // repeated parameter, int and boolean parameters with defaults
                .get("/search", r -> HttpResponse.ok(
                        "q=" + r.requiredParam("q") + ", tags=" + r.params("tag")
                                + ", limit=" + r.paramInt("limit", 10) + ", exact=" + r.paramBool("exact", false)))

                // catch-all: everything after /files/
                .get("/files/{path...}", r -> HttpResponse.ok("would serve: " + r.pathVariable("path")))

                // async route: the event loop is free while we wait
                .getAsync("/slow", r -> {
                    var future = new CompletableFuture<HttpResponse>();
                    background.schedule(() -> future.complete(HttpResponse.ok("finished after 1 second")), 1, TimeUnit.SECONDS);
                    return future;
                })

                // the numbers a monitoring system (Prometheus, Grafana Agent...) scrapes
                .get("/metrics", r -> HttpResponse.status(200)
                        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                        .body(group.metrics().snapshot().toPrometheus("hydrangea").getBytes(StandardCharsets.UTF_8)))

                .notFound(r -> HttpResponse.status(404).text("No route for " + r.method() + " " + r.path()));

        Options options = Options.defaults()
                .withMaxConnections(10_000)              // beyond this, new clients are turned away instead of exhausting memory
                .withMaxConnectionsPerAddress(200);      // ...and no single host can take them all
        HttpConfig limits = new HttpConfig(16 * 1024, 1024 * 1024)       // 16 KiB headers, 1 MiB body
                .withHeaderTimeout(Duration.ofSeconds(10))               // a request head must arrive within 10 s, however slowly it dribbles
                .withKeepAliveTimeout(Duration.ofSeconds(60))            // idle keep-alive connections are closed after a minute
                .withAccessLog(AccessLog.toLogger("access"));            // one log line per response
        Server server = HttpServer.bind(group, new InetSocketAddress(port), options, null, limits, router);

        System.out.println("listening on http://localhost:" + ((InetSocketAddress) server.localAddress()).getPort());
        // Ctrl-C / SIGTERM: stop accepting, let requests in progress finish (up to 10 s), then stop the loops
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdownGracefully(Duration.ofSeconds(10)).join();
            group.close();
            background.shutdownNow();
        }));
        Thread.currentThread().join();
    }
}
