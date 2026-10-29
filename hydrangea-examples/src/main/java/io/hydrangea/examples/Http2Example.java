package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HTTP/2 server. The same {@link HttpRouter} serves HTTP/1.1 and HTTP/2; {@code withHttp2} is all that turns HTTP/2 on.
 * <pre>
 * # cleartext (h2c): prior knowledge, or an HTTP/1.1 request with "Upgrade: h2c"
 * java -cp target/classes io.hydrangea.examples.Http2Example 8080
 * curl --http2-prior-knowledge http://localhost:8080/version
 * curl --http2 http://localhost:8080/version                      # offers the h2c upgrade
 *
 * # https: HTTP/2 is negotiated with ALPN, HTTP/1.1 stays available for clients that do not offer h2
 * keytool -genkeypair -alias srv -keyalg EC -groupname secp256r1 -dname CN=localhost -ext san=dns:localhost -storetype PKCS12 -keystore srv.p12 -storepass changeit
 * java -cp target/classes io.hydrangea.examples.Http2Example 8443 srv.p12 changeit
 * curl -k --http2 -v https://localhost:8443/version               # "using HTTP/2" in the output
 *
 * curl --http2-prior-knowledge http://localhost:8080/            # list of routes
 * curl --http2-prior-knowledge -d 'hello' http://localhost:8080/echo
 * curl --http2-prior-knowledge http://localhost:8080/slow        # async: does not block the loop
 * curl --http2-prior-knowledge --parallel --parallel-immediate $(for i in 1 2 3 4 5 6 7 8; do echo http://localhost:8080/slow; done)   # one connection, 8 streams
 * </pre>
 * Try {@link Http2ClientExample} for the client side.
 */
public final class Http2Example {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        SSLContext tls = args.length > 1 ? tlsFrom(Path.of(args[1]), (args.length > 2 ? args[2] : "changeit").toCharArray()) : null;

        ScheduledExecutorService background = Executors.newScheduledThreadPool(1);   // stands in for a database or a remote call
        HttpRouter router = new HttpRouter()
                .get("/", r -> HttpResponse.ok("""
                        Hydrangea HTTP/2 example
                          GET  /version   the protocol the server saw
                          GET  /plain
                          POST /echo      echoes the body
                          GET  /slow      answers after 300 ms without blocking the loop
                          GET  /page      an HTML page; the server pushes /style.css with it (clients that accept push)
                          GET  /style.css
                        """))
                .get("/version", r -> HttpResponse.ok(r.request().version() + "\n"))
                .get("/plain", r -> HttpResponse.ok("Hello, World!"))
                .post("/echo", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(r.body()))
                .getAsync("/slow", r -> {
                    CompletableFuture<HttpResponse> later = new CompletableFuture<>();
                    background.schedule(() -> later.complete(HttpResponse.ok("slow answer\n")), 300, TimeUnit.MILLISECONDS);
                    return later;
                })
                .get("/page", r -> HttpResponse.ok("<html><link rel=stylesheet href=/style.css><body>hello</body></html>")
                        .header("Content-Type", "text/html; charset=utf-8")
                        .push("/style.css"))
                .get("/style.css", r -> HttpResponse.ok("body { font-family: sans-serif }").header("Content-Type", "text/css"));

        // Http2Config.defaults(): 100 concurrent streams per connection, 1 MiB windows, protection against rapid reset and control-frame floods
        HttpConfig config = HttpConfig.defaults().withHttp2(Http2Config.defaults().withMaxConcurrentStreams(200));
        EventLoopGroup group = new EventLoopGroup();
        HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), tls, config, router);
        System.out.println("HTTP/2 on " + (tls == null ? "http" : "https") + "://localhost:" + port + "/");
        Thread.currentThread().join();
    }

    private static SSLContext tlsFrom(Path keystore, char[] password) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(keystore)) {
            store.load(in, password);
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, password);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }
}
