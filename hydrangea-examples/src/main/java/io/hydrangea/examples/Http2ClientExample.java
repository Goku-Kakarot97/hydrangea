package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * HTTP/2 client: many concurrent requests share one connection (multiplexed streams).
 * <pre>
 * java -cp target/classes io.hydrangea.examples.Http2Example 8080 &amp;      # cleartext server
 * java -cp target/classes io.hydrangea.examples.Http2ClientExample [baseUrl]
 * </pre>
 * Cleartext HTTP/2 needs {@code withCleartextPriorKnowledge(true)} (the server must speak h2c); for https the client offers h2 through ALPN
 * as soon as {@code withHttp2} is set, and falls back to HTTP/1.1 if the server does not pick h2.
 */
public final class Http2ClientExample {

    public static void main(String[] args) throws Exception {
        String base = args.length > 0 ? args[0] : "http://localhost:8080";
        HttpClientConfig config = HttpClientConfig.defaults()
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(base.startsWith("http://")));

        try (HttpClient client = new HttpClient(new EventLoopGroup(1), config)) {
            System.out.println("GET /version -> " + client.send(ClientRequest.get(base + "/version")).join().bodyAsString().trim());

            // 20 requests of 300 ms each: with one connection and multiplexing they finish together in about 300 ms, not 20 x 300 ms
            long start = System.nanoTime();
            List<CompletableFuture<ClientResponse>> calls = IntStream.range(0, 20).mapToObj(i -> client.send(ClientRequest.get(base + "/slow"))).toList();
            long ok = calls.stream().map(CompletableFuture::join).filter(ClientResponse::isSuccessful).count();
            System.out.printf("%d/20 slow requests ok in %d ms%n", ok, (System.nanoTime() - start) / 1_000_000);

            ClientResponse echo = client.send(ClientRequest.post(base + "/echo").body("hello over h2")).join();
            System.out.println("POST /echo -> " + echo.status() + " " + echo.bodyAsString());
        }
        System.exit(0);
    }
}
