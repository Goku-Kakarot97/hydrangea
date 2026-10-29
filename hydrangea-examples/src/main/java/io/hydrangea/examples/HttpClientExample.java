package io.hydrangea.examples;

import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Fetches URLs concurrently with the asynchronous HTTP client.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.HttpClientExample https://example.org/ http://localhost:8080/hello?name=Sinh
 * </pre>
 * Start {@link HttpExample} first to try the localhost URLs.
 */
public final class HttpClientExample {

    public static void main(String[] args) throws Exception {
        List<String> urls = args.length > 0 ? List.of(args) : List.of("http://localhost:8080/hello?name=Sinh", "http://localhost:8080/users/42");

        HttpClientConfig config = HttpClientConfig.defaults()
                .withRequestTimeout(Duration.ofSeconds(10))   // overall deadline per request
                .withMaxConnectionsPerHost(8);                // more requests than this queue up, they do not fail

        try (HttpClient client = new HttpClient(new io.hydrangea.EventLoopGroup(2), config)) {
            // 1. all requests start immediately; connections to the same host are pooled and reused
            List<CompletableFuture<String>> results = urls.stream()
                    .map(url -> client.send(ClientRequest.get(url).header("Accept", "text/plain, */*"))
                            .handle((response, error) -> error != null
                                    ? url + " -> FAILED: " + error
                                    : url + " -> " + response.status() + " " + abbreviate(response)))
                    .toList();
            results.forEach(r -> System.out.println(r.join()));

            // 2. a PUT with a form body; redirects are followed automatically (301/302/303 turn POST into GET)
            ClientResponse created = client.send(ClientRequest.put("http://localhost:8080/users/7")
                            .header("Content-Type", "application/x-www-form-urlencoded") // otherwise the server does not parse it as a form
                            .body("name=An&age=30"))
                    .exceptionally(e -> null)
                    .join();
            System.out.println(created == null ? "PUT failed (is HttpExample running?)" : "PUT -> " + created.status() + " " + created.bodyAsString());
        } // close() ends every connection; the shared group above is ours to shut down
        System.exit(0);
    }

    private static String abbreviate(ClientResponse r) {
        String body = r.bodyAsString().replace('\n', ' ');
        return body.length() > 60 ? body.substring(0, 60) + "..." : body;
    }
}
