package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.http.RequestBody;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Streaming uploads in both directions, without ever holding a file in memory.
 * <pre>
 * # terminal 1: a server that stores PUT /files/{name} into a directory (limit 5 GB, rejects early with 413)
 * java -cp target/classes io.hydrangea.examples.UploadExample server 8080 /path/to/dir
 *
 * # terminal 2: upload a file of any size (waits for the server's go-ahead, so a refusal costs nothing)
 * java -cp target/classes io.hydrangea.examples.UploadExample upload http://localhost:8080/files/backup.tar /path/backup.tar
 * </pre>
 */
public final class UploadExample {

    private static final long LIMIT = 5L * 1024 * 1024 * 1024;

    public static void main(String[] args) throws Exception {
        switch (args.length > 0 ? args[0] : "") {
            case "server" -> server(Integer.parseInt(args[1]), Path.of(args[2]));
            case "upload" -> upload(args[1], Path.of(args[2]));
            default -> System.err.println("usage: server <port> <dir> | upload <url> <file>");
        }
    }

    private static void server(int port, Path dir) throws Exception {
        EventLoopGroup group = new EventLoopGroup();
        HttpRouter router = new HttpRouter().routeStream("PUT", "/files/{name}", (r, body) -> {
            String name = Path.of(r.pathVariable("name")).getFileName().toString(); // never let a client pick a directory
            if (body.contentLength() > LIMIT) {
                // refusing before reading a byte: with Expect: 100-continue the client does not even start sending
                return CompletableFuture.completedFuture(HttpResponse.status(413).text("limit is " + LIMIT + " bytes"));
            }
            var done = new CompletableFuture<HttpResponse>();
            FileChannel file;
            try {
                file = FileChannel.open(dir.resolve(name), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                return CompletableFuture.completedFuture(HttpResponse.status(500).text("cannot open target"));
            }
            long[] received = {0};
            body.listen(new BodyListener() {
                @Override
                public void onData(ByteBuffer data, BodyControl control) throws IOException {
                    received[0] += data.remaining();
                    if (received[0] > LIMIT) { // chunked uploads have no declared length: count as they arrive
                        closeQuietly(file);
                        done.complete(HttpResponse.status(413).text("limit is " + LIMIT + " bytes"));
                        return;
                    }
                    while (data.hasRemaining()) {
                        file.write(data); // a slow disk? control.pause() here and control.resume() when ready: the sender slows down too
                    }
                }

                @Override
                public void onComplete() {
                    closeQuietly(file);
                    done.complete(HttpResponse.ok("stored " + received[0] + " bytes as " + name));
                }

                @Override
                public void onError(Throwable error) {
                    closeQuietly(file); // client vanished or was rejected: keep no half file around in a real application
                }
            });
            return done;
        });
        // idle timeout: a client that stalls (or a paused body) must not hold a connection forever
        Server server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults().withIdleTimeout(Duration.ofMinutes(2)),
                null, HttpConfig.defaults(), router);
        System.out.println("storing uploads in " + dir + " on http://localhost:" + ((InetSocketAddress) server.localAddress()).getPort() + "/files/{name}");
        Thread.currentThread().join();
    }

    private static void upload(String url, Path file) throws Exception {
        HttpClientConfig config = HttpClientConfig.defaults().withRequestTimeout(Duration.ofHours(1)); // a big upload takes a while
        try (HttpClient http = new HttpClient(new EventLoopGroup(1), config)) {
            long size = Files.size(file);
            System.out.println("uploading " + size + " bytes...");
            long t0 = System.nanoTime();
            ClientResponse r = http.send(ClientRequest.put(url).expectContinue().body(RequestBody.file(file))).get();
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("%d %s (%.1f MB/s)%n", r.status(), r.bodyAsString(), size / secs / 1e6);
        }
        System.exit(0);
    }

    private static void closeQuietly(FileChannel f) {
        try {
            f.close();
        } catch (IOException ignored) {
            // nothing to do
        }
    }
}
