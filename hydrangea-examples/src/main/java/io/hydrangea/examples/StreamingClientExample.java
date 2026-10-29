package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.ResponseHead;
import io.hydrangea.http.StreamHandle;
import io.hydrangea.sse.SseClient;
import io.hydrangea.sse.SseClientListener;
import io.hydrangea.sse.SseMessage;
import io.hydrangea.sse.SseSubscription;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Two ways to consume a response as it arrives.
 * <pre>
 * # stream a download straight to disk without holding it in memory, with progress:
 * java -cp target/classes io.hydrangea.examples.StreamingClientExample download http://localhost:8080/big.bin /path/out.bin
 *
 * # follow a Server-Sent Events stream (reconnects by itself and resumes with Last-Event-ID); try it against SseExample:
 * java -cp target/classes io.hydrangea.examples.StreamingClientExample sse http://localhost:8080/events
 * </pre>
 */
public final class StreamingClientExample {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: download <url> <file> | sse <url>");
            return;
        }
        // the stream may stay silent longer than the 30 s default between events: be generous
        HttpClientConfig config = HttpClientConfig.defaults().withIdleTimeout(Duration.ofMinutes(2));
        try (EventLoopGroup group = new EventLoopGroup(1); HttpClient http = new HttpClient(group, config)) {
            switch (args[0]) {
                case "download" -> download(http, args[1], Path.of(args[2]));
                case "sse" -> sse(http, args[1]);
                default -> System.err.println("unknown mode " + args[0]);
            }
        }
    }

    private static void download(HttpClient http, String url, Path target) throws Exception {
        FileChannel file = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        long[] received = {0};
        long[] total = {-1};
        StreamHandle handle = http.stream(ClientRequest.get(url), new BodyListener() {
            @Override
            public void onHead(ResponseHead head, BodyControl control) {
                String len = head.header("content-length");
                total[0] = len == null ? -1 : Long.parseLong(len);
                System.out.println(head.status() + " " + head.reason() + (total[0] >= 0 ? ", " + total[0] + " bytes" : ", unknown length"));
            }

            @Override
            public void onData(ByteBuffer data, BodyControl control) throws IOException {
                received[0] += data.remaining();
                while (data.hasRemaining()) {
                    file.write(data); // a slow disk could call control.pause() here and resume() when ready: the server then slows down too
                }
                System.out.printf("\r%d%s", received[0], total[0] > 0 ? " / " + total[0] + " bytes" : " bytes");
            }

            @Override
            public void onComplete() {
                System.out.println("\ndone");
            }

            @Override
            public void onError(Throwable error) {
                System.out.println("\nfailed: " + error);
            }
        });
        try {
            handle.completion().get(10, TimeUnit.MINUTES);
        } finally {
            file.close();
        }
    }

    private static void sse(HttpClient http, String url) throws Exception {
        CountDownLatch done = new CountDownLatch(1);
        SseSubscription subscription = SseClient.subscribe(http, url, new SseClientListener() {
            @Override
            public void onOpen() {
                System.out.println("* connected");
            }

            @Override
            public void onMessage(SseMessage m) {
                System.out.println(m.event() + (m.id() != null ? " #" + m.id() : "") + ": " + m.data());
            }

            @Override
            public void onError(Throwable error, boolean willReconnect) {
                System.out.println("* " + error.getMessage() + (willReconnect ? " (reconnecting)" : " (giving up)"));
            }

            @Override
            public void onClose() {
                done.countDown();
            }
        });
        System.out.println("listening for 30 seconds...");
        done.await(30, TimeUnit.SECONDS);
        subscription.close();
    }
}
