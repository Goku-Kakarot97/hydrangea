package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Sends a file of any size over one WebSocket message, with constant memory on both ends.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.WebSocketStreamExample server 8080 /path/received.bin
 * java -cp target/classes io.hydrangea.examples.WebSocketStreamExample send ws://localhost:8080/up /path/file.bin
 * </pre>
 * The receiver opts in with {@code streaming() == true}, so the message arrives in pieces instead of being buffered
 * (and limited by {@code maxMessageBytes}). The sender uses {@link WebSocket#sendBinaryStream}, which reads the file in
 * 64 KiB blocks and waits for the socket to drain between fragments.
 */
public final class WebSocketStreamExample {

    public static void main(String[] args) throws Exception {
        switch (args.length > 0 ? args[0] : "") {
            case "server" -> server(Integer.parseInt(args[1]), Path.of(args[2]));
            case "send" -> send(args[1], Path.of(args[2]));
            default -> System.err.println("usage: server <port> <target-file> | send <ws-url> <file>");
        }
    }

    private static void server(int port, Path target) throws Exception {
        EventLoopGroup group = new EventLoopGroup();
        HttpRouter router = new HttpRouter().get("/up", r -> WebSocketUpgrade.accept(r.request(), WebSocketConfig.defaults(), () -> new WebSocketListener() {
            private FileChannel file;
            private long received;

            @Override
            public boolean streaming() {
                return true; // give me the pieces, do not buffer the message
            }

            @Override
            public void onOpen(WebSocket ws) throws IOException {
                file = FileChannel.open(target, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            }

            @Override
            public void onBinaryPart(WebSocket ws, ByteBuffer part, boolean last) throws IOException {
                received += part.remaining();
                while (part.hasRemaining()) {
                    file.write(part); // slow disk? ws.setAutoRead(false) here and setAutoRead(true) when ready
                }
                if (last) {
                    file.close();
                    ws.sendText("stored " + received + " bytes");
                }
            }

            @Override
            public void onClose(WebSocket ws, int code, String reason) {
                try {
                    if (file != null) {
                        file.close();
                    }
                } catch (IOException ignored) {
                    // already closing
                }
            }
        }));
        Server server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), null, HttpConfig.defaults(), router);
        System.out.println("receiving on ws://localhost:" + ((InetSocketAddress) server.localAddress()).getPort() + "/up -> " + target);
        Thread.currentThread().join();
    }

    private static void send(String url, Path file) throws Exception {
        CompletableFuture<String> reply = new CompletableFuture<>();
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            WebSocket ws = WebSocketClient.connect(group, url, new WebSocketListener() {
                @Override
                public void onText(WebSocket ws, String text) {
                    reply.complete(text);
                }
            }).get(10, TimeUnit.SECONDS);
            long t0 = System.nanoTime();
            try (InputStream in = Files.newInputStream(file)) {
                ws.sendBinaryStream(in).get(10, TimeUnit.MINUTES);
            }
            System.out.println(reply.get(1, TimeUnit.MINUTES) + String.format(" (%.1f MB/s)", Files.size(file) / ((System.nanoTime() - t0) / 1e9) / 1e6));
            ws.close().get(10, TimeUnit.SECONDS);
        }
    }
}
