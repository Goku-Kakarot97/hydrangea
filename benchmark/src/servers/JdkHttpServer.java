package servers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/** The low bar: the JDK's built-in server (one dispatcher thread, a virtual thread per request). Usage: JdkHttpServer port ignoredThreads [bindAddress] */
public final class JdkHttpServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        String bind = args.length > 2 ? args[2] : "127.0.0.1";
        byte[] zeros = new byte[64 * 1024 * 1024];
        HttpServer server = HttpServer.create(new InetSocketAddress(bind, port), 4096);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/plain", e -> reply(e, 200, "text/plain", "Hello, World!".getBytes(StandardCharsets.UTF_8)));
        server.createContext("/json", e -> reply(e, 200, "application/json", HydrangeaHttpServer.JSON));
        server.createContext("/health", e -> reply(e, 200, "text/plain", "ok".getBytes(StandardCharsets.UTF_8)));
        server.createContext("/users/", e -> {
            String path = e.getRequestURI().getPath();
            String q = "-";
            String query = e.getRequestURI().getRawQuery();
            if (query != null) for (String kv : query.split("&")) if (kv.startsWith("q=")) q = kv.substring(2);
            reply(e, 200, "text/plain", ("user " + path.substring(7) + " q=" + q).getBytes(StandardCharsets.UTF_8));
        });
        server.createContext("/echo", e -> reply(e, 200, "application/octet-stream", e.getRequestBody().readAllBytes()));
        server.createContext("/download/", e -> {
            int n = Integer.parseInt(e.getRequestURI().getPath().substring(10));
            reply(e, 200, "application/octet-stream", n == zeros.length ? zeros : java.util.Arrays.copyOf(zeros, n));
        });
        server.start();
        System.out.println("READY");
        System.out.flush();
        Thread.currentThread().join();
    }

    private static void reply(HttpExchange e, int status, String type, byte[] body) throws IOException {
        e.getResponseHeaders().set("Content-Type", type);
        e.sendResponseHeaders(status, body.length);
        try (var out = e.getResponseBody()) {
            out.write(body);
        }
    }
}
