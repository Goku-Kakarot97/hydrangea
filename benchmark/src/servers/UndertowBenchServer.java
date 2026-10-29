import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.xnio.Options;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Undertow with the same two routes as the Hydrangea benchmark server (GET /plain, POST /echo).
 * Usage: UndertowBenchServer port ioThreads bindAddress [flags]   flags: comma separated, any of
 * <ul>
 *   <li>h2: enable HTTP/2 (cleartext with prior knowledge, or upgrade)</li>
 *   <li>tuned: HTTP/2 stream window and concurrent streams like Hydrangea's defaults (1 MiB, 100), 16 KiB direct buffers, TCP_NODELAY, no keep-alive timeout,
 *       a bounded number of idle workers</li>
 *   <li>dispatch: run the handler on Undertow's worker pool (default io*8 threads) instead of the I/O thread, as application code normally does</li>
 * </ul>
 */
public final class UndertowBenchServer {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int io = Integer.parseInt(args[1]);
        String bind = args.length > 2 ? args[2] : "127.0.0.1";
        Set<String> flags = args.length > 3 ? Set.of(args[3].split(",")) : Set.of();
        boolean h2 = flags.contains("h2"), tuned = flags.contains("tuned"), dispatch = flags.contains("dispatch");
        ByteBuffer hello = ByteBuffer.wrap("Hello, World!".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();
        HttpHandler work = (HttpServerExchange ex) -> {
            String path = ex.getRequestPath();
            if (path.equals("/plain")) {
                ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                ex.getResponseSender().send(hello.duplicate());
            } else if (path.equals("/echo")) {
                ex.getRequestReceiver().receiveFullBytes((e, bytes) -> {
                    e.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
                    e.getResponseSender().send(ByteBuffer.wrap(bytes));
                });
            } else {
                ex.setStatusCode(404);
                ex.endExchange();
            }
        };
        HttpHandler handler = dispatch ? ex -> ex.dispatch(work) : work;
        Undertow.Builder b = Undertow.builder()
                .addHttpListener(port, bind)
                .setIoThreads(io)
                .setServerOption(UndertowOptions.ENABLE_HTTP2, h2)
                .setHandler(handler);
        if (!dispatch) {
            b.setWorkerThreads(io);
        }
        if (tuned) {
            b.setBufferSize(16 * 1024).setDirectBuffers(true)
                    // not HTTP2_SETTINGS_INITIAL_WINDOW_SIZE = 1 MiB: with it Undertow 2.3.18 never credits the connection-level window (it stays at 65535,
                    // see RESULTS-K8S-HTTP-UNDERTOW.md) and every HTTP/2 connection stalls after 64 KiB of request bodies
                    .setServerOption(UndertowOptions.HTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100)
                    .setServerOption(UndertowOptions.HTTP2_SETTINGS_ENABLE_PUSH, false)
                    .setServerOption(UndertowOptions.NO_REQUEST_TIMEOUT, 600_000)
                    .setSocketOption(Options.TCP_NODELAY, true);
        }
        if (System.getProperty("tls.keystore") != null) {   // a second, https listener on tls.port (ALPN h2 / http/1.1 when h2 is on)
            char[] pw = System.getProperty("tls.pass", "changeit").toCharArray();
            java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            try (var in = java.nio.file.Files.newInputStream(java.nio.file.Path.of(System.getProperty("tls.keystore")))) {
                store.load(in, pw);
            }
            javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(store, pw);
            javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), null, null);
            b.addHttpsListener(Integer.getInteger("tls.port", 8443), bind, ctx);
        }
        Undertow server = b.build();
        server.start();
        System.out.println("READY " + ((java.net.InetSocketAddress) server.getListenerInfo().get(0).getAddress()).getPort());
        Thread.currentThread().join();
    }
}
