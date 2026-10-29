package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.Http3Server;
import io.hydrangea.quic.QuicConfig;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;

/**
 * HTTP/3 server with two routes (GET /plain, POST /echo) and GET /download/{bytes}.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.Http3Example port keystore.p12 [password] [alias] [bindAddress]
 * </pre>
 */
public final class Http3Example {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        char[] password = (args.length > 2 ? args[2] : "changeit").toCharArray();
        String alias = args.length > 3 ? args[3] : "srv";
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(args[1]))) {
            store.load(in, password);
        }
        HttpRouter router = new HttpRouter();
        router.get("/plain", req -> HttpResponse.ok("hello"));
        byte[] zeros = new byte[4_000_000];
        router.get("/download/{n}", req -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(java.util.Arrays.copyOf(zeros, Math.min(req.pathInt("n"), zeros.length))));
        router.post("/echo", req -> HttpResponse.status(200).body(req.body()));
        EventLoopGroup group = new EventLoopGroup(1);
        Http3Server server = Http3Server.bind(group, new InetSocketAddress(args.length > 4 ? args[4] : "127.0.0.1", port), QuicConfig.fromKeyStore(store, alias, password, List.of("h3")),
                HttpConfig.defaults(), router);
        System.out.println("HTTP/3 on " + server.localAddress());
        Thread.currentThread().join();
    }
}
