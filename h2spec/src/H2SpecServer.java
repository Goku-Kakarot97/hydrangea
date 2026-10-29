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

/** Target for h2spec: cleartext HTTP/2 on args[0], HTTPS (ALPN h2) on args[1] with the PKCS12 keystore args[2] (password "changeit"). */
public final class H2SpecServer {
    public static void main(String[] args) throws Exception {
        EventLoopGroup group = new EventLoopGroup(2);
        HttpRouter router = new HttpRouter()
                .get("/", r -> HttpResponse.ok("hello"))
                .post("/", r -> HttpResponse.status(200).body(r.body()));
        HttpConfig cfg = HttpConfig.defaults().withHttp2(Http2Config.defaults());
        HttpServer.bind(group, new InetSocketAddress("0.0.0.0", Integer.parseInt(args[0])), Options.defaults(), null, cfg, router);
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(args[2]))) {
            store.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(store, "changeit".toCharArray());
        SSLContext tls = SSLContext.getInstance("TLS");
        tls.init(kmf.getKeyManagers(), null, null);
        HttpServer.bind(group, new InetSocketAddress("0.0.0.0", Integer.parseInt(args[1])), Options.defaults(), tls, cfg, router);
        System.out.println("READY");
        Thread.currentThread().join();
    }
}
