import io.hydrangea.http.*;
import javax.net.ssl.*;
import java.nio.file.*;
import java.security.KeyStore;
import java.util.concurrent.*;

public class Http2InteropClient {
    public static void main(String[] a) throws Exception {
        if (a[0].equals("proxy")) { proxy(a); return; }
        boolean tls = a[0].equals("tls");
        int port = Integer.parseInt(a[1]);
        HttpClientConfig cfg = HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        if (tls) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(Path.of(a[2]))) { ks.load(in, "changeit".toCharArray()); }
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tmf.init(ks);
            SSLContext ctx = SSLContext.getInstance("TLS"); ctx.init(null, tmf.getTrustManagers(), null);
            cfg = cfg.withSslContext(ctx);
        }
        String base = (tls ? "https://localhost:" : "http://127.0.0.1:") + port;
        try (HttpClient client = new HttpClient(new io.hydrangea.EventLoopGroup(2), cfg)) {
            var r = client.get(base + "/hello").get(10, TimeUnit.SECONDS);
            System.out.println("GET /hello -> " + r.status() + " " + r.bodyAsString());
            byte[] payload = new byte[3_000_000]; new java.util.Random(4).nextBytes(payload);
            var e = client.send(ClientRequest.post(base + "/echo").body(payload)).get(20, TimeUnit.SECONDS);
            System.out.println("POST 3MB echo -> " + e.status() + " identical=" + java.util.Arrays.equals(payload, e.body()) + " sha=" + e.header("x-sha").substring(0, 12));
            var b = client.get(base + "/big").get(20, TimeUnit.SECONDS);
            System.out.println("GET /big -> " + b.status() + " bytes=" + b.body().length);
            var red = client.get(base + "/redirect").get(10, TimeUnit.SECONDS);
            System.out.println("GET /redirect -> " + red.status() + " " + red.bodyAsString() + " uri=" + red.uri().getPath());
            var hs = client.get(base + "/headers").get(10, TimeUnit.SECONDS);
            System.out.println("request headers seen by node: " + hs.bodyAsString().substring(0, Math.min(200, hs.bodyAsString().length())));
            var futures = new java.util.ArrayList<CompletableFuture<ClientResponse>>();
            for (int i = 0; i < 100; i++) futures.add(client.get(base + "/hello"));
            int ok = 0; for (var f : futures) if (f.get(20, TimeUnit.SECONDS).status() == 200) ok++;
            System.out.println("100 concurrent GETs -> " + ok + " ok");
            StringBuilder sse = new StringBuilder(); CountDownLatch done = new CountDownLatch(1);
            client.stream(ClientRequest.get(base + "/sse"), new BodyListener() {
                public void onData(java.nio.ByteBuffer d, BodyControl c) { sse.append(java.nio.charset.StandardCharsets.UTF_8.decode(d)); }
                public void onComplete() { done.countDown(); }
                public void onError(Throwable t) { sse.append("ERR " + t); done.countDown(); }
            });
            done.await(10, TimeUnit.SECONDS);
            System.out.println("SSE stream -> " + sse.toString().replace("\n", "|"));
            try { client.get(base + "/reset").get(10, TimeUnit.SECONDS); System.out.println("reset: unexpected success"); }
            catch (ExecutionException ex) { System.out.println("GET /reset -> " + ex.getCause()); }
            var slowA = client.get(base + "/slow"); var slowB = client.send(ClientRequest.get(base + "/slow").timeout(java.time.Duration.ofMillis(300)));
            try { slowB.get(5, TimeUnit.SECONDS); } catch (ExecutionException ex) { System.out.println("timeout of one stream -> " + ex.getCause().getClass().getSimpleName()); }
            System.out.println("the other slow stream still completes -> " + slowA.get(10, TimeUnit.SECONDS).bodyAsString());

            // ---- transparent gzip / deflate and cookies, against Node's encoder and cookie handling ----
            HttpClientConfig more = cfg.withDecompression(true).withCookieJar(new InMemoryCookieJar());
            try (HttpClient c2 = new HttpClient(new io.hydrangea.EventLoopGroup(2), more)) {
                String expected = "hydrangea-".repeat(100000);
                var gz = c2.get(base + "/gz").get(10, TimeUnit.SECONDS);
                System.out.println("gzip from node -> " + gz.body().length + " bytes decoded, correct=" + new String(gz.body()).equals(expected) + ", content-encoding header now=" + gz.header("content-encoding"));
                var df = c2.get(base + "/deflate").get(10, TimeUnit.SECONDS);
                System.out.println("deflate (zlib) from node -> correct=" + new String(df.body()).equals("hydrangea-".repeat(1000)));
                System.out.println("Accept-Encoding sent -> " + c2.get(base + "/ae").get(10, TimeUnit.SECONDS).bodyAsString());
                c2.get(base + "/cookie").get(10, TimeUnit.SECONDS);
                System.out.println("cookies echoed back by node -> " + c2.get(base + "/show").get(10, TimeUnit.SECONDS).bodyAsString());
            }
        }
        System.exit(0);
    }

    /** args: proxy proxyPort originPlainPort originTlsPort trust.p12 [user:pass] - Node's proxy in front of Node's origins */
    static void proxy(String[] a) throws Exception {
        int proxyPort = Integer.parseInt(a[1]), plain = Integer.parseInt(a[2]), secure = Integer.parseInt(a[3]);
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(a[4]))) { ks.load(in, "changeit".toCharArray()); }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tmf.init(ks);
        SSLContext ctx = SSLContext.getInstance("TLS"); ctx.init(null, tmf.getTrustManagers(), null);
        ProxyConfig proxy = ProxyConfig.all("127.0.0.1", proxyPort);
        if (a.length > 5) proxy = proxy.withCredentials(a[5].split(":")[0], a[5].split(":")[1]);
        HttpClientConfig cfg = HttpClientConfig.defaults().withProxy(proxy).withSslContext(ctx).withHttp2(Http2ClientConfig.defaults());
        try (HttpClient c = new HttpClient(new io.hydrangea.EventLoopGroup(2), cfg)) {
            System.out.println("http via proxy (absolute-form) -> " + c.get("http://127.0.0.1:" + plain + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
            byte[] body = new byte[1_000_000]; new java.util.Random(3).nextBytes(body);
            var e = c.send(ClientRequest.post("http://127.0.0.1:" + plain + "/echo").body(body)).get(20, TimeUnit.SECONDS);
            System.out.println("POST 1 MB through the proxy identical=" + java.util.Arrays.equals(body, e.body()));
            System.out.println("https via CONNECT (ALPN inside the tunnel) -> " + c.get("https://localhost:" + secure + "/hello").get(10, TimeUnit.SECONDS).bodyAsString());
        }
        System.exit(0);
    }
}
