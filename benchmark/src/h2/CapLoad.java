package h2;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Closed-loop capacity load (every reply is checked), Hydrangea client, so it needs nothing but the framework jar.
 * <pre>CapLoad host port scenario inflight connections warmupSec seconds ioThreads</pre>
 * scenarios: grpc-16 | grpc-4k | h2-plain (GET, 13 B) | h2-echo1k | h1-plain | h1-echo1k | h1-close-plain (HTTP/1.1, a new connection per request). Prints RESULT ops=.. p50=.. p90=.. p99=.. p999=.. errors=.. (latency in microseconds).
 */
public final class CapLoad {
    static final int BUCKETS = 4000;   // 10 us wide up to 40 ms, the last bucket collects the rest
    static final AtomicLongArray HIST = new AtomicLongArray(BUCKETS + 1);
    static final AtomicLong OK = new AtomicLong(), ERR = new AtomicLong();
    static volatile boolean measuring, running = true;
    static volatile String firstError = "";

    public static void main(String[] a) throws Exception {
        String host = a[0];
        int port = Integer.parseInt(a[1]);
        String scenario = a[2];
        int inflight = Integer.parseInt(a[3]), conns = Integer.parseInt(a[4]);
        int warmup = Integer.parseInt(a[5]), seconds = Integer.parseInt(a[6]);
        EventLoopGroup group = new EventLoopGroup(Integer.parseInt(a[7]));
        String ts = System.getProperty("tls.truststore");   // set: https, trusting this PKCS12 store
        javax.net.ssl.SSLContext tls = null;
        if (ts != null) {
            java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
            try (var in = java.nio.file.Files.newInputStream(java.nio.file.Path.of(ts))) { store.load(in, System.getProperty("tls.pass", "changeit").toCharArray()); }
            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(store);
            tls = javax.net.ssl.SSLContext.getInstance("TLS");
            tls.init(null, tmf.getTrustManagers(), null);
        }
        String target = (tls != null ? "https://" : "http://") + host + ":" + port;
        GrpcChannel.Builder cb = GrpcChannel.forTarget(target).eventLoopGroup(group).maxConnections(conns);
        if (tls != null) cb.sslContext(tls);
        GrpcChannel ch = cb.build();
        HttpClientConfig hc = HttpClientConfig.defaults().withMaxConnectionsPerHost(conns);
        if (scenario.equals("h1-close-plain")) {
            hc = hc.withMaxIdlePerHost(0);
        }
        if (!scenario.startsWith("h1-")) {   // h2-*: prior knowledge on http://, ALPN on https://; h1-*: HTTP/1.1 only
            hc = hc.withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        }
        if (tls != null) hc = hc.withSslContext(tls);
        HttpClient http = new HttpClient(group, hc);
        byte[] b16 = new byte[16], b4k = new byte[4096], b1k = new byte[1024];
        Arrays.fill(b4k, (byte) 7); Arrays.fill(b1k, (byte) 9);
        Msg m16 = new Msg("x", 7, b16), m4k = new Msg("x", 7, b4k);
        for (int i = 0; i < inflight; i++) {
            switch (scenario) {
                case "grpc-16" -> loopGrpc(ch, m16);
                case "grpc-4k" -> loopGrpc(ch, m4k);
                case "h2-plain" -> loopHttp(http, ClientRequest.get(target + "/plain"), 13);
                case "h2-echo1k", "h1-echo1k" -> loopHttp(http, ClientRequest.post(target + "/echo").body(b1k), 1024);
                case "h1-plain" -> loopHttp(http, ClientRequest.get(target + "/plain"), 13);
                case "h1-close-plain" -> loopHttp(http, ClientRequest.get(target + "/plain"), 13);   // no idle connections kept: a new connection (and TLS handshake) per request
                default -> throw new IllegalArgumentException(scenario);
            }
        }
        Thread.sleep(warmup * 1000L);
        measuring = true; OK.set(0); ERR.set(0);
        long t0 = System.nanoTime();
        Thread.sleep(seconds * 1000L);
        long ok = OK.get(), err = ERR.get();
        double secs = (System.nanoTime() - t0) / 1e9;
        measuring = false; running = false;
        long total = 0; for (int i = 0; i <= BUCKETS; i++) total += HIST.get(i);
        System.out.printf("RESULT scenario=%s inflight=%d conns=%d ops=%.0f p50=%d p90=%d p99=%d p999=%d errors=%d %s%n", scenario, inflight, conns, ok / secs,
                pct(total, .5), pct(total, .9), pct(total, .99), pct(total, .999), err, firstError);
        System.exit(0);
    }

    static long pct(long total, double p) {
        long want = (long) Math.ceil(total * p), seen = 0;
        for (int i = 0; i <= BUCKETS; i++) { seen += HIST.get(i); if (seen >= want) return i * 10L; }
        return BUCKETS * 10L;
    }

    static void done(long start, boolean good, String why) {
        if (!measuring) return;
        if (good) { OK.incrementAndGet(); HIST.incrementAndGet((int) Math.min(BUCKETS, (System.nanoTime() - start) / 10_000)); }
        else { ERR.incrementAndGet(); firstError = why; }
    }

    static void loopGrpc(GrpcChannel ch, Msg m) {
        if (!running) return;
        long s = System.nanoTime();
        ch.unary(GrpcEchoExample.UNARY, m, CallOptions.DEFAULT).whenComplete((r, e) -> {
            done(s, e == null && r.n() == 7 && r.blob().length == m.blob().length, String.valueOf(e));
            if (e != null) { try { Thread.sleep(5); } catch (InterruptedException ignored) { } }
            loopGrpc(ch, m);
        });
    }

    static void loopHttp(HttpClient http, ClientRequest req, int len) {
        if (!running) return;
        long s = System.nanoTime();
        http.send(req).whenComplete((r, e) -> {
            done(s, e == null && r.status() == 200 && r.body().length == len, String.valueOf(e));
            if (e != null) { try { Thread.sleep(5); } catch (InterruptedException ignored) { } }
            loopHttp(http, req, len);
        });
    }
}
