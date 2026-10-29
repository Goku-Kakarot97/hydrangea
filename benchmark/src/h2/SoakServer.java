package h2;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.HealthService;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Soak target: the gRPC echo service and a few HTTP routes on one HTTP/2 port. Prints a "health ..." line every few seconds (live heap after GC,
 * threads, descriptors, connections, the number of calls the server saw cancelled) for {@link SoakLoad}'s verdict.
 * Usage: SoakServer port ioThreads bindAddress
 */
public final class SoakServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);
        String bind = args.length > 2 ? args[2] : "127.0.0.1";
        EventLoopGroup group = new EventLoopGroup(threads);
        byte[] zeros = new byte[4 * 1024 * 1024];
        HttpRouter router = new HttpRouter()
                .get("/plain", r -> HttpResponse.status(200).header("Content-Type", "text/plain").body("Hello, World!".getBytes(StandardCharsets.UTF_8)))
                .post("/echo", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream").body(r.body()))
                .get("/download/{n}", r -> HttpResponse.status(200).header("Content-Type", "application/octet-stream")
                        .body(java.util.Arrays.copyOf(zeros, r.pathInt("n"))))
                .get("/health", r -> HttpResponse.ok("ok"));
        HealthService health = new HealthService();
        GrpcServer grpc = GrpcEchoExample.builder().addService(health).fallback(router).build();
        // A cancel-heavy gRPC client resets many streams per second on one connection; the defaults (200 resets/s) would answer that with GOAWAY
        // ENHANCE_YOUR_CALM. The soak is after leaks and stalls, so the limits are raised here and the defence is covered by its own tests.
        io.hydrangea.http.HttpConfig config = new io.hydrangea.http.HttpConfig(16 * 1024, 1024 * 1024).withKeepAliveTimeout(Duration.ofMinutes(10))
                .withHttp2(io.hydrangea.http.Http2Config.defaults().withLimits(5000, 50000));
        Server server = grpc.bind(group, new InetSocketAddress(bind, port), null, config);
        String ks = System.getProperty("tls.keystore");   // optional second listener with TLS (ALPN h2) on tls.port, for the capacity runs
        if (ks != null) {
            try (var in = java.nio.file.Files.newInputStream(java.nio.file.Path.of(ks))) {
                char[] pw = System.getProperty("tls.pass", "changeit").toCharArray();
                java.security.KeyStore store = java.security.KeyStore.getInstance("PKCS12");
                store.load(in, pw);
                javax.net.ssl.KeyManagerFactory kmf = javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(store, pw);
                javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
                ctx.init(kmf.getKeyManagers(), null, null);
                grpc.bind(group, new InetSocketAddress(bind, Integer.getInteger("tls.port", 8443)), ctx, config);
                System.out.println("TLS " + Integer.getInteger("tls.port", 8443));
            }
        }
        health.setStatus("hydrangea.test.Echo", HealthService.ServingStatus.SERVING);

        int seconds = Integer.getInteger("health.interval.sec", 30);
        var os = (com.sun.management.UnixOperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        var threadBean = java.lang.management.ManagementFactory.getThreadMXBean();
        long started = System.nanoTime();
        Thread reporter = new Thread(() -> {
            for (;;) {
                try {
                    Thread.sleep(seconds * 1000L);
                } catch (InterruptedException e) {
                    return;
                }
                long live = 0;   // what survived the last collection, summed over the heap pools
                for (var pool : java.lang.management.ManagementFactory.getMemoryPoolMXBeans()) {
                    if (pool.getType() == java.lang.management.MemoryType.HEAP && pool.getCollectionUsage() != null) {
                        live += pool.getCollectionUsage().getUsed();
                    }
                }
                Runtime rt = Runtime.getRuntime();
                var m = group.metrics().snapshot();
                System.out.printf("health t=%d heapMB=%d oldGenAfterGcMB=%d threads=%d fds=%d active=%d opened=%d closed=%d rejected=%d http=%d timeouts=%d maxBlockMs=%d cancelled=%d%n",
                        (System.nanoTime() - started) / 1_000_000_000L, (rt.totalMemory() - rt.freeMemory()) >> 20, live >> 20, threadBean.getThreadCount(),
                        os.getOpenFileDescriptorCount(), server.activeConnections(), m.connectionsOpened(), m.connectionsClosed(), m.connectionsRejected(),
                        m.httpRequests(), m.httpTimeouts(), group.metrics().resetMaxBlockNanos() / 1_000_000, GrpcEchoExample.CANCELLED.get());
                System.out.flush();
            }
        }, "health");
        reporter.setDaemon(true);
        reporter.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            health.shutdown();
            server.shutdownGracefully(Duration.ofSeconds(20)).join();
            group.close();
        }));
        System.out.println("READY " + ((InetSocketAddress) server.localAddress()).getPort());
        System.out.flush();
        Thread.currentThread().join();
    }
}
