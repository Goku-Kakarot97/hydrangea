import io.hydrangea.*;
import io.hydrangea.http.*;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** usage: H2Server port delayMs maxStreams -- Hydrangea h1+h2c backend answering "ok" after a fixed delay without blocking the loop. */
public class H2Server {
    public static void main(String[] a) throws Exception {
        int delay = Integer.parseInt(a[1]), streams = Integer.parseInt(a[2]);
        EventLoopGroup g = new EventLoopGroup(4);
        ScheduledExecutorService timer = Executors.newScheduledThreadPool(2);
        ConcurrentHashMap<String, LongAdder> seen = new ConcurrentHashMap<>();
        HttpConfig cfg = HttpConfig.defaults().withHttp2(Http2Config.defaults().withMaxConcurrentStreams(streams).withWindows(1 << 20, 1 << 24));
        HttpHandler h = req -> {
            seen.computeIfAbsent(req.version(), k -> new LongAdder()).increment();
            CompletableFuture<HttpResponse> f = new CompletableFuture<>();
            timer.schedule(() -> f.complete(HttpResponse.ok("ok")), delay, TimeUnit.MILLISECONDS);
            return f;
        };
        HttpServer.bind(g, new InetSocketAddress("127.0.0.1", Integer.parseInt(a[0])), Options.defaults(), null, cfg, h);
        System.out.println("up");
        while (true) { Thread.sleep(3000); System.out.println("versions " + seen); }
    }
}
