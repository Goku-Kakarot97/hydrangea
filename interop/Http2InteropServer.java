import io.hydrangea.*;
import io.hydrangea.http.*;
import io.hydrangea.sse.*;
import io.hydrangea.websocket.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Http2InteropServer {
    public static void main(String[] a) throws Exception {
        EventLoopGroup group = new EventLoopGroup(2);
        var ticker = Executors.newSingleThreadScheduledExecutor();
        HttpRouter router = new HttpRouter()
            .get("/hello", r -> HttpResponse.ok("hello over " + r.request().version()))
            .get("/events", r -> SseResponse.accept(r.request(), () -> new SseListener() {
                public void onOpen(SseEmitter e) { int[] n = {0}; ticker.scheduleAtFixedRate(() -> { if (e.isOpen() && n[0] < 5) e.send("tick " + n[0]++); else if (n[0] >= 5) e.close(); }, 50, 50, TimeUnit.MILLISECONDS); }
            }))
            .get("/ws", r -> WebSocketUpgrade.accept(r.request(), () -> new WebSocketListener() {
                public void onText(WebSocket ws, String t) { ws.sendText("echo:" + t); }
                public void onBinary(WebSocket ws, ByteBuffer d) { ws.sendBinary(d); }
            }))
            .postStream("/sink", (r, body) -> {
                var done = new CompletableFuture<HttpResponse>(); var n = new AtomicLong();
                body.listen(new BodyListener() {
                    public void onData(ByteBuffer d, BodyControl c) { n.addAndGet(d.remaining()); }
                    public void onComplete() { done.complete(HttpResponse.ok(Long.toString(n.get()))); }
                });
                return done;
            });
        HttpServer.bind(group, new InetSocketAddress("127.0.0.1", Integer.parseInt(a[0])), Options.defaults(), null,
                HttpConfig.defaults().withHttp2(Http2Config.defaults()), router);
        System.out.println("READY");
        Thread.currentThread().join();
    }
}
