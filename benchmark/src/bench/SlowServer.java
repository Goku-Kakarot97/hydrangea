import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/** usage: SlowServer port delayMs -- backend that answers "ok" after a fixed delay, one virtual thread per request. */
public class SlowServer {
    public static void main(String[] a) throws Exception {
        int delay = Integer.parseInt(a[1]);
        HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", Integer.parseInt(a[0])), 20000);
        s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        byte[] body = "ok".getBytes();
        s.createContext("/", ex -> {
            try { Thread.sleep(delay); } catch (InterruptedException e) { }
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        s.start();
        System.out.println("up");
    }
}
