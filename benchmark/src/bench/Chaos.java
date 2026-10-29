package bench;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Misbehaving clients, to exercise the server's clean-up paths during a soak: connections that vanish half way through a request head,
 * a body, a WebSocket handshake or an SSE stream, with FIN or RST, and slow-loris style heads.
 * Usage: Chaos host port seconds threads [ratePerSecondPerThread=50]
 */
public final class Chaos {

    public static void main(String[] a) throws Exception {
        String host = a[0];
        int port = Integer.parseInt(a[1]);
        long end = System.currentTimeMillis() + Long.parseLong(a[2]) * 1000L;
        int threads = Integer.parseInt(a[3]);
        int rate = a.length > 4 ? Integer.parseInt(a[4]) : 50;
        AtomicLong attempts = new AtomicLong();
        AtomicLong refused = new AtomicLong();
        Thread[] ts = new Thread[threads];
        for (int t = 0; t < threads; t++) {
            int seed = t;
            ts[t] = new Thread(() -> {
                Random rnd = new Random(seed);
                while (System.currentTimeMillis() < end) {
                    try {
                        misbehave(host, port, rnd);
                        attempts.incrementAndGet();
                    } catch (IOException e) {
                        refused.incrementAndGet(); // the server turned us away or reset us: that is allowed
                    }
                    try {
                        Thread.sleep(1000 / rate);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            });
            ts[t].start();
        }
        for (Thread t : ts) t.join();
        System.out.println("CHAOS attempts=" + attempts + " refused=" + refused);
    }

    static void misbehave(String host, int port, Random rnd) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), 2000);
            s.setSoTimeout(500);
            if (rnd.nextBoolean()) s.setSoLinger(true, 0); // RST instead of FIN
            var out = s.getOutputStream();
            switch (rnd.nextInt(9)) {
                case 0 -> { /* connect and say nothing */ sleep(rnd.nextInt(50)); }
                case 1 -> out.write("GET /plain HTT".getBytes());                                             // cut in the request line
                case 2 -> out.write("GET /plain HTTP/1.1\r\nHost: x\r\nX-Slow: ".getBytes());                 // cut in a header
                case 3 -> out.write("POST /echo HTTP/1.1\r\nHost: x\r\nContent-Length: 100000\r\n\r\nabc".getBytes()); // body never completes
                case 4 -> out.write(("POST /sink HTTP/1.1\r\nHost: x\r\nContent-Length: 5000000\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1)); // streamed upload abandoned
                case 5 -> out.write("POST /sink HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n3\r\nab".getBytes());   // chunked, cut
                case 6 -> { // WebSocket handshake, then a frame cut in half
                    out.write(("GET /ws HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").getBytes());
                    out.flush();
                    sleep(20);
                    out.write(new byte[]{(byte) 0x81, (byte) 0x85, 1, 2});
                }
                case 7 -> { // SSE subscriber that leaves at once
                    out.write("GET /sse HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\n\r\n".getBytes());
                    out.flush();
                    sleep(rnd.nextInt(250));
                }
                default -> { // a request, then leave before reading the response
                    out.write("GET /download/524288 HTTP/1.1\r\nHost: x\r\n\r\n".getBytes());
                }
            }
            out.flush();
            sleep(rnd.nextInt(30));
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) { }
    }
}
