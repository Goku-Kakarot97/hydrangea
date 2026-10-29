package io.hydrangea;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A small forward proxy for tests, on blocking sockets. {@code CONNECT} opens a tunnel; any other request line in absolute form is
 * rewritten to origin form, sent with {@code Connection: close}, and the answer relayed. It can demand Basic credentials, refuse
 * tunnels, and map invented host names ("origin.test") to a real address, which shows that the client never resolves the name itself.
 */
final class TestProxy implements AutoCloseable {

    private final ServerSocket server;
    final List<String> requestLines = new CopyOnWriteArrayList<>();
    final List<String> proxyAuthorizations = new CopyOnWriteArrayList<>();
    final AtomicInteger connects = new AtomicInteger();
    final AtomicInteger forwards = new AtomicInteger();
    final Map<String, InetSocketAddress> hostMap = new ConcurrentHashMap<>();
    volatile String requiredAuthorization;
    volatile int refuseConnectWith;

    TestProxy() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread.ofVirtual().start(() -> {
            try {
                for (;;) {
                    Socket c = server.accept();
                    Thread.ofVirtual().start(() -> handle(c));
                }
            } catch (IOException closed) {
                // shut down
            }
        });
    }

    int port() {
        return server.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    private void handle(Socket client) {
        try (client) {
            InputStream in = client.getInputStream();
            String head = readHead(in);
            if (head == null) {
                return;
            }
            String[] lines = head.split("\r\n");
            String requestLine = lines[0];
            requestLines.add(requestLine);
            String auth = null;
            for (int i = 1; i < lines.length; i++) {
                if (lines[i].toLowerCase().startsWith("proxy-authorization:")) {
                    auth = lines[i].substring(lines[i].indexOf(':') + 1).trim();
                }
            }
            proxyAuthorizations.add(String.valueOf(auth));
            OutputStream out = client.getOutputStream();
            String required = requiredAuthorization;
            if (required != null && !required.equals(auth)) {
                out.write(("HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"test\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.ISO_8859_1));
                return;
            }
            String[] parts = requestLine.split(" ");
            if (parts[0].equals("CONNECT")) {
                connects.incrementAndGet();
                if (refuseConnectWith != 0) {
                    out.write(("HTTP/1.1 " + refuseConnectWith + " Refused\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.ISO_8859_1));
                    return;
                }
                Socket target = connectTo(parts[1]);
                out.write("HTTP/1.1 200 Connection Established\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
                out.flush();
                relay(client, target, null);
            } else {
                forwards.incrementAndGet();
                java.net.URI uri = java.net.URI.create(parts[1]);
                if (uri.getScheme() == null) {
                    out.write("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".getBytes());
                    return;
                }
                int port = uri.getPort() != -1 ? uri.getPort() : 80;
                Socket target = connectTo(uri.getHost() + ":" + port);
                StringBuilder fwd = new StringBuilder();
                String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
                fwd.append(parts[0]).append(' ').append(path).append(uri.getRawQuery() != null ? "?" + uri.getRawQuery() : "").append(" HTTP/1.1\r\n");
                for (int i = 1; i < lines.length; i++) {
                    String l = lines[i].toLowerCase();
                    if (!l.startsWith("proxy-") && !l.startsWith("connection:")) {
                        fwd.append(lines[i]).append("\r\n");
                    }
                }
                fwd.append("Connection: close\r\n\r\n");
                relay(client, target, fwd.toString().getBytes(StandardCharsets.ISO_8859_1));
            }
        } catch (IOException ignored) {
            // the test is over or the peer left
        }
    }

    private Socket connectTo(String authority) throws IOException {
        int colon = authority.lastIndexOf(':');
        String host = authority.substring(0, colon);
        int port = Integer.parseInt(authority.substring(colon + 1));
        InetSocketAddress mapped = hostMap.get(host);
        return mapped != null ? new Socket(mapped.getAddress(), mapped.getPort()) : new Socket(host, port);
    }

    /** Copies both ways until either side closes; {@code firstToTarget} is written before anything else. */
    private static void relay(Socket a, Socket b, byte[] firstToTarget) throws IOException {
        try (b) {
            if (firstToTarget != null) {
                b.getOutputStream().write(firstToTarget);
            }
            Thread up = Thread.ofVirtual().start(() -> pipe(a, b));
            pipe(b, a);
            try {
                up.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void pipe(Socket from, Socket to) {
        byte[] buf = new byte[16 * 1024];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) >= 0) {
                out.write(buf, 0, n);
                out.flush();
            }
            try {
                to.shutdownOutput();
            } catch (IOException ignored) {
                // already closed
            }
        } catch (IOException ignored) {
            // the other direction closed
        }
    }

    /** Reads up to and including the blank line, one byte at a time so nothing behind it is consumed. */
    private static String readHead(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int state = 0;
        int b;
        while ((b = in.read()) >= 0) {
            buf.write(b);
            state = (b == '\r' && (state == 0 || state == 2)) ? state + 1 : (b == '\n' && (state == 1 || state == 3)) ? state + 1 : 0;
            if (state == 4) {
                return buf.toString(StandardCharsets.ISO_8859_1).replaceAll("\r\n\r\n$", "");
            }
            if (buf.size() > 64 * 1024) {
                return null;
            }
        }
        return null;
    }
}
