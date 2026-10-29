package io.hydrangea.testing;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** A TCP relay between a client and a server that a test can break on purpose. */
public final class ChaosProxy implements AutoCloseable {

    private final ServerSocket server;
    private final String host;
    private final int port;
    private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
    private volatile boolean refuse;
    private volatile boolean blackhole;

    public ChaosProxy(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t = new Thread(this::acceptLoop, "chaos-proxy");
        t.setDaemon(true);
        t.start();
    }

    public int port() {
        return server.getLocalPort();
    }

    /** Closes every connection that is open right now (the way a cable pull or a broker crash looks to the client). */
    public void sever() {
        for (Socket s : sockets) {
            closeQuietly(s);
        }
        sockets.clear();
    }

    /** New connections are accepted and dropped at once. */
    public void refuse(boolean on) {
        refuse = on;
    }

    /** Data in either direction is swallowed and new connections get no answer: the network is dead but nothing says so. */
    public void blackhole(boolean on) {
        blackhole = on;
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket client = server.accept();
                if (refuse) {
                    closeQuietly(client);
                    continue;
                }
                sockets.add(client);
                if (blackhole) {
                    Thread.ofVirtual().start(() -> drain(client));
                    continue;
                }
                Socket upstream;
                try {
                    upstream = new Socket(host, port);
                } catch (IOException e) {
                    closeQuietly(client);
                    continue;
                }
                sockets.add(upstream);
                Thread.ofVirtual().start(() -> pump(client, upstream));
                Thread.ofVirtual().start(() -> pump(upstream, client));
            } catch (IOException e) {
                return;
            }
        }
    }

    private void drain(Socket s) {
        try (InputStream in = s.getInputStream()) {
            byte[] buf = new byte[4096];
            while (in.read(buf) >= 0) {
                // ignore
            }
        } catch (IOException ignored) {
            // closed
        }
    }

    private void pump(Socket from, Socket to) {
        byte[] buf = new byte[65536];
        try {
            InputStream in = from.getInputStream();
            OutputStream out = to.getOutputStream();
            int n;
            while ((n = in.read(buf)) >= 0) {
                if (!blackhole) {
                    out.write(buf, 0, n);
                    out.flush();
                }
            }
        } catch (IOException ignored) {
            // one side went away
        } finally {
            closeQuietly(from);
            closeQuietly(to);
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException ignored) {
            // already closed
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        sever();
    }
}
