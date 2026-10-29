package io.hydrangea.redis;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** A scripted Redis for tests: accepts connections, lets the test read the commands the client sends and write any bytes it likes. */
final class FakeRedis implements AutoCloseable {

    private final ServerSocket server;

    FakeRedis() throws IOException {
        server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
    }

    int port() {
        return server.getLocalPort();
    }

    RedisConfig config() {
        return RedisConfig.of("127.0.0.1", port());
    }

    Conn accept() throws IOException {
        server.setSoTimeout(10_000);
        Socket s = server.accept();
        s.setTcpNoDelay(true);
        s.setSoTimeout(5_000);
        return new Conn(s);
    }

    @Override
    public void close() throws IOException {
        server.close();
    }

    static final class Conn implements AutoCloseable {
        final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        Conn(Socket s) throws IOException {
            socket = s;
            in = new java.io.BufferedInputStream(s.getInputStream(), 1 << 16);
            out = s.getOutputStream();
        }

        /** One command as the client sent it (an array of bulk strings); null when the client closed the connection. */
        List<String> read() throws IOException {
            int t = in.read();
            if (t < 0) {
                return null;
            }
            if (t != '*') {
                throw new AssertionError("expected an array, got byte " + t);
            }
            int n = (int) number();
            List<String> parts = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                if (in.read() != '$') {
                    throw new AssertionError("expected a bulk string");
                }
                int len = (int) number();
                byte[] b = in.readNBytes(len);
                if (b.length != len) throw new EOFException();
                in.read();
                in.read();
                parts.add(new String(b, StandardCharsets.UTF_8));
            }
            return parts;
        }

        private long number() throws IOException {
            long v = 0;
            int c;
            while ((c = in.read()) != '\r') {
                if (c < 0) throw new EOFException();
                v = v * 10 + (c - '0');
            }
            in.read();
            return v;
        }

        List<String> read(int timeoutMillis) throws IOException {
            socket.setSoTimeout(timeoutMillis);
            try {
                return read();
            } catch (SocketTimeoutException e) {
                return null;
            } finally {
                socket.setSoTimeout(5_000);
            }
        }

        List<String> expect(String command) throws IOException {
            List<String> c = read();
            if (c == null) throw new AssertionError("the client closed the connection; expected " + command);
            if (!c.get(0).equalsIgnoreCase(command)) throw new AssertionError("expected " + command + " but got " + c);
            return c;
        }

        void write(String s) throws IOException {
            write(s.getBytes(StandardCharsets.ISO_8859_1));
        }

        void write(byte[] b) throws IOException {
            out.write(b);
            out.flush();
        }

        /** Answers the RESP3 login (HELLO) with a small map; returns the command. */
        List<String> loginResp3() throws IOException {
            List<String> hello = expect("HELLO");
            write("%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.4.0\r\n$5\r\nproto\r\n:3\r\n");
            return hello;
        }

        boolean closedByClient(int timeoutMillis) throws IOException {
            socket.setSoTimeout(timeoutMillis);
            try {
                while (true) {
                    if (in.read() < 0) return true;
                }
            } catch (SocketTimeoutException e) {
                return false;
            } catch (java.net.SocketException reset) {
                return true;
            } finally {
                try {
                    socket.setSoTimeout(5_000);
                } catch (IOException ignored) {
                    // closed
                }
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    /** Bulk string wire form. */
    static String bulk(String s) {
        return "$" + s.getBytes(StandardCharsets.UTF_8).length + "\r\n" + s + "\r\n";
    }

    static ByteArrayOutputStream newBuffer() {
        return new ByteArrayOutputStream();
    }
}
