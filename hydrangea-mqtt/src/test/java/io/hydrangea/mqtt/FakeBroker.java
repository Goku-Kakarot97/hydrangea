package io.hydrangea.mqtt;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/** A scripted MQTT peer for tests: accepts connections and lets the test read the client's packets and write any bytes it likes. */
final class FakeBroker implements AutoCloseable {

    /** One packet as the client sent it. */
    record Packet(int type, int flags, byte[] body) {
        int id() {
            return (body[0] & 0xff) << 8 | body[1] & 0xff;
        }
    }

    private final ServerSocket server;

    FakeBroker() throws IOException {
        server = new ServerSocket(0, 10, InetAddress.getLoopbackAddress());
    }

    int port() {
        return server.getLocalPort();
    }

    Conn accept() throws IOException {
        return acceptWithin(10_000);
    }

    /** Throws SocketTimeoutException when nobody connects in time. */
    Conn acceptWithin(int millis) throws IOException {
        server.setSoTimeout(millis);
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
            in = s.getInputStream();
            out = s.getOutputStream();
        }

        /** Reads one packet; null when the client has closed the connection. */
        Packet read() throws IOException {
            int b0 = in.read();
            if (b0 < 0) {
                return null;
            }
            int len = 0;
            for (int k = 0; k < 4; k++) {
                int d = in.read();
                if (d < 0) throw new EOFException();
                len |= (d & 0x7f) << (7 * k);
                if ((d & 0x80) == 0) break;
            }
            byte[] body = in.readNBytes(len);
            if (body.length != len) throw new EOFException();
            return new Packet(b0 >> 4, b0 & 0x0f, body);
        }

        /** Like {@link #read()}, but gives up after the timeout and returns null (and null also means closed: check {@link #closedByClient}). */
        Packet read(int timeoutMillis) throws IOException {
            socket.setSoTimeout(timeoutMillis);
            try {
                return read();
            } catch (SocketTimeoutException e) {
                return null;
            } finally {
                socket.setSoTimeout(5_000);
            }
        }

        Packet expect(int type) throws IOException {
            Packet p = read();
            if (p == null) throw new AssertionError("the client closed the connection; expected packet type " + type);
            if (p.type != type) throw new AssertionError("expected packet type " + type + " but got " + p.type);
            return p;
        }

        /** Reads the CONNECT, answers CONNACK(sessionPresent, code). */
        Packet handshake(boolean sessionPresent, int code) throws IOException {
            Packet connect = expect(Packets.CONNECT);
            write(0x20, 2, sessionPresent ? 1 : 0, code);
            return connect;
        }

        /** CONNACK of MQTT 5: flags, reason code, properties. */
        void connack5(boolean sessionPresent, int reason, Props5 props) throws IOException {
            byte[] enc = props == null ? new byte[0] : props.encode();
            Packets.Writer w = new Packets.Writer(16).u8(sessionPresent ? 1 : 0).u8(reason).vbi(enc.length).raw(enc, 0, enc.length);
            packet(Packets.CONNACK, 0, w.toByteArray());
        }

        /** An acknowledgement of MQTT 5: id, reason code, properties (left out when the reason is 0 and there are none). */
        void ack5(int type, int id, int reason, Props5 props) throws IOException {
            Packets.Writer w = new Packets.Writer(8).u16(id);
            if (reason != 0 || props != null) {
                w.u8(reason);
                if (props != null) {
                    w.props(props.encode());
                }
            }
            packet(type, type == Packets.PUBREL ? 2 : 0, w.toByteArray());
        }

        /** SUBACK / UNSUBACK of MQTT 5: id, properties, one reason code per filter. */
        void suback5(int type, int id, int... reasons) throws IOException {
            Packets.Writer w = new Packets.Writer(8).u16(id).vbi(0);
            for (int r : reasons) w.u8(r);
            packet(type, 0, w.toByteArray());
        }

        /** PUBLISH of MQTT 5 with the given properties. */
        void publish5(String topic, byte[] payload, int qos, int id, Props5 props) throws IOException {
            Packets.Writer w = new Packets.Writer(32).str(topic.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            if (qos > 0) w.u16(id);
            w.props(props == null ? new byte[0] : props.encode()).raw(payload, 0, payload.length);
            packet(Packets.PUBLISH, qos << 1, w.toByteArray());
        }

        void write(int... bytes) throws IOException {
            byte[] b = new byte[bytes.length];
            for (int i = 0; i < b.length; i++) b[i] = (byte) bytes[i];
            writeRaw(b);
        }

        void writeRaw(byte[] b) throws IOException {
            out.write(b);
            out.flush();
        }

        /** A whole packet with a correctly encoded remaining length. */
        void packet(int type, int flags, byte[] body) throws IOException {
            ByteArrayOutputStream o = new ByteArrayOutputStream();
            o.write(type << 4 | flags);
            int len = body.length;
            do {
                int d = len & 0x7f;
                len >>>= 7;
                o.write(len > 0 ? d | 0x80 : d);
            } while (len > 0);
            o.write(body);
            writeRaw(o.toByteArray());
        }

        void publish(String topic, byte[] payload, int qos, int id, boolean dup, boolean retain) throws IOException {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            byte[] t = topic.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            b.write(t.length >> 8);
            b.write(t.length);
            b.write(t);
            if (qos > 0) {
                b.write(id >> 8);
                b.write(id);
            }
            b.write(payload);
            packet(Packets.PUBLISH, (dup ? 8 : 0) | qos << 1 | (retain ? 1 : 0), b.toByteArray());
        }

        void ack(int type, int id) throws IOException {
            packet(type, type == Packets.PUBREL ? 2 : 0, new byte[] {(byte) (id >> 8), (byte) id});
        }

        /** True once the client has closed its side (a read returns end of stream), within the timeout. */
        boolean closedByClient(int timeoutMillis) throws IOException {
            socket.setSoTimeout(timeoutMillis);
            try {
                while (true) {
                    int b = in.read();
                    if (b < 0) return true;
                }
            } catch (SocketTimeoutException e) {
                return false;
            } catch (java.net.SocketException reset) {
                return true;
            } finally {
                try {
                    socket.setSoTimeout(5_000);
                } catch (IOException ignored) {
                    // already closed
                }
            }
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
