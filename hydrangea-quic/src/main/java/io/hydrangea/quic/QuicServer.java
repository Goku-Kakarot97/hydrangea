package io.hydrangea.quic;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.UdpSocket;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A QUIC (RFC 9000) server on one UDP socket: demultiplexes datagrams to connections by connection id, answers unknown versions with Version
 * Negotiation, and starts a connection for each valid client Initial.
 * <p>
 * Experimental: version 1 only, one cipher suite, Retry (address validation) optional, no 0-RTT, no connection migration, no stateless reset. The
 * handshake costs the server an X25519 agreement and a signature, so run it behind a rate limit on untrusted networks.
 */
public final class QuicServer implements AutoCloseable {

    private static final Logger LOG = System.getLogger(QuicServer.class.getName());
    private static final SecureRandom RANDOM = new SecureRandom();

    private final QuicConfig config;
    private final QuicHandler handler;
    private final Map<ByteBuffer, QuicConnection> connections = new HashMap<>();   // socket loop thread only
    private final AtomicInteger active = new AtomicInteger();
    private UdpSocket socket;
    private final SecretKeySpec tokenKey = new SecretKeySpec(randomKey(), "HmacSHA256");
    private static final long TOKEN_LIFETIME_MILLIS = 30_000;
    // RFC 9001 section 5.8
    private static final SecretKeySpec RETRY_KEY = new SecretKeySpec(hex("be0c690b9f66575a1d766b54e368c84e"), "AES");
    private static final byte[] RETRY_NONCE = hex("461599d35d632bf2239825bb");

    private static byte[] randomKey() {
        byte[] k = new byte[32];
        RANDOM.nextBytes(k);
        return k;
    }

    private static byte[] hex(String h) {
        byte[] b = new byte[h.length() / 2];
        for (int i = 0; i < b.length; i++) b[i] = (byte) Integer.parseInt(h.substring(2 * i, 2 * i + 2), 16);
        return b;
    }

    private QuicServer(QuicConfig config, QuicHandler handler) {
        this.config = config;
        this.handler = handler;
    }

    public static QuicServer bind(EventLoopGroup group, SocketAddress address, QuicConfig config, QuicHandler handler) throws IOException {
        QuicServer server = new QuicServer(config, handler);
        server.socket = UdpSocket.bind(group, address, new UdpSocket.Config(4096, false, null), (sock, sender, data) -> server.onDatagram(sender, data));
        return server;
    }

    public SocketAddress localAddress() {
        return socket.localAddress();
    }

    /** Connections that have started a handshake and not ended yet. */
    public int activeConnections() {
        return active.get();
    }

    @Override
    public void close() {
        socket.close();
    }

    void remove(QuicConnection c) {
        if (connections.remove(ByteBuffer.wrap(c.scid)) != null) {
            active.decrementAndGet();
        }
        connections.remove(ByteBuffer.wrap(c.originalDestinationConnectionId()));
    }

    private void onDatagram(SocketAddress sender, ByteBuffer data) {
        byte[] dg = new byte[data.remaining()];
        data.get(dg);
        if (dg.length < 1) {
            return;
        }
        try {
            route(sender, dg);
        } catch (QuicWire.Malformed e) {
            // not a QUIC packet: ignore
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "quic datagram handling failed", e);
        }
    }

    private void route(SocketAddress sender, byte[] dg) {
        int b0 = dg[0] & 0xff;
        if ((b0 & 0x80) == 0) {
            if (dg.length < 1 + 8) {
                return;
            }
            QuicConnection c = connections.get(ByteBuffer.wrap(dg, 1, 8));
            if (c != null) {
                c.onDatagram(dg, sender);
            }
            return;
        }
        QuicWire.Reader r = new QuicWire.Reader(dg, 1, dg.length);
        long version = r.u32();
        byte[] dcid = r.bytes(r.u8());
        byte[] scid = r.bytes(r.u8());
        QuicConnection c = connections.get(ByteBuffer.wrap(dcid));
        if (c != null) {
            c.onDatagram(dg, sender);
            return;
        }
        if (version == 0) {
            return;
        }
        if (version != 1) {
            if (dg.length >= 1200) {
                versionNegotiation(sender, dcid, scid);
            }
            return;
        }
        int type = (b0 >> 4) & 3;
        if (type != 0 || dg.length < 1200 || dcid.length < 8 || dcid.length > 20 || scid.length > 20) {
            return;
        }
        if (active.get() >= config.maxConnections()) {
            return;
        }
        byte[] token = r.bytes(r.varint());
        byte[] original = dcid;
        boolean retried = false;
        if (config.retry()) {
            if (token.length == 0) {
                sendRetry(sender, dcid, scid);
                return;
            }
            original = validToken(sender, token, dcid);
            if (original == null) {
                return;
            }
            retried = true;
        }
        byte[] mine = retried ? dcid : new byte[8];     // after a Retry the client addresses us by the id we chose in the Retry
        if (!retried) {
            do {
                RANDOM.nextBytes(mine);
            } while (connections.containsKey(ByteBuffer.wrap(mine)));
        }
        QuicConnection created = new QuicConnection(this, socket, config, handler, sender, mine, original, dcid, retried, scid);
        connections.put(ByteBuffer.wrap(mine), created);
        if (!retried) {
            connections.put(ByteBuffer.wrap(dcid), created);
        }
        active.incrementAndGet();
        created.onDatagram(dg, sender);
    }

    private void versionNegotiation(SocketAddress sender, byte[] clientDcid, byte[] clientScid) {
        QuicWire.Writer w = new QuicWire.Writer(64);
        w.u8(0x80 | RANDOM.nextInt(0x80)).u32(0);
        w.u8(clientScid.length).bytes(clientScid);
        w.u8(clientDcid.length).bytes(clientDcid);
        w.u32(1);
        socket.send(ByteBuffer.wrap(w.toByteArray()), sender);
    }

    // ---- Retry (RFC 9000 section 8.1.2, RFC 9001 section 5.8) ----

    private static byte[] addressBytes(SocketAddress a) {
        return a instanceof InetSocketAddress i && i.getAddress() != null ? i.getAddress().getAddress() : new byte[0];
    }

    private byte[] tokenMac(byte[] timestamp, byte[] odcid, SocketAddress sender) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(tokenKey);
            mac.update(timestamp);
            mac.update((byte) odcid.length);
            mac.update(odcid);
            mac.update(addressBytes(sender));
            return java.util.Arrays.copyOf(mac.doFinal(), 16);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Token: 8-byte time, odcid length, odcid, 16-byte MAC over those and the client address. Stateless; nothing is kept per token. */
    private byte[] makeToken(SocketAddress sender, byte[] odcid) {
        byte[] ts = ByteBuffer.allocate(8).putLong(System.currentTimeMillis()).array();
        QuicWire.Writer w = new QuicWire.Writer(64);
        w.bytes(ts).u8(odcid.length).bytes(odcid).bytes(tokenMac(ts, odcid, sender));
        return w.toByteArray();
    }

    /** The original destination connection id carried by a valid, fresh token for this address, else null. */
    private byte[] validToken(SocketAddress sender, byte[] token, byte[] retrySourceId) {
        try {
            QuicWire.Reader r = new QuicWire.Reader(token, 0, token.length);
            byte[] ts = r.bytes(8);
            byte[] odcid = r.bytes(r.u8());
            byte[] mac = r.bytes(16);
            if (r.remaining() != 0 || !MessageDigest.isEqual(mac, tokenMac(ts, odcid, sender))) {
                return null;
            }
            long age = System.currentTimeMillis() - ByteBuffer.wrap(ts).getLong();
            return age >= 0 && age <= TOKEN_LIFETIME_MILLIS ? odcid : null;
        } catch (QuicWire.Malformed e) {
            return null;
        }
    }

    private void sendRetry(SocketAddress sender, byte[] clientDcid, byte[] clientScid) {
        byte[] mine = new byte[8];
        RANDOM.nextBytes(mine);
        QuicWire.Writer w = new QuicWire.Writer(128);
        w.u8(0xf0 | RANDOM.nextInt(16)).u32(1);
        w.u8(clientScid.length).bytes(clientScid);
        w.u8(mine.length).bytes(mine);
        w.bytes(makeToken(sender, clientDcid));
        byte[] packet = w.toByteArray();
        try {
            // the tag authenticates the pseudo-packet: original DCID length and value, then the Retry packet
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, RETRY_KEY, new GCMParameterSpec(128, RETRY_NONCE));
            c.updateAAD(new byte[]{(byte) clientDcid.length});
            c.updateAAD(clientDcid);
            c.updateAAD(packet);
            byte[] tag = c.doFinal();
            ByteBuffer out = ByteBuffer.allocate(packet.length + tag.length).put(packet).put(tag).flip();
            socket.send(out, sender);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
