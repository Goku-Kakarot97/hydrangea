package io.hydrangea.quic;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.NamedParameterSpec;
import java.security.spec.PSSParameterSpec;
import java.security.spec.XECPublicKeySpec;
import java.security.interfaces.XECPublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The server half of TLS 1.3 (RFC 8446) as QUIC uses it (RFC 9001): handshake messages come and go as CRYPTO data per encryption level, the record layer
 * is not used, and the traffic secrets are handed to the QUIC layer. One cipher suite (TLS_AES_128_GCM_SHA256), one key exchange (x25519), certificate
 * signatures with ECDSA P-256 or RSA-PSS (SHA-256), ALPN, no client authentication, no session tickets, no HelloRetryRequest, no 0-RTT.
 */
final class Tls13Server {

    enum Level { INITIAL, HANDSHAKE, APPLICATION }

    /** A fatal handshake problem; {@code alert} is the TLS alert description (RFC 8446 section 6), carried in QUIC as crypto error 0x100 + alert. */
    static final class TlsAlert extends Exception {
        final int alert;

        TlsAlert(int alert, String message) {
            super(message);
            this.alert = alert;
        }
    }

    interface Output {
        void crypto(Level level, byte[] data);

        void handshakeKeys(QuicCrypto.Keys client, QuicCrypto.Keys server);

        void applicationKeys(QuicCrypto.Keys client, QuicCrypto.Keys server);

        void handshakeComplete();
    }

    private static final int HANDSHAKE_CLIENT_HELLO = 1, SERVER_HELLO = 2, ENCRYPTED_EXTENSIONS = 8, CERTIFICATE = 11, CERTIFICATE_VERIFY = 15, FINISHED = 20;
    private static final int EXT_SERVER_NAME = 0, EXT_SIGNATURE_ALGORITHMS = 13, EXT_ALPN = 16, EXT_SUPPORTED_VERSIONS = 43, EXT_KEY_SHARE = 51, EXT_QUIC_PARAMS = 0x39;
    private static final int GROUP_X25519 = 0x001d, SIG_ECDSA_P256_SHA256 = 0x0403, SIG_RSA_PSS_RSAE_SHA256 = 0x0804;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final PrivateKey privateKey;
    private final List<byte[]> certificateChain;
    private final List<String> alpnPreference;
    private final byte[] serverTransportParameters;
    private final Output out;

    private final ByteArrayOutputStream transcript = new ByteArrayOutputStream();
    private final Map<Level, ByteArrayOutputStream> inbound = new HashMap<>();
    private boolean clientHelloSeen;
    private byte[] clientHandshakeSecret;
    private byte[] finishedHashForClient;     // transcript hash through the server Finished
    private boolean complete;

    private byte[] peerTransportParameters;
    private String alpn;
    private String serverName;

    Tls13Server(PrivateKey privateKey, List<byte[]> certificateChain, List<String> alpnPreference, byte[] serverTransportParameters, Output out) {
        this.privateKey = privateKey;
        this.certificateChain = certificateChain;
        this.alpnPreference = alpnPreference;
        this.serverTransportParameters = serverTransportParameters;
        this.out = out;
    }

    byte[] peerTransportParameters() {
        return peerTransportParameters;
    }

    String alpn() {
        return alpn;
    }

    String serverName() {
        return serverName;
    }

    boolean complete() {
        return complete;
    }

    /** Consumes in-order CRYPTO data received at {@code level}. */
    void onCrypto(Level level, byte[] data) throws TlsAlert {
        ByteArrayOutputStream buf = inbound.computeIfAbsent(level, l -> new ByteArrayOutputStream());
        buf.writeBytes(data);
        for (;;) {
            byte[] all = buf.toByteArray();
            if (all.length < 4) {
                return;
            }
            int len = ((all[1] & 0xff) << 16) | ((all[2] & 0xff) << 8) | (all[3] & 0xff);
            if (len > 65536) {
                throw new TlsAlert(0x16, "oversized handshake message");   // record_overflow
            }
            if (all.length < 4 + len) {
                return;
            }
            byte[] message = Arrays.copyOfRange(all, 0, 4 + len);
            buf.reset();
            buf.write(all, 4 + len, all.length - 4 - len);
            handle(level, message[0] & 0xff, message);
        }
    }

    private void handle(Level level, int type, byte[] message) throws TlsAlert {
        if (type == HANDSHAKE_CLIENT_HELLO && level == Level.INITIAL && !clientHelloSeen) {
            clientHelloSeen = true;
            clientHello(message);
        } else if (type == FINISHED && level == Level.HANDSHAKE && clientHandshakeSecret != null && !complete) {
            clientFinished(message);
        } else {
            throw new TlsAlert(0x0a, "unexpected handshake message " + type + " at " + level);   // unexpected_message
        }
    }

    // ---- reading the ClientHello ----

    private static final class Reader {
        final byte[] b;
        int pos;
        final int end;

        Reader(byte[] b, int from, int to) {
            this.b = b;
            this.pos = from;
            this.end = to;
        }

        int u8() throws TlsAlert {
            need(1);
            return b[pos++] & 0xff;
        }

        int u16() throws TlsAlert {
            need(2);
            int v = ((b[pos] & 0xff) << 8) | (b[pos + 1] & 0xff);
            pos += 2;
            return v;
        }

        byte[] bytes(int n) throws TlsAlert {
            need(n);
            byte[] r = Arrays.copyOfRange(b, pos, pos + n);
            pos += n;
            return r;
        }

        void need(int n) throws TlsAlert {
            if (n < 0 || pos + n > end) {
                throw new TlsAlert(0x32, "truncated handshake message");   // decode_error
            }
        }

        boolean more() {
            return pos < end;
        }
    }

    private void clientHello(byte[] message) throws TlsAlert {
        Reader r = new Reader(message, 4, message.length);
        r.u16();                                  // legacy_version
        r.bytes(32);                              // random
        byte[] sessionId = r.bytes(r.u8());
        int suitesLen = r.u16();
        boolean suite = false;
        Reader suites = new Reader(message, r.pos, r.pos + suitesLen);
        r.bytes(suitesLen);
        while (suites.more()) {
            suite |= suites.u16() == 0x1301;
        }
        r.bytes(r.u8());                          // legacy_compression_methods
        int extLen = r.u16();
        Reader ext = new Reader(message, r.pos, r.pos + extLen);
        r.bytes(extLen);
        Map<Integer, byte[]> extensions = new HashMap<>();
        while (ext.more()) {
            int t = ext.u16();
            extensions.put(t, ext.bytes(ext.u16()));
        }
        if (!suite) {
            throw new TlsAlert(0x28, "no common cipher suite");   // handshake_failure
        }
        byte[] versions = extensions.get(EXT_SUPPORTED_VERSIONS);
        boolean tls13 = false;
        if (versions != null && versions.length >= 1) {
            for (int i = 1; i + 1 < versions.length; i += 2) {
                tls13 |= (((versions[i] & 0xff) << 8) | (versions[i + 1] & 0xff)) == 0x0304;
            }
        }
        if (!tls13) {
            throw new TlsAlert(0x46, "TLS 1.3 not offered");       // protocol_version
        }
        peerTransportParameters = extensions.get(EXT_QUIC_PARAMS);
        if (peerTransportParameters == null) {
            throw new TlsAlert(0x6d, "missing quic_transport_parameters");   // missing_extension
        }
        byte[] clientShare = null;
        byte[] shares = extensions.get(EXT_KEY_SHARE);
        if (shares != null) {
            Reader s = new Reader(shares, 2, shares.length);
            while (s.more()) {
                int group = s.u16();
                byte[] key = s.bytes(s.u16());
                if (group == GROUP_X25519 && key.length == 32) {
                    clientShare = key;
                }
            }
        }
        if (clientShare == null) {
            throw new TlsAlert(0x28, "no x25519 key share (HelloRetryRequest is not supported)");
        }
        int sigAlg = privateKey.getAlgorithm().equals("EC") ? SIG_ECDSA_P256_SHA256 : SIG_RSA_PSS_RSAE_SHA256;
        byte[] sigs = extensions.get(EXT_SIGNATURE_ALGORITHMS);
        boolean sigOk = false;
        if (sigs != null) {
            Reader s = new Reader(sigs, 2, sigs.length);
            while (s.more()) {
                sigOk |= s.u16() == sigAlg;
            }
        }
        if (!sigOk) {
            throw new TlsAlert(0x28, "the client accepts no signature algorithm we can offer");
        }
        byte[] alpnExt = extensions.get(EXT_ALPN);
        List<String> offered = new ArrayList<>();
        if (alpnExt != null) {
            Reader a = new Reader(alpnExt, 2, alpnExt.length);
            while (a.more()) {
                offered.add(new String(a.bytes(a.u8()), java.nio.charset.StandardCharsets.ISO_8859_1));
            }
        }
        for (String p : alpnPreference) {
            if (offered.contains(p)) {
                alpn = p;
                break;
            }
        }
        if (alpn == null) {
            throw new TlsAlert(0x78, "no application protocol in common");   // no_application_protocol
        }
        byte[] sni = extensions.get(EXT_SERVER_NAME);
        if (sni != null && sni.length > 5) {
            serverName = new String(sni, 5, sni.length - 5, java.nio.charset.StandardCharsets.ISO_8859_1);
        }
        transcript.writeBytes(message);

        try {
            serverFlight(sessionId, clientShare, sigAlg);
        } catch (GeneralSecurityException e) {
            throw new TlsAlert(0x50, e.toString());   // internal_error
        }
    }

    // ---- the server flight ----

    private static byte[] u16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static byte[] u24(int v) {
        return new byte[]{(byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    private static byte[] handshake(int type, byte[] body) {
        return cat(new byte[]{(byte) type}, u24(body.length), body);
    }

    private byte[] hashOfTranscript() {
        try {
            return MessageDigest.getInstance("SHA-256").digest(transcript.toByteArray());
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] emptyHash() {
        try {
            return MessageDigest.getInstance("SHA-256").digest(new byte[0]);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] littleEndian32(BigInteger u) {
        byte[] be = u.toByteArray();
        byte[] le = new byte[32];
        for (int i = 0; i < be.length && i < 33; i++) {
            int dst = be.length - 1 - i;
            if (i < 32) {
                le[i] = be[dst];
            }
        }
        return le;
    }

    private void serverFlight(byte[] sessionId, byte[] clientShare, int sigAlg) throws GeneralSecurityException, TlsAlert {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("X25519");
        KeyPair mine = kpg.generateKeyPair();
        byte[] myShare = littleEndian32(((XECPublicKey) mine.getPublic()).getU());
        byte[] peerU = new byte[32];
        for (int i = 0; i < 32; i++) {
            peerU[i] = clientShare[31 - i];
        }
        peerU[0] &= 0x7f;   // RFC 7748: the top bit of the u-coordinate is ignored
        var peerKey = KeyFactory.getInstance("X25519").generatePublic(new XECPublicKeySpec(NamedParameterSpec.X25519, new BigInteger(1, peerU)));
        javax.crypto.KeyAgreement ka = javax.crypto.KeyAgreement.getInstance("X25519");
        ka.init(mine.getPrivate());
        ka.doPhase(peerKey, true);
        byte[] shared = ka.generateSecret();
        boolean zero = true;
        for (byte b : shared) {
            zero &= b == 0;
        }
        if (zero) {
            throw new TlsAlert(0x2f, "low-order key share");   // illegal_parameter
        }

        byte[] random = new byte[32];
        RANDOM.nextBytes(random);
        byte[] serverHelloExt = cat(
                u16(EXT_SUPPORTED_VERSIONS), u16(2), u16(0x0304),
                u16(EXT_KEY_SHARE), u16(2 + 2 + 32), u16(GROUP_X25519), u16(32), myShare);
        byte[] serverHello = handshake(SERVER_HELLO, cat(u16(0x0303), random, new byte[]{(byte) sessionId.length}, sessionId, u16(0x1301), new byte[]{0},
                u16(serverHelloExt.length), serverHelloExt));
        transcript.writeBytes(serverHello);
        out.crypto(Level.INITIAL, serverHello);

        byte[] empty = emptyHash();
        byte[] early = QuicCrypto.hkdfExtract(new byte[0], new byte[32]);
        byte[] derived = QuicCrypto.deriveSecret(early, "derived", empty);
        byte[] handshakeSecret = QuicCrypto.hkdfExtract(derived, shared);
        byte[] helloHash = hashOfTranscript();
        byte[] cHs = QuicCrypto.deriveSecret(handshakeSecret, "c hs traffic", helloHash);
        byte[] sHs = QuicCrypto.deriveSecret(handshakeSecret, "s hs traffic", helloHash);
        clientHandshakeSecret = cHs;
        out.handshakeKeys(new QuicCrypto.Keys(cHs), new QuicCrypto.Keys(sHs));

        byte[] alpnBytes = alpn.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
        byte[] eeExt = cat(
                u16(EXT_ALPN), u16(2 + 1 + alpnBytes.length), u16(1 + alpnBytes.length), new byte[]{(byte) alpnBytes.length}, alpnBytes,
                u16(EXT_QUIC_PARAMS), u16(serverTransportParameters.length), serverTransportParameters);
        byte[] encryptedExtensions = handshake(ENCRYPTED_EXTENSIONS, cat(u16(eeExt.length), eeExt));
        transcript.writeBytes(encryptedExtensions);

        ByteArrayOutputStream certList = new ByteArrayOutputStream();
        for (byte[] der : certificateChain) {
            certList.writeBytes(u24(der.length));
            certList.writeBytes(der);
            certList.writeBytes(u16(0));
        }
        byte[] certificate = handshake(CERTIFICATE, cat(new byte[]{0}, u24(certList.size()), certList.toByteArray()));
        transcript.writeBytes(certificate);

        byte[] toSign = cat(" ".repeat(64).getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                "TLS 1.3, server CertificateVerify".getBytes(java.nio.charset.StandardCharsets.US_ASCII), new byte[]{0}, hashOfTranscript());
        Signature sig;
        if (sigAlg == SIG_ECDSA_P256_SHA256) {
            sig = Signature.getInstance("SHA256withECDSA");
        } else {
            sig = Signature.getInstance("RSASSA-PSS");
            sig.setParameter(new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1));
        }
        sig.initSign(privateKey);
        sig.update(toSign);
        byte[] signature = sig.sign();
        byte[] certificateVerify = handshake(CERTIFICATE_VERIFY, cat(u16(sigAlg), u16(signature.length), signature));
        transcript.writeBytes(certificateVerify);

        byte[] finishedKey = QuicCrypto.hkdfExpandLabel(sHs, "finished", new byte[0], 32);
        byte[] finished = handshake(FINISHED, QuicCrypto.hmac(finishedKey, hashOfTranscript()));
        transcript.writeBytes(finished);
        finishedHashForClient = hashOfTranscript();
        out.crypto(Level.HANDSHAKE, cat(encryptedExtensions, certificate, certificateVerify, finished));

        byte[] derived2 = QuicCrypto.deriveSecret(handshakeSecret, "derived", empty);
        byte[] master = QuicCrypto.hkdfExtract(derived2, new byte[32]);
        byte[] cAp = QuicCrypto.deriveSecret(master, "c ap traffic", finishedHashForClient);
        byte[] sAp = QuicCrypto.deriveSecret(master, "s ap traffic", finishedHashForClient);
        out.applicationKeys(new QuicCrypto.Keys(cAp), new QuicCrypto.Keys(sAp));
    }

    private void clientFinished(byte[] message) throws TlsAlert {
        byte[] finishedKey = QuicCrypto.hkdfExpandLabel(clientHandshakeSecret, "finished", new byte[0], 32);
        byte[] expected = QuicCrypto.hmac(finishedKey, finishedHashForClient);
        byte[] got = Arrays.copyOfRange(message, 4, message.length);
        if (!MessageDigest.isEqual(expected, got)) {
            throw new TlsAlert(0x33, "bad Finished");   // decrypt_error
        }
        transcript.writeBytes(message);
        complete = true;
        out.handshakeComplete();
    }
}
