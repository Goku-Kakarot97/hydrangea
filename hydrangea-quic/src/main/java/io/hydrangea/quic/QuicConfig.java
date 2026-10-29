package io.hydrangea.quic;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Settings of a {@link QuicServer}.
 *
 * @param privateKey      key of the leaf certificate (EC P-256 or RSA)
 * @param certificateChain certificates, leaf first, DER encoded
 * @param alpn            application protocols in order of preference (for HTTP/3: "h3")
 * @param idleTimeout     a connection with no packets for this long is dropped
 * @param connectionWindow bytes the peer may send on all streams beyond what the application has taken (flow control, renewed as data is consumed)
 * @param streamWindow    the same per stream
 * @param maxBidiStreams  concurrent bidirectional streams the peer may open
 * @param maxUniStreams   concurrent unidirectional streams the peer may open
 * @param maxConnections  connections kept at once; further handshakes are ignored
 * @param retry           answer every first Initial with a Retry carrying an address-validation token (RFC 9000 section 8.1), so no handshake work
 *                        or state is spent on spoofed source addresses; costs the client one round trip
 */
public record QuicConfig(PrivateKey privateKey, List<byte[]> certificateChain, List<String> alpn, Duration idleTimeout, int connectionWindow, int streamWindow,
                         int maxBidiStreams, int maxUniStreams, int maxConnections, boolean retry) {

    public QuicConfig {
        if (alpn.isEmpty() || connectionWindow < 65536 || streamWindow < 4096 || maxBidiStreams < 1 || maxUniStreams < 0 || maxConnections < 1) {
            throw new IllegalArgumentException("invalid QUIC configuration");
        }
        certificateChain = List.copyOf(certificateChain);
        alpn = List.copyOf(alpn);
    }

    public static QuicConfig of(PrivateKey key, List<byte[]> chainDer, List<String> alpn) {
        return new QuicConfig(key, chainDer, alpn, Duration.ofSeconds(30), 4 << 20, 1 << 20, 100, 16, 10_000, false);
    }

    /** From a keystore entry (for example a PKCS12 file made with keytool). */
    public static QuicConfig fromKeyStore(KeyStore store, String alias, char[] password, List<String> alpn) throws Exception {
        PrivateKey key = (PrivateKey) store.getKey(alias, password);
        if (key == null) {
            throw new IllegalArgumentException("no private key under alias " + alias);
        }
        List<byte[]> chain = new ArrayList<>();
        for (Certificate c : store.getCertificateChain(alias)) {
            chain.add(c.getEncoded());
        }
        return of(key, chain, alpn);
    }

    public QuicConfig withIdleTimeout(Duration d) {
        return new QuicConfig(privateKey, certificateChain, alpn, d, connectionWindow, streamWindow, maxBidiStreams, maxUniStreams, maxConnections, retry);
    }

    public QuicConfig withWindows(int connection, int stream) {
        return new QuicConfig(privateKey, certificateChain, alpn, idleTimeout, connection, stream, maxBidiStreams, maxUniStreams, maxConnections, retry);
    }

    public QuicConfig withMaxStreams(int bidi, int uni) {
        return new QuicConfig(privateKey, certificateChain, alpn, idleTimeout, connectionWindow, streamWindow, bidi, uni, maxConnections, retry);
    }

    public QuicConfig withMaxConnections(int n) {
        return new QuicConfig(privateKey, certificateChain, alpn, idleTimeout, connectionWindow, streamWindow, maxBidiStreams, maxUniStreams, n, retry);
    }

    public QuicConfig withRetry(boolean enabled) {
        return new QuicConfig(privateKey, certificateChain, alpn, idleTimeout, connectionWindow, streamWindow, maxBidiStreams, maxUniStreams, maxConnections, enabled);
    }

    public QuicConfig withAlpn(List<String> protocols) {
        return new QuicConfig(privateKey, certificateChain, protocols, idleTimeout, connectionWindow, streamWindow, maxBidiStreams, maxUniStreams, maxConnections, retry);
    }
}
