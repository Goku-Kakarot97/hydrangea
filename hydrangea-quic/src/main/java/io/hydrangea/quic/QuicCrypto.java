package io.hydrangea.quic;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;

/** The key schedule and packet protection of RFC 9001 for TLS_AES_128_GCM_SHA256, built on the JCA only. */
final class QuicCrypto {

    private static final byte[] INITIAL_SALT_V1 = HexFormat.of().parseHex("38762cf7f55934b34d179ae6a4c80cadccbb7f0a");

    private QuicCrypto() {
    }

    // ---- HKDF (RFC 5869) with the TLS 1.3 labels (RFC 8446 section 7.1) ----

    static byte[] hmac(byte[] key, byte[]... parts) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.length == 0 ? new byte[32] : key, "HmacSHA256"));
            for (byte[] p : parts) {
                mac.update(p);
            }
            return mac.doFinal();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] hkdfExtract(byte[] salt, byte[] ikm) {
        return hmac(salt.length == 0 ? new byte[32] : salt, ikm);
    }

    static byte[] hkdfExpand(byte[] prk, byte[] info, int length) {
        byte[] out = new byte[length];
        byte[] t = new byte[0];
        int pos = 0;
        for (int i = 1; pos < length; i++) {
            t = hmac(prk, t, info, new byte[]{(byte) i});
            int n = Math.min(t.length, length - pos);
            System.arraycopy(t, 0, out, pos, n);
            pos += n;
        }
        return out;
    }

    static byte[] hkdfExpandLabel(byte[] secret, String label, byte[] context, int length) {
        byte[] l = ("tls13 " + label).getBytes(StandardCharsets.US_ASCII);
        byte[] info = new byte[2 + 1 + l.length + 1 + context.length];
        info[0] = (byte) (length >> 8);
        info[1] = (byte) length;
        info[2] = (byte) l.length;
        System.arraycopy(l, 0, info, 3, l.length);
        info[3 + l.length] = (byte) context.length;
        System.arraycopy(context, 0, info, 4 + l.length, context.length);
        return hkdfExpand(secret, info, length);
    }

    static byte[] deriveSecret(byte[] secret, String label, byte[] transcriptHash) {
        return hkdfExpandLabel(secret, label, transcriptHash, 32);
    }

    // ---- packet protection keys ----

    /** The keys of one direction at one encryption level. */
    static final class Keys {
        final byte[] secret;
        private final SecretKeySpec key;
        private final byte[] iv;
        private final SecretKeySpec hp;
        private final Cipher hpCipher;
        // Cipher.getInstance dominates per-packet cost; instances are reused (re-init with a fresh nonce).
        private Cipher sealer;
        private Cipher opener;

        Keys(byte[] secret) {
            this.secret = secret;
            this.key = new SecretKeySpec(hkdfExpandLabel(secret, "quic key", new byte[0], 16), "AES");
            this.iv = hkdfExpandLabel(secret, "quic iv", new byte[0], 12);
            this.hp = new SecretKeySpec(hkdfExpandLabel(secret, "quic hp", new byte[0], 16), "AES");
            try {
                this.hpCipher = Cipher.getInstance("AES/ECB/NoPadding");
                this.hpCipher.init(Cipher.ENCRYPT_MODE, hp);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        byte[] keyBytes() {
            return key.getEncoded();
        }

        byte[] iv() {
            return iv.clone();
        }

        byte[] hpBytes() {
            return hp.getEncoded();
        }

        private GCMParameterSpec nonce(long pn) {
            byte[] n = iv.clone();
            for (int i = 0; i < 8; i++) {
                n[11 - i] ^= (byte) (pn >>> (8 * i));
            }
            return new GCMParameterSpec(128, n);
        }

        /** AEAD seal: {@code header} is the associated data; returns ciphertext followed by the 16-byte tag. */
        synchronized byte[] seal(long pn, byte[] header, byte[] plaintext, int off, int len) {
            try {
                Cipher c = sealer != null ? sealer : (sealer = Cipher.getInstance("AES/GCM/NoPadding"));
                c.init(Cipher.ENCRYPT_MODE, key, nonce(pn));
                c.updateAAD(header);
                return c.doFinal(plaintext, off, len);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        /** AEAD open; null if the tag does not verify. */
        synchronized byte[] open(long pn, byte[] header, byte[] ciphertext, int off, int len) {
            try {
                Cipher c = opener != null ? opener : (opener = Cipher.getInstance("AES/GCM/NoPadding"));
                c.init(Cipher.DECRYPT_MODE, key, nonce(pn));
                c.updateAAD(header);
                return c.doFinal(ciphertext, off, len);
            } catch (javax.crypto.AEADBadTagException e) {
                return null;
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        /** The 5-byte header protection mask for a 16-byte ciphertext sample. */
        synchronized byte[] mask(byte[] sample, int off) {
            try {
                byte[] m = hpCipher.doFinal(sample, off, 16);
                return m;
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException(e);
            }
        }

        /** The keys after a key update (RFC 9001 section 6): same header protection key, new packet protection key. */
        Keys next() {
            return new Keys(hkdfExpandLabel(secret, "quic ku", new byte[0], 32), hp, hpCipher);
        }

        private Keys(byte[] secret, SecretKeySpec hp, Cipher hpCipher) {
            this.secret = secret;
            this.key = new SecretKeySpec(hkdfExpandLabel(secret, "quic key", new byte[0], 16), "AES");
            this.iv = hkdfExpandLabel(secret, "quic iv", new byte[0], 12);
            this.hp = hp;
            this.hpCipher = hpCipher;
        }
    }

    /** Initial secrets (RFC 9001 section 5.2): {client, server}. */
    static Keys[] initialKeys(byte[] destinationConnectionId) {
        byte[] initial = hkdfExtract(INITIAL_SALT_V1, destinationConnectionId);
        Keys client = new Keys(hkdfExpandLabel(initial, "client in", new byte[0], 32));
        Keys server = new Keys(hkdfExpandLabel(initial, "server in", new byte[0], 32));
        return new Keys[]{client, server};
    }
}
