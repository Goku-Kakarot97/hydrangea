package io.hydrangea.mysql;

import javax.crypto.Cipher;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

/** The password schemes of MySQL: {@code mysql_native_password} (SHA-1) and {@code caching_sha2_password} (SHA-256, with an RSA exchange when there is no TLS). */
final class Auth {

    static final String NATIVE = "mysql_native_password";
    static final String CACHING_SHA2 = "caching_sha2_password";

    private Auth() {
    }

    private static byte[] digest(String alg, byte[]... parts) {
        try {
            MessageDigest md = MessageDigest.getInstance(alg);
            for (byte[] p : parts) {
                md.update(p);
            }
            return md.digest();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] r = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            r[i] = (byte) (a[i] ^ b[i]);
        }
        return r;
    }

    /** SHA1(password) XOR SHA1(scramble + SHA1(SHA1(password))). */
    static byte[] nativePassword(String password, byte[] scramble) {
        if (password == null || password.isEmpty()) {
            return new byte[0];
        }
        byte[] stage1 = digest("SHA-1", password.getBytes(StandardCharsets.UTF_8));
        byte[] stage2 = digest("SHA-1", stage1);
        return xor(stage1, digest("SHA-1", scramble, stage2));
    }

    /** SHA256(password) XOR SHA256(SHA256(SHA256(password)) + scramble): the fast path of caching_sha2_password, when the server has the password cached. */
    static byte[] cachingSha2(String password, byte[] scramble) {
        if (password == null || password.isEmpty()) {
            return new byte[0];
        }
        byte[] stage1 = digest("SHA-256", password.getBytes(StandardCharsets.UTF_8));
        byte[] stage2 = digest("SHA-256", stage1);
        return xor(stage1, digest("SHA-256", stage2, scramble));
    }

    /** The password for the full authentication without TLS: (password + NUL) XOR scramble, encrypted with the server's RSA key (OAEP, SHA-1). */
    static byte[] rsaEncrypt(String password, byte[] scramble, String pem) {
        try {
            String body = pem.replace("-----BEGIN PUBLIC KEY-----", "").replace("-----END PUBLIC KEY-----", "").replaceAll("\\s", "");
            PublicKey key = KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(body)));
            byte[] pw = password.getBytes(StandardCharsets.UTF_8);
            byte[] plain = new byte[pw.length + 1];
            System.arraycopy(pw, 0, plain, 0, pw.length);
            for (int i = 0; i < plain.length; i++) {
                plain[i] ^= scramble[i % scramble.length];
            }
            Cipher c = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            c.init(Cipher.ENCRYPT_MODE, key);
            return c.doFinal(plain);
        } catch (GeneralSecurityException | IllegalArgumentException e) {
            throw new MyException("cannot encrypt the password with the server's public key: " + e, false, e);
        }
    }

    static byte[] response(String plugin, String password, byte[] scramble) {
        return switch (plugin) {
            case NATIVE -> nativePassword(password, scramble);
            case CACHING_SHA2 -> cachingSha2(password, scramble);
            default -> throw new MyException("unsupported authentication plugin " + plugin + " (supported: " + NATIVE + ", " + CACHING_SHA2 + ")", false);
        };
    }
}
