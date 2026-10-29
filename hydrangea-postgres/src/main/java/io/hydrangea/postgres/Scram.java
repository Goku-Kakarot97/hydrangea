package io.hydrangea.postgres;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * SCRAM-SHA-256 (RFC 5802 / 7677) as PostgreSQL uses it, without channel binding. The password is used as its UTF-8 bytes (no SASLprep
 * normalisation, which only matters for passwords with unusual Unicode). PostgreSQL ignores the user name in the SASL messages and uses the
 * one from the startup message, so {@code user} is empty there.
 */
final class Scram {

    private final byte[] password;
    private final String nonce;
    private final String clientFirstBare;
    private byte[] expectedServerSignature;

    Scram(String user, String password, String nonce) {
        this.password = password.getBytes(StandardCharsets.UTF_8);
        this.nonce = nonce;
        this.clientFirstBare = "n=" + user.replace("=", "=3D").replace(",", "=2C") + ",r=" + nonce;
    }

    static Scram forPostgres(String password) {
        byte[] n = new byte[18];
        new SecureRandom().nextBytes(n);
        return new Scram("", password, Base64.getEncoder().encodeToString(n));
    }

    String clientFirst() {
        return "n,," + clientFirstBare;
    }

    /** The client-final-message for the server's first message. */
    String clientFinal(String serverFirst) {
        String r = null;
        String s = null;
        int iterations = 0;
        for (String part : serverFirst.split(",")) {
            if (part.startsWith("r=")) {
                r = part.substring(2);
            } else if (part.startsWith("s=")) {
                s = part.substring(2);
            } else if (part.startsWith("i=")) {
                iterations = Integer.parseInt(part.substring(2));
            }
        }
        if (r == null || s == null || iterations < 1) {
            throw new IllegalStateException("bad SCRAM server-first-message");
        }
        if (!r.startsWith(nonce)) {
            throw new IllegalStateException("the server's SCRAM nonce does not extend ours");
        }
        byte[] salt = Base64.getDecoder().decode(s);
        byte[] salted = hi(password, salt, iterations);
        byte[] clientKey = hmac(salted, "Client Key".getBytes(StandardCharsets.US_ASCII));
        byte[] storedKey = sha256(clientKey);
        String withoutProof = "c=biws,r=" + r;
        byte[] authMessage = (clientFirstBare + "," + serverFirst + "," + withoutProof).getBytes(StandardCharsets.UTF_8);
        byte[] clientSignature = hmac(storedKey, authMessage);
        byte[] proof = new byte[clientKey.length];
        for (int i = 0; i < proof.length; i++) {
            proof[i] = (byte) (clientKey[i] ^ clientSignature[i]);
        }
        expectedServerSignature = hmac(hmac(salted, "Server Key".getBytes(StandardCharsets.US_ASCII)), authMessage);
        return withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof);
    }

    /** Checks the server-final-message: the server proves it knows the password too. */
    void verifyServerFinal(String serverFinal) {
        if (serverFinal.startsWith("e=")) {
            throw new IllegalStateException("SCRAM: the server refused the login: " + serverFinal.substring(2));
        }
        if (!serverFinal.startsWith("v=") || expectedServerSignature == null
                || !MessageDigest.isEqual(Base64.getDecoder().decode(serverFinal.substring(2)), expectedServerSignature)) {
            throw new IllegalStateException("SCRAM: the server's signature does not match (wrong server?)");
        }
    }

    private static byte[] hi(byte[] password, byte[] salt, int iterations) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            // HMAC zero-pads its key, so one zero byte is the empty key (SecretKeySpec refuses an empty array)
            mac.init(new SecretKeySpec(password.length == 0 ? new byte[]{0} : password, "HmacSHA256"));
            mac.update(salt);
            mac.update(new byte[]{0, 0, 0, 1});
            byte[] u = mac.doFinal();
            byte[] result = u.clone();
            for (int i = 1; i < iterations; i++) {
                u = mac.doFinal(u);
                for (int j = 0; j < result.length; j++) {
                    result[j] ^= u[j];
                }
            }
            return result;
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
