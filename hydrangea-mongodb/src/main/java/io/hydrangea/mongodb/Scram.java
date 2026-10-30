package io.hydrangea.mongodb;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * SCRAM (RFC 5802) as MongoDB uses it: SCRAM-SHA-256 with the password's UTF-8 bytes (no SASLprep, which only matters for unusual Unicode), or SCRAM-SHA-1 with
 * the MongoDB password digest ({@code hex(md5(user:mongo:password))}) in place of the password. No channel binding.
 */
final class Scram {

    private final String hmacAlg, digestAlg;
    private final byte[] password;
    private final String nonce;
    private final String clientFirstBare;
    private byte[] expectedServerSignature;

    private Scram(String mechanism, String user, String password, String nonce) {
        boolean sha256 = mechanism.equals("SCRAM-SHA-256");
        this.hmacAlg = sha256 ? "HmacSHA256" : "HmacSHA1";
        this.digestAlg = sha256 ? "SHA-256" : "SHA-1";
        this.password = sha256 ? password.getBytes(StandardCharsets.UTF_8) : md5Hex(user + ":mongo:" + password).getBytes(StandardCharsets.UTF_8);
        this.nonce = nonce;
        this.clientFirstBare = "n=" + user.replace("=", "=3D").replace(",", "=2C") + ",r=" + nonce;
    }

    static Scram of(String mechanism, String user, String password) {
        byte[] n = new byte[18];
        new SecureRandom().nextBytes(n);
        return new Scram(mechanism, user, password, Base64.getEncoder().encodeToString(n));
    }

    String clientFirst() {
        return "n,," + clientFirstBare;
    }

    /** The client-final-message for the server's first message. */
    String clientFinal(String serverFirst) {
        String r = null, s = null;
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
        if (!r.startsWith(nonce) || r.length() <= nonce.length()) {
            throw new IllegalStateException("the server's SCRAM nonce does not extend ours");
        }
        if (iterations < 4096) {
            throw new IllegalStateException("the server asks for too few SCRAM iterations (" + iterations + "): a downgrade?");
        }
        byte[] salted = hi(Base64.getDecoder().decode(s), iterations);
        byte[] clientKey = hmac(salted, "Client Key".getBytes(StandardCharsets.US_ASCII));
        byte[] storedKey = digest(clientKey);
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
            throw new IllegalStateException("SCRAM: the server's signature does not match (wrong server, or a man in the middle?)");
        }
    }

    private byte[] hi(byte[] salt, int iterations) {
        try {
            Mac mac = Mac.getInstance(hmacAlg);
            mac.init(new SecretKeySpec(password.length == 0 ? new byte[]{0} : password, hmacAlg));
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

    private byte[] hmac(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance(hmacAlg);
            mac.init(new SecretKeySpec(key, hmacAlg));
            return mac.doFinal(data);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private byte[] digest(byte[] data) {
        try {
            return MessageDigest.getInstance(digestAlg).digest(data); // codeql[java/potentially-weak-cryptographic-algorithm] SCRAM-SHA-1 is defined with SHA-1 by MongoDB; SCRAM-SHA-256 is used whenever the user has it
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String md5Hex(String s) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(s.getBytes(StandardCharsets.UTF_8)); // codeql[java/weak-cryptographic-algorithm, java/potentially-weak-cryptographic-algorithm] the MongoDB SCRAM-SHA-1 password digest is defined as MD5(user:mongo:password)
            StringBuilder sb = new StringBuilder(32);
            for (byte b : d) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
