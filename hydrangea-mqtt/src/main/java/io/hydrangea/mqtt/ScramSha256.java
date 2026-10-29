package io.hydrangea.mqtt;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * SCRAM-SHA-256 (RFC 5802 with the SHA-256 of RFC 7677) as an {@link MqttAuthenticator}, the method brokers such as EMQX offer for MQTT 5.
 * The broker's final message is checked, so a broker that does not know the password is rejected too. Channel binding is not used.
 */
public final class ScramSha256 implements MqttAuthenticator {

    public static final String METHOD = "SCRAM-SHA-256";

    private final String user;
    private final char[] password;
    private final String fixedNonce;   // tests only: the RFC's example uses a given client nonce
    private static final SecureRandom RANDOM = new SecureRandom();

    public ScramSha256(String user, String password) {
        this(user, password, null);
    }

    ScramSha256(String user, String password, String nonce) {
        if (user == null || password == null) {
            throw new IllegalArgumentException("user and password are required");
        }
        this.user = user;
        this.password = password.toCharArray();
        this.fixedNonce = nonce;
    }

    @Override
    public String method() {
        return METHOD;
    }

    @Override
    public Exchange begin() {
        return new Run();
    }

    private final class Run implements Exchange {
        private final String nonce;
        private final String firstBare;
        private byte[] serverSignature;
        private int step;

        Run() {
            if (fixedNonce != null) {
                nonce = fixedNonce;
            } else {
                byte[] n = new byte[18];
                RANDOM.nextBytes(n);
                nonce = Base64.getEncoder().withoutPadding().encodeToString(n);
            }
            firstBare = "n=" + user.replace("=", "=3D").replace(",", "=2C") + ",r=" + nonce;
        }

        @Override
        public byte[] initialData() {
            return ("n,," + firstBare).getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public byte[] respond(byte[] challenge) {
            if (step++ != 0) {
                throw new MqttException("SCRAM: the broker sent a second challenge", false);
            }
            String serverFirst = new String(challenge, StandardCharsets.UTF_8);
            Map<String, String> a = parse(serverFirst);
            String r = a.get("r");
            String salt = a.get("s");
            String iter = a.get("i");
            if (r == null || salt == null || iter == null || !r.startsWith(nonce) || r.length() == nonce.length()) {
                throw new MqttException("SCRAM: the broker's first message is not valid", false);
            }
            int iterations;
            try {
                iterations = Integer.parseInt(iter);
            } catch (NumberFormatException e) {
                throw new MqttException("SCRAM: bad iteration count", false);
            }
            if (iterations < 1 || iterations > 1_000_000) {
                throw new MqttException("SCRAM: unreasonable iteration count " + iterations, false);
            }
            try {
                byte[] salted = pbkdf2(Base64.getDecoder().decode(salt), iterations);
                byte[] clientKey = hmac(salted, "Client Key");
                byte[] storedKey = MessageDigest.getInstance("SHA-256").digest(clientKey);
                String withoutProof = "c=biws,r=" + r;   // "biws" is base64 of "n,,"
                byte[] authMessage = (firstBare + "," + serverFirst + "," + withoutProof).getBytes(StandardCharsets.UTF_8);
                byte[] signature = hmacBytes(storedKey, authMessage);
                byte[] proof = new byte[clientKey.length];
                for (int i = 0; i < proof.length; i++) {
                    proof[i] = (byte) (clientKey[i] ^ signature[i]);
                }
                serverSignature = hmacBytes(hmac(salted, "Server Key"), authMessage);
                return (withoutProof + ",p=" + Base64.getEncoder().encodeToString(proof)).getBytes(StandardCharsets.UTF_8);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                throw new MqttException("SCRAM failed: " + e, false, e);
            }
        }

        @Override
        public void finish(byte[] data) {
            if (serverSignature == null) {
                throw new MqttException("SCRAM: the broker finished before the exchange was done", false);
            }
            if (data == null) {
                throw new MqttException("SCRAM: the broker sent no proof of its own", false);
            }
            Map<String, String> a = parse(new String(data, StandardCharsets.UTF_8));
            String v = a.get("v");
            if (v == null || !MessageDigest.isEqual(Base64.getDecoder().decode(v), serverSignature)) {
                throw new MqttException("SCRAM: the broker's proof is wrong (it does not know the password)", false);
            }
        }
    }

    private byte[] pbkdf2(byte[] salt, int iterations) throws GeneralSecurityException {
        PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, 256);
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
        } finally {
            spec.clearPassword();
        }
    }

    private static byte[] hmac(byte[] key, String text) throws GeneralSecurityException {
        return hmacBytes(key, text.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] hmacBytes(byte[] key, byte[] data) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(key, "HmacSHA256"));
        return mac.doFinal(data);
    }

    private static Map<String, String> parse(String message) {
        Map<String, String> m = new HashMap<>();
        for (String part : message.split(",")) {
            if (part.length() >= 2 && part.charAt(1) == '=') {
                m.put(part.substring(0, 1), part.substring(2));
            }
        }
        return m;
    }
}
