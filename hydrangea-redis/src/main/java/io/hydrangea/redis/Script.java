package io.hydrangea.redis;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;

/** A Lua script. {@link RedisClient#eval(Script, java.util.List, java.util.List)} runs it by its SHA-1 and sends the source only if the server does not have it yet. */
public final class Script {

    private final String source;
    private final String sha;

    public Script(String source) {
        if (source == null || source.isBlank()) {
            throw new IllegalArgumentException("source");
        }
        this.source = source;
        try {
            this.sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-1").digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public String source() {
        return source;
    }

    /** The hex SHA-1 of the source, which is how the server names the script. */
    public String sha1() {
        return sha;
    }
}
