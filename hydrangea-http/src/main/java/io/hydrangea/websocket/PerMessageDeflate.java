package io.hydrangea.websocket;

/**
 * Settings for the {@code permessage-deflate} extension (RFC 7692). Compression is off unless
 * {@link WebSocketConfig#withCompression} is given one of these.
 * <p>
 * Costs and risks to weigh:
 * <ul>
 *   <li><b>CPU on the event loop</b>: messages are compressed and decompressed on the connection's loop thread. Large
 *       messages are best sent in parts ({@link WebSocket#sendBinaryStream}) so each step stays short.</li>
 *   <li><b>Memory</b>: with context takeover (the default) every connection that used compression keeps a deflate and an
 *       inflate window of tens of KB. The {@code *NoContextTakeover} options trade ratio for memory.</li>
 *   <li><b>Decompression bombs</b>: a few KB can expand to gigabytes. Incoming messages are checked against
 *       {@link WebSocketConfig#maxMessageBytes()} (or {@link WebSocketConfig#maxStreamedMessageBytes()} for streamed ones)
 *       <em>after</em> decompression and the connection is closed as soon as the limit is crossed. If you stream messages,
 *       set {@code maxStreamedMessageBytes}.</li>
 *   <li><b>Secrets</b>: compressing attacker-influenced data together with secrets in one connection can leak the secrets
 *       through message sizes (CRIME/BREACH style). Keep such data out of the same stream or turn compression off.</li>
 * </ul>
 * <b>Window size</b>: the JDK's deflater always uses a 32 KB window, so offers that demand a smaller
 * {@code server_max_window_bits} are declined (the connection simply runs uncompressed).
 *
 * @param level                   0-9 or -1 for the zlib default
 * @param thresholdBytes          messages smaller than this are sent uncompressed (compressing a few dozen bytes rarely pays off)
 * @param serverNoContextTakeover ask for / insist that the server's compressor forgets its window after every message
 * @param clientNoContextTakeover the same for the client's compressor
 */
public record PerMessageDeflate(int level, int thresholdBytes, boolean serverNoContextTakeover, boolean clientNoContextTakeover) {

    public PerMessageDeflate {
        if (level < -1 || level > 9) {
            throw new IllegalArgumentException("level must be -1 or 0..9");
        }
        if (thresholdBytes < 0) {
            throw new IllegalArgumentException("thresholdBytes must be >= 0");
        }
    }

    public static PerMessageDeflate defaults() {
        return new PerMessageDeflate(-1, 128, false, false);
    }

    public PerMessageDeflate withLevel(int level) {
        return new PerMessageDeflate(level, thresholdBytes, serverNoContextTakeover, clientNoContextTakeover);
    }

    public PerMessageDeflate withThresholdBytes(int bytes) {
        return new PerMessageDeflate(level, bytes, serverNoContextTakeover, clientNoContextTakeover);
    }

    public PerMessageDeflate withNoContextTakeover(boolean server, boolean client) {
        return new PerMessageDeflate(level, thresholdBytes, server, client);
    }
}
