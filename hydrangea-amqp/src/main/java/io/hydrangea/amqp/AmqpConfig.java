package io.hydrangea.amqp;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Connection settings.
 *
 * @param heartbeat       proposed heartbeat interval; the broker's own value is combined with it as the spec says (null or zero disables
 *                        heartbeats on this side; the lower non-zero value wins). Default 60 s.
 * @param frameMax        largest frame this client is willing to use (bytes); the lower of this and the broker's is used
 * @param channelMax      most channels this client wants open at once (0 = whatever the broker allows)
 * @param maxMessageBytes largest message body accepted from the broker; bigger ones close the connection (a peer cannot make us buffer without limit)
 * @param sslContext      non-null = TLS (normally port 5671); the server certificate is verified against {@code host}
 * @param connectionName  shown in the broker's management UI
 */
public record AmqpConfig(
        String host,
        int port,
        String username,
        String password,
        String virtualHost,
        Duration heartbeat,
        int frameMax,
        int channelMax,
        int maxMessageBytes,
        Duration connectTimeout,
        SSLContext sslContext,
        String connectionName) {

    public AmqpConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host");
        }
        if (frameMax < 4096) {
            throw new IllegalArgumentException("frameMax must be at least 4096 (the spec's minimum)");
        }
        if (channelMax < 0 || channelMax > 65535) {
            throw new IllegalArgumentException("channelMax");
        }
        if (maxMessageBytes < 1) {
            throw new IllegalArgumentException("maxMessageBytes");
        }
    }

    /** guest/guest on {@code host:5672}, virtual host "/". */
    public static AmqpConfig of(String host, int port) {
        return new AmqpConfig(host, port, "guest", "guest", "/", Duration.ofSeconds(60), 131072, 0, 64 * 1024 * 1024,
                Duration.ofSeconds(10), null, null);
    }

    public AmqpConfig withCredentials(String user, String password) {
        return new AmqpConfig(host, port, user, password, virtualHost, heartbeat, frameMax, channelMax, maxMessageBytes, connectTimeout, sslContext, connectionName);
    }

    public AmqpConfig withVirtualHost(String v) {
        return new AmqpConfig(host, port, username, password, v, heartbeat, frameMax, channelMax, maxMessageBytes, connectTimeout, sslContext, connectionName);
    }

    public AmqpConfig withHeartbeat(Duration d) {
        return new AmqpConfig(host, port, username, password, virtualHost, d, frameMax, channelMax, maxMessageBytes, connectTimeout, sslContext, connectionName);
    }

    public AmqpConfig withTls(SSLContext c) {
        return new AmqpConfig(host, port, username, password, virtualHost, heartbeat, frameMax, channelMax, maxMessageBytes, connectTimeout, c, connectionName);
    }

    public AmqpConfig withConnectionName(String n) {
        return new AmqpConfig(host, port, username, password, virtualHost, heartbeat, frameMax, channelMax, maxMessageBytes, connectTimeout, sslContext, n);
    }

    public AmqpConfig withMaxMessageBytes(int n) {
        return new AmqpConfig(host, port, username, password, virtualHost, heartbeat, frameMax, channelMax, n, connectTimeout, sslContext, connectionName);
    }
}
