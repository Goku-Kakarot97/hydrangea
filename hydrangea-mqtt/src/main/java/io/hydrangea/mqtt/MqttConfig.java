package io.hydrangea.mqtt;

import io.hydrangea.Backoff;

import javax.net.ssl.SSLContext;
import java.time.Duration;
import java.util.List;

/**
 * Connection settings.
 *
 * @param clientId       identifies the session at the broker; empty = the broker picks one (MQTT 3.1.1: only with {@code cleanSession})
 * @param cleanSession   true = start with no session ("clean start" in MQTT 5); false = the broker keeps subscriptions and unacknowledged QoS 1/2 traffic
 *                       across connections (MQTT 5: for as long as the session expiry interval says), and this client resends its own after a reconnect
 * @param keepAlive      the longest silence on the wire before a PINGREQ; the connection is dropped when the broker is silent for 1.5 times
 *                       this. Null or zero disables it (the broker may then never notice a dead client). An MQTT 5 broker may impose its own.
 * @param password       sent only with a user name (MQTT 3.1.1; MQTT 5 allows a password alone)
 * @param will           message the broker publishes if this client vanishes without DISCONNECT; null = none
 * @param sslContext     non-null = TLS (normally port 8883) for the default TCP transport; the server certificate is verified against {@code host}
 * @param maxPacketBytes largest packet accepted from the broker; a bigger one closes the connection (the packet is buffered whole)
 * @param maxInflight    QoS 1/2 messages sent but not yet acknowledged at any time (and never more than an MQTT 5 broker's receive maximum); more wait in the local queue
 * @param maxQueued      QoS 1/2 messages accepted in all (in flight plus waiting); beyond it {@code publish} fails at once
 * @param reconnect      how to retry after losing an established connection; null = do not reconnect, the client closes
 * @param version        4 = MQTT 3.1.1 (the default), 5 = MQTT 5.0
 * @param v5             MQTT 5 only: session expiry, flow-control and CONNECT properties
 * @param transport      how the bytes travel; null = TCP (with TLS if {@code sslContext} is set). See {@link MqttWebSocket} for MQTT over WebSocket
 */
public record MqttConfig(
        String host,
        int port,
        String clientId,
        boolean cleanSession,
        Duration keepAlive,
        String username,
        String password,
        Will will,
        SSLContext sslContext,
        Duration connectTimeout,
        int maxPacketBytes,
        int maxInflight,
        int maxQueued,
        Backoff reconnect,
        int version,
        V5 v5,
        MqttTransport transport) {

    /**
     * Last will and testament.
     *
     * @param properties   MQTT 5 only: properties of the will message
     * @param delaySeconds MQTT 5 only: how long the broker waits after the connection is lost before publishing the will (the client may come back first)
     */
    public record Will(String topic, byte[] payload, QoS qos, boolean retain, MqttProperties properties, long delaySeconds) {
        public Will {
            Packets.checkTopicName(topic);
            if (payload == null || payload.length > 65535) {
                throw new IllegalArgumentException("will payload must be at most 65535 bytes");
            }
            if (qos == null) {
                throw new IllegalArgumentException("qos");
            }
            if (properties == null) {
                properties = MqttProperties.NONE;
            }
            if (delaySeconds < 0 || delaySeconds > 0xffffffffL) {
                throw new IllegalArgumentException("delaySeconds");
            }
        }

        public Will(String topic, byte[] payload, QoS qos, boolean retain) {
            this(topic, payload, qos, retain, MqttProperties.NONE, 0);
        }
    }

    /**
     * MQTT 5 connection properties.
     *
     * @param sessionExpirySeconds how long the broker keeps the session after the connection ends; 0 = it ends with the connection
     *                             (set it, and {@code cleanSession=false}, to get a session that survives)
     * @param receiveMaximum       how many QoS 1/2 messages this client lets the broker have unacknowledged towards it at once (65535 = the default, not sent)
     * @param topicAliasMaximum    how many topic aliases the broker may use in messages to this client; 0 = none
     * @param maximumPacketSize    the largest packet the broker may send; 0 = no limit asked for
     * @param userProperties       sent in CONNECT
     * @param authenticator        enhanced authentication (AUTH packets); null = none
     * @param outgoingTopicAliases true = use topic aliases for the messages this client publishes, as far as the broker allows (its topic alias maximum):
     *                             a topic that is published again is then sent as a two-byte number instead of its name
     */
    public record V5(long sessionExpirySeconds, int receiveMaximum, int topicAliasMaximum, long maximumPacketSize, List<UserProperty> userProperties,
                     MqttAuthenticator authenticator, boolean outgoingTopicAliases) {
        public static final V5 DEFAULTS = new V5(0, 65535, 0, 0, List.of());

        public V5 {
            if (sessionExpirySeconds < 0 || sessionExpirySeconds > 0xffffffffL || receiveMaximum < 1 || receiveMaximum > 65535
                    || topicAliasMaximum < 0 || topicAliasMaximum > 65535 || maximumPacketSize < 0 || maximumPacketSize > 0xffffffffL) {
                throw new IllegalArgumentException("MQTT 5 option out of range");
            }
            userProperties = userProperties == null ? List.of() : List.copyOf(userProperties);
        }

        public V5(long sessionExpirySeconds, int receiveMaximum, int topicAliasMaximum, long maximumPacketSize, List<UserProperty> userProperties) {
            this(sessionExpirySeconds, receiveMaximum, topicAliasMaximum, maximumPacketSize, userProperties, null, false);
        }

        public V5 withSessionExpiry(long seconds) {
            return new V5(seconds, receiveMaximum, topicAliasMaximum, maximumPacketSize, userProperties, authenticator, outgoingTopicAliases);
        }

        public V5 withReceiveMaximum(int n) {
            return new V5(sessionExpirySeconds, n, topicAliasMaximum, maximumPacketSize, userProperties, authenticator, outgoingTopicAliases);
        }

        public V5 withTopicAliasMaximum(int n) {
            return new V5(sessionExpirySeconds, receiveMaximum, n, maximumPacketSize, userProperties, authenticator, outgoingTopicAliases);
        }

        public V5 withMaximumPacketSize(long n) {
            return new V5(sessionExpirySeconds, receiveMaximum, topicAliasMaximum, n, userProperties, authenticator, outgoingTopicAliases);
        }

        public V5 withUserProperty(String name, String value) {
            List<UserProperty> l = new java.util.ArrayList<>(userProperties);
            l.add(new UserProperty(name, value));
            return new V5(sessionExpirySeconds, receiveMaximum, topicAliasMaximum, maximumPacketSize, l, authenticator, outgoingTopicAliases);
        }

        /** Enhanced authentication with the given method, e.g. {@link ScramSha256}. */
        public V5 withAuthenticator(MqttAuthenticator a) {
            return new V5(sessionExpirySeconds, receiveMaximum, topicAliasMaximum, maximumPacketSize, userProperties, a, outgoingTopicAliases);
        }

        public V5 withOutgoingTopicAliases(boolean on) {
            return new V5(sessionExpirySeconds, receiveMaximum, topicAliasMaximum, maximumPacketSize, userProperties, authenticator, on);
        }
    }

    public MqttConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host");
        }
        if (version != 4 && version != 5) {
            throw new IllegalArgumentException("version must be 4 (MQTT 3.1.1) or 5");
        }
        if (v5 == null) {
            v5 = V5.DEFAULTS;
        }
        if (version == 4 && (v5.authenticator() != null || v5.outgoingTopicAliases())) {
            throw new IllegalArgumentException("enhanced authentication and topic aliases need MQTT 5");
        }
        if (clientId == null) {
            clientId = "";
        }
        if (version == 4 && clientId.isEmpty() && !cleanSession) {
            throw new IllegalArgumentException("an empty client id needs cleanSession (the broker cannot tell whose session to keep)");
        }
        if (keepAlive != null && (keepAlive.isNegative() || keepAlive.toSeconds() > 65535)) {
            throw new IllegalArgumentException("keepAlive must be between 0 and 65535 seconds");
        }
        if (version == 4 && password != null && username == null) {
            throw new IllegalArgumentException("MQTT 3.1.1 does not allow a password without a user name");
        }
        if (maxPacketBytes < 16 || maxPacketBytes > 268_435_455) {
            throw new IllegalArgumentException("maxPacketBytes");
        }
        if (maxInflight < 1 || maxInflight > 65535 || maxQueued < maxInflight) {
            throw new IllegalArgumentException("require 1 <= maxInflight <= 65535 and maxInflight <= maxQueued");
        }
        Packets.utf8(clientId);
    }

    /** MQTT 3.1.1, clean session, keep-alive 60 s, reconnect with {@link Backoff#defaults()}, 100 in flight, 10 000 queued, 4 MiB packets, TCP. */
    public static MqttConfig of(String host, int port) {
        return new MqttConfig(host, port, "", true, Duration.ofSeconds(60), null, null, null, null, Duration.ofSeconds(10),
                4 * 1024 * 1024, 100, 10_000, Backoff.defaults(), 4, V5.DEFAULTS, null);
    }

    public MqttConfig withClientId(String id) {
        return new MqttConfig(host, port, id, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withCleanSession(boolean clean) {
        return new MqttConfig(host, port, clientId, clean, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withKeepAlive(Duration d) {
        return new MqttConfig(host, port, clientId, cleanSession, d, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withCredentials(String user, String password) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, user, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withWill(Will w) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, w, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withTls(SSLContext c) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, c, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withConnectTimeout(Duration d) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, d, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withMaxPacketBytes(int n) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, n, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withLimits(int maxInflight, int maxQueued) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    /** Null = never reconnect. */
    public MqttConfig withReconnect(Backoff b) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, b, version, v5, transport);
    }

    /** 4 = MQTT 3.1.1, 5 = MQTT 5.0. */
    public MqttConfig withVersion(int version) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withV5(V5 v5) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, transport);
    }

    public MqttConfig withTransport(MqttTransport t) {
        return new MqttConfig(host, port, clientId, cleanSession, keepAlive, username, password, will, sslContext, connectTimeout, maxPacketBytes, maxInflight, maxQueued, reconnect, version, v5, t);
    }
}
