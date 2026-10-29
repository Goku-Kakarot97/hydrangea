package io.hydrangea.mqtt;

/**
 * What an MQTT 5 broker said about itself in CONNACK (the defaults of the specification where it said nothing; all of them for MQTT 3.1.1).
 *
 * @param receiveMaximum   the most QoS 1/2 messages it accepts unacknowledged; the client keeps within it
 * @param maximumQoS       the highest QoS it supports for publishing
 * @param maximumPacketSize the largest packet it accepts, 0 = no limit stated
 * @param assignedClientId the identifier it chose when the client sent an empty one, else null
 * @param serverKeepAlive  the keep-alive it imposed in seconds, -1 = none (the client's own is used)
 * @param responseInformation what it offered for building response topics, else null
 * @param sessionExpirySeconds the session lifetime it granted when it changed the request, else -1
 */
public record MqttServerInfo(
        int receiveMaximum,
        int maximumQoS,
        boolean retainAvailable,
        long maximumPacketSize,
        int topicAliasMaximum,
        boolean wildcardSubscriptionAvailable,
        boolean subscriptionIdentifiersAvailable,
        boolean sharedSubscriptionAvailable,
        String assignedClientId,
        int serverKeepAlive,
        String responseInformation,
        long sessionExpirySeconds) {

    static final MqttServerInfo DEFAULTS = new MqttServerInfo(65535, 2, true, 0, 0, true, true, true, null, -1, null, -1);
}
