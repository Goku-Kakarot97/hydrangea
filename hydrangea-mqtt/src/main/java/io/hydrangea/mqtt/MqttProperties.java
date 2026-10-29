package io.hydrangea.mqtt;

import java.util.ArrayList;
import java.util.List;

/**
 * The properties an MQTT 5 application message can carry. All optional; {@link #NONE} is the empty set (and what every MQTT 3.1.1 message has).
 *
 * @param payloadFormat       0 = unspecified bytes, 1 = the payload is UTF-8 text; the client does not check it
 * @param messageExpirySeconds seconds the broker may keep the message before delivering it (on publish); on a received message, the time that was left
 * @param responseTopic       where a reply to this message should go (request/response)
 * @param correlationData     lets the requester match a reply to its request
 * @param userProperties      name/value pairs, in order
 * @param subscriptionIds     only on received messages: the identifiers of the subscriptions that matched (the client set them; it uses them to route)
 */
public record MqttProperties(
        Integer payloadFormat,
        Long messageExpirySeconds,
        String contentType,
        String responseTopic,
        byte[] correlationData,
        List<UserProperty> userProperties,
        List<Integer> subscriptionIds) {

    public static final MqttProperties NONE = new MqttProperties(null, null, null, null, null, List.of(), List.of());

    public MqttProperties {
        userProperties = userProperties == null ? List.of() : List.copyOf(userProperties);
        subscriptionIds = subscriptionIds == null ? List.of() : List.copyOf(subscriptionIds);
        if (payloadFormat != null && payloadFormat != 0 && payloadFormat != 1) {
            throw new IllegalArgumentException("payloadFormat must be 0 or 1");
        }
        if (messageExpirySeconds != null && (messageExpirySeconds < 0 || messageExpirySeconds > 0xffffffffL)) {
            throw new IllegalArgumentException("messageExpirySeconds");
        }
    }

    public MqttProperties withUtf8Payload() {
        return new MqttProperties(1, messageExpirySeconds, contentType, responseTopic, correlationData, userProperties, subscriptionIds);
    }

    public MqttProperties withMessageExpiry(long seconds) {
        return new MqttProperties(payloadFormat, seconds, contentType, responseTopic, correlationData, userProperties, subscriptionIds);
    }

    public MqttProperties withContentType(String type) {
        return new MqttProperties(payloadFormat, messageExpirySeconds, type, responseTopic, correlationData, userProperties, subscriptionIds);
    }

    public MqttProperties withResponseTopic(String topic) {
        return new MqttProperties(payloadFormat, messageExpirySeconds, contentType, topic, correlationData, userProperties, subscriptionIds);
    }

    public MqttProperties withCorrelationData(byte[] data) {
        return new MqttProperties(payloadFormat, messageExpirySeconds, contentType, responseTopic, data, userProperties, subscriptionIds);
    }

    public MqttProperties withUserProperty(String name, String value) {
        List<UserProperty> l = new ArrayList<>(userProperties);
        l.add(new UserProperty(name, value));
        return new MqttProperties(payloadFormat, messageExpirySeconds, contentType, responseTopic, correlationData, l, subscriptionIds);
    }

    /** The first user property called {@code name}, or null. */
    public String userProperty(String name) {
        for (UserProperty p : userProperties) {
            if (p.name().equals(name)) {
                return p.value();
            }
        }
        return null;
    }
}
