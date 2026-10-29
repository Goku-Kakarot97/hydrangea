package io.hydrangea.amqp;

import java.time.Instant;
import java.util.Map;

/** The properties of a message (class "basic"). Every field is optional (null). */
public record BasicProperties(
        String contentType,
        String contentEncoding,
        Map<String, Object> headers,
        Integer deliveryMode,
        Integer priority,
        String correlationId,
        String replyTo,
        String expiration,
        String messageId,
        Instant timestamp,
        String type,
        String userId,
        String appId) {

    public static final BasicProperties EMPTY = new BasicProperties(null, null, null, null, null, null, null, null, null, null, null, null, null);

    /** Survives a broker restart if the queue is durable too. */
    public static final BasicProperties PERSISTENT = EMPTY.withDeliveryMode(2);

    public BasicProperties withContentType(String v) {
        return new BasicProperties(v, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId);
    }

    public BasicProperties withHeaders(Map<String, Object> v) {
        return new BasicProperties(contentType, contentEncoding, v, deliveryMode, priority, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId);
    }

    /** 1 = transient, 2 = persistent. */
    public BasicProperties withDeliveryMode(int v) {
        return new BasicProperties(contentType, contentEncoding, headers, v, priority, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId);
    }

    public BasicProperties withCorrelationId(String v) {
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, v, replyTo, expiration, messageId, timestamp, type, userId, appId);
    }

    public BasicProperties withReplyTo(String v) {
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, v, expiration, messageId, timestamp, type, userId, appId);
    }

    /** Time to live in milliseconds, as a decimal string (the wire format). */
    public BasicProperties withExpiration(String v) {
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, v, messageId, timestamp, type, userId, appId);
    }

    public BasicProperties withMessageId(String v) {
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, expiration, v, timestamp, type, userId, appId);
    }

    public BasicProperties withPriority(int v) {
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, v, correlationId, replyTo, expiration, messageId, timestamp, type, userId, appId);
    }
}
