package io.hydrangea.mqtt;

/**
 * What an MQTT 5 subscription asks for besides the topic filter. MQTT 3.1.1 only has the QoS; the other options are refused there.
 *
 * @param noLocal           do not send this client its own publishes
 * @param retainAsPublished keep the retain flag as the publisher set it (normally the broker clears it on delivery to live subscribers)
 * @param retainHandling    whether to send retained messages when subscribing
 */
public record SubscribeOptions(QoS qos, boolean noLocal, boolean retainAsPublished, RetainHandling retainHandling) {

    public enum RetainHandling {
        /** Send retained messages at subscribe time (the only behaviour of MQTT 3.1.1). */
        SEND_ALWAYS(0),
        /** Send them only if this subscription did not exist yet. */
        SEND_IF_NEW(1),
        /** Never. */
        NEVER(2);

        private final int code;

        RetainHandling(int code) {
            this.code = code;
        }

        public int code() {
            return code;
        }
    }

    public SubscribeOptions {
        if (qos == null || retainHandling == null) {
            throw new IllegalArgumentException("qos and retainHandling are required");
        }
    }

    public static SubscribeOptions of(QoS qos) {
        return new SubscribeOptions(qos, false, false, RetainHandling.SEND_ALWAYS);
    }

    public SubscribeOptions withNoLocal(boolean v) {
        return new SubscribeOptions(qos, v, retainAsPublished, retainHandling);
    }

    public SubscribeOptions withRetainAsPublished(boolean v) {
        return new SubscribeOptions(qos, noLocal, v, retainHandling);
    }

    public SubscribeOptions withRetainHandling(RetainHandling h) {
        return new SubscribeOptions(qos, noLocal, retainAsPublished, h);
    }

    boolean isPlain() {
        return !noLocal && !retainAsPublished && retainHandling == RetainHandling.SEND_ALWAYS;
    }
}
