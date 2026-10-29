package io.hydrangea.mqtt;

/**
 * A failure of the MQTT connection or of one request. {@link #retriable()} says whether trying again later can succeed (a lost
 * connection, a broker that is not ready) or not (bad credentials, a protocol violation, a refused subscription).
 */
public final class MqttException extends RuntimeException {

    /** CONNACK return codes (MQTT 3.1.1, 3.2.2.3). */
    public static final int REFUSED_PROTOCOL_VERSION = 1;
    public static final int REFUSED_IDENTIFIER = 2;
    public static final int REFUSED_SERVER_UNAVAILABLE = 3;
    public static final int REFUSED_BAD_CREDENTIALS = 4;
    public static final int REFUSED_NOT_AUTHORIZED = 5;

    private final int returnCode;
    private final boolean retriable;

    public MqttException(String message, boolean retriable) {
        this(-1, message, retriable, null);
    }

    public MqttException(String message, boolean retriable, Throwable cause) {
        this(-1, message, retriable, cause);
    }

    MqttException(int returnCode, String message, boolean retriable, Throwable cause) {
        super(message, cause);
        this.returnCode = returnCode;
        this.retriable = retriable;
    }

    /** The CONNACK return code (MQTT 3.1.1) or the reason code (MQTT 5: the one in CONNACK, PUBACK, SUBACK, DISCONNECT...) behind this failure; -1 when there is none. */
    public int returnCode() {
        return returnCode;
    }

    /** Same as {@link #returnCode()}, under the MQTT 5 name. */
    public int reasonCode() {
        return returnCode;
    }

    public boolean retriable() {
        return retriable;
    }
}
