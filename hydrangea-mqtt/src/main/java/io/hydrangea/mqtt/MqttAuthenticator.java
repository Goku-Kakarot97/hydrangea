package io.hydrangea.mqtt;

/**
 * Enhanced authentication of MQTT 5 (the AUTH packet): a challenge/response exchange between client and broker, named by a method string
 * such as {@code SCRAM-SHA-256}. Set it with {@link MqttConfig.V5#withAuthenticator}. The client runs one {@link Exchange} for every connection
 * and for every {@link MqttClient#reauthenticate() re-authentication}.
 * <p>
 * The exchange runs on the client's event-loop thread, so its methods must not block: they are for computing a response, not for fetching one.
 */
public interface MqttAuthenticator {

    /** The authentication method name sent in CONNECT; the broker must answer with the same one. */
    String method();

    /** Starts one exchange. Called again for each reconnect and each re-authentication, so an exchange may keep state. */
    Exchange begin();

    interface Exchange {

        /** The data sent with CONNECT (or with the AUTH that starts a re-authentication); null = none. */
        byte[] initialData();

        /**
         * The broker sent a challenge; returns the answer to put in the next AUTH packet. Throw to abandon the exchange: the connection is closed.
         */
        byte[] respond(byte[] challenge);

        /**
         * The broker ended the exchange with success; {@code data} is what it sent along (null if nothing). A mutual-authentication scheme checks the
         * broker's proof here, and throws if it is wrong: the connection is then closed.
         */
        default void finish(byte[] data) {
        }
    }
}
