package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.mqtt.MqttClient;
import io.hydrangea.mqtt.MqttConfig;
import io.hydrangea.mqtt.MqttListener;
import io.hydrangea.mqtt.QoS;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 3.1.1 client: subscribe with a wildcard, publish at QoS 0, 1 and 2, keep a retained message, and see the client reconnect by itself.
 * <pre>
 * docker run -d --name mosquitto -p 1883:1883 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
 * java -cp target/classes io.hydrangea.examples.MqttExample [host] [port]
 * </pre>
 * Handlers and listener callbacks run on the client's event-loop thread, so they must not block.
 */
public final class MqttExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1883;

        try (EventLoopGroup group = new EventLoopGroup(1)) {
            MqttConfig config = MqttConfig.of(host, port)
                    .withClientId("hydrangea-example")
                    .withKeepAlive(Duration.ofSeconds(30))
                    // the broker publishes this if we vanish without saying goodbye
                    .withWill(new MqttConfig.Will("example/status", "offline".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, true));

            MqttClient client = MqttClient.connect(group, config, new MqttListener() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("connection lost: " + cause + " (the client reconnects by itself)");
                }

                @Override
                public void connected(boolean sessionPresent) {
                    System.out.println("connected, session present: " + sessionPresent);
                }
            }).get(10, TimeUnit.SECONDS);

            CountDownLatch received = new CountDownLatch(4);
            QoS granted = client.subscribe("example/#", QoS.EXACTLY_ONCE, m -> {
                System.out.println("received on " + m.topic() + " (" + m.qos() + (m.retained() ? ", retained" : "") + "): " + m.payloadAsString());
                received.countDown();
            }).get(5, TimeUnit.SECONDS);
            System.out.println("the broker granted " + granted);

            client.publish("example/status", "online".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE, true).get(5, TimeUnit.SECONDS);   // PUBACK
            client.publish("example/qos0", "fire and forget".getBytes(StandardCharsets.UTF_8), QoS.AT_MOST_ONCE).get(5, TimeUnit.SECONDS);
            client.publish("example/qos1", "acknowledged".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE).get(5, TimeUnit.SECONDS);
            client.publish("example/qos2", "exactly once".getBytes(StandardCharsets.UTF_8), QoS.EXACTLY_ONCE).get(5, TimeUnit.SECONDS);     // PUBCOMP

            received.await(10, TimeUnit.SECONDS);
            client.publish("example/status", new byte[0], QoS.AT_LEAST_ONCE, true).get(5, TimeUnit.SECONDS);                                   // clear the retained message
            client.disconnect().get(10, TimeUnit.SECONDS);
        }
    }
}
