package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.mqtt.MqttClient;
import io.hydrangea.mqtt.MqttConfig;
import io.hydrangea.mqtt.MqttException;
import io.hydrangea.mqtt.MqttMessage;
import io.hydrangea.mqtt.MqttProperties;
import io.hydrangea.mqtt.MqttWebSocket;
import io.hydrangea.mqtt.QoS;
import io.hydrangea.mqtt.SubscribeOptions;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * MQTT 5: request/response ({@code request} and {@code reply}), a shared subscription, subscription options, and the reason code of a refused publish. Add a third
 * argument {@code ws://host:port/mqtt} to run the same thing over WebSocket.
 * <pre>
 * docker run -d --name mosquitto -p 1883:1883 -p 9001:9001 eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
 * java -cp target/classes io.hydrangea.examples.Mqtt5Example [host] [port] [ws-uri]
 * </pre>
 */
public final class Mqtt5Example {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 1883;
        String wsUri = args.length > 2 ? args[2] : null;

        try (EventLoopGroup group = new EventLoopGroup(1)) {
            MqttConfig base = MqttConfig.of(host, port).withVersion(5);
            if (wsUri != null) {
                base = base.withTransport(MqttWebSocket.of(wsUri));
            }
            MqttClient service = MqttClient.connect(group, base.withClientId("example-service")).get(10, TimeUnit.SECONDS);
            MqttClient client = MqttClient.connect(group, base.withClientId("example-client")).get(10, TimeUnit.SECONDS);
            System.out.println("broker: receive maximum " + client.serverInfo().receiveMaximum() + ", shared subscriptions "
                    + client.serverInfo().sharedSubscriptionAvailable());

            // a service answering requests: reply() sends to the request's response topic with its correlation data
            service.subscribe("$share/workers/example5/upper", SubscribeOptions.of(QoS.AT_LEAST_ONCE), request ->
                    service.reply(request, request.payloadAsString().toUpperCase().getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE))
                    .get(5, TimeUnit.SECONDS);

            // request() subscribes to a response topic once, sets the response topic and a correlation id, and matches the answer to the request
            MqttMessage reply = client.request("example5/upper", "hello".getBytes(StandardCharsets.UTF_8), QoS.AT_LEAST_ONCE,
                    MqttProperties.NONE.withContentType("text/plain").withUserProperty("caller", "example"), Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS);
            System.out.println("reply: " + reply.payloadAsString());

            try {
                client.publish("bad/\0topic", new byte[0], QoS.AT_LEAST_ONCE).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("refused locally: " + e.getCause());
            }
            try {
                client.subscribe("a/#/b", QoS.AT_LEAST_ONCE, m -> { }).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                Throwable cause = e.getCause();
                System.out.println("refused: " + cause + (cause instanceof MqttException m ? " reason 0x" + Integer.toHexString(m.reasonCode()) : ""));
            }
            client.disconnect().get(5, TimeUnit.SECONDS);
            service.disconnect().get(5, TimeUnit.SECONDS);
        }
    }
}
