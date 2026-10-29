package io.hydrangea.kafka;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.UUID;

/** Where the broker of {@code start-kafka.sh} listens; tests that need it are skipped when it is not there. */
final class Kafka {

    static final String HOST = System.getProperty("kafka.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("kafka.port", 29092);
    static final int SASL_PORT = Integer.getInteger("kafka.sasl.port", 29094);

    private Kafka() {
    }

    static void require(int port) {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, port), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no Kafka on " + HOST + ":" + port + " (start one with hydrangea-kafka/src/test/resources/kafka/start-kafka.sh)");
    }

    static KafkaConfig config() {
        return KafkaConfig.of(HOST + ":" + PORT);
    }

    static String topic(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
}
