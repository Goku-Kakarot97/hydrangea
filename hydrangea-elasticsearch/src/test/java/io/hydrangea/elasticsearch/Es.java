package io.hydrangea.elasticsearch;

import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;

/** Where the Elasticsearch of {@code start-elasticsearch.sh} listens; tests that need it are skipped when it is not there. */
final class Es {

    static final String HOST = System.getProperty("es.host", "127.0.0.1");
    static final int PORT = Integer.getInteger("es.port", 29200);
    static final String URL = "http://" + HOST + ":" + PORT;

    private Es() {
    }

    static void require() {
        boolean up;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(HOST, PORT), 500);
            up = true;
        } catch (Exception e) {
            up = false;
        }
        Assumptions.assumeTrue(up, "no Elasticsearch on " + URL + " (start one with hydrangea-elasticsearch/src/test/resources/elasticsearch/start-elasticsearch.sh)");
    }

    static EsConfig config() {
        return EsConfig.of(URL);
    }

    /** A test index name nobody else uses. */
    static String index(String prefix) {
        return "hydra-" + prefix + "-" + Long.toHexString(System.nanoTime());
    }
}
