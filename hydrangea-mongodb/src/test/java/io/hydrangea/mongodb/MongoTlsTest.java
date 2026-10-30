package io.hydrangea.mongodb;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** Against the TLS servers of {@code start-mongodb-tls.sh}. */
class MongoTlsTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void setUp() {
        Mongo.require(Mongo.TLS_PORT, "start-mongodb-tls.sh");
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private static MongoConfig config(int port) {
        return MongoConfig.of(Mongo.HOST, port, "hydra").withConnectTimeout(Duration.ofSeconds(5)).withServerSelectionTimeout(Duration.ofSeconds(5));
    }

    @Test
    void theSessionRunsInsideTls() throws Exception {
        try (MongoClient c = MongoClient.connect(group, config(Mongo.TLS_PORT).withTls(Mongo.trusting())).get(10, TimeUnit.SECONDS)) {
            c.ping().get(10, TimeUnit.SECONDS);
            MongoCollection coll = c.collection(Mongo.collection("tls"));
            Object id = coll.insertOne(Bson.doc("secret", "over tls")).get(10, TimeUnit.SECONDS).id();
            assertEquals("over tls", coll.findById(id).get(10, TimeUnit.SECONDS).get("secret"));
            for (int i = 0; i < 200; i++) {
                coll.insertOne(Bson.doc("n", i));
            }
            assertEquals(201, coll.count(null).get(10, TimeUnit.SECONDS));
            Object ssl = c.command("admin", Bson.doc("serverStatus", 1)).get(10, TimeUnit.SECONDS).get("transportSecurity");
            assertNotNull(ssl, "the server counts TLS connections");
            coll.drop().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void anUntrustedCertificateIsRefused() {
        ExecutionException e = assertThrows(ExecutionException.class, () -> MongoClient.connect(group, config(Mongo.TLS_PORT).withTls(javax.net.ssl.SSLContext.getDefault())).get(15, TimeUnit.SECONDS));
        MongoException m = assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
        assertEquals(0, m.code());
        assertTrue(m.getMessage().contains("no primary found"), m.getMessage());
    }

    @Test
    void aCertificateForAnotherHostIsRefused() throws Exception {
        Mongo.require(Mongo.TLS_WRONG_NAME_PORT, "start-mongodb-tls.sh");
        ExecutionException e = assertThrows(ExecutionException.class, () -> MongoClient.connect(group, config(Mongo.TLS_WRONG_NAME_PORT).withTls(Mongo.trusting())).get(15, TimeUnit.SECONDS));
        assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
    }

    @Test
    void aServerThatRequiresTlsDoesNotTalkInTheClear() {
        ExecutionException e = assertThrows(ExecutionException.class, () -> MongoClient.connect(group, config(Mongo.TLS_PORT)).get(15, TimeUnit.SECONDS));
        MongoException m = assertInstanceOf(MongoException.class, e.getCause(), String.valueOf(e.getCause()));
        assertEquals(0, m.code(), m.getMessage());
    }
}
