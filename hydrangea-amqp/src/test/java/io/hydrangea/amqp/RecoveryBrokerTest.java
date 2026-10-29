package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Recovery from things the broker itself does, driven with {@code rabbitmqctl} inside the test container
 * ({@code hydrangea-amqp-test}, see {@code hydrangea-amqp/src/test/resources/amqp/start-broker.sh}). Skipped when docker or the container is missing.
 */
class RecoveryBrokerTest {

    private static final String CONTAINER = "hydrangea-amqp-test";
    private static final int PORT = Integer.getInteger("amqp.port", 25672);
    private static EventLoopGroup group;

    private static boolean ctl(String... args) {
        try {
            String[] cmd = new String[args.length + 5];
            cmd[0] = "docker";
            cmd[1] = "exec";
            cmd[2] = "-u";
            cmd[3] = "rabbitmq";
            cmd[4] = CONTAINER;
            System.arraycopy(args, 0, cmd, 5, args.length);
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            p.getInputStream().readAllBytes();
            return p.waitFor(60, TimeUnit.SECONDS) && p.exitValue() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    @BeforeAll
    static void start() {
        Assumptions.assumeTrue(ctl("rabbitmqctl", "-q", "status"), "no docker container " + CONTAINER);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static RecoveryPolicy fast() {
        return new RecoveryPolicy(Duration.ofMillis(100), Duration.ofMillis(400), 2.0, 0.0, 0);
    }

    @Test
    void recoversWhenTheBrokerClosesTheConnection() throws Exception {
        RecoveringConnection rc = RecoveringConnection.connect(group, AmqpConfig.of("127.0.0.1", PORT), fast()).get(10, TimeUnit.SECONDS);
        BlockingQueue<String> events = new LinkedBlockingQueue<>();
        rc.addRecoveryListener(new RecoveryListener() {
            @Override
            public void connectionLost(Throwable cause) {
                events.add("lost:" + cause.getMessage());
            }

            @Override
            public void recovered() {
                events.add("recovered");
            }
        });
        try {
            RecoveringChannel ch = rc.openChannel().get(5, TimeUnit.SECONDS);
            String q = ch.queueDeclare().get(5, TimeUnit.SECONDS).name();
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            ch.basicConsume(q, true, d -> got.add(new String(d.body(), StandardCharsets.UTF_8))).get(5, TimeUnit.SECONDS);

            assertTrue(ctl("rabbitmqctl", "close_all_connections", "closed by the test"));
            String lost = events.poll(20, TimeUnit.SECONDS);
            assertNotNull(lost);
            assertTrue(lost.startsWith("lost:320"), "the broker's own close code (CONNECTION_FORCED) is reported: " + lost);
            assertEquals("recovered", events.poll(20, TimeUnit.SECONDS));

            ch.basicPublish("", q, false, null, "back".getBytes(StandardCharsets.UTF_8)).get(5, TimeUnit.SECONDS);
            assertEquals("back", got.poll(5, TimeUnit.SECONDS));
        } finally {
            rc.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void givesUpWhenTheBrokerRefusesTheCredentialsOnReconnect() throws Exception {
        String user = "tmp-" + UUID.randomUUID().toString().substring(0, 8);
        assertTrue(ctl("rabbitmqctl", "add_user", user, "pw"));
        assertTrue(ctl("rabbitmqctl", "set_permissions", "-p", "/", user, ".*", ".*", ".*"));
        RecoveringConnection rc = RecoveringConnection.connect(group, AmqpConfig.of("127.0.0.1", PORT).withCredentials(user, "pw"), fast())
                .get(10, TimeUnit.SECONDS);
        java.util.concurrent.CompletableFuture<Throwable> gaveUp = new java.util.concurrent.CompletableFuture<>();
        rc.addRecoveryListener(new RecoveryListener() {
            @Override
            public void gaveUp(Throwable cause) {
                gaveUp.complete(cause);
            }
        });
        try {
            RecoveringChannel ch = rc.openChannel().get(5, TimeUnit.SECONDS);
            assertTrue(ctl("rabbitmqctl", "delete_user", user));
            ctl("rabbitmqctl", "close_all_connections", "user deleted"); // in case deleting the user left the connection open
            Throwable why = gaveUp.get(30, TimeUnit.SECONDS);
            assertInstanceOf(AmqpException.class, why);
            assertEquals(403, ((AmqpException) why).replyCode());
            rc.closeFuture().get(5, TimeUnit.SECONDS);
            assertFalse(ch.isOpen());
        } finally {
            ctl("rabbitmqctl", "delete_user", user); // already gone on success
            rc.closeAsync().get(10, TimeUnit.SECONDS);
        }
    }
}
