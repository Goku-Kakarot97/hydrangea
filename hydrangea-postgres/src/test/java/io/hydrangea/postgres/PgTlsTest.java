package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** TLS against the containers of {@code start-postgres.sh} (ports 25435 and 25436); skipped when they are not there. */
class PgTlsTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        Pg.notOnRemote();
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        if (group != null) {
            group.close();
        }
    }

    private PgConfig tls() throws Exception {
        return Pg.config(Pg.TLS_PORT).withTls(Pg.trusting());
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(15, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    @Test
    void connectsOverTlsAndTheServerSeesTheSession() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgClient c = Pg.get(PgClient.connect(group, tls()))) {
            PgRow r = Pg.get(c.query("SELECT ssl, version FROM pg_stat_ssl WHERE pid = pg_backend_pid()")).first();
            assertTrue(r.getBoolean("ssl"), "the server says this session is encrypted");
            assertTrue(r.getString("version").startsWith("TLSv1"), r.getString("version"));
            assertEquals(1, Pg.get(c.query("SELECT 1")).first().getInt(0));
        }
    }

    @Test
    void aPlainConnectionToTheSameServerIsNotEncrypted() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgClient c = Pg.get(PgClient.connect(group, Pg.config(Pg.TLS_PORT)))) {
            assertEquals(false, Pg.get(c.query("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")).first().getBoolean(0));
        }
    }

    @Test
    void largeResultsAndPipelinesWorkOverTls() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgClient c = Pg.get(PgClient.connect(group, tls()))) {
            assertEquals(200_000, Pg.get(c.query("SELECT g, repeat('x', 50) FROM generate_series(1, 200000) g")).size());
            String text = "ü".repeat(1_000_000);
            assertEquals(text, Pg.get(c.query("SELECT $1::text", text)).first().getString(0));
            List<CompletableFuture<PgResult>> fs = new ArrayList<>();
            for (int i = 0; i < 5000; i++) {
                fs.add(c.query("SELECT $1::int", i));
            }
            for (int i = 0; i < fs.size(); i++) {
                assertEquals(i, Pg.get(fs.get(i)).first().getInt(0));
            }
        }
    }

    @Test
    void copyInAndOutOverTls() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgClient c = Pg.get(PgClient.connect(group, tls()))) {
            String t = Pg.table("tc");
            Pg.get(c.execute("CREATE TEMP TABLE " + t + " (id int, s text)"));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100_000; i++) {
                sb.append(i).append("\tsome text ").append(i).append('\n');
            }
            byte[] data = sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(100_000L, Pg.get(c.copyIn("COPY " + t + " FROM STDIN", data)).longValue());
            assertArrayEquals(data, Pg.get(c.copyOut("COPY " + t + " TO STDOUT")));
        }
    }

    @Test
    void theCancelOfATimedOutQueryUsesTlsToo() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgClient c = Pg.get(PgClient.connect(group, tls()))) {
            long t0 = System.nanoTime();
            assertTrue(failure(c.query(java.time.Duration.ofMillis(300), "SELECT pg_sleep(30)")).isTimeout());
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
            assertTrue((System.nanoTime() - t0) / 1_000_000 < 5000, "the backend was stopped, not left sleeping for 30 s");
        }
    }

    @Test
    void aPoolOverTls() throws Exception {
        Pg.require(Pg.TLS_PORT);
        try (PgPool pool = Pg.get(PgPool.create(group, tls(), 3))) {
            for (int i = 0; i < 100; i++) {
                assertEquals(i, Pg.get(pool.query("SELECT $1::int", i)).first().getInt(0));
            }
            assertEquals(7, Pg.get(pool.transaction(db -> db.query("SELECT 7"))).first().getInt(0));
        }
    }

    @Test
    void aCertificateTheClientDoesNotTrustIsRefused() throws Exception {
        Pg.require(Pg.TLS_PORT);
        PgException e = failure(PgClient.connect(group, Pg.config(Pg.TLS_PORT).withTls(javax.net.ssl.SSLContext.getDefault())));
        assertTrue(e.getMessage().contains("PKIX path building failed"), e.getMessage());
    }

    @Test
    void aCertificateForAnotherHostNameIsRefused() throws Exception {
        Pg.require(Pg.TLS_WRONG_NAME_PORT);
        PgException e = failure(PgClient.connect(group, Pg.config(Pg.TLS_WRONG_NAME_PORT).withTls(Pg.trusting())));
        assertTrue(e.getMessage().contains("No subject alternative names matching IP address 127.0.0.1"), e.getMessage());
    }

    @Test
    void aServerWithoutTlsIsAnErrorNotAFallbackToPlainText() throws Exception {
        Pg.require(Pg.PORT);
        PgException e = failure(PgClient.connect(group, Pg.config(Pg.PORT).withTls(Pg.trusting())));
        assertTrue(e.getMessage().contains("does not support TLS"), e.getMessage());
    }
}
