package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MyTlsTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        My.require(My.TLS_PORT);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() {
        if (group != null) {
            group.close();
        }
    }

    @Test
    void theSessionRunsInsideTlsAndFullAuthenticationSendsThePasswordOverIt() throws Exception {
        try (MyClient c = MyClient.connect(group, My.config(My.TLS_PORT).withTls(My.trusting())).get(10, TimeUnit.SECONDS)) {
            String cipher = c.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'").get(10, TimeUnit.SECONDS).first().getString(1);
            assertFalse(cipher.isEmpty(), "the server says this session is encrypted");
            for (int i = 0; i < 200; i++) {
                assertEquals(i, c.query("SELECT ? + 0", i).get(10, TimeUnit.SECONDS).first().getInt(0));
            }
        }
        try (MyClient plain = MyClient.connect(group, My.config(My.TLS_PORT)).get(10, TimeUnit.SECONDS)) {
            String cipher = plain.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'").get(10, TimeUnit.SECONDS).first().getString(1);
            assertEquals("", cipher, "without withTls the session is in the clear");
        }
    }

    @Test
    void aCertificateForAnotherHostIsRefused() throws Exception {
        My.require(My.TLS_WRONG_NAME_PORT);
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> MyClient.connect(group, My.config(My.TLS_WRONG_NAME_PORT).withTls(My.trusting())).get(10, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof MyException, e.getCause().toString());
    }

    @Test
    void wrongPasswordOverTls() throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class,
                () -> MyClient.connect(group, My.config(My.TLS_PORT).withTls(My.trusting()).withPassword("nope")).get(10, TimeUnit.SECONDS));
        assertEquals(1045, ((MyException) e.getCause()).errorCode());
    }
}
