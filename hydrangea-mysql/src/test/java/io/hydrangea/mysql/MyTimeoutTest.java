package io.hydrangea.mysql;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MyTimeoutTest {

    static EventLoopGroup group;

    @BeforeAll
    static void start() {
        My.require(My.PORT);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() {
        if (group != null) {
            group.close();
        }
    }

    static MyClient connect() throws Exception {
        return MyClient.connect(group, My.config()).get(10, TimeUnit.SECONDS);
    }

    static MyException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(15, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return (MyException) e.getCause();
        }
        return fail("expected a failure");
    }

    @Test
    void aSlowStatementTimesOutAndIsKilled() throws Exception {
        try (MyClient c = connect()) {
            long t0 = System.nanoTime();
            MyException e = failure(c.query(Duration.ofMillis(200), "SELECT SLEEP(30)"));
            assertTrue(e.isTimeout(), e.toString());
            assertTrue(c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0) == 1);
            long ms = (System.nanoTime() - t0) / 1_000_000;
            assertTrue(ms < 5_000, "the kill freed the connection: the next call took the whole " + ms + " ms");
            assertEquals(0, c.pendingCalls());
        }
    }

    @Test
    void aPreparedCallTimesOutToo() throws Exception {
        try (MyClient c = connect()) {
            MyException e = failure(c.query(Duration.ofMillis(200), "SELECT SLEEP(?)", 30));
            assertTrue(e.isTimeout());
            assertEquals(7, c.query("SELECT ? + 1", 6).get(10, TimeUnit.SECONDS).first().getInt(0));
        }
    }

    @Test
    void aCallThatTimesOutWhileQueuedIsReadAndDroppedInOrder() throws Exception {
        try (MyClient c = connect()) {
            CompletableFuture<MyResult> slow = c.query("SELECT SLEEP(1), 'slow'");
            List<CompletableFuture<MyResult>> short_ = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                short_.add(c.query(Duration.ofMillis(100), "SELECT " + i));
            }
            CompletableFuture<MyResult> after = c.query("SELECT 'after'");
            for (CompletableFuture<MyResult> f : short_) {
                assertTrue(failure(f).isTimeout());
            }
            assertEquals("slow", slow.get(10, TimeUnit.SECONDS).first().getString(1));
            assertEquals("after", after.get(10, TimeUnit.SECONDS).first().getString(0), "the call after the dropped ones still gets its own answer");
            assertEquals(0, c.pendingCalls());
        }
    }

    @Test
    void theConfigLimitAppliesToEveryCall() throws Exception {
        try (MyClient c = MyClient.connect(group, My.config().withQueryTimeout(Duration.ofMillis(200))).get(10, TimeUnit.SECONDS)) {
            assertTrue(failure(c.query("SELECT SLEEP(30)")).isTimeout());
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
        }
    }
}
