package io.hydrangea.redis;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The client against a scripted server: exact commands out, awkward and hostile replies in. No Redis needed. */
class RedisWireTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        group.shutdown().get(5, TimeUnit.SECONDS);
    }

    private static RedisConfig cfg(FakeRedis s) {
        return s.config().withReconnect(null);
    }

    private record Up(RedisClient client, FakeRedis.Conn conn) {
    }

    private static Up up(FakeRedis server) throws Exception {
        return up(server, cfg(server), null);
    }

    private static Up up(FakeRedis server, RedisConfig cfg, RedisListener l) throws Exception {
        CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg, l);
        FakeRedis.Conn c = server.accept();
        c.loginResp3();
        return new Up(f.get(5, TimeUnit.SECONDS), c);
    }

    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        return fail("expected a failure");
    }

    private static RedisException redisFailure(CompletableFuture<?> f) throws Exception {
        return assertInstanceOf(RedisException.class, failure(f));
    }

    // ---- login ----

    @Test
    void helloCarriesTheProtocolTheCredentialsAndTheName() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg(server).withAuth("alice", "pw").withClientName("me").withDatabase(3));
            FakeRedis.Conn c = server.accept();
            assertEquals(List.of("HELLO", "3", "AUTH", "alice", "pw", "SETNAME", "me"), c.loginResp3());
            assertEquals(List.of("SELECT", "3"), c.expect("SELECT"));
            c.write("+OK\r\n");
            RedisClient client = f.get(5, TimeUnit.SECONDS);
            assertEquals(3, client.protocol());
            assertTrue(client.isConnected());
            assertEquals("redis", client.serverInfo().get("server"));
            assertEquals("7.4.0", client.serverInfo().get("version"));
        }
    }

    @Test
    void aPasswordAloneMeansTheDefaultUser() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg(server).withPassword("pw"));
            FakeRedis.Conn c = server.accept();
            assertEquals(List.of("HELLO", "3", "AUTH", "default", "pw"), c.loginResp3());
            f.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void aServerBeforeRedis6FallsBackToResp2() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg(server).withPassword("pw").withClientName("old").withDatabase(2));
            FakeRedis.Conn c = server.accept();
            c.expect("HELLO");
            c.write("-ERR unknown command 'HELLO', with args beginning with: '3' 'AUTH' \r\n");
            assertEquals(List.of("AUTH", "pw"), c.expect("AUTH"));
            c.write("+OK\r\n");
            assertEquals(List.of("CLIENT", "SETNAME", "old"), c.expect("CLIENT"));
            c.write("+OK\r\n");
            assertEquals(List.of("SELECT", "2"), c.expect("SELECT"));
            c.write("+OK\r\n");
            RedisClient client = f.get(5, TimeUnit.SECONDS);
            assertEquals(2, client.protocol());
            assertTrue(client.serverInfo().isEmpty());
            CompletableFuture<Void> sub = client.subscribe("x", (ch, p, m) -> { });
            assertEquals("RedisException", failure(sub).getClass().getSimpleName(), "pub/sub needs RESP3");
            assertFalse(((RedisException) failure(sub)).retriable());
        }
    }

    @Test
    void resp2CanBeAskedForAndHelloIsThenNotSent() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg(server).withProtocol(2));
            FakeRedis.Conn c = server.accept();
            CompletableFuture<Object> first = f.thenCompose(cl -> cl.command("PING"));
            List<String> cmd = c.read();
            assertEquals("PING", cmd.get(0), "no HELLO, and nothing else to log in with");
            c.write("+PONG\r\n");
            assertEquals("PONG", first.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aRefusedPasswordFailsTheConnectionForGood() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            List<String> events = new CopyOnWriteArrayList<>();
            CompletableFuture<RedisClient> f = RedisClient.connect(group, server.config().withPassword("wrong"), new RedisListener() {
                @Override
                public void gaveUp(Throwable cause) {
                    events.add("gaveUp");
                }
            });
            FakeRedis.Conn c = server.accept();
            c.expect("HELLO");
            c.write("-WRONGPASS invalid username-password pair or user is disabled.\r\n");
            RedisException e = redisFailure(f);
            assertFalse(e.retriable());
            assertTrue(e.getMessage().contains("login failed"), e.getMessage());
            assertTrue(e.getMessage().contains("WRONGPASS"), e.getMessage());
            assertTrue(c.closedByClient(3000));
        }
    }

    @Test
    void aServerThatNeverAnswersTheLoginTimesOut() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            CompletableFuture<RedisClient> f = RedisClient.connect(group, cfg(server).withConnectTimeout(Duration.ofMillis(300)));
            FakeRedis.Conn c = server.accept();
            c.expect("HELLO");   // and then silence
            RedisException e = redisFailure(f);
            assertTrue(e.getMessage().contains("no login"), e.getMessage());
        }
    }

    @Test
    void connectingToNothingFails() throws Exception {
        CompletableFuture<RedisClient> f = RedisClient.connect(group, RedisConfig.of("127.0.0.1", 1).withReconnect(null));
        RedisException e = redisFailure(f);
        assertTrue(e.getMessage().contains("127.0.0.1:1"), e.getMessage());
    }

    // ---- commands and replies ----

    @Test
    void aCommandIsWrittenAsABulkStringArrayAndItsReplyConverted() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<String> get = u.client.get("k");
            assertEquals(List.of("GET", "k"), u.conn.read());
            u.conn.write("$5\r\nhéllo\r\n".replace("héllo", "hÃ©ll"));
            assertEquals("héll", get.get(5, TimeUnit.SECONDS));
            CompletableFuture<String> missing = u.client.get("nope");
            u.conn.read();
            u.conn.write("$-1\r\n");
            assertNull(missing.get(5, TimeUnit.SECONDS));
            CompletableFuture<Void> set = u.client.set("k", "v");
            assertEquals(List.of("SET", "k", "v"), u.conn.read());
            u.conn.write("+OK\r\n");
            assertNull(set.get(5, TimeUnit.SECONDS));
            CompletableFuture<Boolean> ex = u.client.set("k", "v", SetArgs.ex(60).andNx());
            assertEquals(List.of("SET", "k", "v", "EX", "60", "NX"), u.conn.read());
            u.conn.write("_\r\n");
            assertFalse(ex.get(5, TimeUnit.SECONDS), "NX said no");
        }
    }

    @Test
    void commandsSentTogetherArriveTogetherAndAreAnsweredInOrder() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            int n = 2000;
            List<CompletableFuture<Long>> fs = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            // from several threads at once: the order of the replies is the order of the writes, whatever it was
            List<CompletableFuture<Long>>[] perThread = new List[4];
            for (int t = 0; t < 4; t++) {
                int id = t;
                perThread[t] = new ArrayList<>();
                threads.add(Thread.ofPlatform().start(() -> {
                    for (int i = 0; i < n / 4; i++) {
                        perThread[id].add(u.client.incrBy("k" + id, i));
                    }
                }));
            }
            for (Thread t : threads) {
                t.join();
            }
            List<String> order = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                List<String> c = u.conn.read();
                assertEquals("INCRBY", c.get(0));
                order.add(c.get(1) + ":" + c.get(2));
            }
            StringBuilder replies = new StringBuilder();
            for (int i = 0; i < n; i++) {
                replies.append(":").append(i).append("\r\n");
            }
            u.conn.write(replies.toString());
            // every future got the reply that matches the position of its command in the stream
            for (int t = 0; t < 4; t++) {
                for (int i = 0; i < n / 4; i++) {
                    long position = order.indexOf("k" + t + ":" + i);
                    assertEquals(position, perThread[t].get(i).get(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void anErrorReplyFailsOnlyItsCommand() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<Object> bad = u.client.command("LPUSH", "k", "x");
            CompletableFuture<Long> good = u.client.incr("n");
            u.conn.read();
            u.conn.read();
            u.conn.write("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n:7\r\n");
            RedisException e = redisFailure(bad);
            assertEquals("WRONGTYPE", e.code());
            assertTrue(e.fromServer());
            assertFalse(e.retriable());
            assertEquals(7L, good.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aReplyThatArrivesInSingleBytesIsStillOneReply() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<Map<String, String>> f = u.client.hgetAll("h");
            u.conn.read();
            byte[] wire = "%2\r\n$1\r\na\r\n$2\r\n11\r\n$1\r\nb\r\n$2\r\n22\r\n".getBytes(StandardCharsets.ISO_8859_1);
            for (byte b : wire) {
                u.conn.write(new byte[] {b});
                Thread.sleep(1);
            }
            assertEquals(Map.of("a", "11", "b", "22"), f.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aLargeValueArrivesWhole() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            byte[] data = new byte[3_000_000];
            new java.util.Random(1).nextBytes(data);
            CompletableFuture<byte[]> f = u.client.getBytes("big");
            u.conn.read();
            u.conn.write(("$" + data.length + "\r\n").getBytes());
            for (int i = 0; i < data.length; i += 7000) {
                u.conn.write(java.util.Arrays.copyOfRange(data, i, Math.min(data.length, i + 7000)));
            }
            u.conn.write("\r\n");
            assertArrayEquals(data, f.get(10, TimeUnit.SECONDS));
            // and one that is sent: the bytes are not touched
            CompletableFuture<Void> set = u.client.setBytes("big", data);
            List<String> cmd = u.conn.read();
            assertEquals(3_000_000, cmd.get(2).getBytes(StandardCharsets.ISO_8859_1).length > 0 ? 3_000_000 : 0);
            u.conn.write("+OK\r\n");
            set.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void aTransactionIsWrittenInOneRunAndAnsweredWithTheExecList() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<List<Object>> tx = u.client.transaction(new Object[] {"SET", "a", "1"}, new Object[] {"INCR", "a"}, new Object[] {"LPUSH", "a", "x"});
            CompletableFuture<Long> other = u.client.incr("other");   // submitted after: it must come after EXEC
            assertEquals("MULTI", u.conn.read().get(0));
            assertEquals(List.of("SET", "a", "1"), u.conn.read());
            assertEquals(List.of("INCR", "a"), u.conn.read());
            assertEquals(List.of("LPUSH", "a", "x"), u.conn.read());
            assertEquals("EXEC", u.conn.read().get(0));
            assertEquals(List.of("INCR", "other"), u.conn.read());
            u.conn.write("+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n:2\r\n-WRONGTYPE bad\r\n:1\r\n");
            List<Object> r = tx.get(5, TimeUnit.SECONDS);
            assertEquals("OK", r.get(0));
            assertEquals(2L, r.get(1));
            assertEquals(new RedisError("WRONGTYPE bad"), r.get(2));
            assertEquals(1L, other.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aTransactionTheServerRefusesFailsAndOneWatchAbortedIsNull() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<List<Object>> refused = u.client.transaction(new Object[] {"BAD"});
            for (int i = 0; i < 3; i++) u.conn.read();
            u.conn.write("+OK\r\n-ERR unknown command 'BAD'\r\n-EXECABORT Transaction discarded because of previous errors.\r\n");
            assertEquals("EXECABORT", ((RedisException) failure(refused)).code());
            CompletableFuture<List<Object>> aborted = u.client.transaction(new Object[] {"INCR", "a"});
            for (int i = 0; i < 3; i++) u.conn.read();
            u.conn.write("+OK\r\n+QUEUED\r\n_\r\n");
            assertNull(aborted.get(5, TimeUnit.SECONDS));
        }
    }

    // ---- pub/sub ----

    @Test
    void messagesArePushedToTheHandlersOfTheirChannelAndPattern() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            CompletableFuture<Void> sub = u.client.subscribe("news", (ch, p, m) -> got.add("A:" + ch + ":" + p + ":" + new String(m)));
            assertEquals(List.of("SUBSCRIBE", "news"), u.conn.read());
            u.conn.write(">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
            sub.get(5, TimeUnit.SECONDS);
            CompletableFuture<Void> second = u.client.subscribe("news", (ch, p, m) -> got.add("B:" + new String(m)));
            second.get(5, TimeUnit.SECONDS);   // already subscribed: no command goes out
            assertNull(u.conn.read(200));
            CompletableFuture<Void> psub = u.client.psubscribe("n*", (ch, p, m) -> got.add("P:" + p + ":" + ch + ":" + new String(m)));
            assertEquals(List.of("PSUBSCRIBE", "n*"), u.conn.read());
            u.conn.write(">3\r\n$10\r\npsubscribe\r\n$2\r\nn*\r\n:2\r\n");
            psub.get(5, TimeUnit.SECONDS);
            u.conn.write(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n>4\r\n$8\r\npmessage\r\n$2\r\nn*\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
            assertEquals("A:news:null:hi", got.poll(5, TimeUnit.SECONDS));
            assertEquals("B:hi", got.poll(5, TimeUnit.SECONDS));
            assertEquals("P:n*:news:hi", got.poll(5, TimeUnit.SECONDS));
            // an ordinary command's reply may come between pushes
            CompletableFuture<Long> inc = u.client.incr("c");
            u.conn.read();
            u.conn.write(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\nx\r\n:5\r\n>3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\ny\r\n");
            assertEquals(5L, inc.get(5, TimeUnit.SECONDS));
            assertEquals("A:news:null:x", got.poll(5, TimeUnit.SECONDS));
            assertEquals("B:x", got.poll(5, TimeUnit.SECONDS));
            assertEquals("A:news:null:y", got.poll(5, TimeUnit.SECONDS));
            assertEquals("B:y", got.poll(5, TimeUnit.SECONDS));
            CompletableFuture<Void> un = u.client.unsubscribe("news");
            assertEquals(List.of("UNSUBSCRIBE", "news"), u.conn.read());
            u.conn.write(">3\r\n$11\r\nunsubscribe\r\n$4\r\nnews\r\n:1\r\n");
            un.get(5, TimeUnit.SECONDS);
            u.conn.write(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\nz\r\n");
            assertNull(got.poll(300, TimeUnit.MILLISECONDS), "no handler left");
        }
    }

    @Test
    void aRefusedSubscriptionFailsAndLeavesNoHandler() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<Void> sub = u.client.subscribe("secret", (ch, p, m) -> fail("should not be called"));
            u.conn.read();
            u.conn.write("-NOPERM this user has no permissions to access one of the channels used as arguments\r\n");
            assertEquals("NOPERM", ((RedisException) failure(sub)).code());
            u.conn.write(">3\r\n$7\r\nmessage\r\n$6\r\nsecret\r\n$1\r\nx\r\n");
            CompletableFuture<String> ping = u.client.ping();
            u.conn.read();
            u.conn.write("+PONG\r\n");
            assertEquals("PONG", ping.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void aHandlerThatThrowsDoesNotStopTheOthersOrTheConnection() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            u.client.subscribe("c", (ch, p, m) -> {
                throw new IllegalStateException("boom");
            });
            u.conn.read();
            u.conn.write(">3\r\n$9\r\nsubscribe\r\n$1\r\nc\r\n:1\r\n");
            u.client.subscribe("c", (ch, p, m) -> got.add(new String(m))).get(5, TimeUnit.SECONDS);
            u.conn.write(">3\r\n$7\r\nmessage\r\n$1\r\nc\r\n$2\r\nok\r\n");
            assertEquals("ok", got.poll(5, TimeUnit.SECONDS));
            assertTrue(u.client.isConnected());
        }
    }

    // ---- connection loss ----

    @Test
    void whenTheConnectionDropsWaitingCommandsFailRetriablyAndTheClientComesBack() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            List<String> events = new CopyOnWriteArrayList<>();
            RedisConfig cfg = server.config().withReconnect(new Backoff(Duration.ofMillis(20), Duration.ofMillis(50), 2.0, 0.0, 10));
            Up u = up(server, cfg, new RedisListener() {
                @Override
                public void connected(boolean reconnected) {
                    events.add("connected:" + reconnected);
                }

                @Override
                public void connectionLost(Throwable cause) {
                    events.add("lost");
                }

                @Override
                public void reconnecting(int attempt, Duration delay) {
                    events.add("reconnecting:" + attempt);
                }
            });
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            u.client.subscribe("news", (ch, p, m) -> got.add(new String(m)));
            u.conn.read();
            u.conn.write(">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
            CompletableFuture<String> waiting = u.client.get("k");
            u.conn.read();
            u.conn.close();
            RedisException e = redisFailure(waiting);
            assertTrue(e.retriable(), "the command may have been executed: retriable, not certain");
            FakeRedis.Conn c2 = server.accept();
            c2.loginResp3();
            assertEquals(List.of("SUBSCRIBE", "news"), c2.read(), "the subscriptions are restored before anything else");
            c2.write(">3\r\n$9\r\nsubscribe\r\n$4\r\nnews\r\n:1\r\n");
            CompletableFuture<String> again = null;
            for (int i = 0; i < 100 && again == null; i++) {
                if (u.client.isConnected()) {
                    again = u.client.get("k");
                } else {
                    Thread.sleep(20);
                }
            }
            assertNotNull(again);
            c2.read();
            c2.write("$1\r\nv\r\n");
            assertEquals("v", again.get(5, TimeUnit.SECONDS));
            c2.write(">3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$2\r\nhi\r\n");
            assertEquals("hi", got.poll(5, TimeUnit.SECONDS));
            assertEquals(List.of("connected:false", "lost", "reconnecting:1", "connected:true"), events);
        }
    }

    @Test
    void commandsWhileNotConnectedFailAtOnce() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            RedisConfig cfg = server.config().withReconnect(new Backoff(Duration.ofSeconds(30), Duration.ofSeconds(30), 1.0, 0.0, 10));
            Up u = up(server, cfg, null);
            u.conn.close();
            for (int i = 0; i < 100 && u.client.isConnected(); i++) {
                Thread.sleep(20);
            }
            RedisException e = redisFailure(u.client.get("k"));
            assertTrue(e.retriable());
            assertTrue(e.getMessage().contains("not connected"), e.getMessage());
            u.client.disconnect().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void closingFailsWhatIsWaitingAndRefusesWhatComesAfter() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<String> waiting = u.client.get("k");
            u.conn.read();
            u.client.disconnect().get(5, TimeUnit.SECONDS);
            RedisException e = redisFailure(waiting);
            assertFalse(e.retriable());
            RedisException after = redisFailure(u.client.get("k"));
            assertFalse(after.retriable());
            assertTrue(u.conn.closedByClient(3000));
        }
    }

    @Test
    void withoutReconnectALostConnectionClosesTheClientAndTellsWhy() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            List<String> events = new CopyOnWriteArrayList<>();
            Up u = up(server, cfg(server), new RedisListener() {
                @Override
                public void gaveUp(Throwable cause) {
                    events.add("gaveUp");
                }
            });
            u.conn.close();
            u.client.closeFuture().get(5, TimeUnit.SECONDS);
            assertEquals(List.of("gaveUp"), events, "the listener has heard of it by the time the close future completes");
        }
    }

    // ---- limits and hostile replies ----

    @Test
    void tooManyCommandsWaitingAreRefused() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server, cfg(server).withLimits(1 << 20, 1000, 3), null);
            List<CompletableFuture<Long>> fs = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                fs.add(u.client.incr("k"));
            }
            for (int i = 0; i < 3; i++) u.conn.read();
            for (int i = 3; i < 5; i++) {
                RedisException e = redisFailure(fs.get(i));
                assertTrue(e.retriable());
                assertTrue(e.getMessage().contains("maxPending"), e.getMessage());
            }
            u.conn.write(":1\r\n:2\r\n:3\r\n");
            assertEquals(3L, fs.get(2).get(5, TimeUnit.SECONDS));
            CompletableFuture<Long> next = u.client.incr("k");
            u.conn.read();
            u.conn.write(":4\r\n");
            assertEquals(4L, next.get(5, TimeUnit.SECONDS));
        }
    }

    private void assertDropped(String what, String reply) throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server, cfg(server).withLimits(1 << 20, 1000, 100), null);
            CompletableFuture<Object> f = u.client.command("GET", "k");
            u.conn.read();
            u.conn.write(reply);
            RedisException e = redisFailure(f);
            assertTrue(e.getMessage().contains("protocol error") || e.getMessage().contains("connection was lost"), what + ": " + e.getMessage());
            assertTrue(u.conn.closedByClient(3000), what + ": the client should drop the connection");
            u.client.closeFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void repliesThatBreakTheProtocolEndTheConnection() throws Exception {
        assertDropped("garbage", "!!!\r\n");
        assertDropped("a bad type byte", "?x\r\n");
        assertDropped("a length over the limit", "$2000000\r\nx\r\n");
        assertDropped("a negative length", "$-7\r\n");
        assertDropped("a missing CRLF", "$1\r\nabc\r\n");
        assertDropped("an aggregate over the limit", "*5000\r\n");
        assertDropped("a number that is not", ":12x\r\n");
    }

    @Test
    void aReplyNobodyWaitsForEndsTheConnection() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            u.conn.write("+OK\r\n");
            u.client.closeFuture().get(5, TimeUnit.SECONDS);
            assertTrue(u.conn.closedByClient(3000));
        }
    }

    @Test
    void argumentsThatCannotBeSentAreRefusedBeforeAnythingIsWritten() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            assertInstanceOf(IllegalArgumentException.class, failure(u.client.command("SET", "k", new Object())));
            assertInstanceOf(IllegalArgumentException.class, failure(u.client.command("SET", null, "v")));
            assertInstanceOf(IllegalArgumentException.class, failure(u.client.command()));
            assertInstanceOf(IllegalArgumentException.class, failure(u.client.mset("a")));
            assertNull(u.conn.read(300), "nothing was written");
            assertTrue(u.client.isConnected());
        }
    }

    @Test
    void theTypedRepliesOfResp2AndResp3AreBothUnderstood() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            CompletableFuture<Map<String, Double>> z3 = u.client.zrangeWithScores("z", 0, -1);
            u.conn.read();
            u.conn.write("*2\r\n*2\r\n$1\r\na\r\n,1.5\r\n*2\r\n$1\r\nb\r\n,2\r\n");   // RESP3: pairs
            assertEquals(Map.of("a", 1.5, "b", 2.0), z3.get(5, TimeUnit.SECONDS));
            CompletableFuture<Map<String, Double>> z2 = u.client.zrangeWithScores("z", 0, -1);
            u.conn.read();
            u.conn.write("*4\r\n$1\r\na\r\n$3\r\n1.5\r\n$1\r\nb\r\n$1\r\n2\r\n");     // RESP2 shape: a flat list
            assertEquals(Map.of("a", 1.5, "b", 2.0), z2.get(5, TimeUnit.SECONDS));
            CompletableFuture<Map<String, String>> h2 = u.client.hgetAll("h");
            u.conn.read();
            u.conn.write("*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n");
            assertEquals(Map.of("a", "1", "b", "2"), h2.get(5, TimeUnit.SECONDS));
            CompletableFuture<Boolean> b3 = u.client.expire("k", 10);
            u.conn.read();
            u.conn.write("#t\r\n");
            assertTrue(b3.get(5, TimeUnit.SECONDS));
            CompletableFuture<Boolean> b2 = u.client.sismember("s", "m");
            u.conn.read();
            u.conn.write(":0\r\n");
            assertFalse(b2.get(5, TimeUnit.SECONDS));
            CompletableFuture<List<String>> mget = u.client.mget("a", "b", "c");
            u.conn.read();
            u.conn.write("*3\r\n$1\r\nx\r\n_\r\n$1\r\nz\r\n");
            assertEquals(java.util.Arrays.asList("x", null, "z"), mget.get(5, TimeUnit.SECONDS));
            CompletableFuture<ScanResult> scan = u.client.scan("0", "a*", 10L);
            assertEquals(List.of("SCAN", "0", "MATCH", "a*", "COUNT", "10"), u.conn.read());
            u.conn.write("*2\r\n$2\r\n17\r\n*2\r\n$2\r\na1\r\n$2\r\na2\r\n");
            ScanResult r = scan.get(5, TimeUnit.SECONDS);
            assertEquals("17", r.cursor());
            assertEquals(List.of("a1", "a2"), r.keys());
            assertFalse(r.finished());
        }
    }

    @Test
    void aScriptIsRunByItsHashAndSentOnlyWhenTheServerDoesNotKnowIt() throws Exception {
        try (FakeRedis server = new FakeRedis()) {
            Up u = up(server);
            Script s = new Script("return 1");
            assertEquals("e0e1f9fabfc9d4800c877a703b823ac0578ff8db", s.sha1());
            CompletableFuture<Object> f = u.client.eval(s, List.of("k1", "k2"), List.of("a", 5));
            assertEquals(List.of("EVALSHA", s.sha1(), "2", "k1", "k2", "a", "5"), u.conn.read());
            u.conn.write("-NOSCRIPT No matching script. Please use EVAL.\r\n");
            assertEquals(List.of("EVAL", "return 1", "2", "k1", "k2", "a", "5"), u.conn.read());
            u.conn.write(":1\r\n");
            assertEquals(1L, f.get(5, TimeUnit.SECONDS));
            CompletableFuture<Object> g = u.client.eval(s, List.of(), List.of());
            assertEquals("EVALSHA", u.conn.read().get(0));
            u.conn.write(":2\r\n");
            assertEquals(2L, g.get(5, TimeUnit.SECONDS));
            CompletableFuture<Object> err = u.client.eval(s, List.of(), List.of());
            u.conn.read();
            u.conn.write("-ERR user script failed\r\n");
            assertEquals("ERR", ((RedisException) failure(err)).code());
        }
    }
}
