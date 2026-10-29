package io.hydrangea.redis;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The client against a real Redis (see {@code start-redis.sh}). Skipped when none listens. */
class RedisClientTest {

    private static EventLoopGroup group;
    private final List<RedisClient> clients = new ArrayList<>();

    @BeforeAll
    static void start() {
        Redis.require(Redis.PORT);
        group = new EventLoopGroup(2);
    }

    @AfterAll
    static void stop() throws Exception {
        if (group != null) {
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    void closeAll() {
        clients.forEach(RedisClient::close);
    }

    private RedisClient connect(RedisConfig cfg) throws Exception {
        return connect(cfg, null);
    }

    private RedisClient connect(RedisConfig cfg, RedisListener l) throws Exception {
        RedisClient c = RedisClient.connect(group, cfg, l).get(10, TimeUnit.SECONDS);
        clients.add(c);
        return c;
    }

    private RedisClient connect() throws Exception {
        return connect(Redis.config());
    }

    private static <T> T get(CompletableFuture<T> f) throws Exception {
        return f.get(20, TimeUnit.SECONDS);
    }

    private static RedisException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(20, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return assertInstanceOf(RedisException.class, e.getCause());
        }
        return fail("expected a failure");
    }

    /** The tests of the ordinary commands run twice: with RESP3 and with RESP2, whose replies have other shapes. */
    private static final int[] PROTOCOLS = {3, 2};

    private interface Body {
        void run(RedisClient c) throws Exception;
    }

    private void bothProtocols(Body body) throws Exception {
        for (int p : PROTOCOLS) {
            RedisClient c = connect(Redis.config().withProtocol(p));
            assertEquals(p, c.protocol());
            try {
                body.run(c);
            } catch (Throwable t) {
                throw new AssertionError("with RESP" + p + ": " + t, t);
            }
        }
    }

    // ---- connection ----

    @Test
    void connectingNegotiatesResp3AndKnowsTheServer() throws Exception {
        RedisClient c = connect(Redis.config().withClientName("hydrangea-test"));
        assertEquals(3, c.protocol());
        assertEquals("redis", c.serverInfo().get("server"));
        assertTrue(String.valueOf(c.serverInfo().get("version")).startsWith("7"));
        assertEquals("PONG", get(c.ping()));
        assertEquals("hi", get(c.echo("hi")));
        assertTrue(String.valueOf(get(c.command("CLIENT", "LIST")) instanceof byte[] b ? new String(b) : "").contains("name=hydrangea-test"));
        assertTrue(c.isConnected());
        assertTrue(c.isWritable());
        c.disconnect().get(5, TimeUnit.SECONDS);
        assertFalse(c.isConnected());
    }

    @Test
    void aDatabaseOfItsOwn() throws Exception {
        RedisClient zero = connect();
        RedisClient five = connect(Redis.config().withDatabase(5));
        String k = Redis.key("db");
        get(five.set(k, "in five"));
        assertNull(get(zero.get(k)));
        assertEquals("in five", get(five.get(k)));
        assertEquals(1L, get(five.del(k)));
    }

    @Test
    void duplicateIsAnotherConnectionWithTheSameSettings() throws Exception {
        RedisClient a = connect(Redis.config().withDatabase(4));
        RedisClient b = a.duplicate().get(10, TimeUnit.SECONDS);
        clients.add(b);
        String k = Redis.key("dup");
        get(a.set(k, "x"));
        assertEquals("x", get(b.get(k)), "the same database");
        assertNotEquals(get(a.command("CLIENT", "ID")), get(b.command("CLIENT", "ID")));
        get(a.del(k));
    }

    // ---- strings, with both protocols ----

    @Test
    void stringsAndNumbers() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("s");
            assertNull(get(c.get(k)));
            get(c.set(k, "héllo ✓ 😀"));
            assertEquals("héllo ✓ 😀", get(c.get(k)));
            assertEquals(1L, get(c.exists(k)));
            assertEquals("string", get(c.type(k)));
            assertEquals(15L, get(c.strlen(k)));
            assertEquals(21L, get(c.append(k, "xxxxxx")));
            assertEquals(1L, get(c.del(k)));
            assertEquals(0L, get(c.del(k)));
            String n = Redis.key("n");
            assertEquals(1L, get(c.incr(n)));
            assertEquals(11L, get(c.incrBy(n, 10)));
            assertEquals(10L, get(c.decr(n)));
            assertEquals(7L, get(c.decrBy(n, 3)));
            assertEquals(8.5, get(c.incrByFloat(n, 1.5)));
            assertEquals("8.5", get(c.get(n)));
            get(c.del(n));
        });
    }

    @Test
    void setOptions() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("opt");
            assertTrue(get(c.set(k, "a", SetArgs.nx())));
            assertFalse(get(c.set(k, "b", SetArgs.nx())), "NX on an existing key");
            assertEquals("a", get(c.get(k)));
            assertTrue(get(c.set(k, "c", SetArgs.xx())));
            assertFalse(get(c.set(Redis.key("absent"), "d", SetArgs.xx())));
            assertTrue(get(c.set(k, "e", SetArgs.ex(100))));
            long ttl = get(c.ttl(k));
            assertTrue(ttl > 90 && ttl <= 100, "ttl " + ttl);
            assertEquals("e", get(c.setGet(k, "f", SetArgs.NONE)), "the old value");
            assertEquals(-1L, get(c.ttl(k)), "a SET without options drops the expiry");
            assertTrue(get(c.set(k, "f2", SetArgs.ex(100))));
            assertTrue(get(c.set(k, "g", SetArgs.NONE.andKeepTtl())));
            assertTrue(get(c.ttl(k)) > 0, "KEEPTTL kept the expiry");
            assertTrue(get(c.set(k, "h", SetArgs.px(50_000))));
            assertTrue(get(c.pttl(k)) > 40_000);
            assertTrue(get(c.persist(k)));
            assertEquals(-1L, get(c.ttl(k)));
            assertTrue(get(c.expire(k, 5)));
            assertTrue(get(c.pexpire(k, 3000)));
            assertEquals(-2L, get(c.ttl(Redis.key("gone"))));
            assertEquals("h", get(c.getDel(k)));
            assertNull(get(c.get(k)));
            assertThrows(IllegalArgumentException.class, () -> SetArgs.nx().andXx());
            assertThrows(IllegalArgumentException.class, () -> new SetArgs(1L, 2L, false, false, false, false));
        });
    }

    @Test
    void keysAndValuesAreBinarySafe() throws Exception {
        RedisClient c = connect();
        byte[] all = new byte[256];
        for (int i = 0; i < 256; i++) {
            all[i] = (byte) i;
        }
        String k = Redis.key("bin");
        get(c.setBytes(k, all));
        assertArrayEquals(all, get(c.getBytes(k)));
        get(c.command("SET", Redis.key("raw").getBytes(StandardCharsets.UTF_8), all));
        String weird = Redis.key("k y\r\n with\0 odd ✓ chars");
        get(c.set(weird, "value"));
        assertEquals("value", get(c.get(weird)));
        get(c.set(k, ""));
        assertEquals("", get(c.get(k)));
        get(c.del(k, weird));
    }

    @Test
    void manyKeysAtOnce() throws Exception {
        bothProtocols(c -> {
            String p = Redis.key("m");
            get(c.mset(p + "1", "a", p + "2", "b", p + "3", "c"));
            assertEquals(Arrays.asList("a", "b", null, "c"), get(c.mget(p + "1", p + "2", p + "x", p + "3")));
            assertEquals(3L, get(c.exists(p + "1", p + "2", p + "3")));
            assertEquals(3L, get(c.unlink(p + "1", p + "2", p + "3")));
            get(c.del(p));
        });
    }

    @Test
    void renameTypeAndScan() throws Exception {
        bothProtocols(c -> {
            String p = Redis.key("scan");
            for (int i = 0; i < 250; i++) {
                get(c.set(p + ":" + i, "v"));
            }
            List<String> found = new ArrayList<>();
            String cursor = "0";
            do {
                ScanResult page = get(c.scan(cursor, p + ":*", 100L));
                found.addAll(page.keys());
                cursor = page.cursor();
            } while (!"0".equals(cursor));
            assertEquals(250, found.stream().distinct().count());
            get(c.rename(p + ":0", p + "-renamed"));
            assertEquals("string", get(c.type(p + "-renamed")));
            assertEquals("none", get(c.type(p + ":0")));
            assertEquals("ERR", failure(c.rename(p + ":nope", "x")).code());
            for (String k : found) {
                get(c.del(k));
            }
            get(c.del(p + "-renamed"));
            assertTrue(get(c.dbSize()) >= 0);
            assertTrue(get(c.info("server")).contains("redis_version"));
        });
    }

    // ---- hashes, lists, sets, sorted sets ----

    @Test
    void hashes() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("h");
            assertEquals(1L, get(c.hset(k, "a", "1")));
            Map<String, String> more = new LinkedHashMap<>();
            more.put("b", "2");
            more.put("c", "héllo");
            assertEquals(2L, get(c.hset(k, more)));
            assertEquals("1", get(c.hget(k, "a")));
            assertNull(get(c.hget(k, "zz")));
            assertEquals(Map.of("a", "1", "b", "2", "c", "héllo"), get(c.hgetAll(k)));
            assertEquals(Arrays.asList("1", null, "2"), get(c.hmget(k, "a", "zz", "b")));
            assertEquals(3L, get(c.hlen(k)));
            assertTrue(get(c.hexists(k, "a")));
            assertFalse(get(c.hexists(k, "zz")));
            assertEquals(11L, get(c.hincrBy(k, "a", 10)));
            assertEquals(List.of("a", "b", "c"), get(c.hkeys(k)));
            assertEquals(List.of("11", "2", "héllo"), get(c.hvals(k)));
            assertEquals(2L, get(c.hdel(k, "a", "b", "nope")));
            assertEquals(Map.of("c", "héllo"), get(c.hgetAll(k)));
            assertEquals(Map.of(), get(c.hgetAll(Redis.key("nothing"))));
            get(c.del(k));
        });
    }

    @Test
    void lists() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("l");
            assertEquals(3L, get(c.rpush(k, "a", "b", "c")));
            assertEquals(4L, get(c.lpush(k, "z")));
            assertEquals(List.of("z", "a", "b", "c"), get(c.lrange(k, 0, -1)));
            assertEquals(List.of("a", "b"), get(c.lrange(k, 1, 2)));
            assertEquals("b", get(c.lindex(k, 2)));
            assertEquals(4L, get(c.llen(k)));
            assertEquals("z", get(c.lpop(k)));
            assertEquals("c", get(c.rpop(k)));
            get(c.ltrim(k, 0, 0));
            assertEquals(List.of("a"), get(c.lrange(k, 0, -1)));
            assertEquals("a", get(c.rpop(k)));
            assertNull(get(c.lpop(k)));
            assertEquals(List.of(), get(c.lrange(k, 0, -1)));
        });
    }

    @Test
    void sets() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("set");
            assertEquals(3L, get(c.sadd(k, "a", "b", "c")));
            assertEquals(1L, get(c.sadd(k, "c", "d")));
            assertEquals(4L, get(c.scard(k)));
            assertTrue(get(c.sismember(k, "a")));
            assertFalse(get(c.sismember(k, "x")));
            assertEquals(List.of("a", "b", "c", "d"), get(c.smembers(k)).stream().sorted().toList());
            assertEquals(2L, get(c.srem(k, "a", "b", "x")));
            assertNotNull(get(c.spop(k)));
            get(c.del(k));
        });
    }

    @Test
    void sortedSets() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("z");
            assertEquals(1L, get(c.zadd(k, 1.5, "a")));
            assertEquals(1L, get(c.zadd(k, 3, "c")));
            assertEquals(1L, get(c.zadd(k, 2, "b")));
            assertEquals(0L, get(c.zadd(k, 2.5, "b")), "an update");
            assertEquals(List.of("a", "b", "c"), get(c.zrange(k, 0, -1)));
            Map<String, Double> scored = get(c.zrangeWithScores(k, 0, -1));
            assertEquals(List.of("a", "b", "c"), List.copyOf(scored.keySet()));
            assertEquals(List.of(1.5, 2.5, 3.0), List.copyOf(scored.values()));
            assertEquals(2.5, get(c.zscore(k, "b")));
            assertNull(get(c.zscore(k, "x")));
            assertEquals(1L, get(c.zrank(k, "b")));
            assertNull(get(c.zrank(k, "x")));
            assertEquals(4.5, get(c.zincrBy(k, 2, "b")));
            assertEquals(3L, get(c.zcard(k)));
            assertEquals(2L, get(c.zrem(k, "a", "c", "x")));
            get(c.del(k));
        });
    }

    @Test
    void errorsAreTheServersErrors() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("wrong");
            get(c.set(k, "string"));
            RedisException e = failure(c.lpush(k, "x"));
            assertEquals("WRONGTYPE", e.code());
            assertTrue(e.fromServer());
            assertFalse(e.retriable());
            assertEquals("ERR", failure(c.command("NOSUCHCOMMAND")).code());
            assertEquals("ERR", failure(c.incr(k)).code(), "not an integer");
            assertEquals("string", get(c.get(k)), "the connection is fine");
            get(c.del(k));
        });
    }

    // ---- pipelining and sharing ----

    @Test
    void aHundredThousandCommandsInFlightAreAnsweredInOrder() throws Exception {
        RedisClient c = connect();
        String k = Redis.key("pipe");
        int n = 100_000;
        List<CompletableFuture<Long>> fs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            fs.add(c.incr(k));
        }
        for (int i = 0; i < n; i++) {
            assertEquals(i + 1L, get(fs.get(i)));
        }
        assertEquals(Integer.toString(n), get(c.get(k)));
        get(c.del(k));
    }

    @Test
    void manyThreadsShareOneConnection() throws Exception {
        RedisClient c = connect();
        String p = Redis.key("share");
        int threads = 16;
        int each = 2000;
        List<Thread> ts = new ArrayList<>();
        List<Throwable> errors = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int t = 0; t < threads; t++) {
            int id = t;
            ts.add(Thread.ofVirtual().start(() -> {
                try {
                    for (int i = 0; i < each; i++) {
                        String key = p + ":" + id;
                        get(c.set(key, "v" + i));
                        String got = get(c.get(key));
                        if (!("v" + i).equals(got)) {
                            errors.add(new AssertionError("thread " + id + " step " + i + " read " + got));
                        }
                    }
                } catch (Throwable e) {
                    errors.add(e);
                }
            }));
        }
        for (Thread t : ts) {
            t.join();
        }
        assertEquals(List.of(), errors);
        for (int t = 0; t < threads; t++) {
            get(c.del(p + ":" + t));
        }
    }

    @Test
    void valuesOfTensOfMegabytesGoBothWays() throws Exception {
        RedisClient c = connect();
        byte[] big = new byte[64 * 1024 * 1024];
        new java.util.Random(5).nextBytes(big);
        String k = Redis.key("big");
        get(c.setBytes(k, big));
        assertArrayEquals(big, get(c.getBytes(k)));
        assertEquals((long) big.length, get(c.strlen(k)));
        get(c.del(k));
        assertEquals("PONG", get(c.ping()), "nothing was left behind in the connection");
    }

    @Test
    void aBigReplyOfManyElements() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("many");
            String[] values = new String[200_000];
            for (int i = 0; i < values.length; i++) {
                values[i] = "value-" + i;
            }
            for (int i = 0; i < values.length; i += 10_000) {
                get(c.rpush(k, Arrays.copyOfRange(values, i, i + 10_000)));
            }
            List<String> back = get(c.lrange(k, 0, -1));
            assertEquals(values.length, back.size());
            assertEquals("value-199999", back.get(199_999));
            get(c.del(k));
        });
    }

    // ---- transactions and scripts ----

    @Test
    void aTransactionRunsAllItsCommandsTogether() throws Exception {
        bothProtocols(c -> {
            String k = Redis.key("tx");
            List<CompletableFuture<Long>> others = new ArrayList<>();
            CompletableFuture<List<Object>> tx = c.transaction(new Object[] {"SET", k, "1"}, new Object[] {"INCR", k}, new Object[] {"INCR", k}, new Object[] {"GET", k});
            for (int i = 0; i < 100; i++) {
                others.add(c.incr(Redis.key("other")));
            }
            List<Object> r = get(tx);
            assertEquals("OK", r.get(0));
            assertEquals(2L, r.get(1));
            assertEquals(3L, r.get(2));
            assertArrayEquals("3".getBytes(), (byte[]) r.get(3));
            for (CompletableFuture<Long> f : others) {
                assertEquals(1L, get(f));
            }
            // one command of the transaction fails, the others do not
            get(c.set(k, "text"));
            List<Object> mixed = get(c.transaction(new Object[] {"INCR", k}, new Object[] {"SET", k, "fine"}));
            assertInstanceOf(RedisError.class, mixed.get(0));
            assertEquals("OK", mixed.get(1));
            // a command the server refuses while queuing: nothing runs
            RedisException refused = failure(c.transaction(new Object[] {"SET", k, "never"}, new Object[] {"NOSUCH"}));
            assertEquals("EXECABORT", refused.code());
            assertEquals("fine", get(c.get(k)));
            get(c.del(k));
        });
    }

    @Test
    void watchOnAConnectionOfItsOwnMakesATransactionConditional() throws Exception {
        RedisClient shared = connect();
        RedisClient mine = connect();
        String k = Redis.key("watched");
        get(shared.set(k, "1"));
        get(mine.command("WATCH", k));
        get(shared.set(k, "changed meanwhile"));
        List<Object> r = get(mine.transaction(new Object[] {"SET", k, "mine"}));
        assertNull(r, "the key changed: EXEC did nothing");
        assertEquals("changed meanwhile", get(shared.get(k)));
        get(mine.command("WATCH", k));
        assertEquals(List.of("OK"), get(mine.transaction(new Object[] {"SET", k, "mine"})));
        assertEquals("mine", get(shared.get(k)));
        get(shared.del(k));
    }

    @Test
    void scriptsRunByHashAndLoadThemselves() throws Exception {
        RedisClient c = connect();
        Script s = new Script("redis.call('set', KEYS[1], ARGV[1]); return {KEYS[1], ARGV[1], tonumber(ARGV[2]) + 1}");
        get(c.command("SCRIPT", "FLUSH"));
        String k = Redis.key("script");
        @SuppressWarnings("unchecked")
        List<Object> r = (List<Object>) get(c.eval(s, List.of(k), List.of("v", 41)));
        assertArrayEquals(k.getBytes(), (byte[]) r.get(0));
        assertArrayEquals("v".getBytes(), (byte[]) r.get(1));
        assertEquals(42L, r.get(2));
        assertEquals("v", get(c.get(k)));
        get(c.eval(s, List.of(k), List.of("w", 1)));   // by hash this time
        assertEquals("w", get(c.get(k)));
        assertEquals(List.of(1L), get(c.command("SCRIPT", "EXISTS", s.sha1())));
        assertEquals("ERR", failure(c.eval(new Script("return redis.call('nosuch')"), List.of(), List.of())).code());
        get(c.del(k));
    }

    // ---- pub/sub ----

    @Test
    void publishAndSubscribe() throws Exception {
        RedisClient pub = connect();
        RedisClient sub = connect();
        String ch = Redis.key("chan");
        BlockingQueue<String> got = new LinkedBlockingQueue<>();
        get(sub.subscribe(ch, (channel, pattern, payload) -> got.add(channel.equals(ch) + ":" + pattern + ":" + new String(payload, StandardCharsets.UTF_8))));
        assertEquals(1L, get(pub.publish(ch, "héllo")));
        assertEquals("true:null:héllo", got.poll(5, TimeUnit.SECONDS));
        for (int i = 0; i < 1000; i++) {
            pub.publish(ch, "m" + i);
        }
        for (int i = 0; i < 1000; i++) {
            assertEquals("true:null:m" + i, got.poll(10, TimeUnit.SECONDS), "in order");
        }
        assertEquals(1L, get(pub.publish(ch, new byte[] {0, 1, 2, (byte) 255})) );
        assertNotNull(got.poll(5, TimeUnit.SECONDS));
        // the subscribing connection can still run commands (RESP3)
        assertEquals("PONG", get(sub.ping()));
        get(sub.unsubscribe(ch));
        assertEquals(0L, get(pub.publish(ch, "nobody")));
    }

    @Test
    void patternsAndSeveralHandlers() throws Exception {
        RedisClient pub = connect();
        RedisClient sub = connect();
        String base = Redis.key("pat");
        BlockingQueue<String> got = new LinkedBlockingQueue<>();
        get(sub.psubscribe(base + ".*", (channel, pattern, payload) -> got.add("P:" + channel.substring(base.length()) + ":" + pattern.substring(base.length()))));
        get(sub.subscribe(base + ".a", (channel, pattern, payload) -> got.add("A")));
        get(sub.subscribe(base + ".a", (channel, pattern, payload) -> got.add("A2")));
        assertEquals(2L, get(pub.publish(base + ".a", "x")), "one subscription by pattern, one by channel");
        assertEquals(List.of("A", "A2", "P:.a:.*"), List.of(got.poll(5, TimeUnit.SECONDS), got.poll(5, TimeUnit.SECONDS), got.poll(5, TimeUnit.SECONDS)), "channel handlers first, then the pattern's");
        get(sub.punsubscribe(base + ".*"));
        assertEquals(0L, get(pub.publish(base + ".b", "y")), "no pattern left: nobody gets it");
        assertEquals(1L, get(pub.publish(base + ".a", "z")));
        get(sub.unsubscribe(base + ".a"));
    }

    @Test
    void subscriptionsComeBackAfterTheConnectionIsKilled() throws Exception {
        List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();
        RedisClient sub = connect(Redis.config().withReconnect(new io.hydrangea.Backoff(Duration.ofMillis(20), Duration.ofMillis(100), 2.0, 0.0, 20)), new RedisListener() {
            @Override
            public void connected(boolean reconnected) {
                events.add("connected:" + reconnected);
            }

            @Override
            public void connectionLost(Throwable cause) {
                events.add("lost");
            }
        });
        RedisClient admin = connect();
        RedisClient pub = connect();
        String ch = Redis.key("kill");
        BlockingQueue<String> got = new LinkedBlockingQueue<>();
        get(sub.subscribe(ch, (c, p, m) -> got.add(new String(m))));
        get(sub.psubscribe(ch + "*", (c, p, m) -> got.add("p" + new String(m))));
        Object id = get(sub.command("CLIENT", "ID"));
        assertEquals(1L, get(admin.command("CLIENT", "KILL", "ID", id)));
        for (int i = 0; i < 200 && !events.contains("connected:true"); i++) {
            Thread.sleep(25);
        }
        assertEquals(List.of("connected:false", "lost", "connected:true"), events);
        assertEquals("PONG", get(sub.ping()));
        long delivered = 0;
        for (int i = 0; i < 100 && delivered < 2; i++) {
            delivered = get(pub.publish(ch, "back"));
            if (delivered < 2) {
                Thread.sleep(20);
            }
        }
        assertEquals(2L, delivered, "both subscriptions are back");
        assertEquals(List.of("back", "pback"), List.of(got.poll(5, TimeUnit.SECONDS), got.poll(5, TimeUnit.SECONDS)));
    }

    @Test
    void commandsFailRetriablyWhenTheConnectionIsKilled() throws Exception {
        RedisClient victim = connect(Redis.config().withReconnect(null));
        RedisClient admin = connect();
        Object id = get(victim.command("CLIENT", "ID"));
        CompletableFuture<Object> blocked = victim.command("BLPOP", Redis.key("empty"), 30);
        Thread.sleep(200);
        get(admin.command("CLIENT", "KILL", "ID", id));
        RedisException e = failure(blocked);
        assertTrue(e.retriable());
        victim.closeFuture().get(5, TimeUnit.SECONDS);
        assertFalse(victim.isConnected());
    }

    @Test
    void aBlockingCommandBelongsOnAConnectionOfItsOwn() throws Exception {
        RedisClient shared = connect();
        RedisClient waiter = shared.duplicate().get(10, TimeUnit.SECONDS);
        clients.add(waiter);
        String list = Redis.key("queue");
        CompletableFuture<Object> pop = waiter.command("BLPOP", list, 10);
        Thread.sleep(100);
        long t0 = System.nanoTime();
        assertEquals("PONG", get(shared.ping()), "the shared connection is not held up");
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) < 1000);
        get(shared.rpush(list, "item"));
        @SuppressWarnings("unchecked")
        List<Object> r = (List<Object>) get(pop);
        assertArrayEquals(list.getBytes(), (byte[]) r.get(0));
        assertArrayEquals("item".getBytes(), (byte[]) r.get(1));
    }

    // ---- security ----

    @Test
    void passwordAndAclUsers() throws Exception {
        Redis.require(Redis.AUTH_PORT);
        RedisConfig base = RedisConfig.of(Redis.HOST, Redis.AUTH_PORT).withReconnect(null);
        RedisException none = failure(RedisClient.connect(group, base));
        assertEquals("NOAUTH", none.code(), "no password");
        RedisException wrong = failure(RedisClient.connect(group, base.withPassword("wrong")));
        assertEquals("WRONGPASS", wrong.code());
        assertFalse(wrong.retriable());
        RedisClient admin = connect(base.withPassword("secret"));
        assertEquals("PONG", get(admin.ping()));
        String key = "allowed:" + Redis.key("a");
        get(admin.set(key, "x"));
        RedisClient alice = connect(base.withAuth("alice", "alicepw"));
        assertEquals("x", get(alice.get(key)));
        assertEquals("NOPERM", failure(alice.get("forbidden:key")).code(), "a key outside her pattern");
        assertEquals("NOPERM", failure(alice.del(key)).code(), "a command she may not run");
        assertEquals("PONG", get(alice.ping()), "refusals do not hurt the connection");
        assertEquals("WRONGPASS", failure(RedisClient.connect(group, base.withAuth("alice", "nope"))).code());
        // RESP2 logs in with AUTH
        RedisClient resp2 = connect(base.withAuth("alice", "alicepw").withProtocol(2));
        assertEquals("x", get(resp2.get(key)));
        get(admin.del(key));
    }

    @Test
    void tlsWithTheRightCaAndNotWithAnother() throws Exception {
        Redis.require(Redis.TLS_PORT);
        RedisConfig tls = RedisConfig.of(Redis.HOST, Redis.TLS_PORT).withReconnect(null);
        RedisClient c = connect(tls.withTls(Redis.trusting("/redis/ca.pem")));
        assertEquals("PONG", get(c.ping()));
        String big = "x".repeat(2_000_000);
        String k = Redis.key("tls");
        get(c.set(k, big));
        assertEquals(big, get(c.get(k)));
        get(c.del(k));
        failure(RedisClient.connect(group, tls.withTls(Redis.trusting("/redis/wrongname.pem"))));   // a certificate the server's is not signed by
        failure(RedisClient.connect(group, tls.withTls(javax.net.ssl.SSLContext.getDefault())));      // the JVM does not trust the test CA
        failure(RedisClient.connect(group, tls));                                                    // plain text to a TLS port
    }

    @Test
    void theLimitsOnWhatIsAcceptedFromTheServer() throws Exception {
        RedisClient c = connect(Redis.config().withLimits(2048, 1000, 1000).withReconnect(null));
        String k = Redis.key("limit");
        get(c.set(k, "x".repeat(5000)));
        RedisException e = failure(c.get(k));
        assertTrue(e.getMessage().contains("protocol error") || e.getMessage().contains("lost"), e.getMessage());
        c.closeFuture().get(5, TimeUnit.SECONDS);
        RedisClient admin = connect();
        get(admin.del(k));
    }
}
