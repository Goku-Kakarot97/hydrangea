package io.hydrangea.kafka;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The client against a real broker (see {@code start-kafka.sh}). Skipped when none listens. */
class KafkaClientTest {

    private static EventLoopGroup group;
    private static KafkaClient kafka;

    @BeforeAll
    static void start() {
        Kafka.require(Kafka.PORT);
        group = new EventLoopGroup(2);
        kafka = KafkaClient.create(group, Kafka.config());
    }

    @AfterAll
    static void stop() throws Exception {
        if (kafka != null) {
            kafka.close();
            group.shutdown().get(5, TimeUnit.SECONDS);
        }
    }

    private static <T> T get(CompletableFuture<T> f) throws Exception {
        return f.get(30, TimeUnit.SECONDS);
    }

    private static KafkaException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return assertInstanceOf(KafkaException.class, e.getCause());
        }
        return fail("expected a failure");
    }

    private static String newTopic(String prefix, int partitions) throws Exception {
        String t = Kafka.topic(prefix);
        get(kafka.createTopic(t, partitions, 1));
        return t;
    }

    /** Reads until {@code n} records have arrived (or 20 s). */
    private static List<ConsumerRecord> readN(KafkaConsumer c, int n) throws Exception {
        List<ConsumerRecord> all = new ArrayList<>();
        long end = System.currentTimeMillis() + 20_000;
        while (all.size() < n && System.currentTimeMillis() < end) {
            all.addAll(get(c.poll(Duration.ofSeconds(1))));
        }
        return all;
    }

    private static KafkaConsumer assigned(String topic, int partitions) throws Exception {
        KafkaConsumer c = kafka.consumer(ConsumerConfig.defaults());
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < partitions; i++) tps.add(new TopicPartition(topic, i));
        get(c.assign(tps));
        return c;
    }

    @Test
    void topicsAreCreatedOnceAndHaveTheirPartitions() throws Exception {
        String t = newTopic("create", 4);
        assertEquals(4, get(kafka.partitionsFor(t)));
        assertEquals(KafkaException.TOPIC_ALREADY_EXISTS, failure(kafka.createTopic(t, 4, 1)).code());
        assertEquals(KafkaException.UNKNOWN_TOPIC_OR_PARTITION, failure(kafka.partitionsFor(Kafka.topic("nothing"))).code());
    }

    @Test
    void whatIsSentIsReadBackWithKeysValuesHeadersAndTimestamps() throws Exception {
        String t = newTopic("roundtrip", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            long before = System.currentTimeMillis();
            RecordMetadata m = get(p.send(t, 0, b("key"), b("héllo ✓"), List.of(new Header("h1", b("v1")), new Header("h2", null))));
            assertEquals(0, m.offset());
            assertEquals(0, m.partition());
            get(p.send(t, 0, null, b("no key"), List.of()));
            get(p.send(t, 0, b("no value"), null, List.of()));
            get(p.send(t, 0, new byte[0], new byte[0], List.of()));
            KafkaConsumer c = assigned(t, 1);
            List<ConsumerRecord> got = readN(c, 4);
            assertEquals(4, got.size());
            assertEquals("key", got.get(0).keyAsString());
            assertEquals("héllo ✓", got.get(0).valueAsString());
            assertEquals("h1", got.get(0).headers().get(0).key());
            assertArrayEquals(b("v1"), got.get(0).headers().get(0).value());
            assertNull(got.get(0).headers().get(1).value());
            assertTrue(got.get(0).timestamp() >= before - 1000);
            assertNull(got.get(1).key());
            assertNull(got.get(2).value());
            assertEquals(0, got.get(3).key().length);
            assertEquals(List.of(0L, 1L, 2L, 3L), got.stream().map(ConsumerRecord::offset).toList());
            c.close();
        }
    }

    @Test
    void recordsWithTheSameKeyGoToTheSamePartitionAndKeepTheirOrder() throws Exception {
        String t = newTopic("keys", 6);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withAcks(-1))) {
            List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
            for (int i = 0; i < 3000; i++) {
                fs.add(p.send(t, b("k" + (i % 10)), b(Integer.toString(i))));
            }
            Map<String, Integer> partitionOf = new HashMap<>();
            for (int i = 0; i < 3000; i++) {
                RecordMetadata m = get(fs.get(i));
                Integer prev = partitionOf.put("k" + (i % 10), m.partition());
                assertTrue(prev == null || prev == m.partition(), "key k" + (i % 10) + " moved partitions");
            }
            assertEquals((KafkaProducer.murmur2(b("k3")) & 0x7fffffff) % 6, partitionOf.get("k3"), "the partitioner is the Java client's");
            KafkaConsumer c = assigned(t, 6);
            List<ConsumerRecord> got = readN(c, 3000);
            assertEquals(3000, got.size());
            Map<String, Integer> last = new HashMap<>();
            for (ConsumerRecord r : got) {
                int v = Integer.parseInt(r.valueAsString());
                Integer was = last.put(r.keyAsString(), v);
                assertTrue(was == null || was < v, "order within a key");
            }
            c.close();
        }
    }

    @Test
    void everyAcksSettingAndGzipWork() throws Exception {
        for (int acks : new int[] {0, 1, -1}) {
            for (int codec : new int[] {0, 1}) {
                String t = newTopic("acks", 1);
                try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withoutIdempotence().withAcks(acks).withCompression(codec))) {
                    List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
                    for (int i = 0; i < 500; i++) fs.add(p.send(t, 0, null, b("value number " + i + " ".repeat(50)), List.of()));
                    for (var f : fs) get(f);
                    KafkaConsumer c = assigned(t, 1);
                    List<ConsumerRecord> got = readN(c, 500);
                    assertEquals(500, got.size(), "acks " + acks + " codec " + codec);
                    assertTrue(got.get(499).valueAsString().startsWith("value number 499"));
                    c.close();
                }
            }
        }
    }

    @Test
    void anIdempotentProducerWithBatchesInFlightKeepsOrderAndStoresEachRecordOnce() throws Exception {
        String t = newTopic("idem", 3);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withIdempotence().withBatchBytes(2000).withLinger(Duration.ofMillis(1)))) {
            List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
            for (int i = 0; i < 6000; i++) {
                fs.add(p.send(t, i % 3, null, b(Integer.toString(i)), List.of()));
            }
            for (var f : fs) get(f);
            KafkaConsumer c = assigned(t, 3);
            List<ConsumerRecord> got = readN(c, 6000);
            assertEquals(6000, got.size());
            int[] last = {-1, -1, -1};
            for (ConsumerRecord r : got) {
                int v = Integer.parseInt(r.valueAsString());
                assertEquals(v % 3, r.partition());
                assertTrue(v > last[r.partition()], "order within partition " + r.partition());
                last[r.partition()] = v;
            }
            assertEquals(6000, got.stream().map(ConsumerRecord::valueAsString).distinct().count());
            c.close();
        }
    }

    @Test
    void bigRecordsAndTooBigRecords() throws Exception {
        String t = newTopic("big", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withMaxRequestBytes(1_000_000))) {
            byte[] big = new byte[900_000];
            new java.util.Random(1).nextBytes(big);
            get(p.send(t, 0, null, big, List.of()));
            KafkaConsumer c = assigned(t, 1);
            List<ConsumerRecord> got = readN(c, 1);
            assertArrayEquals(big, got.get(0).value());
            c.close();
            KafkaException e = failure(p.send(t, 0, null, new byte[1_100_000], List.of()));
            assertEquals(KafkaException.MESSAGE_TOO_LARGE, e.code());
            assertFalse(e.retriable());
            assertEquals(0, p.bufferedBytes());
        }
    }

    @Test
    void manyVirtualThreadsShareOneProducer() throws Exception {
        String t = newTopic("threads", 3);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            List<Thread> ts = new ArrayList<>();
            List<Throwable> errors = new CopyOnWriteArrayList<>();
            Set<Long> offsets = java.util.concurrent.ConcurrentHashMap.newKeySet();
            for (int i = 0; i < 200; i++) {
                int id = i;
                ts.add(Thread.ofVirtual().start(() -> {
                    try {
                        for (int k = 0; k < 20; k++) {
                            RecordMetadata m = get(p.send(t, "t" + id, "v" + k));
                            offsets.add(m.partition() * 1_000_000L + m.offset());
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }));
            }
            for (Thread th : ts) th.join();
            assertEquals(List.of(), errors);
            assertEquals(4000, offsets.size(), "every record got a place of its own");
        }
    }

    @Test
    void flushWaitsForEverythingAndALingerOfAMinuteDoesNotDelayIt() throws Exception {
        String t = newTopic("flush", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withLinger(Duration.ofMinutes(1)))) {
            List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
            for (int i = 0; i < 100; i++) fs.add(p.send(t, 0, null, b("x" + i), List.of()));
            assertFalse(fs.get(0).isDone());
            long t0 = System.nanoTime();
            get(p.flush());
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) < 5000);
            assertTrue(fs.stream().allMatch(CompletableFuture::isDone));
            assertEquals(0, p.bufferedBytes());
        }
    }

    @Test
    void aFullBufferRefusesInsteadOfGrowing() throws Exception {
        String t = newTopic("buffer", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults().withBufferBytes(10_000).withLinger(Duration.ofSeconds(30)))) {
            get(p.send(t, 0, null, b("warm"), List.of()).completeOnTimeout(null, 1, TimeUnit.MILLISECONDS) == null ? CompletableFuture.completedFuture(null) : CompletableFuture.completedFuture(null));
            KafkaException e = null;
            for (int i = 0; i < 100 && e == null; i++) {
                CompletableFuture<RecordMetadata> f = p.send(t, 0, null, new byte[1000], List.of());
                if (f.isCompletedExceptionally()) e = failure(f);
            }
            assertNotNull(e, "the buffer limit was never hit");
            assertTrue(e.getMessage().contains("buffer full"));
            assertTrue(e.retriable());
            get(p.flush());
            assertEquals(0, p.bufferedBytes());
        }
    }

    @Test
    void sendingToAMissingTopicFailsAndAClosedProducerRefuses() throws Exception {
        KafkaProducer p = kafka.producer(ProducerConfig.defaults());
        KafkaException e = failure(p.send(Kafka.topic("missing"), "k", "v"));
        assertEquals(KafkaException.UNKNOWN_TOPIC_OR_PARTITION, e.code());
        String t = newTopic("one", 1);
        assertEquals(KafkaException.UNKNOWN_TOPIC_OR_PARTITION, failure(p.send(t, 5, null, b("x"), List.of())).code(), "partition 5 of a one-partition topic");
        p.close();
        assertFalse(failure(p.send(t, "k", "v")).retriable());
        assertEquals(0, p.bufferedBytes());
    }

    // ---- consumer without a group ----

    @Test
    void startingPointsSeekAndPosition() throws Exception {
        String t = newTopic("seek", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            for (int i = 0; i < 100; i++) p.send(t, 0, null, b("v" + i), List.of());
            get(p.flush());
        }
        TopicPartition tp = new TopicPartition(t, 0);
        KafkaConsumer latest = kafka.consumer(ConsumerConfig.defaults().withEarliest(false));
        get(latest.assign(List.of(tp)));
        assertEquals(100, get(latest.position(tp)), "latest: the end");
        assertEquals(0, get(latest.poll(Duration.ofMillis(300))).size());
        latest.seek(tp, 90);
        List<ConsumerRecord> tail = readN(latest, 10);
        assertEquals("v90", tail.get(0).valueAsString());
        assertEquals(100, get(latest.position(tp)));
        latest.seek(tp, 5);
        assertEquals("v5", readN(latest, 1).get(0).valueAsString());
        // an offset beyond the end is out of range: the consumer resets to its starting point instead of failing
        latest.seek(tp, 5000);
        assertEquals(0, get(latest.poll(Duration.ofMillis(1500))).size());
        assertEquals(100, get(latest.position(tp)));
        latest.close();
        KafkaConsumer earliest = assigned(t, 1);
        assertEquals(0, get(earliest.position(tp)));
        assertEquals(100, readN(earliest, 100).size());
        earliest.close();
    }

    @Test
    void aConsumerAheadOfPollPausesAtTheBufferLimit() throws Exception {
        String t = newTopic("pause", 1);
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            for (int i = 0; i < 5000; i++) p.send(t, 0, null, b("v" + i), List.of());
            get(p.flush());
        }
        KafkaConsumer c = kafka.consumer(ConsumerConfig.defaults().withBuffer(1000, 100));
        get(c.assign(List.of(new TopicPartition(t, 0))));
        Thread.sleep(1000);
        long pos = get(c.position(new TopicPartition(t, 0)));
        assertEquals(0, pos, "nothing was returned by poll yet");
        int total = 0;
        while (total < 5000) {
            List<ConsumerRecord> r = get(c.poll(Duration.ofSeconds(2)));
            assertFalse(r.isEmpty());
            assertTrue(r.size() <= 100);
            total += r.size();
        }
        c.close();
    }

    @Test
    void onlyOnePollAtATime() throws Exception {
        String t = newTopic("poll", 1);
        KafkaConsumer c = assigned(t, 1);
        CompletableFuture<List<ConsumerRecord>> first = c.poll(Duration.ofSeconds(2));
        Thread.sleep(100);
        assertInstanceOf(IllegalStateException.class, assertThrows(ExecutionException.class, () -> c.poll(Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS)).getCause());
        assertEquals(0, get(first).size());
        c.close();
        assertInstanceOf(KafkaException.class, assertThrows(ExecutionException.class, () -> c.poll(Duration.ofSeconds(1)).get(5, TimeUnit.SECONDS)).getCause());
    }

    @Test
    void assigningAMissingPartitionFails() throws Exception {
        String t = newTopic("assign", 1);
        KafkaConsumer c = kafka.consumer(ConsumerConfig.defaults());
        assertEquals(KafkaException.UNKNOWN_TOPIC_OR_PARTITION, failure(c.assign(List.of(new TopicPartition(t, 3)))).code());
        assertEquals(KafkaException.UNKNOWN_TOPIC_OR_PARTITION, failure(c.assign(List.of(new TopicPartition(Kafka.topic("none"), 0)))).code());
        c.close();
    }

    // ---- groups ----

    @Test
    void aGroupMemberGetsAllPartitionsAndResumesFromItsCommit() throws Exception {
        String t = newTopic("group", 3);
        String group = Kafka.topic("g");
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            for (int i = 0; i < 300; i++) p.send(t, i % 3, null, b("v" + i), List.of());
            get(p.flush());
        }
        KafkaConsumer c1 = kafka.consumer(ConsumerConfig.defaults().withGroup(group));
        List<Set<TopicPartition>> assignments = new CopyOnWriteArrayList<>();
        get(c1.subscribe(List.of(t), new RebalanceListener() {
            @Override
            public void assigned(Collection<TopicPartition> ps) {
                assignments.add(new HashSet<>(ps));
            }
        }));
        assertEquals(3, assignments.get(0).size());
        List<ConsumerRecord> first = new ArrayList<>();
        while (first.size() < 200) first.addAll(get(c1.poll(Duration.ofSeconds(2))));
        // commit exactly what was returned; the rest of the 300 is fetched ahead but not committed
        get(c1.commit());
        long returned = first.size();
        c1.close();
        KafkaConsumer c2 = kafka.consumer(ConsumerConfig.defaults().withGroup(group));
        get(c2.subscribe(List.of(t)));
        List<ConsumerRecord> rest = readN(c2, 300 - (int) returned);
        assertEquals(300 - returned, rest.size(), "the second member starts where the first one's commit says");
        Set<String> seen = new HashSet<>();
        first.forEach(r -> seen.add(r.valueAsString()));
        rest.forEach(r -> assertTrue(seen.add(r.valueAsString()), "no record twice: " + r.valueAsString()));
        assertEquals(300, seen.size());
        c2.close();
    }

    @Test
    void twoMembersShareThePartitionsAndOneLeavingGivesThemBack() throws Exception {
        String t = newTopic("share", 4);
        String group = Kafka.topic("g");
        List<String> events = new CopyOnWriteArrayList<>();
        KafkaConsumer c1 = kafka.consumer(ConsumerConfig.defaults().withGroup(group));
        Map<String, Set<TopicPartition>> now = new java.util.concurrent.ConcurrentHashMap<>();
        get(c1.subscribe(List.of(t), new RebalanceListener() {
            @Override
            public void revoked(Collection<TopicPartition> ps) {
                events.add("c1 revoked " + ps.size());
            }

            @Override
            public void assigned(Collection<TopicPartition> ps) {
                events.add("c1 assigned " + ps.size());
                now.put("c1", new HashSet<>(ps));
            }
        }));
        assertEquals(4, now.get("c1").size());
        KafkaConsumer c2 = kafka.consumer(ConsumerConfig.defaults().withGroup(group));
        get(c2.subscribe(List.of(t), new RebalanceListener() {
            @Override
            public void assigned(Collection<TopicPartition> ps) {
                now.put("c2", new HashSet<>(ps));
            }
        }));
        // c1 learns of the rebalance by its heartbeat (3 s) and rejoins
        for (int i = 0; i < 100 && (now.get("c1").size() != 2 || now.get("c2").size() != 2); i++) Thread.sleep(200);
        assertEquals(2, now.get("c1").size(), events.toString());
        assertEquals(2, now.get("c2").size());
        Set<TopicPartition> union = new HashSet<>(now.get("c1"));
        union.addAll(now.get("c2"));
        assertEquals(4, union.size(), "no partition twice, none missing");
        assertTrue(events.contains("c1 revoked 4"), events.toString());
        c2.close();
        for (int i = 0; i < 100 && now.get("c1").size() != 4; i++) Thread.sleep(200);
        assertEquals(4, now.get("c1").size(), "c1 got everything back when c2 left");
        c1.close();
    }

    @Test
    void autoCommitCommitsWhatWasReturned() throws Exception {
        String t = newTopic("auto", 1);
        String group = Kafka.topic("g");
        try (KafkaProducer p = kafka.producer(ProducerConfig.defaults())) {
            for (int i = 0; i < 50; i++) p.send(t, 0, null, b("v" + i), List.of());
            get(p.flush());
        }
        KafkaConsumer c1 = kafka.consumer(ConsumerConfig.defaults().withGroup(group).withAutoCommit(Duration.ofMillis(200)));
        get(c1.subscribe(List.of(t)));
        assertEquals(50, readN(c1, 50).size());
        Thread.sleep(600);
        c1.close();
        KafkaConsumer c2 = kafka.consumer(ConsumerConfig.defaults().withGroup(group));
        get(c2.subscribe(List.of(t)));
        assertEquals(0, get(c2.poll(Duration.ofMillis(800))).size(), "everything was committed");
        c2.close();
    }

    @Test
    void groupCallsWithoutAGroupAreRefused() throws Exception {
        KafkaConsumer c = kafka.consumer(ConsumerConfig.defaults());
        assertInstanceOf(IllegalStateException.class, assertThrows(ExecutionException.class, () -> c.subscribe(List.of("x")).get(5, TimeUnit.SECONDS)).getCause());
        assertInstanceOf(IllegalStateException.class, assertThrows(ExecutionException.class, () -> c.commit(Map.of(new TopicPartition("x", 0), 1L)).get(5, TimeUnit.SECONDS)).getCause());
        c.close();
    }

    // ---- security ----

    @Test
    void saslPlainAcceptsTheRightPasswordAndRefusesTheWrongOne() throws Exception {
        Kafka.require(Kafka.SASL_PORT);
        KafkaClient ok = KafkaClient.create(group, KafkaConfig.of(Kafka.HOST + ":" + Kafka.SASL_PORT).withSaslPlain("alice", "alicepw"));
        String t = Kafka.topic("sasl");
        get(ok.createTopic(t, 1, 1));
        KafkaProducer p = ok.producer(ProducerConfig.defaults());
        assertEquals(0, get(p.send(t, 0, null, b("secret"), List.of())).offset());
        ok.close();
        KafkaClient bad = KafkaClient.create(group, KafkaConfig.of(Kafka.HOST + ":" + Kafka.SASL_PORT).withSaslPlain("alice", "wrong"));
        KafkaException e = failure(bad.createTopic(Kafka.topic("sasl"), 1, 1));
        assertTrue(e.getMessage().contains("SASL"), e.getMessage());
        assertFalse(e.retriable());
        KafkaClient none = KafkaClient.create(group, KafkaConfig.of(Kafka.HOST + ":" + Kafka.SASL_PORT).withTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(2)));
        failure(none.createTopic(Kafka.topic("sasl"), 1, 1));   // plain text to a SASL listener: the broker drops the connection
    }

    @Test
    void anUnreachableClusterFailsWithARetriableError() throws Exception {
        KafkaClient dead = KafkaClient.create(group, KafkaConfig.of("127.0.0.1:1").withTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(1)));
        KafkaException e = failure(dead.partitionsFor("t"));
        assertTrue(e.retriable());
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
