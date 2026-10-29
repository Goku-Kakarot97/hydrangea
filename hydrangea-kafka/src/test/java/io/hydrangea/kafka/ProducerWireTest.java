package io.hydrangea.kafka;

import io.hydrangea.Backoff;
import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntUnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

/** The producer against a scripted broker: what exactly goes on the wire for idempotence and retries. */
class ProducerWireTest {

    private static EventLoopGroup group;

    @BeforeAll
    static void start() {
        group = new EventLoopGroup(1);
    }

    @AfterAll
    static void stop() throws Exception {
        group.shutdown().get(5, TimeUnit.SECONDS);
    }

    /** A produce request as the fake saw it: for one partition, the header fields of its batch. */
    record Seen(int partition, long producerId, int epoch, int baseSequence, int count, byte[] bytes) {
    }

    /** A broker that answers Metadata, InitProducerId and Produce; {@code errorFor} chooses the error code for the n-th produce request (0-based). */
    private static final class Fake implements AutoCloseable {
        final ServerSocket server;
        final List<Seen> produced = new CopyOnWriteArrayList<>();
        final AtomicInteger initCalls = new AtomicInteger();
        final AtomicLong nextPid = new AtomicLong(1000);
        final AtomicLong offset = new AtomicLong();
        volatile IntUnaryOperator errorFor = n -> 0;
        final AtomicInteger produceCalls = new AtomicInteger();
        final int partitions;

        Fake(int partitions) throws IOException {
            this.partitions = partitions;
            server = new ServerSocket(0, 5, InetAddress.getLoopbackAddress());
            Thread.ofPlatform().daemon().start(() -> {
                while (!server.isClosed()) {
                    try {
                        Socket s = server.accept();
                        Thread.ofPlatform().daemon().start(() -> serve(s));
                    } catch (IOException e) {
                        return;
                    }
                }
            });
        }

        KafkaConfig config() {
            return KafkaConfig.of("127.0.0.1:" + server.getLocalPort()).withRetries(10, new Backoff(Duration.ofMillis(10), Duration.ofMillis(30), 2.0, 0.0, 0))
                    .withTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(5));
        }

        void serve(Socket s) {
            try (s) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                for (;;) {
                    int len = in.readInt();
                    Wire.Reader r = new Wire.Reader(in.readNBytes(len));
                    int api = r.i16();
                    r.i16();
                    int corr = r.i32();
                    r.str();
                    Wire.Writer body = new Wire.Writer(64);
                    switch (api) {
                        case Broker.Api.METADATA -> {
                            body.i32(0).i32(1).i32(0).str("127.0.0.1").i32(server.getLocalPort()).str(null).str("cluster").i32(0).i32(1);
                            body.i16(0).str("t").i8(0).i32(partitions);
                            for (int p = 0; p < partitions; p++) {
                                body.i16(0).i32(p).i32(0).i32(1).i32(0).i32(1).i32(0);
                            }
                        }
                        case Broker.Api.INIT_PRODUCER_ID -> {
                            initCalls.incrementAndGet();
                            body.i32(0).i16(0).i64(nextPid.getAndIncrement()).i16(0);
                        }
                        case Broker.Api.PRODUCE -> {
                            r.str();
                            r.i16();
                            r.i32();
                            int nt = r.arrayLength();
                            List<int[]> parts = new ArrayList<>();
                            int call = produceCalls.getAndIncrement();
                            int err = errorFor.applyAsInt(call);
                            body.i32(nt);
                            for (int i = 0; i < nt; i++) {
                                String topic = r.str();
                                int np = r.arrayLength();
                                body.str(topic).i32(np);
                                for (int k = 0; k < np; k++) {
                                    int part = r.i32();
                                    byte[] batch = r.bytes();
                                    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(batch);
                                    int count = bb.getInt(57);
                                    produced.add(new Seen(part, bb.getLong(43), bb.getShort(51), bb.getInt(53), count, batch));
                                    long base = err == 0 ? offset.getAndAdd(count) : -1;
                                    body.i32(part).i16(err).i64(base).i64(-1);
                                }
                            }
                            body.i32(0);
                        }
                        default -> throw new IOException("unexpected api " + api);
                    }
                    Wire.Writer w = new Wire.Writer(16);
                    w.i32(4 + body.len).i32(corr).raw(body.buf, 0, body.len);
                    out.write(w.buf, 0, w.len);
                    out.flush();
                }
            } catch (IOException ignored) {
                // closed
            }
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private static <T> T get(CompletableFuture<T> f) throws Exception {
        return f.get(15, TimeUnit.SECONDS);
    }

    private static byte[] b(String s) {
        return s.getBytes();
    }

    private KafkaProducer producer(Fake fake, ProducerConfig cfg) {
        return KafkaClient.create(group, fake.config()).producer(cfg);
    }

    @Test
    void anIdempotentProducerNumbersItsBatchesPerPartition() throws Exception {
        try (Fake fake = new Fake(2)) {
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence().withLinger(Duration.ofSeconds(30)));
            for (int round = 0; round < 3; round++) {
                for (int i = 0; i < 5; i++) {
                    p.send("t", 0, null, b("a"), List.of());
                }
                for (int i = 0; i < 3; i++) {
                    p.send("t", 1, null, b("b"), List.of());
                }
                get(p.flush());
            }
            assertEquals(1, fake.initCalls.get());
            List<Seen> p0 = fake.produced.stream().filter(s -> s.partition() == 0).toList();
            List<Seen> p1 = fake.produced.stream().filter(s -> s.partition() == 1).toList();
            assertEquals(List.of(0, 5, 10), p0.stream().map(Seen::baseSequence).toList());
            assertEquals(List.of(0, 3, 6), p1.stream().map(Seen::baseSequence).toList());
            assertTrue(fake.produced.stream().allMatch(s -> s.producerId() == 1000 && s.epoch() == 0));
            p.close();
        }
    }

    @Test
    void aProducerThatIsNotIdempotentSendsNoProducerId() throws Exception {
        try (Fake fake = new Fake(1)) {
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withoutIdempotence());
            get(p.send("t", 0, null, b("x"), List.of()));
            assertEquals(0, fake.initCalls.get());
            assertEquals(-1, fake.produced.get(0).producerId());
            assertEquals(-1, fake.produced.get(0).baseSequence());
            p.close();
        }
    }

    @Test
    void aRetryCarriesTheSameProducerIdAndSequenceAndTheSameBytes() throws Exception {
        try (Fake fake = new Fake(1)) {
            fake.errorFor = n -> n < 2 ? KafkaException.NOT_LEADER_OR_FOLLOWER : 0;
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence());
            RecordMetadata m = get(p.send("t", 0, null, b("once"), List.of()));
            assertEquals(0, m.offset());
            assertEquals(3, fake.produced.size(), "two refusals and the success");
            for (Seen s : fake.produced) {
                assertEquals(0, s.baseSequence());
                assertEquals(1000, s.producerId());
            }
            assertArrayEquals(fake.produced.get(0).bytes(), fake.produced.get(2).bytes());
            p.close();
        }
    }

    @Test
    void severalBatchesInFlightEachKeepTheirNumbersWhenOneIsRetried() throws Exception {
        try (Fake fake = new Fake(1)) {
            fake.errorFor = n -> n == 0 ? KafkaException.NOT_LEADER_OR_FOLLOWER : 0;
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence().withBatchBytes(50).withLinger(Duration.ofMillis(1)));
            List<CompletableFuture<RecordMetadata>> fs = new ArrayList<>();
            for (int i = 0; i < 30; i++) {
                fs.add(p.send("t", 0, null, b("record-" + i + "-0123456789"), List.of()));
            }
            for (var f : fs) get(f);
            // by sequence number the batches tile 0..30 with no gap and no overlap, the refused one appearing twice with the same number
            java.util.TreeMap<Integer, Integer> bySeq = new java.util.TreeMap<>();
            for (Seen s : fake.produced) {
                Integer was = bySeq.put(s.baseSequence(), s.count());
                assertTrue(was == null || was == s.count(), "the same sequence with another size");
            }
            int next = 0;
            for (var e : bySeq.entrySet()) {
                assertEquals(next, e.getKey());
                next += e.getValue();
            }
            assertEquals(30, next);
            assertEquals(1, fake.produced.size() - bySeq.size(), "exactly one batch was sent twice");
            p.close();
        }
    }

    @Test
    void aDuplicateAnswerCountsAsDelivered() throws Exception {
        try (Fake fake = new Fake(1)) {
            fake.errorFor = n -> KafkaException.DUPLICATE_SEQUENCE_NUMBER;
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence());
            RecordMetadata m = get(p.send("t", 0, null, b("x"), List.of()));
            assertEquals(-1, m.offset(), "the broker has it but does not say where");
            assertEquals(1, fake.produced.size());
            p.close();
        }
    }

    @Test
    void aBrokerThatForgotTheProducerIdGetsANewOneAndTheBatchStartsAtZero() throws Exception {
        try (Fake fake = new Fake(1)) {
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence());
            get(p.send("t", 0, null, b("first"), List.of()));
            get(p.send("t", 0, null, b("second"), List.of()));
            fake.errorFor = n -> n == 2 ? KafkaException.UNKNOWN_PRODUCER_ID : 0;
            get(p.send("t", 0, null, b("third"), List.of()));
            assertEquals(2, fake.initCalls.get());
            Seen last = fake.produced.get(fake.produced.size() - 1);
            assertEquals(1001, last.producerId());
            assertEquals(0, last.baseSequence());
            p.close();
        }
    }

    @Test
    void aBatchThatFailsForGoodFailsTheNumberedBatchesAfterItAndTakesANewId() throws Exception {
        try (Fake fake = new Fake(1)) {
            fake.errorFor = n -> n == 0 ? KafkaException.MESSAGE_TOO_LARGE : 0;
            KafkaProducer p = producer(fake, ProducerConfig.defaults().withIdempotence());
            ExecutionException e = assertThrows(ExecutionException.class, () -> get(p.send("t", 0, null, b("big"), List.of())));
            assertEquals(KafkaException.MESSAGE_TOO_LARGE, ((KafkaException) e.getCause()).code());
            get(p.send("t", 0, null, b("next"), List.of()));
            assertEquals(2, fake.initCalls.get(), "a hole in the sequence: a new id");
            Seen last = fake.produced.get(fake.produced.size() - 1);
            assertEquals(1001, last.producerId());
            assertEquals(0, last.baseSequence());
            p.close();
        }
    }

    @Test
    void theDefaultIsIdempotent() throws Exception {
        ProducerConfig d = ProducerConfig.defaults();
        assertTrue(d.idempotent());
        assertEquals(-1, d.acks());
        assertEquals(5, d.maxInFlight());
        ProducerConfig off = d.withoutIdempotence().withAcks(1);
        assertFalse(off.idempotent());
        assertEquals(1, off.maxInFlight());
        assertThrows(IllegalArgumentException.class, () -> d.withAcks(1), "acks=1 needs withoutIdempotence() first");
        try (Fake fake = new Fake(1)) {
            KafkaProducer p = producer(fake, d);
            get(p.send("t", 0, null, b("x"), List.of()));
            assertEquals(1, fake.initCalls.get());
            assertEquals(1000, fake.produced.get(0).producerId());
            p.close();
        }
    }

    @Test
    void theConfigurationRefusesWhatCannotBeIdempotent() {
        assertThrows(IllegalArgumentException.class, () -> ProducerConfig.defaults().withIdempotence().withAcks(1));
        assertThrows(IllegalArgumentException.class, () -> ProducerConfig.defaults().withIdempotence().withMaxInFlight(6));
        assertThrows(IllegalArgumentException.class, () -> ProducerConfig.defaults().withIdempotence().withRetries(0));
        ProducerConfig c = ProducerConfig.defaults().withIdempotence();
        assertEquals(-1, c.acks());
        assertEquals(5, c.maxInFlight());
        assertTrue(c.idempotent());
    }
}
