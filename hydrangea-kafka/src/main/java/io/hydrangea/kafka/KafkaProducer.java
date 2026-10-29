package io.hydrangea.kafka;

import io.hydrangea.Latency;
import io.hydrangea.Metrics;
import io.hydrangea.Scheduled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sends records. {@link #send} returns at once from any thread (virtual threads included) with a future for where the broker stored the record; records for
 * the same partition are batched and travel together. <b>Order</b>: records of one partition are stored in the order they were sent, also across retries,
 * because a partition has at most one batch in flight (a request carries one batch of each of many partitions, so the connection is still kept busy).
 * Without idempotence a retry after a lost answer can store a record twice (at-least-once); this client has no idempotent producer and no transactions.
 * Futures complete on the client's event-loop thread: do not block there.
 */
public final class KafkaProducer implements AutoCloseable {

    private final Cluster cluster;
    private final ProducerConfig config;

    /** A record on its way to the loop. It is also the node of the queue it travels in, so that handing it over allocates nothing more. */
    private static final class Pend {
        final String topic;
        final Integer partition;
        final Records.Out record;
        final CompletableFuture<RecordMetadata> future;
        final int size;
        volatile Pend next;

        Pend(String topic, Integer partition, Records.Out record, CompletableFuture<RecordMetadata> future, int size) {
            this.topic = topic;
            this.partition = partition;
            this.record = record;
            this.future = future;
            this.size = size;
        }
    }

    /**
     * Many threads put, the loop takes: a queue of the kind where putting is one atomic exchange (Vyukov's), instead of the compare-and-swap loop of
     * ConcurrentLinkedQueue that threads fight over with the loop. {@code head} is the node taken last (a stand-in at the start); its {@code next} is the next to take.
     */
    private final java.util.concurrent.atomic.AtomicReference<Pend> inboxTail;
    private Pend inboxHead;                   // loop only
    private final AtomicBoolean inboxScheduled = new AtomicBoolean();
    private final AtomicLong buffered = new AtomicLong();

    private static final class Batch {
        final TopicPartition tp;
        final List<Records.Out> records = new ArrayList<>();
        final List<CompletableFuture<RecordMetadata>> futures = new ArrayList<>();
        int bytes;              // what the buffer limit counts for this batch (the records' sizes plus a little per record for the objects around them)
        int wireBytes;          // what the records will take in the batch on the wire: this is what fills a batch up to batchBytes
        long deadline = Long.MAX_VALUE;
        long created;           // the clock when the batch was opened (0 = latency is not being recorded): its age when acknowledged is the send-to-ack time of its first record
        int attempts;
        boolean sealed;
        boolean inflight;
        Scheduled linger;
        long pid = -1;          // idempotent producer: assigned when first sent, kept on every retry
        short epoch = -1;
        int seq = -1;
        byte[] encoded;         // a retry sends the same bytes

        Batch(TopicPartition tp) {
            this.tp = tp;
        }
    }

    /** What the producer keeps for a partition: its batches in order (the ones acknowledged are removed) and the sequence counter. */
    private static final class Part {
        final ArrayList<Batch> batches = new ArrayList<>();
        int inFlight;
        boolean recovering;     // a batch failed: nothing new is sent until those in flight have answered, then everything unacknowledged goes again in order
        long retryAt;
        long seqPid = -2;
        int nextSeq;
        long ackedPid = -2;     // the producer id under which this partition has had a batch acknowledged
    }

    // loop thread only
    private final Map<TopicPartition, Part> parts = new LinkedHashMap<>();
    private final Map<String, List<Pend>> parked = new HashMap<>();
    private final Map<String, Integer> sticky = new HashMap<>();
    private final Map<String, Integer> metaAttempts = new HashMap<>();
    private final Set<String> refreshing = new HashSet<>();          // topics whose metadata parked records wait for
    private final Set<String> refreshingLeaders = new HashSet<>();    // topics whose leader is being looked up for batches that are ready
    private boolean flushing;
    private boolean drainQueued;
    private long unfinished;
    private final List<CompletableFuture<Void>> flushWaiters = new ArrayList<>();
    private volatile boolean closed;
    // idempotence
    private long producerId = -1;
    private short producerEpoch = -1;
    private boolean initializing;
    private boolean needNewId;

    // metrics: counted per batch, not per record, so the cost is a few additions per request
    private final Metrics.Count mOk;
    private final Metrics.Count mFailed;
    private final Metrics.Count mBytes;
    private final Metrics.Count mRetries;
    private final Metrics.Count mRejectedFull;
    private final Metrics.Count mRejectedLarge;
    private final Latency mBatchTime;

    KafkaProducer(Cluster cluster, ProducerConfig config) {
        this.cluster = cluster;
        this.config = config;
        Metrics m = cluster.loop.metrics();
        mOk = m.counter("kafka.producer.records", null, "outcome", "ok");
        mFailed = m.counter("kafka.producer.records", null, "outcome", "failed");
        mBytes = m.counter("kafka.producer.bytes", "bytes");
        mRetries = m.counter("kafka.producer.retries", null);
        mRejectedFull = m.counter("kafka.producer.rejected", null, "reason", "buffer_full");
        mRejectedLarge = m.counter("kafka.producer.rejected", null, "reason", "too_large");
        mBatchTime = m.latency("kafka.producer.batch.duration");
        Pend stub = new Pend(null, null, null, null, 0);
        this.inboxHead = stub;
        this.inboxTail = new java.util.concurrent.atomic.AtomicReference<>(stub);
    }

    private void inboxPut(Pend p) {
        Pend prev = inboxTail.getAndSet(p);
        prev.next = p;
    }

    /** The next record, or null if there is none (or the thread putting one has not yet linked it: it will be seen on the next call, which its scheduling guarantees). Loop only. */
    private Pend inboxTake() {
        Pend next = inboxHead.next;
        if (next == null) {
            return null;
        }
        Pend old = inboxHead;
        inboxHead = next;
        // cut the link: a taken node that kept pointing at the next one would, once it has aged into the old generation, keep alive every node linked after it
        // until a collection of that generation (the garbage collector's "nepotism"), which made the heap and the pauses several times bigger
        old.next = null;
        return next;
    }

    private boolean inboxEmpty() {
        return inboxHead.next == null && inboxTail.get() == inboxHead;
    }

    public CompletableFuture<RecordMetadata> send(String topic, String key, String value) {
        return send(topic, null, key == null ? null : key.getBytes(StandardCharsets.UTF_8), value == null ? null : value.getBytes(StandardCharsets.UTF_8), List.of());
    }

    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value) {
        return send(topic, null, key, value, List.of());
    }

    /** {@code partition} null lets the partitioner choose: the hash of the key (murmur2, as the Java client does), or, without a key, a partition kept until its batch is full. */
    public CompletableFuture<RecordMetadata> send(String topic, Integer partition, byte[] key, byte[] value, List<Header> headers) {
        if (closed) {
            return CompletableFuture.failedFuture(new KafkaException("the producer is closed", false));
        }
        if (topic == null || topic.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("topic"));
        }
        if (partition != null && partition < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("partition"));
        }
        Records.Out out = new Records.Out(key, value, headers == null ? List.of() : headers, System.currentTimeMillis());
        int size = out.size() + 12;
        if (size + 61 > config.maxRequestBytes()) {
            mRejectedLarge.increment();
            return CompletableFuture.failedFuture(new KafkaException("record of " + size + " bytes exceeds maxRequestBytes " + config.maxRequestBytes(), KafkaException.MESSAGE_TOO_LARGE, false, null));
        }
        long now = buffered.addAndGet(size);
        if (now > config.bufferBytes()) {
            buffered.addAndGet(-size);
            mRejectedFull.increment();
            return CompletableFuture.failedFuture(new KafkaException("buffer full: " + now + " bytes waiting (limit " + config.bufferBytes() + ")", true));
        }
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        inboxPut(new Pend(topic, partition, out, f, size));
        if (!inboxScheduled.get() && inboxScheduled.compareAndSet(false, true)) {
            try {
                cluster.loop.execute(this::drainInbox);
            } catch (RuntimeException e) {
                inboxScheduled.set(false);
                failInbox(new KafkaException("the client is closed", false));
            }
        }
        return f;
    }

    private void failInbox(KafkaException e) {
        for (Pend p; (p = inboxTake()) != null; ) {
            buffered.addAndGet(-p.size);
            mFailed.increment();
            p.future.completeExceptionally(e);
        }
    }

    /** Bytes of records accepted and not yet acknowledged. */
    public long bufferedBytes() {
        return buffered.get();
    }

    private void drainInbox() {
        inboxScheduled.set(false);
        for (Pend p; (p = inboxTake()) != null; ) {
            unfinished++;
            place(p);
        }
        schedule();
    }

    private void place(Pend p) {
        Cluster.Topic t = cluster.topic(p.topic);
        if (t == null || t.error() != 0 || t.partitions().isEmpty()) {
            parked.computeIfAbsent(p.topic, k -> new ArrayList<>()).add(p);
            fetchMetadata(p.topic);
            return;
        }
        int n = t.partitions().size();
        int part;
        if (p.partition != null) {
            if (p.partition >= n) {
                finishRecord(p, new KafkaException("topic " + p.topic + " has " + n + " partitions, not " + (p.partition + 1), KafkaException.UNKNOWN_TOPIC_OR_PARTITION, false, null));
                return;
            }
            part = p.partition;
        } else if (p.record.key() != null) {
            part = (murmur2(p.record.key()) & 0x7fffffff) % n;
        } else {
            Integer chosen = sticky.get(p.topic);
            if (chosen == null || chosen >= n) {
                chosen = leastLoadedPartition(p.topic, n);
                sticky.put(p.topic, chosen);
            }
            part = chosen;
        }
        TopicPartition tp = new TopicPartition(p.topic, part);
        Part ps = parts.computeIfAbsent(tp, k -> new Part());
        Batch b = ps.batches.isEmpty() ? null : ps.batches.get(ps.batches.size() - 1);
        int wire = p.size - 12;
        if (b == null || b.sealed || (b.wireBytes + wire > config.batchBytes() && !b.records.isEmpty())) {
            if (b != null) {
                b.sealed = true;
            }
            b = new Batch(tp);
            b.created = mBatchTime.begin();
            b.deadline = System.nanoTime() + config.deliveryTimeout().toNanos();
            ps.batches.add(b);
            if (p.partition == null && p.record.key() == null) {
                sticky.remove(p.topic);   // a new batch: the next keyless record may go elsewhere
            }
            Batch armed = b;
            b.linger = cluster.loop.schedule(() -> {
                armed.sealed = true;
                schedule();
            }, config.linger().toNanos(), TimeUnit.NANOSECONDS);
        }
        b.records.add(p.record);
        b.futures.add(p.future);
        b.bytes += p.size;
        b.wireBytes += wire;
        if (b.wireBytes >= config.batchBytes()) {
            b.sealed = true;
        }
    }

    private int rotate;

    /**
     * Where the next batch of records without a key goes: the partition with the fewest batches waiting or in flight (ties in turn). A random choice sends batches to a
     * partition whose window of batches in flight is full while the others are idle, and the records in them wait for nothing but that; the Java client's adaptive
     * partitioning (KIP-794) does the same for the same reason.
     */
    private int leastLoadedPartition(String topic, int n) {
        int best = -1;
        int bestLoad = Integer.MAX_VALUE;
        int start = rotate++ % n;
        for (int k = 0; k < n; k++) {
            int cand = (start + k) % n;
            Part ps = parts.get(new TopicPartition(topic, cand));
            int load = ps == null ? 0 : ps.batches.size();
            if (load < bestLoad) {
                bestLoad = load;
                best = cand;
            }
        }
        return best;
    }

    private void fetchMetadata(String topic) {
        if (!refreshing.add(topic)) {
            return;
        }
        cluster.refresh(List.of(topic)).whenComplete((v, e) -> {
            refreshing.remove(topic);
            List<Pend> waiting = parked.remove(topic);
            if (waiting == null) {
                return;
            }
            Cluster.Topic t = cluster.topic(topic);
            int attempts = metaAttempts.merge(topic, 1, Integer::sum);
            boolean unknown = e == null && t != null && t.error() == KafkaException.UNKNOWN_TOPIC_OR_PARTITION;
            boolean ok = e == null && t != null && t.error() == 0 && !t.partitions().isEmpty();
            if (ok) {
                metaAttempts.remove(topic);
                waiting.forEach(this::place);
                schedule();
                return;
            }
            KafkaException cause = e != null ? (e instanceof KafkaException k ? k : new KafkaException("metadata: " + e, true, e))
                    : KafkaException.of(t == null ? KafkaException.UNKNOWN_TOPIC_OR_PARTITION : t.error(), "topic " + topic);
            if (unknown || attempts > config.retries() || cause.retriable() == false) {
                metaAttempts.remove(topic);
                waiting.forEach(p -> finishRecord(p, cause));
                return;
            }
            parked.computeIfAbsent(topic, k -> new ArrayList<>()).addAll(waiting);
            cluster.loop.schedule(() -> fetchMetadata(topic), cluster.config.retryBackoff().delayFor(attempts).toNanos(), TimeUnit.NANOSECONDS);
        });
    }

    private void finishRecord(Pend p, KafkaException e) {
        buffered.addAndGet(-p.size);
        unfinished--;
        mFailed.increment();
        p.future.completeExceptionally(e);
        checkFlush();
    }

    // ---- sending ----

    private void schedule() {
        if (!drainQueued) {
            drainQueued = true;
            cluster.loop.execute(() -> {
                drainQueued = false;
                sendReady();
            });
        }
    }

    private boolean ready(Batch b) {
        return b.sealed || flushing || closed;
    }

    private static int addSequence(int seq, int n) {
        return seq > Integer.MAX_VALUE - n ? n - (Integer.MAX_VALUE - seq) - 1 : seq + n;
    }

    /** Asks for a producer id (once); sends resume when it is there. */
    private void ensureProducerId() {
        if (initializing || closed) {
            return;
        }
        initializing = true;
        cluster.requestAny(Broker.Api.INIT_PRODUCER_ID, 0, w -> w.str(null).i32(60_000)).whenComplete((r, e) -> {
            initializing = false;
            try {
                if (e != null) {
                    throw Cluster.unwrap(e) instanceof KafkaException k ? k : new KafkaException("init producer id: " + e, true, e);
                }
                r.i32();
                int err = r.i16();
                long id = r.i64();
                short epoch = (short) r.i16();
                if (err != 0) {
                    throw KafkaException.of(err, "init producer id");
                }
                producerId = id;
                producerEpoch = epoch;
                needNewId = false;
                schedule();
            } catch (KafkaException k) {
                if (k.retriable() && !closed) {
                    cluster.loop.schedule(this::schedule, cluster.config.retryBackoff().delayFor(2).toNanos(), TimeUnit.NANOSECONDS);
                } else {
                    // nothing can be sent without an id: fail what waits
                    failAll(k);
                }
            }
        });
    }

    private void failAll(KafkaException e) {
        for (Part ps : parts.values()) {
            for (Batch b : new ArrayList<>(ps.batches)) {
                if (!b.inflight) {
                    dropBatch(ps, b, e);
                }
            }
        }
    }

    private void dropBatch(Part ps, Batch b, KafkaException e) {
        ps.batches.remove(b);
        if (b.linger != null) {
            b.linger.cancel();
        }
        buffered.addAndGet(-b.bytes);
        unfinished -= b.records.size();
        mFailed.add(b.records.size());
        b.futures.forEach(f -> f.completeExceptionally(e));
        checkFlush();
    }

    private boolean sending;

    /** Sends what is ready, now if the loop is not already doing so (as when an answer has just freed a slot), else on the next turn. */
    private void kick() {
        if (sending) {
            schedule();
        } else {
            sendReady();
        }
    }

    private void sendReady() {
        sending = true;
        try {
            sendReady0();
        } finally {
            sending = false;
        }
    }

    private void sendReady0() {
        Set<String> needMeta = new HashSet<>();
        long now = System.nanoTime();
        long earliestRetry = Long.MAX_VALUE;
        boolean again;
        do {
            again = false;
            Map<Integer, List<Batch>> byNode = new LinkedHashMap<>();
            Map<Integer, Integer> sizes = new HashMap<>();
            for (var e : parts.entrySet()) {
                TopicPartition tp = e.getKey();
                Part ps = e.getValue();
                if (ps.recovering && ps.inFlight == 0) {
                    ps.recovering = false;
                }
                if (ps.recovering || ps.inFlight >= config.maxInFlight()) {
                    continue;
                }
                Batch b = null;
                for (Batch x : ps.batches) {
                    if (!x.inflight) {
                        b = x;
                        break;
                    }
                }
                if (b == null || !ready(b)) {
                    continue;
                }
                if (ps.retryAt > now) {
                    earliestRetry = Math.min(earliestRetry, ps.retryAt);
                    continue;
                }
                if (config.idempotent() && ps.inFlight >= 1 && ps.ackedPid != (b.seq >= 0 ? b.pid : producerId)) {
                    // The broker takes the first batch it sees of a producer for a partition whatever its sequence number (it may have forgotten the producer). If batches
                    // 2 and 3 got in before a failed batch 1, batch 1 would be refused for ever. So until one batch has been acknowledged, only one is in flight.
                    continue;
                }
                Cluster.Topic t = cluster.topic(tp.topic());
                int leader = -1;
                if (t != null && tp.partition() < t.partitions().size()) {
                    leader = t.partitions().get(tp.partition()).leader();
                }
                if (leader < 0 || cluster.node(leader) == null) {
                    needMeta.add(tp.topic());
                    continue;
                }
                if (config.idempotent() && b.seq < 0) {
                    if (needNewId) {
                        producerId = -1;
                    }
                    if (producerId < 0) {
                        ensureProducerId();
                        continue;
                    }
                    if (ps.seqPid != producerId) {
                        ps.seqPid = producerId;
                        ps.nextSeq = 0;
                    }
                    b.pid = producerId;
                    b.epoch = producerEpoch;
                    b.seq = ps.nextSeq;
                    ps.nextSeq = addSequence(ps.nextSeq, b.records.size());
                }
                if (b.encoded == null) {
                    b.encoded = Records.encode(b.records, config.compression(), b.pid, b.epoch, b.seq);
                }
                int size = sizes.getOrDefault(leader, 0);
                if (size > 0 && size + b.bytes > config.maxRequestBytes()) {
                    again = true;   // the next round
                    continue;
                }
                sizes.put(leader, size + b.bytes);
                byNode.computeIfAbsent(leader, k -> new ArrayList<>()).add(b);
                b.inflight = true;
                ps.inFlight++;
                again = true;
            }
            byNode.forEach(this::sendRequest);
        } while (again);
        if (earliestRetry != Long.MAX_VALUE) {
            // a batch waits for its retry time: be there when it comes (nothing else would wake the sender)
            cluster.loop.schedule(this::schedule, Math.max(earliestRetry - now, 1_000_000), TimeUnit.NANOSECONDS);
        }
        for (String topic : needMeta) {
            if (refreshingLeaders.add(topic)) {
                cluster.refresh(List.of(topic)).whenComplete((v, err) -> {
                    refreshingLeaders.remove(topic);
                    cluster.loop.schedule(this::schedule, cluster.config.retryBackoff().delayFor(1).toNanos(), TimeUnit.NANOSECONDS);
                });
            }
        }
    }

    private void sendRequest(int node, List<Batch> batches) {
        for (Batch b : batches) {
            if (b.linger != null) {
                b.linger.cancel();
            }
            b.sealed = true;
        }
        Map<String, List<Batch>> byTopic = new LinkedHashMap<>();
        for (Batch b : batches) {
            byTopic.computeIfAbsent(b.tp.topic(), k -> new ArrayList<>()).add(b);
        }
        java.util.function.Consumer<Wire.Writer> body = w -> {
            w.str(null).i16(config.acks()).i32(30_000).i32(byTopic.size());
            byTopic.forEach((topic, bs) -> {
                w.str(topic).i32(bs.size());
                for (Batch b : bs) {
                    w.i32(b.tp.partition()).bytes(b.encoded);
                }
            });
        };
        if (config.acks() == 0) {
            cluster.broker(node).whenComplete((br, err) -> {
                if (err != null) {
                    for (Batch b : batches) {
                        failOrRetry(b, Cluster.unwrap(err));
                    }
                    return;
                }
                try {
                    br.sendNoReply(Broker.Api.PRODUCE, 3, body);
                    for (Batch b : batches) {
                        complete(b, -1, -1);
                    }
                    kick();
                } catch (KafkaException e) {
                    for (Batch b : batches) {
                        failOrRetry(b, e);
                    }
                }
            });
            return;
        }
        int hint = 64;
        for (Batch b : batches) {
            hint += b.encoded.length + 16 + b.tp.topic().length();
        }
        cluster.request(node, Broker.Api.PRODUCE, 3, hint, body).whenComplete((r, err) -> {
            if (err != null) {
                Throwable cause = Cluster.unwrap(err);
                cluster.forgetConnectionIfLost(node, cause);
                for (Batch b : batches) {
                    failOrRetry(b, cause);
                }
                return;
            }
            try {
                Map<TopicPartition, int[]> errors = new HashMap<>();
                Map<TopicPartition, long[]> offsets = new HashMap<>();
                int nt = r.arrayLength();
                for (int i = 0; i < nt; i++) {
                    String topic = r.str();
                    int np = r.arrayLength();
                    for (int p = 0; p < np; p++) {
                        int part = r.i32();
                        int code = r.i16();
                        long base = r.i64();
                        long ts = r.i64();
                        errors.put(new TopicPartition(topic, part), new int[] {code});
                        offsets.put(new TopicPartition(topic, part), new long[] {base, ts});
                    }
                }
                for (Batch b : batches) {
                    int[] code = errors.get(b.tp);
                    if (code == null) {
                        failOrRetry(b, new KafkaException("protocol error: no result for " + b.tp, false));
                    } else if (code[0] == 0) {
                        long[] o = offsets.get(b.tp);
                        complete(b, o[0], o[1]);
                    } else if (code[0] == KafkaException.DUPLICATE_SEQUENCE_NUMBER) {
                        complete(b, -1, -1);   // the broker has it already (a retry after a lost answer); where, it no longer says
                    } else {
                        failOrRetry(b, KafkaException.of(code[0], "produce to " + b.tp));
                    }
                }
                kick();   // the slots these batches held are free: send what waits for them now, not on the next turn of the loop
            } catch (KafkaException e) {
                for (Batch b : batches) {
                    failOrRetry(b, e);
                }
            }
        });
    }

    private void complete(Batch b, long base, long logAppendTime) {
        Part ps = parts.get(b.tp);
        ps.batches.remove(b);
        ps.inFlight--;
        b.inflight = false;
        if (b.pid >= 0) {
            ps.ackedPid = b.pid;
        }
        if (ps.inFlight == 0) {
            ps.retryAt = 0;
        }
        for (int i = 0; i < b.futures.size(); i++) {
            long ts = logAppendTime > 0 ? logAppendTime : b.records.get(i).timestamp();
            b.futures.get(i).complete(new RecordMetadata(b.tp.topic(), b.tp.partition(), base < 0 ? -1 : base + i, ts));
        }
        buffered.addAndGet(-b.bytes);
        unfinished -= b.records.size();
        mOk.add(b.records.size());
        mBytes.add(b.wireBytes);
        mBatchTime.end(b.created);
        checkFlush();
    }

    private void failOrRetry(Batch b, Throwable cause) {
        KafkaException e = cause instanceof KafkaException k ? k : new KafkaException("produce failed: " + cause, true, cause);
        Part ps = parts.get(b.tp);
        b.inflight = false;
        ps.inFlight--;
        int code = e.code();
        boolean retriable = e.retriable();
        if (config.idempotent()) {
            if (code == KafkaException.OUT_OF_ORDER_SEQUENCE_NUMBER) {
                retriable = true;   // an earlier batch of the partition failed; this one goes again after it
            } else if (code == KafkaException.UNKNOWN_PRODUCER_ID || code == KafkaException.INVALID_PRODUCER_EPOCH) {
                // the broker forgot our id (or fenced it): it never stored this batch, so it can go again under a new id
                needNewId = true;
                b.pid = -1;
                b.seq = -1;
                b.epoch = -1;
                b.encoded = null;
                retriable = true;
            }
        }
        boolean retry = retriable && b.attempts < config.retries() && System.nanoTime() < b.deadline && !closed;
        if (retry) {
            mRetries.increment();
            b.attempts++;
            ps.recovering = true;   // what is still in flight answers first, then everything unacknowledged is sent again in order
            ps.retryAt = System.nanoTime() + cluster.config.retryBackoff().delayFor(b.attempts).toNanos();
            cluster.refresh(List.of(b.tp.topic())).whenComplete((v, err) -> schedule());   // sendReady waits for retryAt itself
            return;
        }
        dropBatch(ps, b, e);
        if (config.idempotent() && b.seq >= 0) {
            // this batch leaves a hole in the sequence of the partition, so the batches after it that already have numbers would be refused for ever: fail them
            // with it and take a new producer id for what comes next
            needNewId = true;
            for (Batch other : new ArrayList<>(ps.batches)) {
                if (!other.inflight && other.seq >= 0) {
                    dropBatch(ps, other, e);
                }
            }
        }
        schedule();
    }

    private void checkFlush() {
        if (unfinished <= 0 && inboxEmpty()) {
            flushing = false;
            List<CompletableFuture<Void>> w = new ArrayList<>(flushWaiters);
            flushWaiters.clear();
            w.forEach(f -> f.complete(null));
        }
    }

    /** Completes when every record accepted so far (and any sent meanwhile) has been acknowledged or has failed. Batches are sent without waiting for the linger. */
    public CompletableFuture<Void> flush() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        try {
            cluster.loop.execute(() -> {
                drainInbox();
                if (unfinished <= 0) {
                    f.complete(null);
                    return;
                }
                flushing = true;
                flushWaiters.add(f);
                schedule();
            });
        } catch (RuntimeException e) {
            f.complete(null);
        }
        return f;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        try {
            flush().get(cluster.config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception ignored) {
            // what is still waiting fails below
        }
        closed = true;
        try {
            cluster.loop.execute(() -> {
                KafkaException e = new KafkaException("the producer is closed", false);
                for (Part ps : parts.values()) {
                    for (Batch b : ps.batches) {
                        b.futures.forEach(f -> f.completeExceptionally(e));
                    }
                }
                parts.clear();
                parked.values().forEach(l -> l.forEach(p -> p.future.completeExceptionally(e)));
                parked.clear();
                failInbox(e);
            });
        } catch (RuntimeException ignored) {
            // the loop is gone
        }
    }

    /** The partitioner of the Java client, so that records with the same key land on the same partition whichever client wrote them. */
    static int murmur2(byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        final int m = 0x5bd1e995;
        final int r = 24;
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++) {
            int i4 = i * 4;
            int k = (data[i4] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }
}
