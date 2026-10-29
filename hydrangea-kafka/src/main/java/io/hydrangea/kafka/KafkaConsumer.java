package io.hydrangea.kafka;

import io.hydrangea.Latency;
import io.hydrangea.Metrics;
import io.hydrangea.Scheduled;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Reads records. With {@link #assign} you choose the partitions; with {@link #subscribe} (and a {@code groupId}) the group coordinator shares the topics' partitions among
 * the consumers of the group (the "range" strategy, eager rebalancing: all members give up their partitions and take new ones). Records are fetched ahead of
 * {@link #poll} up to {@link ConsumerConfig#bufferedRecords()}, then fetching pauses. {@code poll} returns a future so that thousands of virtual threads can wait on it.
 * <p>
 * <b>Offsets.</b> {@link #commit()} commits the position after the last record {@code poll} returned (not after what is merely buffered), so a crash replays at most
 * what was returned and not committed: at-least-once. A rebalance commits the same before giving partitions up.
 * <p>
 * Everything runs on one event loop; listener callbacks and futures complete there. Not covered: cooperative rebalancing, static membership, regex subscriptions, a
 * rebalance when a topic gains partitions, rack awareness, read-committed isolation (it reads uncommitted records and skips transaction markers).
 */
public final class KafkaConsumer implements AutoCloseable {

    private final Cluster cluster;
    private final ConsumerConfig config;

    private static final class PState {
        long fetchPos = -1;        // next offset to ask the broker for
        long consumePos = -1;      // next offset poll will return
        long committed = -1;       // what was last committed (to skip commits that change nothing)
        int epoch;                 // bumped when the position is changed from outside: older fetch answers are dropped
        boolean resolving;
        long hwm = -1;             // the broker's high watermark as of the last fetch answer
        long lagReported;          // what this partition has added to the lag gauge
    }

    // loop thread only
    private final Map<TopicPartition, PState> parts = new LinkedHashMap<>();
    private final ArrayDeque<ConsumerRecord> buffer = new ArrayDeque<>();
    private final Set<Integer> fetching = new HashSet<>();
    private CompletableFuture<List<ConsumerRecord>> waiting;
    private Scheduled waitTimer;
    private boolean closed;
    private boolean failed;

    // group state
    private RebalanceListener listener = new RebalanceListener() { };
    private List<String> topics = List.of();
    private String memberId = "";
    private int generation = -1;
    private enum G { IDLE, JOINING, STABLE }
    private G gstate = G.IDLE;
    private int joinRound;
    private Broker coordinator;
    private Scheduled heartbeatTimer;
    private Scheduled commitTimer;
    private CompletableFuture<Void> assignedFuture;

    // metrics: counted per fetch answer / per poll batch, not per record
    private final Metrics.Count mRecords;
    private final Metrics.Count mBytes;
    private final Metrics.Count mFetches;
    private final Metrics.Count mFetchErrors;
    private final Metrics.Count mLag;
    private final Metrics.Count mRebalances;
    private final Latency mRebalanceTime;
    private final Metrics.Count mCommitsOk;
    private final Metrics.Count mCommitsFailed;
    private final Metrics.Count mHeartbeatsFailed;
    private long rebalanceStart;

    KafkaConsumer(Cluster cluster, ConsumerConfig config) {
        this.cluster = cluster;
        this.config = config;
        Metrics m = cluster.loop.metrics();
        mRecords = m.counter("kafka.consumer.records", null);
        mBytes = m.counter("kafka.consumer.bytes", "bytes");
        mFetches = m.counter("kafka.consumer.fetches", null);
        mFetchErrors = m.counter("kafka.consumer.fetch.errors", null);
        mLag = m.gauge("kafka.consumer.lag", null);
        mRebalances = m.counter("kafka.consumer.rebalances", null);
        mRebalanceTime = m.latency("kafka.consumer.rebalance.duration");
        mCommitsOk = m.counter("kafka.consumer.commits", null, "outcome", "ok");
        mCommitsFailed = m.counter("kafka.consumer.commits", null, "outcome", "failed");
        mHeartbeatsFailed = m.counter("kafka.consumer.heartbeat.failures", null);
    }

    /** Brings the partition's share of the lag gauge up to date: the broker's high watermark minus the next offset poll will return. */
    private void updateLag(PState p) {
        long lag = p.hwm < 0 || p.consumePos < 0 ? 0 : Math.max(0, p.hwm - p.consumePos);
        if (lag != p.lagReported) {
            mLag.add(lag - p.lagReported);
            p.lagReported = lag;
        }
    }

    /** Forgets the assignment; its lag leaves the gauge with it. */
    private void clearParts() {
        for (PState p : parts.values()) {
            mLag.add(-p.lagReported);
            p.lagReported = 0;
        }
        parts.clear();
    }

    private void onLoop(Runnable r) {
        if (cluster.loop.inEventLoop()) {
            r.run();
        } else {
            cluster.loop.execute(r);
        }
    }

    // ---- assign / seek ----

    /** Reads these partitions, without a group. Replaces any earlier assignment. Starts where {@link #seek} says, else at the start or end (per config). */
    public CompletableFuture<Void> assign(Collection<TopicPartition> partitions) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            if (config.groupId() != null && gstate != G.IDLE) {
                f.completeExceptionally(new IllegalStateException("subscribed to a group"));
                return;
            }
            Set<String> names = new HashSet<>();
            partitions.forEach(tp -> names.add(tp.topic()));
            cluster.refresh(names).whenComplete((v, e) -> onLoop(() -> {
                if (e != null) {
                    f.completeExceptionally(Cluster.unwrap(e));
                    return;
                }
                for (TopicPartition tp : partitions) {
                    Cluster.Topic t = cluster.topic(tp.topic());
                    if (t == null || t.error() != 0 || tp.partition() >= t.partitions().size()) {
                        f.completeExceptionally(KafkaException.of(t == null || t.error() == 0 ? KafkaException.UNKNOWN_TOPIC_OR_PARTITION : t.error(), "assign " + tp));
                        return;
                    }
                }
                setAssignment(new ArrayList<>(partitions)).whenComplete((x, err) -> {
                    if (err != null) f.completeExceptionally(Cluster.unwrap(err)); else f.complete(null);
                });
            }));
        });
        return f;
    }

    /** Next record to read in this partition. */
    public void seek(TopicPartition tp, long offset) {
        onLoop(() -> {
            PState p = parts.get(tp);
            if (p == null) {
                throw new IllegalStateException(tp + " is not assigned");
            }
            p.fetchPos = offset;
            p.consumePos = offset;
            p.epoch++;
            buffer.removeIf(r -> r.topic().equals(tp.topic()) && r.partition() == tp.partition());
            pump();
        });
    }

    /** The assigned partitions (a copy). */
    public CompletableFuture<Set<TopicPartition>> assignment() {
        CompletableFuture<Set<TopicPartition>> f = new CompletableFuture<>();
        onLoop(() -> f.complete(new java.util.LinkedHashSet<>(parts.keySet())));
        return f;
    }

    /** Position (the next offset {@code poll} will return) of an assigned partition, or -1. */
    public CompletableFuture<Long> position(TopicPartition tp) {
        CompletableFuture<Long> f = new CompletableFuture<>();
        onLoop(() -> {
            PState p = parts.get(tp);
            f.complete(p == null ? -1L : p.consumePos);
        });
        return f;
    }

    /** Takes {@code list} as the assignment: positions are looked up (committed offset with a group, else the config's start), then fetching starts. */
    private CompletableFuture<Void> setAssignment(List<TopicPartition> list) {
        clearParts();
        buffer.clear();
        fetching.clear();
        for (TopicPartition tp : list) {
            parts.put(tp, new PState());
        }
        return resolvePositions(new ArrayList<>(list)).thenRun(() -> onLoop(this::pump));
    }

    private CompletableFuture<Void> resolvePositions(List<TopicPartition> list) {
        if (list.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Map<TopicPartition, Long>> committed = config.groupId() != null && coordinator != null ? fetchCommitted(list)
                : CompletableFuture.completedFuture(Map.of());
        return committed.thenCompose(map -> {
            List<TopicPartition> need = new ArrayList<>();
            for (TopicPartition tp : list) {
                PState p = parts.get(tp);
                if (p == null) continue;
                Long off = map.get(tp);
                if (off != null && off >= 0) {
                    p.fetchPos = off;
                    p.consumePos = off;
                    p.committed = off;
                } else {
                    need.add(tp);
                }
            }
            return listOffsets(need, config.earliest() ? -2L : -1L).thenAccept(res -> {
                res.forEach((tp, off) -> {
                    PState p = parts.get(tp);
                    if (p != null) {
                        p.fetchPos = off;
                        p.consumePos = off;
                    }
                });
            });
        });
    }

    private CompletableFuture<Map<TopicPartition, Long>> listOffsets(List<TopicPartition> list, long time) {
        Map<Integer, List<TopicPartition>> byLeader = new HashMap<>();
        for (TopicPartition tp : list) {
            Cluster.Topic t = cluster.topic(tp.topic());
            int leader = t == null || tp.partition() >= t.partitions().size() ? -1 : t.partitions().get(tp.partition()).leader();
            if (leader < 0) {
                return CompletableFuture.failedFuture(KafkaException.of(KafkaException.LEADER_NOT_AVAILABLE, "no leader for " + tp));
            }
            byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(tp);
        }
        Map<TopicPartition, Long> out = new HashMap<>();
        CompletableFuture<Void> all = CompletableFuture.completedFuture(null);
        for (var e : byLeader.entrySet()) {
            List<TopicPartition> tps = e.getValue();
            all = all.thenCompose(x -> cluster.request(e.getKey(), Broker.Api.LIST_OFFSETS, 1, w -> {
                w.i32(-1);
                Map<String, List<Integer>> bt = new LinkedHashMap<>();
                tps.forEach(tp -> bt.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition()));
                w.i32(bt.size());
                bt.forEach((topic, ps) -> {
                    w.str(topic).i32(ps.size());
                    ps.forEach(p -> w.i32(p).i64(time));
                });
            }).thenAccept(r -> {
                int nt = r.arrayLength();
                for (int i = 0; i < nt; i++) {
                    String topic = r.str();
                    int np = r.arrayLength();
                    for (int p = 0; p < np; p++) {
                        int part = r.i32();
                        int err = r.i16();
                        r.i64();
                        long off = r.i64();
                        if (err != 0) {
                            throw KafkaException.of(err, "list offsets " + topic + "-" + part);
                        }
                        out.put(new TopicPartition(topic, part), off);
                    }
                }
            }));
        }
        return all.thenApply(x -> out);
    }

    // ---- fetching ----

    private void pump() {
        if (closed || failed || gstate == G.JOINING) {
            return;
        }
        if (buffer.size() >= config.bufferedRecords()) {
            return;
        }
        Map<Integer, List<TopicPartition>> byLeader = new LinkedHashMap<>();
        boolean needMeta = false;
        for (var e : parts.entrySet()) {
            PState p = e.getValue();
            if (p.fetchPos < 0) {
                continue;
            }
            TopicPartition tp = e.getKey();
            Cluster.Topic t = cluster.topic(tp.topic());
            int leader = t == null || tp.partition() >= t.partitions().size() ? -1 : t.partitions().get(tp.partition()).leader();
            if (leader < 0 || cluster.node(leader) == null) {
                needMeta = true;
                continue;
            }
            if (!fetching.contains(leader)) {
                byLeader.computeIfAbsent(leader, k -> new ArrayList<>()).add(tp);
            }
        }
        if (needMeta) {
            Set<String> names = new HashSet<>();
            parts.keySet().forEach(tp -> names.add(tp.topic()));
            cluster.refresh(names).whenComplete((v, err) -> cluster.loop.schedule(this::pump, 200, TimeUnit.MILLISECONDS));
        }
        byLeader.forEach(this::fetch);
    }

    private void fetch(int node, List<TopicPartition> tps) {
        fetching.add(node);
        Map<TopicPartition, long[]> asked = new HashMap<>();   // position and epoch at the time of the request
        for (TopicPartition tp : tps) {
            PState p = parts.get(tp);
            asked.put(tp, new long[] {p.fetchPos, p.epoch});
        }
        int perPartition = config.partitionMaxBytes();
        cluster.request(node, Broker.Api.FETCH, 4, w -> {
            w.i32(-1).i32((int) config.fetchMaxWait().toMillis()).i32(config.fetchMinBytes()).i32(config.fetchMaxBytes()).i8(0);
            Map<String, List<TopicPartition>> bt = new LinkedHashMap<>();
            tps.forEach(tp -> bt.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp));
            w.i32(bt.size());
            bt.forEach((topic, ps) -> {
                w.str(topic).i32(ps.size());
                for (TopicPartition tp : ps) {
                    w.i32(tp.partition()).i64(asked.get(tp)[0]).i32(perPartition);
                }
            });
        }).whenComplete((r, err) -> {
            fetching.remove(node);
            if (closed) {
                return;
            }
            if (err != null) {
                Throwable c = Cluster.unwrap(err);
                cluster.forgetConnectionIfLost(node, c);
                cluster.loop.schedule(this::pump, cluster.config.retryBackoff().delayFor(1).toNanos(), TimeUnit.NANOSECONDS);
                return;
            }
            try {
                onFetched(node, r, asked);
            } catch (KafkaException e) {
                fail(e);
            }
        });
    }

    private void onFetched(int node, Wire.Reader r, Map<TopicPartition, long[]> asked) {
        r.i32();   // throttle
        mFetches.increment();
        int nt = r.arrayLength();
        boolean refresh = false;
        List<TopicPartition> reset = new ArrayList<>();
        for (int i = 0; i < nt; i++) {
            String topic = r.str();
            int np = r.arrayLength();
            for (int k = 0; k < np; k++) {
                int part = r.i32();
                int err = r.i16();
                long hwm = r.i64();   // high watermark
                r.i64();   // last stable offset
                int na = r.i32();
                for (int a = 0; a < Math.max(na, 0); a++) {
                    r.i64();
                    r.i64();
                }
                byte[] data = r.bytes();
                TopicPartition tp = new TopicPartition(topic, part);
                PState p = parts.get(tp);
                long[] was = asked.get(tp);
                if (p == null || was == null || p.epoch != was[1] || p.fetchPos != was[0]) {
                    continue;   // moved or revoked meanwhile
                }
                if (err != 0) {
                    mFetchErrors.increment();
                }
                if (err == KafkaException.OFFSET_OUT_OF_RANGE) {
                    reset.add(tp);
                    continue;
                }
                if (err != 0) {
                    if (KafkaException.retriableCode(err)) {
                        refresh = true;
                        continue;
                    }
                    throw KafkaException.of(err, "fetch " + tp);
                }
                p.hwm = hwm;
                updateLag(p);
                if (data == null || data.length == 0) {
                    continue;
                }
                mBytes.add(data.length);
                long last = Records.decode(topic, part, data, p.fetchPos, 256 * 1024 * 1024, buffer);
                if (last >= 0) {
                    p.fetchPos = last + 1;
                }
            }
        }
        if (refresh) {
            Set<String> names = new HashSet<>();
            parts.keySet().forEach(tp -> names.add(tp.topic()));
            cluster.refresh(names).whenComplete((v, e) -> cluster.loop.schedule(this::pump, 100, TimeUnit.MILLISECONDS));
        }
        if (!reset.isEmpty()) {
            for (TopicPartition tp : reset) {
                parts.get(tp).fetchPos = -1;
            }
            listOffsets(reset, config.earliest() ? -2L : -1L).whenComplete((res, e) -> onLoop(() -> {
                if (e != null) {
                    fail(Cluster.unwrap(e));
                    return;
                }
                res.forEach((tp, off) -> {
                    PState p = parts.get(tp);
                    if (p != null) {
                        p.fetchPos = off;
                        p.consumePos = off;
                        p.epoch++;
                    }
                });
                pump();
            }));
        }
        deliver();
        pump();
    }

    private void fail(Throwable cause) {
        if (failed || closed) {
            return;
        }
        failed = true;
        KafkaException e = cause instanceof KafkaException k ? k : new KafkaException("consumer failed: " + cause, false, cause);
        if (waiting != null) {
            var f = waiting;
            waiting = null;
            if (waitTimer != null) waitTimer.cancel();
            f.completeExceptionally(e);
        }
        try {
            listener.failed(e);
        } catch (RuntimeException ignored) {
            // the listener's problem
        }
    }

    // ---- poll ----

    /** Completes with up to {@code maxPollRecords} records as soon as there are some, or empty after {@code timeout}. One poll at a time. */
    public CompletableFuture<List<ConsumerRecord>> poll(Duration timeout) {
        CompletableFuture<List<ConsumerRecord>> f = new CompletableFuture<>();
        onLoop(() -> {
            if (closed) {
                f.completeExceptionally(new KafkaException("the consumer is closed", false));
                return;
            }
            if (failed) {
                f.completeExceptionally(new KafkaException("the consumer has failed", false));
                return;
            }
            if (waiting != null) {
                f.completeExceptionally(new IllegalStateException("another poll is waiting"));
                return;
            }
            waiting = f;
            if (!buffer.isEmpty()) {
                deliver();
                return;
            }
            waitTimer = cluster.loop.schedule(() -> {
                if (waiting == f) {
                    waiting = null;
                    f.complete(List.of());
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
        });
        return f;
    }

    private void deliver() {
        if (waiting == null || buffer.isEmpty() || gstate == G.JOINING) {
            return;
        }
        int n = Math.min(config.maxPollRecords(), buffer.size());
        List<ConsumerRecord> out = new ArrayList<>(n);
        // consecutive records are nearly always of the same partition: look its state up once per run, not once per record
        String runTopic = null;
        int runPartition = -1;
        PState p = null;
        for (int i = 0; i < n; i++) {
            ConsumerRecord r = buffer.poll();
            if (r.topic() != runTopic || r.partition() != runPartition || p == null) {
                if (p != null) {
                    updateLag(p);
                }
                runTopic = r.topic();
                runPartition = r.partition();
                p = parts.get(new TopicPartition(runTopic, runPartition));
            }
            if (p == null) {
                continue;
            }
            p.consumePos = r.offset() + 1;
            out.add(r);
        }
        if (p != null) {
            updateLag(p);
        }
        mRecords.add(out.size());
        var f = waiting;
        waiting = null;
        if (waitTimer != null) {
            waitTimer.cancel();
        }
        f.complete(out);
        pump();
    }

    // ---- committing ----

    /** Commits the position after the last record returned by {@code poll}, for every assigned partition. Needs a group. */
    public CompletableFuture<Void> commit() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            Map<TopicPartition, Long> offs = new LinkedHashMap<>();
            parts.forEach((tp, p) -> {
                if (p.consumePos >= 0 && p.consumePos != p.committed) {
                    offs.put(tp, p.consumePos);
                }
            });
            commitOffsets(offs).whenComplete((v, e) -> {
                if (e != null) f.completeExceptionally(Cluster.unwrap(e)); else f.complete(null);
            });
        });
        return f;
    }

    /** Commits these "next offsets" (the offset after the last record processed). */
    public CompletableFuture<Void> commit(Map<TopicPartition, Long> nextOffsets) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> commitOffsets(nextOffsets).whenComplete((v, e) -> {
            if (e != null) f.completeExceptionally(Cluster.unwrap(e)); else f.complete(null);
        }));
        return f;
    }

    private CompletableFuture<Void> commitOffsets(Map<TopicPartition, Long> offs) {
        CompletableFuture<Void> f = commitOffsets0(offs);
        if (!offs.isEmpty()) {
            f.whenComplete((v, e) -> (e == null ? mCommitsOk : mCommitsFailed).increment());
        }
        return f;
    }

    private CompletableFuture<Void> commitOffsets0(Map<TopicPartition, Long> offs) {
        if (config.groupId() == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("no group id: offsets cannot be committed"));
        }
        if (offs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        if (coordinator == null || gstate != G.STABLE) {
            return CompletableFuture.failedFuture(new KafkaException("the group is rebalancing or not joined", true));
        }
        Map<String, List<Map.Entry<TopicPartition, Long>>> bt = new LinkedHashMap<>();
        offs.entrySet().forEach(e -> bt.computeIfAbsent(e.getKey().topic(), k -> new ArrayList<>()).add(e));
        int gen = generation;
        String member = memberId;
        return coordinator.send(Broker.Api.OFFSET_COMMIT, 2, w -> {
            w.str(config.groupId()).i32(gen).str(member).i64(-1).i32(bt.size());
            bt.forEach((topic, es) -> {
                w.str(topic).i32(es.size());
                es.forEach(e -> w.i32(e.getKey().partition()).i64(e.getValue()).str(null));
            });
        }).thenAccept(r -> {
            int nt = r.arrayLength();
            for (int i = 0; i < nt; i++) {
                String topic = r.str();
                int np = r.arrayLength();
                for (int k = 0; k < np; k++) {
                    int part = r.i32();
                    int err = r.i16();
                    if (err != 0) {
                        handleGroupError(err);
                        throw KafkaException.of(err, "commit " + topic + "-" + part);
                    }
                    PState p = parts.get(new TopicPartition(topic, part));
                    if (p != null) {
                        p.committed = offs.get(new TopicPartition(topic, part));
                    }
                }
            }
        });
    }

    private CompletableFuture<Map<TopicPartition, Long>> fetchCommitted(List<TopicPartition> list) {
        Map<String, List<Integer>> bt = new LinkedHashMap<>();
        list.forEach(tp -> bt.computeIfAbsent(tp.topic(), k -> new ArrayList<>()).add(tp.partition()));
        return coordinator.send(Broker.Api.OFFSET_FETCH, 1, w -> {
            w.str(config.groupId()).i32(bt.size());
            bt.forEach((topic, ps) -> {
                w.str(topic).i32(ps.size());
                ps.forEach(w::i32);
            });
        }).thenApply(r -> {
            Map<TopicPartition, Long> out = new HashMap<>();
            int nt = r.arrayLength();
            for (int i = 0; i < nt; i++) {
                String topic = r.str();
                int np = r.arrayLength();
                for (int k = 0; k < np; k++) {
                    int part = r.i32();
                    long off = r.i64();
                    r.str();
                    int err = r.i16();
                    if (err != 0) {
                        throw KafkaException.of(err, "fetch committed offset of " + topic + "-" + part);
                    }
                    out.put(new TopicPartition(topic, part), off);
                }
            }
            return out;
        });
    }

    // ---- the group ----

    /**
     * Joins the group and takes the partitions it is given; the future completes when the first assignment has been taken (positions resolved).
     * Needs {@code groupId}. A rebalance later calls the listener.
     */
    public CompletableFuture<Void> subscribe(Collection<String> topicNames, RebalanceListener rebalanceListener) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            if (config.groupId() == null) {
                f.completeExceptionally(new IllegalStateException("subscribe needs a groupId"));
                return;
            }
            if (gstate != G.IDLE || !parts.isEmpty()) {
                f.completeExceptionally(new IllegalStateException("already subscribed or assigned"));
                return;
            }
            topics = List.copyOf(new java.util.TreeSet<>(topicNames));
            listener = rebalanceListener != null ? rebalanceListener : new RebalanceListener() { };
            assignedFuture = f;
            rejoin();
            if (config.autoCommit() != null) {
                scheduleAutoCommit();
            }
        });
        return f;
    }

    public CompletableFuture<Void> subscribe(Collection<String> topicNames) {
        return subscribe(topicNames, null);
    }

    private void scheduleAutoCommit() {
        commitTimer = cluster.loop.schedule(() -> {
            if (closed) return;
            if (gstate == G.STABLE) {
                commit().whenComplete((v, e) -> scheduleAutoCommit());
            } else {
                scheduleAutoCommit();
            }
        }, config.autoCommit().toNanos(), TimeUnit.NANOSECONDS);
    }

    private void handleGroupError(int err) {
        switch (err) {
            case KafkaException.REBALANCE_IN_PROGRESS -> rejoin();
            case KafkaException.ILLEGAL_GENERATION, KafkaException.UNKNOWN_MEMBER_ID, KafkaException.FENCED_INSTANCE_ID -> {
                memberId = "";
                rejoin();
            }
            case KafkaException.NOT_COORDINATOR, KafkaException.COORDINATOR_NOT_AVAILABLE, KafkaException.COORDINATOR_LOAD_IN_PROGRESS -> {
                dropCoordinator();
                rejoin();
            }
            default -> { }
        }
    }

    private void dropCoordinator() {
        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }
    }

    /** Gives the partitions up (committing first) and joins again. Several calls in a row are one. */
    private void rejoin() {
        if (closed || failed || gstate == G.JOINING) {
            return;
        }
        gstate = G.JOINING;
        rebalanceStart = mRebalanceTime.begin();
        int round = ++joinRound;
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
        }
        List<TopicPartition> old = new ArrayList<>(parts.keySet());
        Map<TopicPartition, Long> toCommit = new LinkedHashMap<>();
        parts.forEach((tp, p) -> {
            if (p.consumePos >= 0 && p.consumePos != p.committed) toCommit.put(tp, p.consumePos);
        });
        CompletableFuture<Void> committed = CompletableFuture.completedFuture(null);
        if (coordinator != null && !toCommit.isEmpty() && generation >= 0) {
            gstate = G.STABLE;   // commitOffsets needs it; set back right after the request is on its way
            CompletableFuture<Void> c = commitOffsets(toCommit).handle((v, e) -> null);
            gstate = G.JOINING;
            committed = c;
        }
        committed.thenRun(() -> onLoop(() -> {
            if (round != joinRound || closed) return;
            clearParts();
            buffer.clear();
            fetching.clear();
            if (!old.isEmpty()) {
                safely(() -> listener.revoked(old));
            }
            joinStep(round, 0);
        }));
    }

    private void safely(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            System.getLogger(KafkaConsumer.class.getName()).log(System.Logger.Level.WARNING, "rebalance listener failed", e);
        }
    }

    private void retryJoin(int round, int attempt, Throwable e) {
        KafkaException k = e instanceof KafkaException ke ? ke : new KafkaException("group join failed: " + e, true, e);
        if (round != joinRound || closed) return;
        if (!k.retriable() && k.code() != KafkaException.UNKNOWN_MEMBER_ID && k.code() != KafkaException.ILLEGAL_GENERATION && k.code() != KafkaException.REBALANCE_IN_PROGRESS) {
            gstate = G.IDLE;
            fail(k);
            if (assignedFuture != null) {
                assignedFuture.completeExceptionally(k);
                assignedFuture = null;
            }
            return;
        }
        if (k.code() == KafkaException.UNKNOWN_MEMBER_ID || k.code() == KafkaException.ILLEGAL_GENERATION) {
            memberId = "";
        }
        if (k.code() == KafkaException.NOT_COORDINATOR || k.code() == KafkaException.COORDINATOR_NOT_AVAILABLE || k.retriable()) {
            dropCoordinator();
        }
        cluster.loop.schedule(() -> joinStep(round, attempt + 1), cluster.config.retryBackoff().delayFor(Math.min(attempt + 1, 8)).toNanos(), TimeUnit.NANOSECONDS);
    }

    private void joinStep(int round, int attempt) {
        if (round != joinRound || closed) return;
        ensureCoordinator().thenCompose(b -> join(b)).thenCompose(this::sync).whenComplete((assignment, e) -> onLoop(() -> {
            if (round != joinRound || closed) return;
            if (e != null) {
                retryJoin(round, attempt, Cluster.unwrap(e));
                return;
            }
            Map<TopicPartition, PState> got = new LinkedHashMap<>();
            List<TopicPartition> list = new ArrayList<>(assignment);
            cluster.refresh(topics).whenComplete((v, me) -> onLoop(() -> {
                if (round != joinRound || closed) return;
                setAssignment(list).whenComplete((x, pe) -> onLoop(() -> {
                    if (round != joinRound || closed) return;
                    if (pe != null) {
                        retryJoin(round, attempt, Cluster.unwrap(pe));
                        return;
                    }
                    gstate = G.STABLE;
                    mRebalances.increment();
                    mRebalanceTime.end(rebalanceStart);
                    scheduleHeartbeat();
                    safely(() -> listener.assigned(list));
                    if (assignedFuture != null) {
                        assignedFuture.complete(null);
                        assignedFuture = null;
                    }
                    pump();
                }));
            }));
        }));
    }

    private CompletableFuture<Broker> ensureCoordinator() {
        if (coordinator != null && coordinator.isOnline()) {
            return CompletableFuture.completedFuture(coordinator);
        }
        return cluster.requestAny(Broker.Api.FIND_COORDINATOR, 1, w -> w.str(config.groupId()).i8(0)).thenCompose(r -> {
            r.i32();
            int err = r.i16();
            r.str();
            int id = r.i32();
            String host = r.str();
            int port = r.i32();
            if (err != 0) {
                throw KafkaException.of(err, "find coordinator of group " + config.groupId());
            }
            Cluster.Node n = new Cluster.Node(id, host, port);
            cluster.addNode(n);
            return cluster.dedicated(n);
        }).thenApply(b -> {
            coordinator = b;
            b.onLost(lost -> onLoop(() -> {
                if (coordinator == lost) {
                    coordinator = null;
                    if (gstate == G.STABLE && !closed) {
                        rejoin();
                    }
                }
            }));
            return b;
        });
    }

    private record Joined(Broker coordinator, int generation, String leader, Map<String, List<String>> members) {
    }

    private CompletableFuture<Joined> join(Broker b) {
        Wire.Writer meta = new Wire.Writer(64);
        meta.i16(0).i32(topics.size());
        topics.forEach(meta::str);
        meta.bytes(null);
        byte[] metadata = meta.toArray();
        return b.send(Broker.Api.JOIN_GROUP, 2, w -> {
            w.str(config.groupId()).i32((int) config.sessionTimeout().toMillis()).i32((int) config.rebalanceTimeout().toMillis()).str(memberId).str("consumer")
                    .i32(1).str("range").bytes(metadata);
        }).thenApply(r -> {
            r.i32();
            int err = r.i16();
            int gen = r.i32();
            r.str();
            String leader = r.str();
            String member = r.str();
            int n = r.arrayLength();
            Map<String, List<String>> members = new TreeMap<>();
            for (int i = 0; i < n; i++) {
                String id = r.str();
                byte[] md = r.bytes();
                Wire.Reader mr = new Wire.Reader(md == null ? new byte[0] : md);
                List<String> ts = new ArrayList<>();
                if (mr.remaining() >= 6) {
                    mr.i16();
                    int nt = mr.arrayLength();
                    for (int k = 0; k < nt; k++) ts.add(mr.str());
                }
                members.put(id, ts);
            }
            if (err != 0) {
                if (err == KafkaException.UNKNOWN_MEMBER_ID) {
                    memberId = "";
                }
                throw KafkaException.of(err, "join group " + config.groupId());
            }
            memberId = member;
            generation = gen;
            return new Joined(b, gen, leader, members);
        });
    }

    /** The leader assigns (range), everyone asks for its share. Returns the partitions assigned to this member. */
    private CompletableFuture<List<TopicPartition>> sync(Joined j) {
        boolean leader = j.leader.equals(memberId);
        CompletableFuture<Map<String, byte[]>> assignments;
        if (leader) {
            Set<String> all = new HashSet<>();
            j.members.values().forEach(all::addAll);
            assignments = cluster.refresh(all).thenApply(v -> assign(all, j.members));
        } else {
            assignments = CompletableFuture.completedFuture(Map.of());
        }
        return assignments.thenCompose(map -> j.coordinator.send(Broker.Api.SYNC_GROUP, 1, w -> {
            w.str(config.groupId()).i32(j.generation).str(memberId).i32(map.size());
            map.forEach((m, bytes) -> w.str(m).bytes(bytes));
        })).thenApply(r -> {
            r.i32();
            int err = r.i16();
            byte[] a = r.bytes();
            if (err != 0) {
                throw KafkaException.of(err, "sync group " + config.groupId());
            }
            List<TopicPartition> list = new ArrayList<>();
            if (a != null && a.length >= 6) {
                Wire.Reader ar = new Wire.Reader(a);
                ar.i16();
                int nt = ar.arrayLength();
                for (int i = 0; i < nt; i++) {
                    String topic = ar.str();
                    int np = ar.arrayLength();
                    for (int k = 0; k < np; k++) list.add(new TopicPartition(topic, ar.i32()));
                }
            }
            return list;
        });
    }

    /** The "range" assignor: for each topic, the members subscribed to it (by id) get consecutive partitions, the first ones one more when they do not divide evenly. */
    private Map<String, byte[]> assign(Set<String> allTopics, Map<String, List<String>> members) {
        Map<String, Map<String, List<Integer>>> per = new TreeMap<>();
        members.keySet().forEach(m -> per.put(m, new TreeMap<>()));
        for (String topic : new java.util.TreeSet<>(allTopics)) {
            Cluster.Topic t = cluster.topic(topic);
            if (t == null || t.error() != 0) {
                continue;
            }
            List<String> subs = new ArrayList<>();
            members.forEach((m, ts) -> {
                if (ts.contains(topic)) subs.add(m);
            });
            int n = t.partitions().size();
            int each = n / Math.max(subs.size(), 1);
            int extra = n % Math.max(subs.size(), 1);
            int next = 0;
            for (int i = 0; i < subs.size(); i++) {
                int count = each + (i < extra ? 1 : 0);
                List<Integer> mine = per.get(subs.get(i)).computeIfAbsent(topic, k -> new ArrayList<>());
                for (int k = 0; k < count; k++) mine.add(t.partitions().get(next++).partition());
            }
        }
        Map<String, byte[]> out = new LinkedHashMap<>();
        per.forEach((m, byTopic) -> {
            Wire.Writer w = new Wire.Writer(64);
            w.i16(0).i32(byTopic.size());
            byTopic.forEach((topic, ps) -> {
                w.str(topic).i32(ps.size());
                ps.forEach(w::i32);
            });
            w.bytes(null);
            out.put(m, w.toArray());
        });
        return out;
    }

    private void scheduleHeartbeat() {
        if (heartbeatTimer != null) heartbeatTimer.cancel();
        int round = joinRound;
        heartbeatTimer = cluster.loop.schedule(() -> {
            if (closed || round != joinRound || gstate != G.STABLE || coordinator == null) {
                return;
            }
            int gen = generation;
            String member = memberId;
            coordinator.send(Broker.Api.HEARTBEAT, 1, w -> w.str(config.groupId()).i32(gen).str(member)).whenComplete((r, e) -> onLoop(() -> {
                if (closed || round != joinRound) return;
                if (e != null) {
                    dropCoordinator();
                    rejoin();
                    return;
                }
                r.i32();
                int err = r.i16();
                if (err != 0) {
                    mHeartbeatsFailed.increment();
                    handleGroupError(err);
                    if (err != KafkaException.REBALANCE_IN_PROGRESS && err != KafkaException.ILLEGAL_GENERATION && err != KafkaException.UNKNOWN_MEMBER_ID
                            && err != KafkaException.NOT_COORDINATOR && err != KafkaException.COORDINATOR_NOT_AVAILABLE && err != KafkaException.COORDINATOR_LOAD_IN_PROGRESS) {
                        fail(KafkaException.of(err, "heartbeat"));
                    }
                    return;
                }
                scheduleHeartbeat();
            }));
        }, config.heartbeat().toNanos(), TimeUnit.NANOSECONDS);
    }

    @Override
    public void close() {
        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            cluster.loop.execute(() -> {
                if (closed) {
                    done.complete(null);
                    return;
                }
                CompletableFuture<Void> leave = CompletableFuture.completedFuture(null);
                if (config.autoCommit() != null && gstate == G.STABLE) {
                    leave = commit().handle((v, e) -> null);
                }
                leave.thenCompose(x -> {
                    closed = true;
                    if (heartbeatTimer != null) heartbeatTimer.cancel();
                    if (commitTimer != null) commitTimer.cancel();
                    if (waitTimer != null) waitTimer.cancel();
                    if (waiting != null) {
                        waiting.completeExceptionally(new KafkaException("the consumer is closed", false));
                        waiting = null;
                    }
                    if (coordinator != null && coordinator.isOnline() && !memberId.isEmpty()) {
                        String member = memberId;
                        return coordinator.send(Broker.Api.LEAVE_GROUP, 1, w -> w.str(config.groupId()).str(member)).handle((r, e) -> null);
                    }
                    return CompletableFuture.completedFuture(null);
                }).whenComplete((v, e) -> {
                    dropCoordinator();
                    cluster.close();
                    done.complete(null);
                });
            });
            done.get(5, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // the loop is gone or slow: the connections are closed with it
        }
    }
}
