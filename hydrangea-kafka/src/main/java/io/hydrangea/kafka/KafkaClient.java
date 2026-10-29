package io.hydrangea.kafka;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Entry point: holds the settings for reaching the cluster and makes producers, consumers and does the few admin calls. Every producer and consumer has connections of its
 * own (a broker answers one request per connection at a time, so a fetch waiting for data would hold up a produce on a shared connection).
 */
public final class KafkaClient implements AutoCloseable {

    private final EventLoopGroup group;
    private final KafkaConfig config;
    private final List<AutoCloseable> made = new java.util.concurrent.CopyOnWriteArrayList<>();

    private KafkaClient(EventLoopGroup group, KafkaConfig config) {
        this.group = group;
        this.config = config;
    }

    public static KafkaClient create(EventLoopGroup group, KafkaConfig config) {
        return new KafkaClient(group, config);
    }

    public KafkaProducer producer(ProducerConfig producerConfig) {
        EventLoop loop = group.next();
        Cluster c = new Cluster(config, loop);
        KafkaProducer p = new KafkaProducer(c, producerConfig);
        made.add(p);
        return p;
    }

    public KafkaConsumer consumer(ConsumerConfig consumerConfig) {
        EventLoop loop = group.next();
        Cluster c = new Cluster(config, loop);
        KafkaConsumer k = new KafkaConsumer(c, consumerConfig);
        made.add(k);
        return k;
    }

    /** Creates a topic (fails with code 36, TOPIC_ALREADY_EXISTS, if there is one). */
    public CompletableFuture<Void> createTopic(String name, int partitions, int replication) {
        EventLoop loop = group.next();
        Cluster c = new Cluster(config, loop);
        CompletableFuture<Void> f = new CompletableFuture<>();
        loop.execute(() -> c.requestAny(Broker.Api.CREATE_TOPICS, 2, w -> {
            w.i32(1).str(name).i32(partitions).i16(replication).i32(0).i32(0).i32(30_000).i8(0);
        }).whenComplete((r, e) -> {
            c.close();
            if (e != null) {
                f.completeExceptionally(Cluster.unwrap(e));
                return;
            }
            try {
                r.i32();
                int n = r.arrayLength();
                for (int i = 0; i < n; i++) {
                    r.str();
                    int err = r.i16();
                    String msg = r.str();
                    if (err != 0) {
                        f.completeExceptionally(new KafkaException("create topic " + name + ": " + KafkaException.name(err) + (msg != null ? " (" + msg + ")" : ""), err, false, null));
                        return;
                    }
                }
                f.complete(null);
            } catch (RuntimeException ex) {
                f.completeExceptionally(ex);
            }
        }));
        return f;
    }

    /** The number of partitions of a topic. */
    public CompletableFuture<Integer> partitionsFor(String topic) {
        EventLoop loop = group.next();
        Cluster c = new Cluster(config, loop);
        CompletableFuture<Integer> f = new CompletableFuture<>();
        loop.execute(() -> c.refresh(List.of(topic)).whenComplete((v, e) -> {
            Cluster.Topic t = c.topic(topic);
            c.close();
            if (e != null) {
                f.completeExceptionally(Cluster.unwrap(e));
            } else if (t == null || t.error() != 0) {
                f.completeExceptionally(KafkaException.of(t == null ? KafkaException.UNKNOWN_TOPIC_OR_PARTITION : t.error(), "topic " + topic));
            } else {
                f.complete(t.partitions().size());
            }
        }));
        return f;
    }

    /** Closes every producer and consumer made here. */
    @Override
    public void close() {
        for (AutoCloseable a : made) {
            try {
                a.close();
            } catch (Exception ignored) {
                // closing
            }
        }
        made.clear();
    }
}
