package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.kafka.ConsumerConfig;
import io.hydrangea.kafka.ConsumerRecord;
import io.hydrangea.kafka.KafkaClient;
import io.hydrangea.kafka.KafkaConfig;
import io.hydrangea.kafka.KafkaConsumer;
import io.hydrangea.kafka.KafkaProducer;
import io.hydrangea.kafka.ProducerConfig;
import io.hydrangea.kafka.RebalanceListener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Kafka: create a topic, send records from any number of threads through one producer, read them back as a member of a consumer group and commit.
 * <pre>
 * sh hydrangea-kafka/src/test/resources/kafka/start-kafka.sh      (or any broker; auto-create of topics is not needed)
 * java -cp target/classes io.hydrangea.examples.KafkaExample [host:port]
 * </pre>
 */
public final class KafkaExample {

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:29092";
        try (EventLoopGroup group = new EventLoopGroup(1); KafkaClient kafka = KafkaClient.create(group, KafkaConfig.of(bootstrap))) {
            String topic = "example-" + System.currentTimeMillis();
            kafka.createTopic(topic, 3, 1).get(30, TimeUnit.SECONDS);

            KafkaProducer producer = kafka.producer(ProducerConfig.defaults());   // idempotent by default: no duplicates, order kept within a partition
            List<CompletableFuture<?>> sent = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                sent.add(producer.send(topic, "user-" + (i % 10), "event " + i));   // the same key always lands on the same partition
            }
            for (CompletableFuture<?> f : sent) {
                f.get(30, TimeUnit.SECONDS);
            }
            System.out.println("sent 1000, e.g. " + sent.get(0).get());

            KafkaConsumer consumer = kafka.consumer(ConsumerConfig.defaults().withGroup("example-group"));
            consumer.subscribe(List.of(topic), new RebalanceListener() {
                @Override
                public void assigned(java.util.Collection<io.hydrangea.kafka.TopicPartition> partitions) {
                    System.out.println("assigned " + partitions);
                }
            }).get(30, TimeUnit.SECONDS);
            int read = 0;
            while (read < 1000) {
                List<ConsumerRecord> records = consumer.poll(Duration.ofSeconds(2)).get();
                read += records.size();
            }
            consumer.commit().get(10, TimeUnit.SECONDS);
            System.out.println("read and committed " + read);
            consumer.close();
            producer.close();
        }
    }
}
