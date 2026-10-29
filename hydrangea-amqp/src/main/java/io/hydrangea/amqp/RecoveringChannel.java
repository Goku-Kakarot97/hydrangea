package io.hydrangea.amqp;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A channel of a {@link RecoveringConnection}: the same operations as {@link AmqpChannel}, and it stays usable across reconnects.
 * What was set up through it (prefetch, confirm mode, consumers) is set up again on the new connection; the queues, exchanges and
 * bindings it declared are declared again by the connection. See {@link RecoveringConnection} for what the application still has to
 * handle (calls fail while the link is down; delivery tags; broker-named queues).
 * <p>
 * The delivery tags the application sees keep increasing across a reconnect, so a tag from before a break is never mistaken for a
 * tag of the new channel. Acknowledging such an old tag does nothing.
 */
public final class RecoveringChannel {

    private final RecoveringConnection conn;
    private volatile AmqpChannel delegate;

    private volatile long tagOffset;                          // added to the real delivery tag
    private final AtomicLong highestTag = new AtomicLong();   // highest tag the application has seen
    private final ConcurrentHashMap<String, ConsumerDecl> consumers = new ConcurrentHashMap<>();
    private volatile Integer prefetch;
    private volatile boolean confirms;
    private volatile Consumer<ReturnedMessage> returnListener;
    private volatile boolean userClosed;
    private volatile Throwable failed;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    RecoveringChannel(RecoveringConnection conn, AmqpChannel delegate) {
        this.conn = conn;
        this.delegate = delegate;
    }

    /** One consumer as the application registered it; also the object the broker's deliveries go to. */
    private final class ConsumerDecl implements AmqpConsumer {
        final String queue;
        final boolean autoAck;
        final boolean exclusive;
        final Map<String, Object> args;
        final AmqpConsumer user;
        volatile String tag;

        ConsumerDecl(String queue, boolean autoAck, boolean exclusive, Map<String, Object> args, AmqpConsumer user) {
            this.queue = queue;
            this.autoAck = autoAck;
            this.exclusive = exclusive;
            this.args = args;
            this.user = user;
        }

        @Override
        public void onDelivery(Delivery d) {
            user.onDelivery(visible(d));
        }

        @Override
        public void onCancel(String consumerTag, Throwable cause) {
            boolean connectionLevel = cause instanceof AmqpException a && a.isConnectionError();
            if (connectionLevel && conn.state() != RecoveringConnection.State.CLOSED) {
                return; // the link broke: the connection is recovering and will put this consumer back (or tell it, if it gives up)
            }
            if (consumers.remove(consumerTag, this)) {
                user.onCancel(consumerTag, cause);
            }
        }
    }

    private Delivery visible(Delivery d) {
        long tag = d.deliveryTag() + tagOffset;
        highestTag.accumulateAndGet(tag, Math::max);
        return new Delivery(d.consumerTag(), tag, d.redelivered(), d.exchange(), d.routingKey(), d.properties(), d.body());
    }

    /** The channel on the current connection, or null while there is none. */
    private AmqpChannel live() {
        AmqpChannel d = delegate;
        return failed == null && !userClosed && conn.state() == RecoveringConnection.State.CONNECTED && d.isOpen() ? d : null;
    }

    private AmqpException unavailable() {
        if (failed instanceof AmqpException a) {
            return a;
        }
        if (userClosed || failed != null) {
            return new AmqpException(AmqpException.CHANNEL_ERROR, "the channel is closed", false);
        }
        return conn.state() == RecoveringConnection.State.CONNECTED
                ? new AmqpException(AmqpException.CHANNEL_ERROR, "the channel is closed", false)
                : conn.unavailable();
    }

    private <T> CompletableFuture<T> with(Function<AmqpChannel, CompletableFuture<T>> call) {
        AmqpChannel d = live();
        return d == null ? CompletableFuture.failedFuture(unavailable()) : call.apply(d);
    }

    public boolean isOpen() {
        return live() != null;
    }

    /** False while the link is down (nothing can be published) or backed up; see {@link AmqpConnection#isWritable()}. */
    public boolean isWritable() {
        AmqpChannel d = live();
        return d != null && d.isWritable();
    }

    /**
     * Completes when publishing may continue ({@link AmqpConnection#whenWritable()}). Fails like any other call while the link is down, and
     * if the connection is lost while waiting; a producer treats that failure the way it treats a failed publish.
     */
    public CompletableFuture<Void> whenWritable() {
        return with(AmqpChannel::whenWritable);
    }

    /** The id on the current connection (changes after a recovery). */
    public int id() {
        return delegate.id();
    }

    /** Completes when the channel is closed for good (by {@link #close()}, a broker error on it, or the connection giving up). */
    public CompletableFuture<Void> closeFuture() {
        return closeFuture;
    }

    // ---- exchanges and queues ----

    public CompletableFuture<Void> exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        return with(d -> d.exchangeDeclare(exchange, type, durable, autoDelete, arguments)).thenApply(v -> {
            conn.recordExchange(new RecoveringConnection.ExchangeDecl(exchange, type, durable, autoDelete, arguments));
            return v;
        });
    }

    public CompletableFuture<Void> exchangeDelete(String exchange, boolean ifUnused) {
        return with(d -> d.exchangeDelete(exchange, ifUnused)).thenApply(v -> {
            conn.forgetExchange(exchange);
            return v;
        });
    }

    public CompletableFuture<AmqpChannel.QueueInfo> queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        return with(d -> d.queueDeclare(conn.alias(queue), durable, exclusive, autoDelete, arguments)).thenApply(info -> {
            conn.recordQueue(new RecoveringConnection.QueueDecl(queue, durable, exclusive, autoDelete, arguments, false));
            return info;
        });
    }

    /** A queue the broker names. After a recovery it has another name on the broker; keep using the name returned here. */
    public CompletableFuture<AmqpChannel.QueueInfo> queueDeclare() {
        return with(AmqpChannel::queueDeclare).thenApply(info -> {
            conn.recordQueue(new RecoveringConnection.QueueDecl(info.name(), false, true, true, null, true));
            return info;
        });
    }

    public CompletableFuture<AmqpChannel.QueueInfo> queueDeclarePassive(String queue) {
        return with(d -> d.queueDeclarePassive(conn.alias(queue)));
    }

    public CompletableFuture<Void> queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return with(d -> d.queueBind(conn.alias(queue), exchange, routingKey, arguments)).thenApply(v -> {
            conn.recordBinding(new RecoveringConnection.BindingDecl(queue, exchange, routingKey, arguments));
            return v;
        });
    }

    public CompletableFuture<Void> queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return with(d -> d.queueUnbind(conn.alias(queue), exchange, routingKey, arguments)).thenApply(v -> {
            conn.forgetBinding(new RecoveringConnection.BindingDecl(queue, exchange, routingKey, arguments));
            return v;
        });
    }

    public CompletableFuture<Long> queuePurge(String queue) {
        return with(d -> d.queuePurge(conn.alias(queue)));
    }

    public CompletableFuture<Long> queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        return with(d -> d.queueDelete(conn.alias(queue), ifUnused, ifEmpty)).thenApply(n -> {
            conn.forgetQueue(queue);
            return n;
        });
    }

    // ---- consuming ----

    public CompletableFuture<Void> basicQos(int prefetchCount) {
        return with(d -> d.basicQos(prefetchCount)).thenApply(v -> {
            prefetch = prefetchCount;
            return v;
        });
    }

    public CompletableFuture<String> basicConsume(String queue, boolean autoAck, AmqpConsumer consumer) {
        return basicConsume(queue, "", autoAck, false, null, consumer);
    }

    /** The consumer is registered again after a recovery under the same tag. */
    public CompletableFuture<String> basicConsume(String queue, String consumerTag, boolean autoAck, boolean exclusive, Map<String, Object> arguments,
                                                  AmqpConsumer consumer) {
        ConsumerDecl decl = new ConsumerDecl(queue, autoAck, exclusive, arguments, consumer);
        return with(d -> d.basicConsume(conn.alias(queue), consumerTag, autoAck, exclusive, arguments, decl)).thenApply(tag -> {
            decl.tag = tag;
            consumers.put(tag, decl);
            return tag;
        });
    }

    public CompletableFuture<Void> basicCancel(String consumerTag) {
        return with(d -> d.basicCancel(consumerTag)).thenApply(v -> {
            consumers.remove(consumerTag);
            return v;
        });
    }

    public CompletableFuture<Delivery> basicGet(String queue, boolean autoAck) {
        return with(d -> d.basicGet(conn.alias(queue), autoAck)).thenApply(x -> x == null ? null : visible(x));
    }

    /** Ignored (not an error) for a delivery from before a reconnect: the broker has already put that message back on its queue. */
    public void basicAck(long deliveryTag, boolean multiple) {
        long real = deliveryTag - tagOffset;
        AmqpChannel d = live();
        if (real > 0 && d != null) {
            d.basicAck(real, multiple);
        }
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        long real = deliveryTag - tagOffset;
        AmqpChannel d = live();
        if (real > 0 && d != null) {
            d.basicNack(real, multiple, requeue);
        }
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        long real = deliveryTag - tagOffset;
        AmqpChannel d = live();
        if (real > 0 && d != null) {
            d.basicReject(real, requeue);
        }
    }

    // ---- publishing ----

    public CompletableFuture<Void> confirmSelect() {
        return with(AmqpChannel::confirmSelect).thenApply(v -> {
            confirms = true;
            return v;
        });
    }

    public void onReturn(Consumer<ReturnedMessage> listener) {
        returnListener = listener;
        AmqpChannel d = delegate;
        if (d != null) {
            d.onReturn(listener);
        }
    }

    /**
     * As {@link AmqpChannel#basicPublish}. While the link is down the future fails at once; a confirmation still pending when it breaks
     * fails as well (the broker may have the message: republishing is at-least-once).
     */
    public CompletableFuture<Void> basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties properties, byte[] body) {
        String key = exchange.isEmpty() ? conn.alias(routingKey) : routingKey; // the default exchange routes by queue name
        return with(d -> d.basicPublish(exchange, key, mandatory, properties, body));
    }

    // ---- closing and recovery ----

    public CompletableFuture<Void> close() {
        userClosed = true;
        conn.removeChannel(this);
        AmqpChannel d = delegate;
        if (d != null && d.isOpen() && conn.state() == RecoveringConnection.State.CONNECTED) {
            d.close().whenComplete((v, e) -> closeFuture.complete(null));
        } else {
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    /** Puts this channel back on a new connection: a channel, prefetch, confirm mode, return listener, consumers. Blocks (recovery thread). */
    void recover(AmqpConnection c) throws Exception {
        if (userClosed || failed != null) {
            return;
        }
        AmqpChannel d = c.openChannel().get(30, TimeUnit.SECONDS);
        tagOffset = highestTag.get(); // new real tags start at 1: everything the application saw before stays below them
        Integer p = prefetch;
        if (p != null) {
            d.basicQos(p).get(30, TimeUnit.SECONDS);
        }
        if (confirms) {
            d.confirmSelect().get(30, TimeUnit.SECONDS);
        }
        Consumer<ReturnedMessage> rl = returnListener;
        if (rl != null) {
            d.onReturn(rl);
        }
        for (ConsumerDecl cd : consumers.values()) {
            d.basicConsume(conn.alias(cd.queue), cd.tag, cd.autoAck, cd.exclusive, cd.args, cd).get(30, TimeUnit.SECONDS);
        }
        delegate = d;
    }

    /** The channel will not come back: fail what is left and tell the consumers. */
    void failPermanently(Throwable cause) {
        if (failed == null) {
            failed = cause;
        }
        for (Map.Entry<String, ConsumerDecl> e : consumers.entrySet()) {
            if (consumers.remove(e.getKey(), e.getValue())) {
                try {
                    e.getValue().user.onCancel(e.getKey(), cause);
                } catch (Throwable ignored) {
                    // a consumer that throws from onCancel must not stop the others hearing it
                }
            }
        }
        closeFuture.complete(null);
    }
}
