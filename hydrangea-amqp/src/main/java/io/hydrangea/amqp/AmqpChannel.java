package io.hydrangea.amqp;

import java.lang.System.Logger;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One AMQP channel: declare exchanges and queues, publish, consume, acknowledge. Obtain it from {@link AmqpConnection#openChannel()}.
 * <p>
 * All methods may be called from any thread. Calls made from the connection's loop thread run at once; calls from other threads are
 * queued to the loop in order. Futures and consumer callbacks complete on the loop thread: do not block there.
 * <p>
 * A channel is closed by the broker when you break a rule (declaring a queue again with other settings = 406, using a missing queue = 404):
 * every call still waiting fails with the broker's {@link AmqpException}, and the channel cannot be used again. Open a new one.
 */
public final class AmqpChannel {

    private static final Logger LOG = System.getLogger(AmqpChannel.class.getName());

    /** The answer to a queue declaration. */
    public record QueueInfo(String name, long messageCount, long consumerCount) {
    }

    private static int key(int cls, int method) {
        return cls << 16 | method;
    }

    private static final int CHANNEL_OPEN_OK = 20 << 16 | 11;
    private static final int CHANNEL_FLOW = 20 << 16 | 20;
    private static final int CHANNEL_CLOSE = 20 << 16 | 40;
    private static final int CHANNEL_CLOSE_OK = 20 << 16 | 41;
    private static final int BASIC_CANCEL = 60 << 16 | 30;
    private static final int BASIC_RETURN = 60 << 16 | 50;
    private static final int BASIC_DELIVER = 60 << 16 | 60;
    private static final int BASIC_GET_OK = 60 << 16 | 71;
    private static final int BASIC_GET_EMPTY = 60 << 16 | 72;
    private static final int BASIC_ACK = 60 << 16 | 80;
    private static final int BASIC_NACK = 60 << 16 | 120;

    private static final class Rpc<T> {
        final int key;
        final int altKey;
        final CompletableFuture<T> future;
        final Function<Wire.Reader, T> decode;

        Rpc(int key, int altKey, CompletableFuture<T> future, Function<Wire.Reader, T> decode) {
            this.key = key;
            this.altKey = altKey;
            this.future = future;
            this.decode = decode;
        }
    }

    /** A message being assembled from its method, header and body frames. */
    private static final class Content {
        final int kind; // BASIC_DELIVER, BASIC_GET_OK or BASIC_RETURN
        String consumerTag;
        long deliveryTag;
        boolean redelivered;
        String exchange;
        String routingKey;
        int replyCode;
        String replyText;
        Rpc<Delivery> getRpc;
        BasicProperties properties;
        boolean headerSeen;
        long size;
        byte[] body = new byte[0];
        int filled;

        Content(int kind) {
            this.kind = kind;
        }
    }

    private final AmqpConnection conn;
    private final int id;

    // loop thread only
    private final ArrayDeque<Rpc<?>> pending = new ArrayDeque<>();
    private final Map<String, AmqpConsumer> consumers = new HashMap<>();
    private final TreeMap<Long, CompletableFuture<Void>> unconfirmed = new TreeMap<>();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private Content current;
    private boolean confirmMode;
    private long nextPublishSeq = 1;
    private boolean closing;
    private boolean closed;
    private Consumer<ReturnedMessage> returnListener;

    // metrics (per group): messages published and delivered, publisher confirms and how long they take, acknowledgements sent, unconfirmed publishes
    private final io.hydrangea.Metrics.Count mPublished;
    private final io.hydrangea.Metrics.Count mDelivered;
    private final io.hydrangea.Metrics.Count mConfirmed;
    private final io.hydrangea.Metrics.Count mNacked;
    private final io.hydrangea.Metrics.Count mFailed;
    private final io.hydrangea.Metrics.Count mUnconfirmed;
    private final io.hydrangea.Metrics.Count mReturned;
    private final io.hydrangea.Metrics.Count mAck;
    private final io.hydrangea.Metrics.Count mNack;
    private final io.hydrangea.Metrics.Count mReject;
    private final io.hydrangea.Latency mConfirmTime;

    /** A publish that waits for a confirm, with the clock when it was written (0 = not timed). */
    private static final class Publish extends CompletableFuture<Void> {
        long start;
    }

    AmqpChannel(AmqpConnection conn, int id) {
        this.conn = conn;
        this.id = id;
        io.hydrangea.Metrics m = conn.metrics();
        mPublished = m.counter("amqp.messages", null, "direction", "published");
        mDelivered = m.counter("amqp.messages", null, "direction", "delivered");
        mConfirmed = m.counter("amqp.confirms", null, "result", "ack");
        mNacked = m.counter("amqp.confirms", null, "result", "nack");
        mFailed = m.counter("amqp.publish.failures", null);
        mUnconfirmed = m.gauge("amqp.unconfirmed", null);
        mReturned = m.counter("amqp.returned", null);
        mAck = m.counter("amqp.acks", null, "type", "ack");
        mNack = m.counter("amqp.acks", null, "type", "nack");
        mReject = m.counter("amqp.acks", null, "type", "reject");
        mConfirmTime = m.latency("amqp.confirm.duration");
    }

    public int id() {
        return id;
    }

    public boolean isOpen() {
        return !closing && !closed;
    }

    /** See {@link AmqpConnection#isWritable()}: false means stop publishing for now. */
    public boolean isWritable() {
        return conn.isWritable();
    }

    /** See {@link AmqpConnection#whenWritable()}: completes when publishing may continue; fails if the connection closes first. */
    public CompletableFuture<Void> whenWritable() {
        return conn.whenWritable();
    }

    // ---- opening and closing ----

    void open(CompletableFuture<AmqpChannel> result) {
        Rpc<AmqpChannel> rpc = new Rpc<>(CHANNEL_OPEN_OK, CHANNEL_OPEN_OK, result, r -> {
            r.longStr(); // reserved
            return this;
        });
        pending.add(rpc);
        try {
            conn.sendMethodFrame(id, new Wire.Writer(16).shortInt(20).shortInt(10).shortStr(""));
        } catch (RuntimeException e) {
            pending.clear();
            conn.removeChannel(id);
            result.completeExceptionally(e);
        }
    }

    /** Closes the channel (Channel.Close). Messages delivered but not acknowledged return to their queue. Idempotent. */
    public CompletableFuture<Void> close() {
        conn.onLoop(() -> {
            if (closing || closed) {
                return;
            }
            closing = true;
            try {
                conn.sendMethodFrame(id, new Wire.Writer(32).shortInt(20).shortInt(40).shortInt(AmqpException.REPLY_SUCCESS).shortStr("OK").shortInt(0).shortInt(0));
            } catch (RuntimeException e) {
                terminate(e instanceof AmqpException a ? a : new AmqpException("close failed", e));
            }
        });
        return closeFuture;
    }

    /** The connection started closing: no new work, the rest is failed when it is gone. */
    void connectionClosing() {
        closing = true;
    }

    void connectionLost(AmqpException why) {
        terminate(why);
    }

    private void terminate(AmqpException cause) {
        if (closed) {
            return;
        }
        closed = true;
        closing = true;
        conn.removeChannel(id);
        AmqpException err = cause != null ? cause : new AmqpException(AmqpException.CHANNEL_ERROR, "the channel is closed", false);
        for (Rpc<?> rpc : new ArrayDeque<>(pending)) {
            rpc.future.completeExceptionally(err);
        }
        pending.clear();
        for (CompletableFuture<Void> f : unconfirmed.values()) {
            f.completeExceptionally(err);
        }
        mFailed.add(unconfirmed.size());
        mUnconfirmed.add(-unconfirmed.size());
        unconfirmed.clear();
        if (current != null && current.getRpc != null) {
            current.getRpc.future.completeExceptionally(err);
        }
        current = null;
        Map<String, AmqpConsumer> gone = new HashMap<>(consumers);
        consumers.clear();
        gone.forEach((tag, c) -> {
            try {
                c.onCancel(tag, cause);
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "consumer onCancel failed", t);
            }
        });
        closeFuture.complete(null);
    }

    // ---- exchanges and queues ----

    /** @param type "direct", "fanout", "topic" or "headers" */
    public CompletableFuture<Void> exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        return call(new Wire.Writer(64).shortInt(40).shortInt(10).shortInt(0).shortStr(exchange).shortStr(type)
                .bits(false, durable, autoDelete, false, false).table(arguments), key(40, 11), r -> null);
    }

    public CompletableFuture<Void> exchangeDelete(String exchange, boolean ifUnused) {
        return call(new Wire.Writer(32).shortInt(40).shortInt(20).shortInt(0).shortStr(exchange).bits(ifUnused, false), key(40, 21), r -> null);
    }

    /** Declares a queue (creates it, or checks it exists with the same settings; different settings close the channel with 406). */
    public CompletableFuture<QueueInfo> queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        return declare(queue, false, durable, exclusive, autoDelete, arguments);
    }

    /** A queue the broker names; it is exclusive to this connection and deleted when the connection closes. */
    public CompletableFuture<QueueInfo> queueDeclare() {
        return declare("", false, false, true, true, null);
    }

    /** Checks that the queue exists. If it does not, the broker closes the channel with 404. */
    public CompletableFuture<QueueInfo> queueDeclarePassive(String queue) {
        return declare(queue, true, false, false, false, null);
    }

    private CompletableFuture<QueueInfo> declare(String queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        return call(new Wire.Writer(64).shortInt(50).shortInt(10).shortInt(0).shortStr(queue)
                .bits(passive, durable, exclusive, autoDelete, false).table(arguments), key(50, 11),
                r -> new QueueInfo(r.shortStr(), r.longInt(), r.longInt()));
    }

    public CompletableFuture<Void> queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return call(new Wire.Writer(64).shortInt(50).shortInt(20).shortInt(0).shortStr(queue).shortStr(exchange).shortStr(routingKey)
                .bits(false).table(arguments), key(50, 21), r -> null);
    }

    public CompletableFuture<Void> queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return call(new Wire.Writer(64).shortInt(50).shortInt(50).shortInt(0).shortStr(queue).shortStr(exchange).shortStr(routingKey)
                .table(arguments), key(50, 51), r -> null);
    }

    /** Removes all ready messages; returns how many. */
    public CompletableFuture<Long> queuePurge(String queue) {
        return call(new Wire.Writer(32).shortInt(50).shortInt(30).shortInt(0).shortStr(queue).bits(false), key(50, 31), Wire.Reader::longInt);
    }

    /** Deletes the queue; returns how many messages it held. */
    public CompletableFuture<Long> queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        return call(new Wire.Writer(32).shortInt(50).shortInt(40).shortInt(0).shortStr(queue).bits(ifUnused, ifEmpty, false), key(50, 41), Wire.Reader::longInt);
    }

    // ---- consuming ----

    /**
     * Limits how many unacknowledged messages the broker sends to each consumer of this channel. Without it the broker pushes the whole
     * queue as fast as the network allows. Only meaningful with {@code autoAck = false}.
     */
    public CompletableFuture<Void> basicQos(int prefetchCount) {
        if (prefetchCount < 0 || prefetchCount > 65535) {
            throw new IllegalArgumentException("prefetchCount");
        }
        return call(new Wire.Writer(16).shortInt(60).shortInt(10).longInt(0).shortInt(prefetchCount).bits(false), key(60, 11), r -> null);
    }

    /** Starts a consumer with a broker-chosen tag; returns the tag. With {@code autoAck = false} acknowledge each delivery. */
    public CompletableFuture<String> basicConsume(String queue, boolean autoAck, AmqpConsumer consumer) {
        return basicConsume(queue, "", autoAck, false, null, consumer);
    }

    public CompletableFuture<String> basicConsume(String queue, String consumerTag, boolean autoAck, boolean exclusive, Map<String, Object> arguments,
                                                  AmqpConsumer consumer) {
        return call(new Wire.Writer(64).shortInt(60).shortInt(20).shortInt(0).shortStr(queue).shortStr(consumerTag)
                .bits(false, autoAck, exclusive, false).table(arguments), key(60, 21), r -> {
                    String tag = r.shortStr();
                    consumers.put(tag, consumer); // before the next frame is read: deliveries may follow at once
                    return tag;
                });
    }

    /** Stops a consumer. Deliveries already on the way are still handed to it until the broker confirms. */
    public CompletableFuture<Void> basicCancel(String consumerTag) {
        return call(new Wire.Writer(32).shortInt(60).shortInt(30).shortStr(consumerTag).bits(false), key(60, 31), r -> {
            consumers.remove(r.shortStr());
            return null;
        });
    }

    /** Fetches one message by polling; completes with null if the queue is empty. Prefer {@link #basicConsume} for continuous work. */
    public CompletableFuture<Delivery> basicGet(String queue, boolean autoAck) {
        CompletableFuture<Delivery> f = new CompletableFuture<>();
        Wire.Writer w = new Wire.Writer(32).shortInt(60).shortInt(70).shortInt(0).shortStr(queue).bits(autoAck);
        conn.onLoop(() -> {
            if (!isOpen()) {
                f.completeExceptionally(closedError());
                return;
            }
            pending.add(new Rpc<>(BASIC_GET_OK, BASIC_GET_EMPTY, f, r -> null));
            send(w, f);
        });
        return f;
    }

    public void basicAck(long deliveryTag, boolean multiple) {
        mAck.increment();
        fire(new Wire.Writer(16).shortInt(60).shortInt(80).longLong(deliveryTag).bits(multiple));
    }

    /** Rejects one or (multiple = every delivery up to this tag) several messages; {@code requeue} puts them back on the queue. */
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        mNack.increment();
        fire(new Wire.Writer(16).shortInt(60).shortInt(120).longLong(deliveryTag).bits(multiple, requeue));
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        mReject.increment();
        fire(new Wire.Writer(16).shortInt(60).shortInt(90).longLong(deliveryTag).bits(requeue));
    }

    // ---- publishing ----

    /** Puts the channel in publisher-confirm mode: from now on {@link #basicPublish} completes when the broker has taken responsibility for the message. */
    public CompletableFuture<Void> confirmSelect() {
        return call(new Wire.Writer(16).shortInt(85).shortInt(10).bits(false), key(85, 11), r -> {
            confirmMode = true;
            return null;
        });
    }

    /** Where messages published with {@code mandatory = true} that no queue accepted come back to. */
    public void onReturn(Consumer<ReturnedMessage> listener) {
        conn.onLoop(() -> returnListener = listener);
    }

    /**
     * Publishes a message. In confirm mode ({@link #confirmSelect}) the future completes when the broker confirms it (or fails on a
     * nack); otherwise it completes as soon as the message is handed to the socket, which says nothing about the broker having it.
     * When {@link AmqpConnection#isWritable()} is false the data is still queued in memory: pause the producer.
     *
     * @param mandatory if no queue accepts the message, the broker returns it to {@link #onReturn}
     */
    public CompletableFuture<Void> basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties properties, byte[] body) {
        BasicProperties props = properties == null ? BasicProperties.EMPTY : properties;
        int max = conn.frameMax() - 8;
        Wire.Writer method = new Wire.Writer(64).shortInt(60).shortInt(40).shortInt(0).shortStr(exchange).shortStr(routingKey).bits(mandatory, false);
        Wire.Writer header = new Wire.Writer(64).shortInt(60).shortInt(0).longLong(body.length);
        writeProperties(header, props);
        int frames = Math.max(1, (body.length + max - 1) / max);
        Wire.Writer out = new Wire.Writer(method.size() + header.size() + body.length + 8 * (frames + 2));
        AmqpConnection.frame(out, AmqpConnection.FRAME_METHOD, id, method.array(), method.size());
        AmqpConnection.frame(out, AmqpConnection.FRAME_HEADER, id, header.array(), header.size());
        for (int off = 0; off < body.length; off += max) {
            int n = Math.min(max, body.length - off);
            out.octet(AmqpConnection.FRAME_BODY).shortInt(id).longInt(n).raw(body, off, n).octet(AmqpConnection.FRAME_END);
        }
        Publish f = new Publish();
        conn.onLoop(() -> {
            if (!isOpen()) {
                mFailed.increment();
                f.completeExceptionally(closedError());
                return;
            }
            try {
                conn.write(out);
            } catch (RuntimeException e) {
                mFailed.increment();
                f.completeExceptionally(e);
                return;
            }
            mPublished.increment();
            if (confirmMode) {
                f.start = mConfirmTime.begin();
                mUnconfirmed.increment();
                unconfirmed.put(nextPublishSeq++, f);
            } else {
                f.complete(null);
            }
        });
        return f;
    }

    private static void writeProperties(Wire.Writer w, BasicProperties p) {
        int flags = 0;
        Wire.Writer v = new Wire.Writer(64);
        if (p.contentType() != null) { flags |= 1 << 15; v.shortStr(p.contentType()); }
        if (p.contentEncoding() != null) { flags |= 1 << 14; v.shortStr(p.contentEncoding()); }
        if (p.headers() != null) { flags |= 1 << 13; v.table(p.headers()); }
        if (p.deliveryMode() != null) { flags |= 1 << 12; v.octet(p.deliveryMode()); }
        if (p.priority() != null) { flags |= 1 << 11; v.octet(p.priority()); }
        if (p.correlationId() != null) { flags |= 1 << 10; v.shortStr(p.correlationId()); }
        if (p.replyTo() != null) { flags |= 1 << 9; v.shortStr(p.replyTo()); }
        if (p.expiration() != null) { flags |= 1 << 8; v.shortStr(p.expiration()); }
        if (p.messageId() != null) { flags |= 1 << 7; v.shortStr(p.messageId()); }
        if (p.timestamp() != null) { flags |= 1 << 6; v.longLong(p.timestamp().getEpochSecond()); }
        if (p.type() != null) { flags |= 1 << 5; v.shortStr(p.type()); }
        if (p.userId() != null) { flags |= 1 << 4; v.shortStr(p.userId()); }
        if (p.appId() != null) { flags |= 1 << 3; v.shortStr(p.appId()); }
        w.shortInt(flags);
        w.raw(v.array(), 0, v.size());
    }

    private static BasicProperties readProperties(Wire.Reader r) {
        int flags = r.shortInt();
        int more = flags;
        while ((more & 1) != 0) { // continuation words: basic has no properties beyond the first word, but they must be consumed
            more = r.shortInt();
        }
        String contentType = (flags & 1 << 15) != 0 ? r.shortStr() : null;
        String contentEncoding = (flags & 1 << 14) != 0 ? r.shortStr() : null;
        Map<String, Object> headers = (flags & 1 << 13) != 0 ? r.table() : null;
        Integer deliveryMode = (flags & 1 << 12) != 0 ? r.octet() : null;
        Integer priority = (flags & 1 << 11) != 0 ? r.octet() : null;
        String correlationId = (flags & 1 << 10) != 0 ? r.shortStr() : null;
        String replyTo = (flags & 1 << 9) != 0 ? r.shortStr() : null;
        String expiration = (flags & 1 << 8) != 0 ? r.shortStr() : null;
        String messageId = (flags & 1 << 7) != 0 ? r.shortStr() : null;
        Instant timestamp = (flags & 1 << 6) != 0 ? Instant.ofEpochSecond(r.longLong()) : null;
        String type = (flags & 1 << 5) != 0 ? r.shortStr() : null;
        String userId = (flags & 1 << 4) != 0 ? r.shortStr() : null;
        String appId = (flags & 1 << 3) != 0 ? r.shortStr() : null;
        if ((flags & 1 << 2) != 0) {
            r.shortStr(); // cluster-id (deprecated)
        }
        return new BasicProperties(contentType, contentEncoding, headers, deliveryMode, priority, correlationId, replyTo, expiration, messageId, timestamp,
                type, userId, appId);
    }

    // ---- sending helpers ----

    private AmqpException closedError() {
        return new AmqpException(AmqpException.CHANNEL_ERROR, "the channel is closed", false);
    }

    private <T> CompletableFuture<T> call(Wire.Writer method, int replyKey, Function<Wire.Reader, T> decode) {
        CompletableFuture<T> f = new CompletableFuture<>();
        conn.onLoop(() -> {
            if (!isOpen()) {
                f.completeExceptionally(closedError());
                return;
            }
            pending.add(new Rpc<>(replyKey, replyKey, f, decode));
            send(method, f);
        });
        return f;
    }

    private void send(Wire.Writer method, CompletableFuture<?> f) {
        try {
            conn.sendMethodFrame(id, method);
        } catch (RuntimeException e) {
            pending.pollLast();
            f.completeExceptionally(e);
        }
    }

    private void fire(Wire.Writer method) {
        conn.onLoop(() -> {
            if (isOpen()) {
                try {
                    conn.sendMethodFrame(id, method);
                } catch (RuntimeException e) {
                    LOG.log(Logger.Level.DEBUG, "could not send {0}", e.toString());
                }
            }
        });
    }

    // ---- inbound (loop thread, called by AmqpConnection) ----

    void method(int cls, int m, Wire.Reader r) {
        int key = key(cls, m);
        if (closing && !closed) {
            if (key == CHANNEL_CLOSE_OK) {
                terminate(null);
            } else if (key == CHANNEL_CLOSE) { // both sides closed at once
                conn.sendMethodFrame(id, new Wire.Writer(8).shortInt(20).shortInt(41));
                terminate(null);
            }
            return; // anything else for a closing channel is discarded
        }
        if (closed) {
            return;
        }
        if (current != null) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "method frame in the middle of a message", true);
        }
        switch (key) {
            case BASIC_DELIVER -> {
                Content c = new Content(BASIC_DELIVER);
                c.consumerTag = r.shortStr();
                c.deliveryTag = r.longLong();
                c.redelivered = r.bits(1)[0];
                c.exchange = r.shortStr();
                c.routingKey = r.shortStr();
                current = c;
            }
            case BASIC_RETURN -> {
                Content c = new Content(BASIC_RETURN);
                c.replyCode = r.shortInt();
                c.replyText = r.shortStr();
                c.exchange = r.shortStr();
                c.routingKey = r.shortStr();
                current = c;
            }
            case BASIC_ACK -> confirm(r.longLong(), r.bits(1)[0], false);
            case BASIC_NACK -> {
                long tag = r.longLong();
                confirm(tag, r.bits(2)[0], true);
            }
            case BASIC_CANCEL -> {
                String tag = r.shortStr();
                boolean noWait = r.bits(1)[0];
                AmqpConsumer c = consumers.remove(tag);
                if (c != null) {
                    try {
                        c.onCancel(tag, null);
                    } catch (Throwable t) {
                        LOG.log(Logger.Level.WARNING, "consumer onCancel failed", t);
                    }
                }
                if (!noWait) {
                    conn.sendMethodFrame(id, new Wire.Writer(16).shortInt(60).shortInt(31).shortStr(tag));
                }
            }
            case CHANNEL_FLOW -> {
                boolean active = r.bits(1)[0];
                conn.sendMethodFrame(id, new Wire.Writer(8).shortInt(20).shortInt(21).bits(active));
            }
            case CHANNEL_CLOSE -> {
                int code = r.shortInt();
                String text = r.shortStr();
                conn.sendMethodFrame(id, new Wire.Writer(8).shortInt(20).shortInt(41));
                terminate(new AmqpException(code, text, false));
            }
            case BASIC_GET_OK -> {
                Rpc<?> rpc = pending.peekFirst();
                if (rpc == null || rpc.key != BASIC_GET_OK) {
                    throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "Basic.GetOk without a Basic.Get", true);
                }
                pending.pollFirst();
                Content c = new Content(BASIC_GET_OK);
                @SuppressWarnings("unchecked")
                Rpc<Delivery> getRpc = (Rpc<Delivery>) rpc;
                c.getRpc = getRpc;
                c.deliveryTag = r.longLong();
                c.redelivered = r.bits(1)[0];
                c.exchange = r.shortStr();
                c.routingKey = r.shortStr();
                current = c;
            }
            default -> reply(key, r);
        }
    }

    @SuppressWarnings("unchecked")
    private void reply(int key, Wire.Reader r) {
        Rpc<?> rpc = pending.peekFirst();
        if (rpc == null || (key != rpc.key && key != rpc.altKey)) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME,
                    "unexpected method " + (key >>> 16) + "." + (key & 0xffff) + " on channel " + id, true);
        }
        pending.pollFirst();
        Rpc<Object> o = (Rpc<Object>) rpc;
        if (key == BASIC_GET_EMPTY) {
            o.future.complete(null);
            return;
        }
        o.future.complete(o.decode.apply(r));
    }

    private void confirm(long tag, boolean multiple, boolean nack) {
        if (!confirmMode) {
            throw new AmqpException(AmqpException.COMMAND_INVALID, "publisher confirm on a channel without confirm mode", true);
        }
        Map<Long, CompletableFuture<Void>> done = multiple ? unconfirmed.headMap(tag, true) : null;
        java.util.List<CompletableFuture<Void>> fs = new java.util.ArrayList<>();
        if (multiple) {
            fs.addAll(done.values());
            done.clear();
        } else {
            CompletableFuture<Void> f = unconfirmed.remove(tag);
            if (f != null) {
                fs.add(f);
            }
        }
        mUnconfirmed.add(-fs.size());
        long now = mConfirmTime.now();
        for (CompletableFuture<Void> f : fs) {
            if (f instanceof Publish p) {
                mConfirmTime.endAt(p.start, now);
            }
            if (nack) {
                mNacked.increment();
                f.completeExceptionally(new AmqpException(AmqpException.INTERNAL_ERROR, "the broker did not accept the message (nack)", false));
            } else {
                mConfirmed.increment();
                f.complete(null);
            }
        }
    }

    void header(Wire.Reader r) {
        if (closing || closed) {
            return;
        }
        Content c = current;
        if (c == null || c.headerSeen) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "content header without a message", true);
        }
        int cls = r.shortInt();
        r.shortInt(); // weight
        long size = r.longLong();
        if (cls != 60) {
            throw new AmqpException(AmqpException.SYNTAX_ERROR, "content header for class " + cls, true);
        }
        if (size < 0 || size > conn.maxMessageBytes()) {
            throw new AmqpException(AmqpException.RESOURCE_ERROR, "message of " + Long.toUnsignedString(size) + " bytes exceeds the limit " + conn.maxMessageBytes(), true);
        }
        c.properties = readProperties(r);
        c.headerSeen = true;
        c.size = size;
        c.body = new byte[(int) Math.min(size, 64 * 1024)]; // grows with the bytes that really arrive: a claimed size is not memory
        if (size == 0) {
            complete(c);
        }
    }

    void body(java.nio.ByteBuffer p) {
        if (closing || closed) {
            return;
        }
        Content c = current;
        if (c == null || !c.headerSeen) {
            throw new AmqpException(AmqpException.UNEXPECTED_FRAME, "content body without a header", true);
        }
        int n = p.remaining();
        if (c.filled + (long) n > c.size) {
            throw new AmqpException(AmqpException.FRAME_ERROR, "message body longer than announced", true);
        }
        if (c.filled + n > c.body.length) {
            c.body = Arrays.copyOf(c.body, (int) Math.min(c.size, Math.max((long) c.body.length * 2, c.filled + n)));
        }
        p.get(c.body, c.filled, n);
        c.filled += n;
        if (c.filled == c.size) {
            complete(c);
        }
    }

    private void complete(Content c) {
        current = null;
        switch (c.kind) {
            case BASIC_DELIVER -> {
                AmqpConsumer consumer = consumers.get(c.consumerTag);
                Delivery d = new Delivery(c.consumerTag, c.deliveryTag, c.redelivered, c.exchange, c.routingKey, c.properties, c.body);
                mDelivered.increment();
                if (consumer == null) {
                    LOG.log(Logger.Level.WARNING, "delivery for unknown consumer {0} dropped (tag {1})", c.consumerTag, c.deliveryTag);
                    return;
                }
                try {
                    consumer.onDelivery(d);
                } catch (Throwable t) {
                    LOG.log(Logger.Level.WARNING, "consumer " + c.consumerTag + " failed on delivery " + c.deliveryTag, t);
                }
            }
            case BASIC_GET_OK -> c.getRpc.future.complete(new Delivery(null, c.deliveryTag, c.redelivered, c.exchange, c.routingKey, c.properties, c.body));
            case BASIC_RETURN -> {
                mReturned.increment();
                Consumer<ReturnedMessage> l = returnListener;
                if (l != null) {
                    try {
                        l.accept(new ReturnedMessage(c.replyCode, c.replyText, c.exchange, c.routingKey, c.properties, c.body));
                    } catch (Throwable t) {
                        LOG.log(Logger.Level.WARNING, "return listener failed", t);
                    }
                }
            }
            default -> throw new IllegalStateException();
        }
    }
}
