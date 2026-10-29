package io.hydrangea.mqtt;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;

import java.lang.System.Logger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * An MQTT client (3.1.1 and 5.0) on a Hydrangea event loop. Everything is asynchronous: methods return {@link CompletableFuture}s, and handlers,
 * listeners and future continuations run on the client's loop thread, so they must not block. One client is one broker connection, and it
 * keeps its state on one loop for its whole life, including across reconnects.
 * <p>
 * <b>Delivery.</b> A QoS 0 publish completes when the packet is queued to the socket (no guarantee it arrived). A QoS 1 publish completes on
 * PUBACK, a QoS 2 publish on PUBCOMP. Messages received at QoS 1 are acknowledged after the handler returns; at QoS 2 a redelivered
 * packet is recognised and not handed to the handler twice. A handler that throws is logged and the message is still acknowledged.
 * <p>
 * <b>Reconnecting.</b> With {@link MqttConfig#reconnect()} (the default) a lost connection is re-established with back-off. QoS 1/2
 * messages not yet acknowledged stay in the client and are sent again (with the DUP flag) when the broker still has the session
 * ({@code cleanSession=false}); when it has not, they fail with a retriable {@link MqttException}, because resending could duplicate a QoS 2 message.
 * Messages accepted while the client is down wait in the queue. QoS 0 publishes, subscribes and unsubscribes fail at once while it is down.
 * Subscriptions are put back by themselves when the broker lost the session. The state lives in memory only: it does not survive a restart of this process.
 * <p>
 * <b>MQTT 5</b> ({@link MqttConfig#withVersion(int) withVersion(5)}): reason codes (a refused publish or subscription fails with the broker's code in
 * {@link MqttException#reasonCode()}), message and connection properties, subscription options (no-local, retain handling, retain-as-published), shared
 * subscriptions ({@code $share/group/filter}), subscription identifiers (used to route a message to exactly the handlers whose subscription matched, so
 * overlapping filters do not deliver twice), the broker's receive maximum and maximum packet size, server keep-alive, assigned client identifiers, will delay,
 * session expiry, topic aliases in both directions, message expiry that counts down while a message waits or is resent, enhanced authentication
 * (AUTH packets: {@link MqttAuthenticator}, {@link ScramSha256}, {@link #reauthenticate()}), and request/response ({@link #request}, {@link #reply}).
 */
public final class MqttClient implements AutoCloseable {

    private static final Logger LOG = System.getLogger(MqttClient.class.getName());

    private enum State { CONNECTING, ONLINE, OFFLINE, CLOSED }

    /** A QoS 1/2 message accepted but not yet acknowledged. */
    private static final class Outgoing {
        final String topic;
        final byte[] payload;
        final int qos;
        final boolean retain;
        final byte[] props;   // encoded MQTT 5 properties without message expiry and topic alias (those depend on when and on which connection it is sent), null for 3.1.1
        final Long expiry;    // message expiry interval asked for, in seconds
        final long acceptedAt = System.nanoTime();
        final CompletableFuture<Void> done = new CompletableFuture<>();
        int id;
        boolean released;   // QoS 2: PUBREC received, PUBREL sent; only PUBCOMP is awaited

        Outgoing(String topic, byte[] payload, int qos, boolean retain, byte[] props, Long expiry) {
            this.expiry = expiry;
            this.topic = topic;
            this.payload = payload;
            this.qos = qos;
            this.retain = retain;
            this.props = props;
        }
    }

    private record Sub(String filter, MqttHandler handler, SubscribeOptions options, QoS granted, int id) {
    }

    /** A SUBSCRIBE or UNSUBSCRIBE waiting for its acknowledgement. */
    private static final class Request {
        final boolean subscribe;
        final List<String> filters;
        final List<SubscribeOptions> options;
        final int subscriptionId;
        final List<Sub> previous;               // what each filter was before (null = not subscribed), to put back on failure
        final CompletableFuture<QoS> done;      // null for the automatic resubscribe

        Request(boolean subscribe, List<String> filters, List<SubscribeOptions> options, int subscriptionId, List<Sub> previous, CompletableFuture<QoS> done) {
            this.subscribe = subscribe;
            this.filters = filters;
            this.options = options;
            this.subscriptionId = subscriptionId;
            this.previous = previous;
            this.done = done;
        }
    }

    /** A request() waiting for its response. */
    private record Pending(CompletableFuture<MqttMessage> done, Scheduled timer) {
    }

    private final MqttConfig config;
    private final MqttListener listener;
    private final MqttTransport transport;
    private final boolean v5;
    private final CompletableFuture<MqttClient> ready = new CompletableFuture<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private EventLoopGroup group;
    private EventLoop home;

    // loop thread only
    private State state = State.CONNECTING;
    private int gen;
    private boolean everOnline;
    private int attempt;
    private boolean sessionPresent;
    private Scheduled reconnectTimer;
    private Scheduled connackTimer;
    private Scheduled keepAliveTimer;
    private long lastSend;
    private long lastReceive;
    private boolean pingOutstanding;
    private Throwable lossCause;
    private int lastId;
    private String clientId;                       // what CONNECT carries: the configured one, or the one the broker assigned
    private long keepAliveNanos;
    private int nextSubscriptionId = 1;
    private final Map<Integer, String> topicAliases = new HashMap<>();   // inbound MQTT 5 aliases, per connection
    private final LinkedHashMap<String, Integer> outAliases = new LinkedHashMap<>(16, 0.75f, true);   // outbound aliases, per connection, least recently used first
    private MqttAuthenticator.Exchange exchange;           // enhanced authentication of the connection being set up
    private MqttAuthenticator.Exchange reauthExchange;     // ... and of a re-authentication in progress
    private CompletableFuture<Void> reauthDone;
    private final Map<String, CompletableFuture<Void>> responseSubs = new HashMap<>();
    private final Map<Long, Pending> pendingRequests = new HashMap<>();
    private long nextCorrelation = 1;
    private String autoResponseTopic;
    private final LinkedHashMap<Integer, Outgoing> inflight = new LinkedHashMap<>();
    private final ArrayDeque<Outgoing> queue = new ArrayDeque<>();
    private final LinkedHashMap<Integer, Request> requests = new LinkedHashMap<>();
    private final LinkedHashMap<String, Sub> subs = new LinkedHashMap<>();
    private Sub[] subArray = new Sub[0];           // snapshot of subs, so a handler may subscribe while messages are being delivered
    private Map<Integer, Sub> subsById = Map.of();
    private final Set<Integer> incomingQos2 = new HashSet<>();
    private volatile MqttTransport.Link link;
    private volatile MqttServerInfo serverInfo = MqttServerInfo.DEFAULTS;

    // metrics (per group): messages by QoS, publishes that failed, publish-to-acknowledgement time, messages waiting for a broker's answer
    private final io.hydrangea.Metrics.Count[] mPublished = new io.hydrangea.Metrics.Count[3];
    private final io.hydrangea.Metrics.Count[] mReceived = new io.hydrangea.Metrics.Count[3];
    private io.hydrangea.Metrics.Count mFailed;
    private io.hydrangea.Metrics.Count mPending;
    private io.hydrangea.Metrics.Count mLost;
    private io.hydrangea.Metrics.Count mReconnects;
    private io.hydrangea.Latency mAckTime;
    private int pendingReported;

    private void initMetrics(io.hydrangea.Metrics m) {
        for (int q = 0; q < 3; q++) {
            mPublished[q] = m.counter("mqtt.client.messages", null, "direction", "published", "qos", Integer.toString(q));
            mReceived[q] = m.counter("mqtt.client.messages", null, "direction", "received", "qos", Integer.toString(q));
        }
        mFailed = m.counter("mqtt.client.publish.failures", null);
        mPending = m.gauge("mqtt.client.pending", null);
        mLost = m.counter("mqtt.client.connections.lost", null);
        mReconnects = m.counter("mqtt.client.reconnects", null);
        mAckTime = m.latency("mqtt.client.publish.duration");
    }

    private void failPublish(Outgoing m, Throwable why) {
        mFailed.increment();
        m.done.completeExceptionally(why);
    }

    /** Tells the pending gauge how many messages wait for the broker (queued or in flight) since it was last told. */
    private void reportPending() {
        int n = queue.size() + inflight.size();
        if (n != pendingReported) {
            mPending.add(n - pendingReported);
            pendingReported = n;
        }
    }

    private MqttClient(MqttConfig config, MqttListener listener) {
        this.config = config;
        this.v5 = config.version() == 5;
        this.clientId = config.clientId();
        this.transport = config.transport() != null ? config.transport() : new TcpTransport();
        this.listener = listener != null ? listener : new MqttListener() { };
    }

    /** Connects and completes when the broker has accepted the connection (CONNACK). A first attempt that fails is not retried. */
    public static CompletableFuture<MqttClient> connect(EventLoopGroup group, MqttConfig config) {
        return connect(group, config, null);
    }

    public static CompletableFuture<MqttClient> connect(EventLoopGroup group, MqttConfig config, MqttListener listener) {
        MqttClient c = new MqttClient(config, listener);
        c.group = group;
        c.home = group.next();
        c.initMetrics(group.metrics());
        c.onLoop(c::startConnecting);
        return c.ready;
    }

    // ---- public API ----

    /** As {@link #publish(String, byte[], QoS, boolean)}, not retained. */
    public CompletableFuture<Void> publish(String topic, byte[] payload, QoS qos) {
        return publish(topic, payload, qos, false, MqttProperties.NONE);
    }

    public CompletableFuture<Void> publish(String topic, byte[] payload, QoS qos, boolean retain) {
        return publish(topic, payload, qos, retain, MqttProperties.NONE);
    }

    /**
     * Publishes a message. The payload array is not copied: leave it alone until the future completes. See the class comment for when it completes.
     * Fails at once for a topic that is empty or has wildcards, a message over the protocol's 256 MB limit, or properties on an MQTT 3.1.1 client.
     * With MQTT 5 it also fails (not retriable) when the broker's CONNACK said it cannot take it: QoS above its maximum, retain not available,
     * a packet over its maximum size. A PUBACK or PUBREC with a failure reason code (not authorized, quota exceeded...) fails the future with that code.
     */
    public CompletableFuture<Void> publish(String topic, byte[] payload, QoS qos, boolean retain, MqttProperties properties) {
        byte[] props = null;
        Long expiry = null;
        try {
            Packets.checkTopicName(topic);
            if (payload == null || qos == null) {
                throw new IllegalArgumentException("payload and qos are required");
            }
            if ((long) payload.length + topic.length() * 3L + 10 > Packets.MAX_REMAINING) {
                throw new IllegalArgumentException("message of " + payload.length + " bytes is over the MQTT limit");
            }
            MqttProperties p = properties == null ? MqttProperties.NONE : properties;
            if (v5) {
                Props5 pp = Props5.of(p);
                expiry = pp.messageExpiry;
                pp.messageExpiry = null;   // added when the packet is built, minus the time spent waiting
                props = pp.encode();
            } else if (!p.equals(MqttProperties.NONE) && (p.payloadFormat() != null || p.messageExpirySeconds() != null || p.contentType() != null
                    || p.responseTopic() != null || p.correlationData() != null || !p.userProperties().isEmpty())) {
                throw new IllegalArgumentException("message properties need MQTT 5");
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        Outgoing m = new Outgoing(topic, payload, qos.level(), retain, props, expiry);
        onLoop(() -> {
            if (state == State.CLOSED) {
                failPublish(m, new MqttException("the client is closed", false));
                return;
            }
            MqttException refused = checkServerLimits(m);
            if (refused != null) {
                failPublish(m, refused);
            } else if (qos == QoS.AT_MOST_ONCE) {
                if (state != State.ONLINE) {
                    failPublish(m, new MqttException("not connected", true));
                } else {
                    try {
                        send(buildPublish(m, 0, false, false));
                        mPublished[0].increment();
                        m.done.complete(null);
                    } catch (IllegalArgumentException | MqttException e) {
                        failPublish(m, e);
                    }
                }
            } else if (queue.size() + inflight.size() >= config.maxQueued()) {
                failPublish(m, new MqttException("too many unacknowledged messages (maxQueued " + config.maxQueued() + ")", true));
            } else {
                queue.add(m);
                pump();
            }
        });
        return m.done;
    }

    /** As {@link #subscribe(String, SubscribeOptions, MqttHandler)} with just a QoS. */
    public CompletableFuture<QoS> subscribe(String filter, QoS qos, MqttHandler handler) {
        if (qos == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("qos is required"));
        }
        return subscribe(filter, SubscribeOptions.of(qos), handler);
    }

    /**
     * Subscribes to a topic filter (wildcards {@code +} and {@code #} allowed; with MQTT 5 also {@code $share/group/filter} for a shared subscription);
     * {@code handler} gets every matching message, including retained ones the broker replays. Completes with the QoS the broker granted, which may be
     * lower than asked. Subscribing to a filter again replaces its handler.
     * <p>
     * With MQTT 3.1.1, or an MQTT 5 broker that does not support subscription identifiers, a message that matches several of your filters goes to each of
     * their handlers, and the broker may send it once per subscription, so a handler can see it more than once: avoid overlapping filters there. With
     * MQTT 5 and identifiers, each handler gets exactly the messages of its own subscription.
     * <p>
     * Fails if the broker refuses (the reason code is in {@link MqttException#reasonCode()} with MQTT 5) or, with MQTT 3.1.1, if the options are anything
     * but a QoS.
     */
    public CompletableFuture<QoS> subscribe(String filter, SubscribeOptions options, MqttHandler handler) {
        try {
            Packets.checkTopicFilter(filter);
            if (options == null || handler == null) {
                throw new IllegalArgumentException("options and handler are required");
            }
            if (!v5 && !options.isPlain()) {
                throw new IllegalArgumentException("noLocal, retainAsPublished and retainHandling need MQTT 5");
            }
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<QoS> f = new CompletableFuture<>();
        onLoop(() -> {
            if (!requireOnline(f)) {
                return;
            }
            Sub before = subs.get(filter);
            int id = !v5 || !serverInfo.subscriptionIdentifiersAvailable() ? 0 : before != null && before.id > 0 ? before.id : nextSubscriptionId++;
            // registered now: a retained message may arrive before the SUBACK
            subs.put(filter, new Sub(filter, handler, options, options.qos(), id));
            rebuildSubs();
            sendRequest(new Request(true, List.of(filter), List.of(options), id, java.util.Collections.singletonList(before), f));
        });
        return f;
    }

    /**
     * Publishes {@code payload} to {@code topic} and completes with the answer: the request/response pattern of MQTT 5. The request carries a response
     * topic and a correlation id; the client subscribes to the response topic once (at QoS 1, when the first request is made) and matches each
     * answer to its request by the correlation id. Use {@link #reply} on the other side. The response topic is
     * {@code hydrangea/response/<random>} unless {@code properties} names one (then it must be a topic this client may subscribe to, and the
     * responder must publish to it). {@code properties} may not carry correlation data: the client sets it.
     * <p>
     * The future fails if the publish fails, if no answer arrives within {@code timeout} (retriable) which counts from this call and so includes
     * subscribing, or when the client closes. A connection that drops does not fail it: with a persistent session the answer can still arrive.
     * A late answer to a request that timed out is ignored.
     */
    public CompletableFuture<MqttMessage> request(String topic, byte[] payload, QoS qos, MqttProperties properties, Duration timeout) {
        MqttProperties p = properties == null ? MqttProperties.NONE : properties;
        if (!v5) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request/response needs MQTT 5"));
        }
        if (p.correlationData() != null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("request() sets the correlation data itself"));
        }
        if (timeout == null || timeout.isZero() || timeout.isNegative()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("a positive timeout is required"));
        }
        if (p.responseTopic() != null) {
            try {
                Packets.checkTopicName(p.responseTopic());
            } catch (IllegalArgumentException e) {
                return CompletableFuture.failedFuture(e);
            }
        }
        CompletableFuture<MqttMessage> result = new CompletableFuture<>();
        onLoop(() -> {
            if (state == State.CLOSED) {
                result.completeExceptionally(new MqttException("the client is closed", false));
                return;
            }
            String responseTopic = p.responseTopic();
            if (responseTopic == null) {
                if (autoResponseTopic == null) {
                    autoResponseTopic = "hydrangea/response/" + java.util.UUID.randomUUID();
                }
                responseTopic = autoResponseTopic;
            }
            long id = nextCorrelation++;
            byte[] correlation = java.nio.ByteBuffer.allocate(8).putLong(id).array();
            Scheduled timer = home.schedule(() -> {
                if (pendingRequests.remove(id) != null) {
                    result.completeExceptionally(new MqttException("no response to the request to " + topic + " within " + timeout.toMillis() + " ms", true));
                }
            }, timeout.toNanos(), TimeUnit.NANOSECONDS);
            pendingRequests.put(id, new Pending(result, timer));
            MqttProperties sent = p.withResponseTopic(responseTopic).withCorrelationData(correlation);
            responseSubscription(responseTopic).thenCompose(v -> publish(topic, payload, qos, false, sent)).whenComplete((v, err) -> {
                if (err != null) {
                    Pending pending = pendingRequests.remove(id);
                    if (pending != null) {
                        pending.timer.cancel();
                        result.completeExceptionally(err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err);
                    }
                }
            });
        });
        return result;
    }

    /** As {@link #request(String, byte[], QoS, MqttProperties, Duration)} with no properties. */
    public CompletableFuture<MqttMessage> request(String topic, byte[] payload, QoS qos, Duration timeout) {
        return request(topic, payload, qos, MqttProperties.NONE, timeout);
    }

    /** Subscribes to a response topic once; a failed subscription is forgotten so that the next request tries again. */
    private CompletableFuture<Void> responseSubscription(String responseTopic) {
        CompletableFuture<Void> have = responseSubs.get(responseTopic);
        if (have != null) {
            return have;
        }
        CompletableFuture<Void> f = subscribe(responseTopic, QoS.AT_LEAST_ONCE, this::onResponse).thenApply(q -> (Void) null);
        responseSubs.put(responseTopic, f);
        f.whenComplete((v, err) -> {
            if (err != null) {
                responseSubs.remove(responseTopic, f);
            }
        });
        return f;
    }

    private void onResponse(MqttMessage m) {
        byte[] c = m.properties().correlationData();
        if (c == null || c.length != 8) {
            return;
        }
        Pending pending = pendingRequests.remove(java.nio.ByteBuffer.wrap(c).getLong());
        if (pending != null) {
            pending.timer.cancel();
            pending.done.complete(m);
        }
    }

    /**
     * Answers a received request: publishes to its response topic with its correlation data. Fails if the message has no response topic.
     * (Needs MQTT 5, like the request.)
     */
    public CompletableFuture<Void> reply(MqttMessage request, byte[] payload, QoS qos) {
        String to = request.properties().responseTopic();
        if (to == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("the message has no response topic"));
        }
        MqttProperties p = request.properties().correlationData() == null ? MqttProperties.NONE
                : MqttProperties.NONE.withCorrelationData(request.properties().correlationData());
        return publish(to, payload, qos, false, p);
    }

    /**
     * Runs enhanced authentication again on the open connection (for credentials that expire): sends AUTH and plays the exchange of the configured
     * {@link MqttAuthenticator} with a new {@link MqttAuthenticator.Exchange}. Other traffic goes on meanwhile. Completes when the broker confirms; if it
     * refuses it closes the connection, and the future fails.
     */
    public CompletableFuture<Void> reauthenticate() {
        MqttAuthenticator auth = config.v5().authenticator();
        if (!v5 || auth == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("re-authentication needs MQTT 5 and an authenticator in the V5 options"));
        }
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            if (!requireOnline(f)) {
                return;
            }
            if (reauthDone != null) {
                f.completeExceptionally(new MqttException("a re-authentication is already running", true));
                return;
            }
            try {
                reauthExchange = auth.begin();
                send(Packets.auth(0x19, auth.method(), reauthExchange.initialData()));
                reauthDone = f;
            } catch (RuntimeException e) {
                reauthExchange = null;
                f.completeExceptionally(authFailure(e));
            }
        });
        return f;
    }

    public CompletableFuture<Void> unsubscribe(String filter) {
        try {
            Packets.checkTopicFilter(filter);
        } catch (IllegalArgumentException e) {
            return CompletableFuture.failedFuture(e);
        }
        CompletableFuture<QoS> f = new CompletableFuture<>();
        onLoop(() -> {
            if (!requireOnline(f)) {
                return;
            }
            Sub before = subs.remove(filter);
            rebuildSubs();
            sendRequest(new Request(false, List.of(filter), List.of(), 0, java.util.Collections.singletonList(before), f));
        });
        return f.thenApply(q -> null);
    }

    /**
     * Sends DISCONNECT (so the broker does not publish the will) and closes. Does not wait for outstanding acknowledgements: QoS 1/2 messages
     * still in flight fail. The client does not reconnect afterwards.
     */
    public CompletableFuture<Void> disconnect() {
        onLoop(() -> {
            if (state == State.CLOSED) {
                return;
            }
            boolean online = state == State.ONLINE;
            state = State.CLOSED;
            cancelTimers();
            MqttException why = new MqttException("the client was closed", false);
            failAll(why);
            if (!ready.isDone()) {
                ready.completeExceptionally(why);
            }
            MqttTransport.Link l = link;
            if (l != null) {
                if (online) {
                    try {
                        l.write(Packets.disconnect());
                        l.closeAfterFlush();
                    } catch (Exception e) {
                        l.close();
                    }
                } else {
                    l.close();
                }
            } else {
                closed.complete(null);
            }
        });
        return closed;
    }

    @Override
    public void close() {
        disconnect();
    }

    /** True while an accepted connection is up. */
    public boolean isConnected() {
        return state == State.ONLINE;
    }

    /** Whether the broker still had our session at the last connect (always false for a clean session). */
    public boolean sessionPresent() {
        return sessionPresent;
    }

    /** What the broker said about itself in the last CONNACK (the specification's defaults for MQTT 3.1.1 or before the first connect). */
    public MqttServerInfo serverInfo() {
        return serverInfo;
    }

    /** The client identifier in use: the configured one, or, if that was empty, the one an MQTT 5 broker assigned (kept across reconnects so the session can resume). */
    public String clientId() {
        return clientId;
    }

    /** False when more than the high-water mark of data is queued for the socket: a producer should pause. See {@link #whenWritable()}. */
    public boolean isWritable() {
        MqttTransport.Link l = link;
        return l == null || l.isWritable();
    }

    /**
     * Completes at once if the connection can take more data, otherwise when the queue has drained; fails if the client is not connected or
     * closes meanwhile, so a producer waiting on it cannot hang. Publishing never blocks: a producer that ignores this queues without bound
     * (QoS 1/2 is capped by {@link MqttConfig#maxQueued()}).
     */
    public CompletableFuture<Void> whenWritable() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        onLoop(() -> {
            MqttTransport.Link l = link;
            if (state != State.ONLINE || l == null) {
                f.completeExceptionally(new MqttException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            } else {
                l.whenWritable().whenComplete((v, t) -> {
                    if (t == null) {
                        f.complete(null);
                    } else {
                        f.completeExceptionally(new MqttException("the connection was lost", true, t));
                    }
                });
            }
        });
        return f;
    }

    /** QoS 1/2 messages accepted and not yet acknowledged (in flight and waiting). Thread-safe, approximate. */
    public CompletableFuture<Integer> pendingMessages() {
        CompletableFuture<Integer> f = new CompletableFuture<>();
        onLoop(() -> f.complete(queue.size() + inflight.size()));
        return f;
    }

    /** Completes when the client is closed for good (by {@link #disconnect()} or after giving up). */
    public CompletableFuture<Void> closeFuture() {
        return closed;
    }

    // ---- connecting ----

    private void onLoop(Runnable r) {
        if (home.inEventLoop()) {
            r.run();
        } else {
            try {
                home.execute(r);
            } catch (RuntimeException shutDown) {
                // the loop is gone; nothing more can happen to this client
            }
        }
    }

    private boolean requireOnline(CompletableFuture<?> f) {
        if (state != State.ONLINE) {
            f.completeExceptionally(new MqttException(state == State.CLOSED ? "the client is closed" : "not connected", state != State.CLOSED));
            return false;
        }
        return true;
    }

    private void startConnecting() {
        if (state == State.CLOSED) {
            return;
        }
        state = State.CONNECTING;
        lossCause = null;
        int g = ++gen;
        try {
            transport.connect(group, home, config, new Session(g));
        } catch (RuntimeException e) {
            onLoop(() -> attemptFailed(g, e));
        }
    }

    private void attemptFailed(int g, Throwable err) {
        if (g == gen && state == State.CONNECTING) {
            connectionEnded(err);
        }
    }

    /** The one place a connection (or an attempt to make one) ends. */
    private void connectionEnded(Throwable cause) {
        exchange = null;
        failReauth(cause);
        cancel(connackTimer);
        cancel(keepAliveTimer);
        link = null;
        boolean wasOnline = state == State.ONLINE;
        failRequests(cause);
        if (state == State.CLOSED) {
            closed.complete(null);
            return;
        }
        if (!everOnline) {   // the first attempt: the caller decides whether to try again
            state = State.CLOSED;
            ready.completeExceptionally(cause);
            giveUp(cause, false);
            return;
        }
        if (wasOnline) {
            mLost.increment();
            safely(() -> listener.connectionLost(cause));
        }
        boolean retriable = !(cause instanceof MqttException m) || m.retriable();
        if (!retriable || config.reconnect() == null) {
            giveUp(cause, true);
            return;
        }
        int max = config.reconnect().maxAttempts();
        attempt++;
        if (max > 0 && attempt > max) {
            giveUp(new MqttException("gave up reconnecting after " + max + " attempts", false, cause), true);
            return;
        }
        Duration delay = config.reconnect().delayFor(attempt);
        state = State.OFFLINE;
        mReconnects.increment();
        safely(() -> listener.reconnecting(attempt, delay));
        reconnectTimer = home.schedule(this::startConnecting, delay.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void giveUp(Throwable cause, boolean notify) {
        state = State.CLOSED;
        failAll(new MqttException("the client closed: " + cause, false, cause));
        if (notify) {
            safely(() -> listener.gaveUp(cause));
        }
        closed.complete(null);   // after the listener: whoever waits for the close has then been told why
    }

    private void failAll(Throwable why) {
        for (Outgoing m : inflight.values()) {
            failPublish(m, why);
        }
        inflight.clear();
        for (Outgoing m; (m = queue.poll()) != null; ) {
            failPublish(m, why);
        }
        reportPending();
        failRequests(why);
        failReauth(why);
        incomingQos2.clear();
        for (Pending pending : new ArrayList<>(pendingRequests.values())) {
            pending.timer.cancel();
            pending.done.completeExceptionally(why);
        }
        pendingRequests.clear();
    }

    private void failReauth(Throwable why) {
        reauthExchange = null;
        CompletableFuture<Void> f = reauthDone;
        reauthDone = null;
        if (f != null) {
            f.completeExceptionally(why instanceof MqttException ? why : new MqttException("the connection was lost during re-authentication: " + why, true, why));
        }
    }

    /** Subscribes and unsubscribes in flight fail; what they had changed locally is put back. */
    private void failRequests(Throwable why) {
        if (requests.isEmpty()) {
            return;
        }
        MqttException e = why instanceof MqttException m ? m : new MqttException("the connection was lost: " + why, true, why);
        for (Request r : new ArrayList<>(requests.values())) {
            if (r.subscribe) {
                restore(r);
            }
            if (r.done != null) {
                r.done.completeExceptionally(e);
            }
        }
        requests.clear();
    }

    private void restore(Request r) {
        for (int i = 0; i < r.filters.size(); i++) {
            Sub before = r.previous == null ? null : r.previous.get(i);
            if (before == null) {
                subs.remove(r.filters.get(i));
            } else {
                subs.put(r.filters.get(i), before);
            }
        }
        rebuildSubs();
    }

    private void rebuildSubs() {
        subArray = subs.values().toArray(new Sub[0]);
        Map<Integer, Sub> byId = new HashMap<>();
        for (Sub s : subArray) {
            if (s.id > 0) {
                byId.put(s.id, s);
            }
        }
        subsById = byId;
    }

    private void cancelTimers() {
        cancel(reconnectTimer);
        cancel(connackTimer);
        cancel(keepAliveTimer);
    }

    private static void cancel(Scheduled s) {
        if (s != null) {
            s.cancel();
        }
    }

    private void safely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "MQTT listener failed", t);
        }
    }

    // ---- sending ----

    private void send(ByteBuffer packet) {
        MqttTransport.Link l = link;
        if (l != null) {
            lastSend = System.nanoTime();
            try {
                l.write(packet);
            } catch (Exception e) {
                fail(new MqttException("write failed: " + e, true, e));
            }
        }
    }

    private int nextId() {
        for (int i = 0; i < 65535; i++) {
            lastId = lastId % 65535 + 1;
            if (!inflight.containsKey(lastId) && !requests.containsKey(lastId)) {
                return lastId;
            }
        }
        return -1;
    }

    /** What an MQTT 5 broker told us in CONNACK that makes this message impossible; null if it can go. */
    private MqttException checkServerLimits(Outgoing m) {
        if (!v5 || !everOnline) {
            return null;   // before the first CONNACK nothing is known; the check runs again when the message is sent
        }
        MqttServerInfo s = serverInfo;
        if (m.qos > s.maximumQoS()) {
            return new MqttException(0x9B, "the broker supports QoS " + s.maximumQoS() + " at most", false, null);
        }
        if (m.retain && !s.retainAvailable()) {
            return new MqttException(0x9A, "the broker does not support retained messages", false, null);
        }
        return null;
    }

    /**
     * The PUBLISH packet for {@code m}. With MQTT 5 it carries the message expiry interval minus the time the message has waited here, and, if
     * topic aliases are on, an alias instead of the topic name when the broker already knows it. Returns null for a message that expired while
     * it waited (never for a resend: its packet identifier cannot be dropped, so it goes again with the shortest interval, 1 s).
     * Throws when the packet is over the broker's size limit.
     */
    private ByteBuffer buildPublish(Outgoing m, int id, boolean dup, boolean resend) {
        byte[] props = m.props;
        String wireTopic = m.topic;
        int alias = 0;
        boolean define = false;
        String evict = null;
        if (v5) {
            long expiry = -1;
            if (m.expiry != null) {
                long waited = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - m.acceptedAt);
                expiry = m.expiry - waited;
                if (waited > 0 && expiry <= 0) {
                    if (!resend) {
                        return null;
                    }
                    expiry = 1;   // 0 would mean "no expiry" to some brokers
                }
                expiry = Math.max(expiry, 0);
            }
            if (config.v5().outgoingTopicAliases() && serverInfo.topicAliasMaximum() > 0 && m.topic.length() > 3) {
                Integer known = outAliases.get(m.topic);
                if (known != null) {
                    alias = known;
                    wireTopic = "";
                } else if (outAliases.size() < serverInfo.topicAliasMaximum()) {
                    alias = outAliases.size() + 1;
                    define = true;
                } else {
                    evict = outAliases.keySet().iterator().next();   // the least recently used
                    alias = outAliases.get(evict);
                    define = true;
                }
            }
            if (expiry >= 0 || alias > 0) {
                Packets.Writer w = new Packets.Writer(props.length + 10).raw(props, 0, props.length);
                if (expiry >= 0) {
                    w.u8(0x02).u32(expiry);
                }
                if (alias > 0) {
                    w.u8(0x23).u16(alias);
                }
                props = w.toByteArray();
            }
        }
        ByteBuffer packet = Packets.publish(wireTopic, m.payload, m.qos, m.retain, dup, id, props);
        MqttException tooBig = checkPacketSize(packet);
        if (tooBig != null) {
            throw tooBig;
        }
        if (define) {   // only now: the broker will learn the alias from this very packet
            if (evict != null) {
                outAliases.remove(evict);
            }
            outAliases.put(m.topic, alias);
        }
        return packet;
    }

    private MqttException checkPacketSize(ByteBuffer packet) {
        long max = v5 ? serverInfo.maximumPacketSize() : 0;
        if (max > 0 && packet.remaining() > max) {
            return new MqttException(0x95, "the packet is " + packet.remaining() + " bytes, over the broker's maximum of " + max, false, null);
        }
        return null;
    }

    /** Moves queued QoS 1/2 messages into flight while there is room. */
    private void pump() {
        pump0();
        reportPending();
    }

    private void pump0() {
        int window = Math.min(config.maxInflight(), v5 ? serverInfo.receiveMaximum() : 65535);
        while (state == State.ONLINE && !queue.isEmpty() && inflight.size() < window) {
            int id = nextId();
            if (id < 0) {
                return;
            }
            Outgoing m = queue.poll();
            MqttException refused = checkServerLimits(m);
            if (refused != null) {
                failPublish(m, refused);
                continue;
            }
            try {
                ByteBuffer packet = buildPublish(m, id, false, false);
                if (packet == null) {
                    failPublish(m, new MqttException("the message expired (" + m.expiry + " s) before it could be sent", false));
                    continue;
                }
                m.id = id;
                inflight.put(id, m);
                send(packet);
                mPublished[m.qos].increment();
            } catch (IllegalArgumentException | MqttException e) {
                failPublish(m, e);
            }
        }
    }

    private void sendRequest(Request r) {
        int id = nextId();
        if (id < 0) {
            if (r.subscribe) {
                restore(r);
            }
            if (r.done != null) {
                r.done.completeExceptionally(new MqttException("no free packet identifier", true));
            }
            return;
        }
        requests.put(id, r);
        send(r.subscribe ? Packets.subscribe(id, r.filters, r.options, r.subscriptionId, v5) : Packets.unsubscribe(id, r.filters, v5));
    }

    private void keepAliveTick() {
        if (state != State.ONLINE) {
            return;
        }
        long now = System.nanoTime();
        if (now - lastReceive > keepAliveNanos * 3 / 2) {
            fail(new MqttException("the broker was silent for " + Duration.ofNanos(now - lastReceive).toMillis() + " ms (keep-alive "
                    + Duration.ofNanos(keepAliveNanos).toMillis() + " ms)", true));
            return;
        }
        if (!pingOutstanding && now - lastSend >= keepAliveNanos / 2) {
            pingOutstanding = true;
            send(Packets.pingreq());
        }
        armKeepAlive();
    }

    private void armKeepAlive() {
        long tick = Math.max(TimeUnit.MILLISECONDS.toNanos(20), keepAliveNanos / 4);
        keepAliveTimer = home.schedule(this::keepAliveTick, tick, TimeUnit.NANOSECONDS);
    }

    /** Closes the connection because of {@code cause}; the close handler does the rest. */
    private void fail(Throwable cause) {
        if (lossCause == null) {
            lossCause = cause;
        }
        MqttTransport.Link l = link;
        if (l != null) {
            l.close();
        }
    }

    // ---- inbound ----

    /** What the transport tells us about one connection attempt; calls from any thread are moved onto the loop. */
    private final class Session implements MqttTransport.Events {
        private final int g;
        private Accumulator acc = new Accumulator();
        private boolean awaitingConnack = true;
        private boolean ended;

        Session(int g) {
            this.g = g;
        }

        @Override
        public void connected(MqttTransport.Link l) {
            onLoop(() -> {
                if (g != gen || state != State.CONNECTING) {   // closed (or superseded) while connecting
                    l.close();
                    return;
                }
                link = l;
                lastReceive = System.nanoTime();
                pingOutstanding = false;
                topicAliases.clear();
                outAliases.clear();
                byte[] authData = null;
                exchange = null;
                MqttAuthenticator auth = config.v5().authenticator();
                if (auth != null) {
                    try {
                        exchange = auth.begin();
                        authData = exchange.initialData();
                    } catch (RuntimeException e) {
                        fail(authFailure(e));
                        return;
                    }
                }
                send(Packets.connect(config, clientId, authData));
                if (config.connectTimeout() != null) {
                    connackTimer = home.schedule(() -> {
                        if (g == gen && state == State.CONNECTING) {
                            fail(new MqttException("no CONNACK within " + config.connectTimeout().toMillis() + " ms", true));
                        }
                    }, config.connectTimeout().toNanos(), TimeUnit.NANOSECONDS);
                }
            });
        }

        @Override
        public void data(ByteBuffer bytes) {
            if (home.inEventLoop()) {
                handle(bytes);
            } else {
                ByteBuffer copy = ByteBuffer.allocate(bytes.remaining()).put(bytes).flip();   // the caller reuses its buffer
                onLoop(() -> handle(copy));
            }
        }

        private void handle(ByteBuffer bytes) {
            if (g != gen || ended) {
                return;
            }
            acc.append(bytes);
            try {
                parse(this);
            } catch (MqttException e) {
                fail(e);
            } catch (RuntimeException e) {
                LOG.log(Logger.Level.WARNING, "MQTT packet handling failed", e);
                fail(new MqttException("internal error: " + e, false, e));
            }
        }

        @Override
        public void closed(Throwable cause) {
            onLoop(() -> {
                acc.release();
                if (!ended && g == gen) {
                    ended = true;
                    connectionEnded(lossCause != null ? lossCause
                            : cause != null ? cause : new java.io.IOException("the connection was closed"));
                }
            });
        }
    }

    private void parse(Session st) {
        ByteBuffer in = st.acc.buffer();
        while (state != State.CLOSED && link != null) {
            if (in.remaining() < 2) {
                return;
            }
            int p = in.position();
            int first = in.get(p) & 0xff;
            long len = 0;
            int header = 0;
            for (int k = 1; k <= 4; k++) {
                if (p + k >= in.limit()) {
                    return;   // the length itself is incomplete
                }
                int d = in.get(p + k) & 0xff;
                len += (long) (d & 0x7f) << (7 * (k - 1));
                if ((d & 0x80) == 0) {
                    header = k + 1;
                    break;
                }
                if (k == 4) {
                    throw new MqttException("malformed remaining length (more than four bytes)", false);
                }
            }
            if (len > config.maxPacketBytes()) {
                throw new MqttException("packet of " + len + " bytes exceeds maxPacketBytes " + config.maxPacketBytes(), false);
            }
            if (in.remaining() < header + len) {
                return;
            }
            ByteBuffer body = in.slice(p + header, (int) len);
            in.position(p + header + (int) len);
            lastReceive = System.nanoTime();
            dispatch(st, first >> 4, first & 0x0f, body);
        }
    }

    private void dispatch(Session st, int type, int flags, ByteBuffer body) {
        if (st.awaitingConnack) {
            if (type == Packets.AUTH && exchange != null) {
                checkFlags(type, flags, 0);
                onAuth(new Packets.Reader(body));
                return;
            }
            if (type != Packets.CONNACK) {
                throw new MqttException("the first packet from the broker was type " + type + ", not CONNACK", false);
            }
            checkFlags(type, flags, 0);
            st.awaitingConnack = false;
            onConnack(new Packets.Reader(body));
            return;
        }
        Packets.Reader r = new Packets.Reader(body);
        switch (type) {
            case Packets.PUBLISH -> onPublish(flags, r);
            case Packets.PUBACK -> {
                checkFlags(type, flags, 0);
                onPuback(r);
            }
            case Packets.PUBREC -> {
                checkFlags(type, flags, 0);
                onPubrec(r);
            }
            case Packets.PUBREL -> {
                checkFlags(type, flags, 2);
                onPubrel(r);
            }
            case Packets.PUBCOMP -> {
                checkFlags(type, flags, 0);
                onPubcomp(r);
            }
            case Packets.SUBACK -> {
                checkFlags(type, flags, 0);
                onSuback(r);
            }
            case Packets.UNSUBACK -> {
                checkFlags(type, flags, 0);
                onUnsuback(r);
            }
            case Packets.PINGRESP -> {
                checkFlags(type, flags, 0);
                r.end();
                pingOutstanding = false;
            }
            case Packets.DISCONNECT -> {
                if (!v5) {
                    throw new MqttException("a broker may not send packet type " + type, false);
                }
                checkFlags(type, flags, 0);
                onServerDisconnect(r);
            }
            case Packets.AUTH -> {
                if (!v5) {
                    throw new MqttException("a broker may not send packet type " + type, false);
                }
                checkFlags(type, flags, 0);
                onAuth(r);
            }
            default -> throw new MqttException("a broker may not send packet type " + type, false);
        }
    }

    private static void checkFlags(int type, int flags, int expected) {
        if (flags != expected) {
            throw new MqttException("packet type " + type + " with flags " + flags + " (must be " + expected + ")", false);
        }
    }

    // ---- reason codes ----

    private static String describe(int code) {
        return switch (code) {
            case 0x00 -> "success";
            case 0x01 -> "unacceptable protocol version / granted QoS 1";
            case 0x02 -> "client identifier rejected / granted QoS 2";
            case 0x03 -> "server unavailable";
            case 0x04 -> "bad user name or password";
            case 0x05 -> "not authorized";
            case 0x10 -> "no matching subscribers";
            case 0x11 -> "no subscription existed";
            case 0x80 -> "unspecified error";
            case 0x81 -> "malformed packet";
            case 0x82 -> "protocol error";
            case 0x83 -> "implementation specific error";
            case 0x84 -> "unsupported protocol version";
            case 0x85 -> "client identifier not valid";
            case 0x86 -> "bad user name or password";
            case 0x87 -> "not authorized";
            case 0x88 -> "server unavailable";
            case 0x89 -> "server busy";
            case 0x8A -> "banned";
            case 0x8B -> "server shutting down";
            case 0x8C -> "bad authentication method";
            case 0x8D -> "keep alive timeout";
            case 0x8E -> "session taken over";
            case 0x8F -> "topic filter invalid";
            case 0x90 -> "topic name invalid";
            case 0x91 -> "packet identifier in use";
            case 0x92 -> "packet identifier not found";
            case 0x93 -> "receive maximum exceeded";
            case 0x94 -> "topic alias invalid";
            case 0x95 -> "packet too large";
            case 0x96 -> "message rate too high";
            case 0x97 -> "quota exceeded";
            case 0x98 -> "administrative action";
            case 0x99 -> "payload format invalid";
            case 0x9A -> "retain not supported";
            case 0x9B -> "QoS not supported";
            case 0x9C -> "use another server";
            case 0x9D -> "server moved";
            case 0x9E -> "shared subscriptions not supported";
            case 0x9F -> "connection rate exceeded";
            case 0xA0 -> "maximum connect time";
            case 0xA1 -> "subscription identifiers not supported";
            case 0xA2 -> "wildcard subscriptions not supported";
            default -> "reason code 0x" + Integer.toHexString(code);
        };
    }

    /** Whether trying again later can help after an MQTT 5 failure with this reason code. */
    private static boolean retriableReason(int code) {
        return switch (code) {
            case 0x80, 0x83, 0x88, 0x89, 0x8B, 0x97, 0x9F -> true;
            default -> false;
        };
    }

    private static MqttException reason(int code, String what, Props5 p) {
        String text = what + ": " + describe(code) + (p != null && p.reasonString != null ? " (" + p.reasonString + ")" : "");
        return new MqttException(code, text, retriableReason(code), null);
    }

    // ---- packet handlers ----

    private void onConnack(Packets.Reader r) {
        cancel(connackTimer);
        int flags = r.u8();
        int code = r.u8();
        Props5 props = v5 ? Props5.read(r) : null;
        r.end();
        if ((flags & 0xfe) != 0) {
            throw new MqttException("reserved bits set in CONNACK", false);
        }
        if (code != 0) {
            exchange = null;
            if (v5) {
                if (code < 0x80) {
                    throw new MqttException("unknown CONNACK reason code " + code, false);
                }
                fail(reason(code, "the broker refused the connection", props));
                return;
            }
            if (code > 5) {
                throw new MqttException("unknown CONNACK return code " + code, false);
            }
            fail(new MqttException(code, "the broker refused the connection: " + describe(code), code == MqttException.REFUSED_SERVER_UNAVAILABLE, null));
            return;
        }
        boolean present = (flags & 1) != 0;
        if (present && config.cleanSession()) {
            throw new MqttException("the broker reports a session although a clean session was asked", false);
        }
        if (v5 && exchange != null) {   // the broker's last word of the exchange; a method that proves the broker to us checks it here
            MqttAuthenticator.Exchange done = exchange;
            exchange = null;
            if (props.authMethod != null && !props.authMethod.equals(config.v5().authenticator().method())) {
                throw new MqttException("the broker answered with authentication method " + props.authMethod, false);
            }
            try {
                done.finish(props.authData);
            } catch (RuntimeException e) {
                throw authFailure(e);
            }
        }
        keepAliveNanos = config.keepAlive() == null ? 0 : config.keepAlive().toNanos();
        if (v5) {
            MqttServerInfo info = props.toServerInfo(MqttServerInfo.DEFAULTS);
            serverInfo = info;
            if (info.assignedClientId() != null && config.clientId().isEmpty()) {
                clientId = info.assignedClientId();   // reuse it on reconnect, or the session could never resume
            }
            if (info.serverKeepAlive() >= 0) {
                keepAliveNanos = TimeUnit.SECONDS.toNanos(info.serverKeepAlive());
            }
        }
        boolean first = !everOnline;
        everOnline = true;
        attempt = 0;
        state = State.ONLINE;
        sessionPresent = present;
        if (!present) {
            // the broker has forgotten everything: what was in flight can no longer be completed, and we must say so
            MqttException lost = new MqttException("the broker no longer has the session; the message was not acknowledged", true);
            for (Outgoing m : inflight.values()) {
                failPublish(m, lost);
            }
            inflight.clear();
            reportPending();
            incomingQos2.clear();
        } else {
            for (var it = inflight.values().iterator(); it.hasNext(); ) {
                Outgoing m = it.next();
                try {
                    send(m.released ? Packets.ack(Packets.PUBREL, m.id) : buildPublish(m, m.id, true, true));
                } catch (IllegalArgumentException | MqttException e) {
                    it.remove();
                    failPublish(m, e);
                }
            }
        }
        if (!present && !subs.isEmpty()) {
            resubscribeAll();
        }
        if (first) {
            ready.complete(this);
        }
        safely(() -> listener.connected(present));
        if (keepAliveNanos > 0) {
            armKeepAlive();
        }
        pump();
    }

    private void resubscribeAll() {
        List<Sub> all = new ArrayList<>(subs.values());
        if (v5) {
            for (Sub s : all) {   // each with its own subscription identifier, so one packet each
                sendRequest(new Request(true, List.of(s.filter), List.of(s.options), s.id, null, null));
            }
            return;
        }
        for (int i = 0; i < all.size(); i += 100) {
            List<Sub> part = all.subList(i, Math.min(all.size(), i + 100));
            List<String> filters = new ArrayList<>();
            List<SubscribeOptions> opts = new ArrayList<>();
            for (Sub s : part) {
                filters.add(s.filter);
                opts.add(s.options);
            }
            sendRequest(new Request(true, filters, opts, 0, null, null));
        }
    }

    private void onPublish(int flags, Packets.Reader r) {
        boolean dup = (flags & 8) != 0;
        int qosLevel = (flags >> 1) & 3;
        boolean retain = (flags & 1) != 0;
        if (qosLevel == 3) {
            throw new MqttException("PUBLISH with QoS 3", false);
        }
        if (qosLevel == 0 && dup) {
            throw new MqttException("PUBLISH with QoS 0 and the DUP flag", false);
        }
        mReceived[qosLevel].increment();
        String topic = r.str();
        int id = 0;
        if (qosLevel > 0) {
            id = r.u16();
            if (id == 0) {
                throw new MqttException("PUBLISH with packet identifier 0", false);
            }
        }
        Props5 props = v5 ? Props5.read(r) : null;
        if (v5 && props.topicAlias != null) {
            int alias = props.topicAlias;
            if (alias > config.v5().topicAliasMaximum()) {
                throw new MqttException("topic alias " + alias + " is over the maximum of " + config.v5().topicAliasMaximum(), false);
            }
            if (topic.isEmpty()) {
                topic = topicAliases.get(alias);
                if (topic == null) {
                    throw new MqttException("topic alias " + alias + " was never defined", false);
                }
            } else {
                topicAliases.put(alias, topic);
            }
        }
        if (topic.isEmpty() || topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0) {
            throw new MqttException("PUBLISH with an invalid topic name", false);
        }
        MqttMessage m = new MqttMessage(topic, r.rest(), QoS.of(qosLevel), retain, dup, props == null ? MqttProperties.NONE : props.toPublic());
        switch (qosLevel) {
            case 0 -> deliver(m);
            case 1 -> {
                deliver(m);
                send(Packets.ack(Packets.PUBACK, id));
            }
            default -> {
                if (incomingQos2.add(id)) {   // a repeat of a packet we already handed over is only answered
                    deliver(m);
                }
                send(Packets.ack(Packets.PUBREC, id));
            }
        }
    }

    private void deliver(MqttMessage m) {
        boolean handled = false;
        List<Integer> ids = m.properties().subscriptionIds();
        if (v5 && !ids.isEmpty()) {
            for (int id : ids) {
                Sub s = subsById.get(id);
                if (s != null) {
                    handled = true;
                    call(s, m);
                }
            }
        } else {
            for (Sub s : subArray) {
                if (Packets.matches(s.filter, m.topic()) || sharedMatches(s.filter, m.topic())) {
                    handled = true;
                    call(s, m);
                }
            }
        }
        if (!handled) {
            safely(() -> listener.unhandledMessage(m));
        }
    }

    /** A shared subscription {@code $share/group/filter} receives messages of {@code filter}. */
    private static boolean sharedMatches(String filter, String topic) {
        if (!filter.startsWith("$share/")) {
            return false;
        }
        int slash = filter.indexOf('/', 7);
        return slash > 0 && Packets.matches(filter.substring(slash + 1), topic);
    }

    private void call(Sub s, MqttMessage m) {
        try {
            s.handler.onMessage(m);
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "MQTT message handler failed", t);
        }
    }

    /** The packet identifier of an acknowledgement, and (MQTT 5) its reason code and properties, which may be left out when it is success. */
    private record Ack(int id, int reason, Props5 props) {
    }

    private Ack readAck(Packets.Reader r) {
        int id = r.u16();
        int reason = 0;
        Props5 props = null;
        if (v5 && r.remaining() > 0) {
            reason = r.u8();
            if (r.remaining() > 0) {
                props = Props5.read(r);
            }
        }
        r.end();
        return new Ack(id, reason, props);
    }

    private void onPuback(Packets.Reader r) {
        Ack a = readAck(r);
        Outgoing m = inflight.get(a.id);
        if (m == null) {
            return;   // a repeated acknowledgement after a retransmission
        }
        if (m.qos != 1) {
            throw new MqttException("PUBACK for a QoS " + m.qos + " message", false);
        }
        inflight.remove(a.id);
        if (a.reason >= 0x80) {
            failPublish(m, reason(a.reason, "the broker refused the message", a.props));
        } else {
            mAckTime.endAt(m.acceptedAt, mAckTime.now());
            m.done.complete(null);   // 0x00 success, 0x10 no matching subscribers
        }
        pump();
    }

    private void onPubrec(Packets.Reader r) {
        Ack a = readAck(r);
        Outgoing m = inflight.get(a.id);
        if (m != null && m.qos != 2) {
            throw new MqttException("PUBREC for a QoS " + m.qos + " message", false);
        }
        if (m != null && a.reason >= 0x80) {   // refused: the handshake ends here, there is no PUBREL
            inflight.remove(a.id);
            failPublish(m, reason(a.reason, "the broker refused the message", a.props));
            pump();
            return;
        }
        if (m != null) {
            m.released = true;
        }
        send(Packets.ack(Packets.PUBREL, a.id));   // also for an id we no longer know: the broker is waiting for it
    }

    private void onPubcomp(Packets.Reader r) {
        Ack a = readAck(r);
        Outgoing m = inflight.get(a.id);
        if (m == null) {
            return;
        }
        if (m.qos != 2 || !m.released) {
            throw new MqttException("PUBCOMP before PUBREC", false);
        }
        inflight.remove(a.id);
        if (a.reason >= 0x80 && a.reason != 0x92) {   // 0x92: the broker has no record of it, because it already completed it
            failPublish(m, reason(a.reason, "the broker could not complete the message", a.props));
        } else {
            mAckTime.endAt(m.acceptedAt, mAckTime.now());
            m.done.complete(null);
        }
        pump();
    }

    private void onPubrel(Packets.Reader r) {
        Ack a = readAck(r);
        incomingQos2.remove(a.id);
        send(Packets.ack(Packets.PUBCOMP, a.id));
    }

    private void onSuback(Packets.Reader r) {
        int id = r.u16();
        Props5 props = v5 ? Props5.read(r) : null;
        Request req = requests.get(id);
        if (req == null || !req.subscribe) {
            return;
        }
        requests.remove(id);
        if (r.remaining() != req.filters.size()) {
            throw new MqttException("SUBACK has " + r.remaining() + " return codes for " + req.filters.size() + " filters", false);
        }
        QoS first = null;
        MqttException refused = null;
        for (int i = 0; i < req.filters.size(); i++) {
            int code = r.u8();
            String filter = req.filters.get(i);
            if (code >= 0x80) {
                Sub before = req.previous == null ? null : req.previous.get(i);
                if (before == null) {
                    subs.remove(filter);
                } else if (req.previous != null) {
                    subs.put(filter, before);
                }
                if (req.done == null) {
                    subs.remove(filter);
                    safely(() -> listener.resubscribeFailed(filter));
                } else {
                    refused = v5 ? reason(code, "the broker refused the subscription to " + filter, props)
                            : new MqttException(code, "the broker refused the subscription to " + filter, false, null);
                }
            } else if (code > 2) {
                throw new MqttException("unknown SUBACK return code " + code, false);
            } else {
                Sub cur = subs.get(filter);
                QoS granted = QoS.of(code);
                if (cur != null) {
                    subs.put(filter, new Sub(filter, cur.handler, cur.options, granted, cur.id));
                }
                if (first == null) {
                    first = granted;
                }
            }
        }
        rebuildSubs();
        if (req.done != null) {
            if (refused != null) {
                req.done.completeExceptionally(refused);
            } else {
                req.done.complete(first);
            }
        }
    }

    private void onUnsuback(Packets.Reader r) {
        int id = r.u16();
        Props5 props = v5 ? Props5.read(r) : null;
        Request req = requests.get(id);
        if (req == null || req.subscribe) {
            if (!v5) {
                r.end();
            }
            return;
        }
        requests.remove(id);
        MqttException failure = null;
        if (v5) {
            if (r.remaining() != req.filters.size()) {
                throw new MqttException("UNSUBACK has " + r.remaining() + " reason codes for " + req.filters.size() + " filters", false);
            }
            for (int i = 0; i < req.filters.size(); i++) {
                int code = r.u8();
                if (code >= 0x80) {
                    failure = reason(code, "the broker refused to remove the subscription to " + req.filters.get(i), props);
                }
            }
        } else {
            r.end();
        }
        if (req.done != null) {
            if (failure != null) {
                req.done.completeExceptionally(failure);
            } else {
                req.done.complete(null);
            }
        }
    }

    private static MqttException authFailure(RuntimeException e) {
        return e instanceof MqttException m ? m : new MqttException("authentication failed: " + e, false, e);
    }

    /** AUTH from the broker: a challenge while connecting or re-authenticating (0x18), or the end of a re-authentication (0x00). */
    private void onAuth(Packets.Reader r) {
        int code = 0;
        Props5 p = null;
        if (r.remaining() > 0) {
            code = r.u8();
            if (r.remaining() > 0) {
                p = Props5.read(r);
            }
        }
        r.end();
        boolean connecting = state == State.CONNECTING;
        MqttAuthenticator.Exchange ex = connecting ? exchange : reauthExchange;
        if (ex == null) {
            throw new MqttException("the broker sent AUTH, which we did not ask for", false);
        }
        String method = config.v5().authenticator().method();
        if (p == null || !method.equals(p.authMethod)) {
            throw new MqttException("AUTH with authentication method " + (p == null ? null : p.authMethod) + ", not " + method, false);
        }
        if (code == 0x18) {
            byte[] answer;
            try {
                answer = ex.respond(p.authData);
            } catch (RuntimeException e) {
                throw authFailure(e);
            }
            send(Packets.auth(0x18, method, answer));
        } else if (code == 0x00 && !connecting) {
            CompletableFuture<Void> f = reauthDone;
            reauthExchange = null;
            reauthDone = null;
            try {
                ex.finish(p.authData);
            } catch (RuntimeException e) {
                MqttException failure = authFailure(e);
                if (f != null) {
                    f.completeExceptionally(failure);
                }
                throw failure;
            }
            if (f != null) {
                f.complete(null);
            }
        } else {
            throw new MqttException("AUTH with reason code 0x" + Integer.toHexString(code) + (connecting ? " while connecting" : ""), false);
        }
    }

    private void onServerDisconnect(Packets.Reader r) {
        int code = r.remaining() > 0 ? r.u8() : 0;
        Props5 props = r.remaining() > 0 ? Props5.read(r) : null;
        r.end();
        MqttException e = reason(code, "the broker disconnected the client", props);
        // 0x8E: another connection took our session; taking it back would only start a fight with whoever took it
        boolean again = code != 0x8E && (retriableReason(code) || code == 0 || code == 0x8D || code == 0x98);
        fail(new MqttException(code, e.getMessage(), again, null));
    }

    @Override
    public String toString() {
        return "MqttClient[" + config.host() + ":" + config.port() + ", " + state + "]";
    }
}
