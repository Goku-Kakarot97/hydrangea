package io.hydrangea.amqp;

import io.hydrangea.EventLoopGroup;

import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * An AMQP connection that comes back by itself. When the link to the broker is lost (network, broker restart, missed heartbeats, a
 * broker-initiated close) it reconnects with back-off ({@link RecoveryPolicy}) and puts everything back the way the application had it:
 * exchanges, queues and bindings it declared, every open channel with its prefetch limit and confirm mode, and every consumer. The
 * {@link RecoveringChannel} objects the application holds stay valid.
 * <p>
 * <b>What the application must still handle</b> (nothing here can do it for it):
 * <ul>
 *   <li>Calls made while the connection is down fail at once with an {@link AmqpException} (connection error) instead of waiting.
 *       A publish whose confirmation was still pending when the link broke fails too: the broker may or may not have the message,
 *       so republishing means <i>at least once</i> delivery; make consumers tolerate duplicates.</li>
 *   <li>Messages delivered but not yet acknowledged when the link broke are redelivered by the broker (with the redelivered flag).
 *       Acknowledging a delivery from before the break is ignored, not an error: it can no longer be acknowledged.</li>
 *   <li>A queue named by the broker ({@code queueDeclare()} without a name) gets a <i>new</i> name after recovery. Use the name you were
 *       given as before: publishing to the default exchange and the calls of this class translate it ({@link #queueName}).</li>
 *   <li>Declarations are replayed as they were made. One the broker now refuses (the queue exists with other settings, 406) is dropped and
 *       reported to {@link RecoveryListener#declarationFailed}; recovery goes on with the rest. A channel whose own setup (consume, prefetch)
 *       the broker refuses is closed with the broker's error and its consumers are told.</li>
 * </ul>
 * Recovery runs on its own virtual thread, never on an event loop. {@link #closeAsync()} stops it.
 */
public final class RecoveringConnection implements AutoCloseable {

    private static final Logger LOG = System.getLogger(RecoveringConnection.class.getName());

    enum State { CONNECTED, RECOVERING, CLOSED }

    record ExchangeDecl(String name, String type, boolean durable, boolean autoDelete, Map<String, Object> args) {
    }

    record QueueDecl(String name, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> args, boolean serverNamed) {
    }

    record BindingDecl(String queue, String exchange, String routingKey, Map<String, Object> args) {
    }

    private final EventLoopGroup group;
    private final AmqpConfig config;
    private final RecoveryPolicy policy;
    private final Object lock = new Object();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final CopyOnWriteArrayList<RecoveringChannel> channels = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<RecoveryListener> listeners = new CopyOnWriteArrayList<>();

    // guarded by lock
    private final LinkedHashMap<String, ExchangeDecl> exchanges = new LinkedHashMap<>();
    private final LinkedHashMap<String, QueueDecl> queues = new LinkedHashMap<>();
    private final List<BindingDecl> bindings = new ArrayList<>();
    private Thread recoveryThread;

    private final Map<String, String> aliases = new ConcurrentHashMap<>(); // name the application knows -> name on the broker now
    private volatile State state = State.RECOVERING;
    private volatile AmqpConnection current;

    private RecoveringConnection(EventLoopGroup group, AmqpConfig config, RecoveryPolicy policy) {
        this.group = group;
        this.config = config;
        this.policy = policy;
    }

    /**
     * Connects. The first attempt is not retried: a wrong address or password fails the future at once. Only a connection that was
     * working and is lost gets recovered.
     */
    public static CompletableFuture<RecoveringConnection> connect(EventLoopGroup group, AmqpConfig config, RecoveryPolicy policy) {
        RecoveringConnection rc = new RecoveringConnection(group, config, policy);
        return AmqpConnection.connect(group, config).thenApply(c -> {
            rc.attach(c);
            return rc;
        });
    }

    public static CompletableFuture<RecoveringConnection> connect(EventLoopGroup group, AmqpConfig config) {
        return connect(group, config, RecoveryPolicy.defaults());
    }

    public void addRecoveryListener(RecoveryListener listener) {
        listeners.add(listener);
    }

    /** Opens a channel; fails while the connection is being recovered. */
    public CompletableFuture<RecoveringChannel> openChannel() {
        AmqpConnection c = current;
        if (state != State.CONNECTED || c == null) {
            return CompletableFuture.failedFuture(unavailable());
        }
        return c.openChannel().thenApply(d -> {
            RecoveringChannel ch = new RecoveringChannel(this, d);
            channels.add(ch);
            return ch;
        });
    }

    /** True while connected; false while recovering or after close. */
    public boolean isOpen() {
        return state == State.CONNECTED;
    }

    public boolean isRecovering() {
        return state == State.RECOVERING;
    }

    /** Completes when the connection is closed for good: by {@link #closeAsync()} or because recovery gave up. */
    public CompletableFuture<Void> closeFuture() {
        return closed;
    }

    /** The current name on the broker of a queue the application knows by {@code name} (differs only for broker-named queues after recovery). */
    public String queueName(String name) {
        return aliases.getOrDefault(name, name);
    }

    /** Closes for good: stops any recovery in progress, closes the connection politely. */
    public CompletableFuture<Void> closeAsync() {
        Thread t;
        AmqpConnection c;
        synchronized (lock) {
            if (state == State.CLOSED) {
                return closed;
            }
            state = State.CLOSED;
            t = recoveryThread;
            c = current;
        }
        if (t != null) {
            t.interrupt();
        }
        AmqpException why = new AmqpException(AmqpException.CHANNEL_ERROR, "the connection was closed", true);
        for (RecoveringChannel ch : channels) {
            ch.failPermanently(why);
        }
        if (c != null && c.isOpen()) {
            c.closeAsync().whenComplete((v, e) -> closed.complete(null));
        } else {
            closed.complete(null);
        }
        return closed;
    }

    @Override
    public void close() {
        closeAsync();
    }

    // ---- used by RecoveringChannel ----

    State state() {
        return state;
    }

    String alias(String queue) {
        return aliases.getOrDefault(queue, queue);
    }

    void removeChannel(RecoveringChannel ch) {
        channels.remove(ch);
    }

    AmqpException unavailable() {
        return state == State.CLOSED
                ? new AmqpException(AmqpException.CHANNEL_ERROR, "the connection is closed", true)
                : new AmqpException(AmqpException.CHANNEL_ERROR, "the connection to the broker is being recovered; retry shortly", true);
    }

    void recordExchange(ExchangeDecl d) {
        synchronized (lock) {
            exchanges.put(d.name(), d);
        }
    }

    void forgetExchange(String name) {
        synchronized (lock) {
            exchanges.remove(name);
            bindings.removeIf(b -> b.exchange().equals(name));
        }
    }

    void recordQueue(QueueDecl d) {
        synchronized (lock) {
            queues.put(d.name(), d);
        }
    }

    void forgetQueue(String name) {
        synchronized (lock) {
            queues.remove(name);
            bindings.removeIf(b -> b.queue().equals(name));
            aliases.remove(name);
        }
    }

    void recordBinding(BindingDecl b) {
        synchronized (lock) {
            if (!bindings.contains(b)) {
                bindings.add(b);
            }
        }
    }

    void forgetBinding(BindingDecl b) {
        synchronized (lock) {
            bindings.remove(b);
        }
    }

    // ---- recovery ----

    private void attach(AmqpConnection c) {
        synchronized (lock) {
            current = c;
            state = State.CONNECTED;
        }
        c.closeFuture().thenRun(() -> lost(c));
    }

    private void lost(AmqpConnection c) {
        synchronized (lock) {
            if (c != current || state != State.CONNECTED) {
                return; // closed by the application, or an older connection
            }
            state = State.RECOVERING;
            AmqpException why = c.lostReason() != null ? c.lostReason()
                    : new AmqpException(AmqpException.CHANNEL_ERROR, "the connection to the broker was lost", true);
            recoveryThread = Thread.ofVirtual().name("amqp-recovery").unstarted(() -> recover(why));
            recoveryThread.start();
        }
    }

    private void recover(Throwable cause) {
        fire(l -> l.connectionLost(cause));
        Throwable last = cause;
        for (int attempt = 1; ; attempt++) {
            if (policy.maxAttempts() > 0 && attempt > policy.maxAttempts()) {
                giveUp(last);
                return;
            }
            try {
                Thread.sleep(policy.delayFor(attempt));
            } catch (InterruptedException closing) {
                return;
            }
            if (state == State.CLOSED) {
                return;
            }
            int a = attempt;
            fire(l -> l.attemptStarting(a));
            AmqpConnection c = null;
            try {
                c = AmqpConnection.connect(group, config).get(60, TimeUnit.SECONDS);
                replay(c);
                synchronized (lock) {
                    if (state == State.CLOSED) {
                        c.close();
                        return;
                    }
                    current = c;
                    state = State.CONNECTED;
                    recoveryThread = null;
                }
                AmqpConnection fresh = c;
                c.closeFuture().thenRun(() -> lost(fresh));
                fire(RecoveryListener::recovered);
                return;
            } catch (InterruptedException closing) {
                if (c != null) {
                    c.close();
                }
                return;
            } catch (Exception e) {
                last = unwrap(e);
                if (c != null) {
                    c.close();
                }
                if (last instanceof AmqpException a2 && (a2.replyCode() == 403 || a2.replyCode() == 530)) {
                    giveUp(last); // refused: bad credentials or no such virtual host; waiting will not change that
                    return;
                }
                LOG.log(Logger.Level.INFO, "AMQP recovery attempt {0} failed: {1}", attempt, last.toString());
            }
        }
    }

    private static Throwable unwrap(Throwable e) {
        Throwable t = e;
        while ((t instanceof ExecutionException || t instanceof CompletionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    /** Declares everything again on {@code c}, then recovers each channel. Runs on the recovery thread; blocks. */
    private void replay(AmqpConnection c) throws Exception {
        List<ExchangeDecl> ex;
        List<QueueDecl> qs;
        List<BindingDecl> bs;
        synchronized (lock) {
            ex = new ArrayList<>(exchanges.values());
            qs = new ArrayList<>(queues.values());
            bs = new ArrayList<>(bindings);
        }
        AmqpChannel t = c.openChannel().get(30, TimeUnit.SECONDS);
        for (ExchangeDecl d : ex) {
            t = declare(c, t, "exchange " + d.name(), () -> forgetExchange(d.name()),
                    ch -> ch.exchangeDeclare(d.name(), d.type(), d.durable(), d.autoDelete(), d.args()).get(30, TimeUnit.SECONDS));
        }
        for (QueueDecl d : qs) {
            t = declare(c, t, "queue " + d.name(), () -> forgetQueue(d.name()), ch -> {
                if (d.serverNamed()) {
                    aliases.put(d.name(), ch.queueDeclare().get(30, TimeUnit.SECONDS).name());
                } else {
                    ch.queueDeclare(d.name(), d.durable(), d.exclusive(), d.autoDelete(), d.args()).get(30, TimeUnit.SECONDS);
                }
            });
        }
        for (BindingDecl b : bs) {
            t = declare(c, t, "binding " + b.queue() + " -> " + b.exchange() + " (" + b.routingKey() + ")", () -> forgetBinding(b),
                    ch -> ch.queueBind(alias(b.queue()), b.exchange(), b.routingKey(), b.args()).get(30, TimeUnit.SECONDS));
        }
        if (t.isOpen()) {
            t.close().get(30, TimeUnit.SECONDS);
        }
        for (RecoveringChannel ch : channels) {
            try {
                ch.recover(c);
            } catch (ExecutionException | CompletionException e) {
                Throwable cause = unwrap(e);
                if (cause instanceof AmqpException a && !a.isConnectionError()) {
                    ch.failPermanently(a); // this channel cannot be put back (e.g. its queue is gone); the others carry on
                } else {
                    throw e;
                }
            }
        }
    }

    private interface Declaration {
        void run(AmqpChannel channel) throws Exception;
    }

    /**
     * Makes one declaration. A refusal by the broker that only closes the channel (406, 404, ...) will refuse again next time, so the declaration is
     * dropped, the listeners are told, and a fresh channel carries on; anything else (the connection itself failing) propagates and fails the attempt.
     */
    private AmqpChannel declare(AmqpConnection c, AmqpChannel t, String what, Runnable forget, Declaration d) throws Exception {
        try {
            d.run(t);
            return t;
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof AmqpException a && !a.isConnectionError()) {
                LOG.log(Logger.Level.WARNING, "AMQP recovery: the broker refused to declare {0} again ({1}); dropping it", what, a.getMessage());
                forget.run();
                fire(l -> l.declarationFailed(what, a));
                return c.openChannel().get(30, TimeUnit.SECONDS); // the broker closed the old one
            }
            throw e;
        }
    }

    private void giveUp(Throwable cause) {
        synchronized (lock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            recoveryThread = null;
        }
        for (RecoveringChannel ch : channels) {
            ch.failPermanently(cause);
        }
        fire(l -> l.gaveUp(cause));
        closed.complete(null);
    }

    private void fire(Consumer<RecoveryListener> call) {
        for (RecoveryListener l : listeners) {
            try {
                call.accept(l);
            } catch (Throwable t) {
                LOG.log(Logger.Level.WARNING, "recovery listener failed", t);
            }
        }
    }
}
