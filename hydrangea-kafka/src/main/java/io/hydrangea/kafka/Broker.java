package io.hydrangea.kafka;

import io.hydrangea.Client;
import io.hydrangea.EventLoop;
import io.hydrangea.Options;
import io.hydrangea.Scheduled;
import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;

import java.lang.System.Logger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * One connection to one broker. A request carries a correlation id; the broker answers in the order of the requests, so the replies are matched from a queue
 * (and the id is checked). Everything runs on the loop given to it. A connection that is lost fails its waiting requests; the owner makes a new one.
 */
final class Broker {

    private static final Logger LOG = System.getLogger(Broker.class.getName());

    final int nodeId;
    final String host;
    final int port;
    private final KafkaConfig config;
    private final EventLoop loop;
    private Context ctx;
    private boolean closed;
    private boolean online;
    private int correlation;
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final CompletableFuture<Broker> ready = new CompletableFuture<>();
    private Accumulator acc = new Accumulator();
    private int frameLength = -1;
    private Scheduled connectTimer;
    private Consumer<Broker> onLost = b -> { };

    private static final class Pending {
        final int id;
        final int api;
        final CompletableFuture<Wire.Reader> future;
        Scheduled timeout;

        Pending(int id, int api, CompletableFuture<Wire.Reader> future) {
            this.id = id;
            this.api = api;
            this.future = future;
        }
    }

    Broker(int nodeId, String host, int port, KafkaConfig config, EventLoop loop) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.config = config;
        this.loop = loop;
    }

    void onLost(Consumer<Broker> r) {
        this.onLost = r;
    }

    boolean isOnline() {
        return online && !closed;
    }

    /** Connects (and logs in with SASL when configured). Call on the loop. */
    CompletableFuture<Broker> connect() {
        Thread.startVirtualThread(() -> {
            InetSocketAddress addr;
            try {
                addr = new InetSocketAddress(host, port);
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(host);
                }
            } catch (Exception e) {
                loop.execute(() -> lost(new KafkaException("cannot resolve " + host + ": " + e, true, e)));
                return;
            }
            Stage stage = new BrokerStage();
            Pipeline pipeline = config.sslContext() == null ? Pipeline.of(stage) : Pipeline.of(TlsStage.client(config.sslContext(), host, port), stage);
            Options options = Options.defaults().withKeepAlive(true).withConnectTimeout(config.connectTimeout());
            Client.connect(loop, addr, options, pipeline).whenComplete((c, err) -> {
                if (err != null) {
                    loop.execute(() -> lost(new KafkaException("cannot connect to " + host + ":" + port + ": " + err, true, err)));
                }
            });
        });
        return ready;
    }

    private final class BrokerStage implements Stage {
        @Override
        public void onActive(Context c) {
            if (closed) {
                c.connection().close();
                return;
            }
            ctx = c;
            if (config.saslUser() == null) {
                online = true;
                ready.complete(Broker.this);
            } else {
                login();
            }
        }

        @Override
        public void onRead(Context c, Object msg) {
            try {
                onData((ByteBuffer) msg);
            } catch (KafkaException e) {
                lost(e);
            } catch (RuntimeException e) {
                LOG.log(Logger.Level.WARNING, "Kafka response handling failed", e);
                lost(new KafkaException("internal error: " + e, false, e));
            }
        }

        @Override
        public void onClose(Context c, Throwable cause) {
            lost(new KafkaException("connection to " + host + ":" + port + " lost" + (cause != null ? ": " + cause : ""), true, cause));
            c.fireClose(cause);
        }
    }

    /** SASL/PLAIN: SaslHandshake v1 then SaslAuthenticate v0 with "\0user\0password". */
    private void login() {
        send(Api.SASL_HANDSHAKE, 1, w -> w.str("PLAIN")).whenComplete((r, e) -> {
            if (e != null) {
                lost(e);
                return;
            }
            int err = r.i16();
            if (err != 0) {
                lost(KafkaException.of(err, "SASL handshake with " + host + ":" + port));
                return;
            }
            byte[] token = ("\0" + config.saslUser() + "\0" + config.saslPassword()).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            send(Api.SASL_AUTHENTICATE, 0, w -> w.bytes(token)).whenComplete((r2, e2) -> {
                if (e2 != null) {
                    lost(e2);
                    return;
                }
                int err2 = r2.i16();
                String msg = r2.str();
                if (err2 != 0) {
                    lost(new KafkaException("SASL login failed: " + KafkaException.name(err2) + (msg != null ? " (" + msg + ")" : ""), err2, false, null));
                    return;
                }
                online = true;
                ready.complete(this);
            });
        });
    }

    /** Sends a request; completes with a reader positioned after the correlation id. Fails with a retriable exception if the connection goes. */
    CompletableFuture<Wire.Reader> send(int api, int version, java.util.function.Consumer<Wire.Writer> body) {
        return send(api, version, 256, body);
    }

    /** {@code sizeHint}: about how many bytes the request will take, so that its buffer is not grown and copied over and over. */
    CompletableFuture<Wire.Reader> send(int api, int version, int sizeHint, java.util.function.Consumer<Wire.Writer> body) {
        CompletableFuture<Wire.Reader> f = new CompletableFuture<>();
        if (closed || ctx == null) {
            f.completeExceptionally(new KafkaException("not connected to " + host + ":" + port, true));
            return f;
        }
        int id = ++correlation;
        Wire.Writer w = new Wire.Writer(sizeHint);
        w.i32(0).i16(api).i16(version).i32(id).str(config.clientId());
        body.accept(w);
        w.patchI32(0, w.len - 4);
        Pending p = new Pending(id, api, f);
        p.timeout = loop.schedule(() -> {
            if (pending.remove(p)) {
                f.completeExceptionally(new KafkaException("no response to request " + api + " from " + host + ":" + port + " within " + config.requestTimeout().toMillis() + " ms", true));
                // a late answer would be taken for the next one: the connection cannot be trusted any more
                lost(new KafkaException("request timed out", true));
            }
        }, config.requestTimeout().toNanos(), TimeUnit.NANOSECONDS);
        pending.add(p);
        try {
            ctx.write(ByteBuffer.wrap(w.buf, 0, w.len));
        } catch (Exception e) {
            lost(new KafkaException("write failed: " + e, true, e));
        }
        return f;
    }

    /** Sends a request the broker does not answer (Produce with acks=0). */
    void sendNoReply(int api, int version, java.util.function.Consumer<Wire.Writer> body) {
        if (closed || ctx == null) {
            throw new KafkaException("not connected to " + host + ":" + port, true);
        }
        Wire.Writer w = new Wire.Writer(256);
        w.i32(0).i16(api).i16(version).i32(++correlation).str(config.clientId());
        body.accept(w);
        w.patchI32(0, w.len - 4);
        try {
            ctx.write(ByteBuffer.wrap(w.buf, 0, w.len));
        } catch (Exception e) {
            lost(new KafkaException("write failed: " + e, true, e));
            throw new KafkaException("write failed: " + e, true, e);
        }
    }

    private void onData(ByteBuffer data) {
        ByteBuffer in;
        if (acc.remaining() == 0) {
            in = data;
        } else {
            acc.append(data);
            in = acc.buffer();
        }
        for (;;) {
            if (frameLength < 0) {
                if (in.remaining() < 4) {
                    break;
                }
                frameLength = in.getInt();
                if (frameLength < 4 || frameLength > config.maxResponseBytes()) {
                    throw new KafkaException("protocol error: response of " + frameLength + " bytes (limit " + config.maxResponseBytes() + ")", false);
                }
            }
            if (in.remaining() < frameLength) {
                break;
            }
            byte[] frame = new byte[frameLength];
            in.get(frame);
            frameLength = -1;
            if (closed) {
                return;
            }
            dispatch(frame);
        }
        if (in == data && data.hasRemaining()) {
            acc.append(data);
        }
    }

    private void dispatch(byte[] frame) {
        Wire.Reader r = new Wire.Reader(frame);
        int id = r.i32();
        Pending p = pending.poll();
        if (p == null) {
            throw new KafkaException("protocol error: a response with no request waiting (correlation id " + id + ")", false);
        }
        if (p.timeout != null) {
            p.timeout.cancel();
        }
        if (p.id != id) {
            p.future.completeExceptionally(new KafkaException("protocol error: correlation id " + id + ", expected " + p.id, false));
            throw new KafkaException("protocol error: correlation id " + id + ", expected " + p.id, false);
        }
        p.future.complete(r);
    }

    private void lost(Throwable cause) {
        if (closed) {
            return;
        }
        closed = true;
        online = false;
        if (connectTimer != null) {
            connectTimer.cancel();
        }
        acc.release();
        KafkaException e = cause instanceof KafkaException k ? k : new KafkaException("connection lost: " + cause, true, cause);
        ready.completeExceptionally(e);
        for (Pending p; (p = pending.poll()) != null; ) {
            if (p.timeout != null) {
                p.timeout.cancel();
            }
            p.future.completeExceptionally(e);
        }
        Context c = ctx;
        ctx = null;
        if (c != null) {
            c.connection().close();
        }
        onLost.accept(this);
    }

    void close() {
        lost(new KafkaException("the client is closed", false));
    }

    /** Number of requests sent and not yet answered. */
    int inFlight() {
        return pending.size();
    }

    static final class Api {
        static final int PRODUCE = 0, FETCH = 1, LIST_OFFSETS = 2, METADATA = 3, OFFSET_COMMIT = 8, OFFSET_FETCH = 9, FIND_COORDINATOR = 10, JOIN_GROUP = 11,
                HEARTBEAT = 12, LEAVE_GROUP = 13, SYNC_GROUP = 14, SASL_HANDSHAKE = 17, CREATE_TOPICS = 19, INIT_PRODUCER_ID = 22, SASL_AUTHENTICATE = 36;

        private Api() {
        }
    }
}
