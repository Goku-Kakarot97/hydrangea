package io.hydrangea.grpc;

import io.hydrangea.EventLoop;
import io.hydrangea.Scheduled;
import io.hydrangea.http.HalfCloseListener;
import io.hydrangea.http.Headers;
import io.hydrangea.http.Http2Stream;
import io.hydrangea.http.HttpRequest;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.lang.System.Logger;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * One gRPC call on the server: the stage that owns an HTTP/2 stream after the response head went out. It reads length-prefixed request
 * messages from the stream, hands them to the registered handler, writes response messages, and ends the stream with the status in the
 * trailers (RFC-style: {@code grpc-status}, {@code grpc-message}, then whatever the handler put in {@link #trailers()}).
 * <p>
 * Whatever happens the call ends exactly once: normally with the handler's status; with CANCELLED if the client resets the stream or the
 * connection drops (nothing can be sent then, but the handler is told); with DEADLINE_EXCEEDED if {@code grpc-timeout} passes; with INTERNAL
 * or RESOURCE_EXHAUSTED for protocol violations (bad framing, a message over the limit, more requests than the method takes).
 */
final class ServerCall<Req, Resp> implements Stage, HalfCloseListener, CallContext {

    private static final Logger LOG = System.getLogger(ServerCall.class.getName());

    private final MethodDescriptor<Req, Resp> method;
    private final Object handler;
    private final HttpRequest head;
    private final Metadata requestMetadata;
    private final Duration timeout;
    private final boolean gzipOut;
    private final GrpcFraming.Decoder decoder;
    private final Serial serial;
    private final Metadata trailers = new Metadata();
    private final Metadata responseHeaders = new Metadata();
    private boolean headersFlushed;

    private final List<ServerInterceptor> interceptors;
    private final Channelz.ServerEntry channelz;
    private final BinaryLog binaryLog;
    private volatile BinaryLog.CallLog blog;
    private Channelz.SocketEntry socketStats;
    private final byte[] webBody;          // gRPC-Web: the whole request, already received; null for native gRPC
    private final boolean webChunked;
    private final boolean webText;
    private final List<ServerInterceptor.Listener> listeners = new ArrayList<>();
    private boolean closeNotified;

    private Context ctx;
    private Http2Stream stream;
    private Instant deadline;
    private Scheduled deadlineTimer;

    private final Object lock = new Object();
    private boolean finished;                 // a status was sent (or the call died): nothing more goes out
    private volatile boolean cancelled;
    private int responsesSent;
    private final List<Runnable> cancelCallbacks = new ArrayList<>();
    private final List<Runnable> readyCallbacks = new ArrayList<>();
    private boolean wasNotWritable;

    private Req singleRequest;
    private int requestsReceived;
    private StreamObserver<Req> requestObserver;
    private boolean requestsCompleted;

    @SuppressWarnings("unchecked")
    ServerCall(GrpcServer.Registered reg, HttpRequest head, Duration timeout, boolean gzipIn, boolean gzipOut, int maxInbound, Executor executor,
               List<ServerInterceptor> interceptors, Channelz.ServerEntry channelz, BinaryLog binaryLog, byte[] webBody, boolean webChunked, boolean webText) {
        this.webText = webText;
        this.webBody = webBody;
        this.webChunked = webChunked;
        this.interceptors = interceptors;
        this.channelz = channelz;
        this.binaryLog = binaryLog;
        this.method = (MethodDescriptor<Req, Resp>) reg.method();
        this.handler = reg.handler();
        this.head = head;
        this.requestMetadata = Metadata.fromHeaders(head.headers());
        this.timeout = timeout;
        this.gzipOut = gzipOut;
        this.decoder = new GrpcFraming.Decoder(maxInbound, gzipIn);
        this.serial = new Serial(executor);
    }

    // ---- Stage (event loop) ----

    @Override
    public void onActive(Context ctx) {
        this.ctx = ctx;
        if (channelz != null) {
            socketStats = channelz.socket(ctx.connection().remoteAddress(), ctx.connection().localAddress());
            channelz.calls.start();
            socketStats.streams.start();
            socketStats.activeStreams.incrementAndGet();
            socketStats.lastRemoteStreamMillis = System.currentTimeMillis();
        }
        if (binaryLog != null) {
            blog = binaryLog.start(BinaryLog.Side.SERVER, method.path());
            if (blog != null) {
                String authority = head.headers().get(":authority") != null ? head.headers().get(":authority") : head.headers().get("host");
                blog.clientHeader(requestMetadata, method.path(), authority, timeout, ctx.connection().remoteAddress());
            }
        }
        this.stream = webBody != null ? new GrpcWeb.WebStream(ctx.connection(), webChunked, webText, head.headers()) : (Http2Stream) ctx.connection();
        if (timeout != null) {
            deadline = Instant.now().plus(timeout);
            deadlineTimer = ctx.connection().eventLoop().schedule(this::deadlineExceeded, timeout.toNanos(), TimeUnit.NANOSECONDS);
        }
        try {
            for (ServerInterceptor i : interceptors) {
                ServerInterceptor.Listener l = i.start(method, this);
                if (l != null) {
                    listeners.add(l);
                }
            }
        } catch (Throwable t) {
            closeWith(Status.fromThrowable(t), t);
            return;
        }
        if (webBody != null) {
            ctx.connection().eventLoop().execute(() -> {   // after this activation has finished
                onRead(ctx, ByteBuffer.wrap(webBody));
                onPeerHalfClose();
            });
            return;
        }
        if (method.type().clientStreams()) {
            serial.execute(() -> {
                try {
                    requestObserver = invokeStreaming();
                } catch (Throwable t) {
                    closeWith(Status.fromThrowable(t), t);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private StreamObserver<Req> invokeStreaming() throws Exception {
        return ((GrpcServer.StreamingHandler<Req, Resp>) handler).call(this, new Responses());
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        if (isDone()) {
            return;
        }
        try {
            decoder.feed((ByteBuffer) msg, this::onRequestMessage);
        } catch (StatusException e) {
            closeWith(e.status(), e);
        }
    }

    private void onRequestMessage(byte[] payload) throws StatusException {
        Req request;
        try {
            request = method.requestMarshaller().decode(payload);
        } catch (Exception e) {
            throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("failed to parse the request message").withCause(e));
        }
        requestsReceived++;
        if (blog != null) {
            blog.clientMessage(payload);
        }
        if (socketStats != null) {
            socketStats.messageReceived();
        }
        notifyListeners(l -> l.onRequest(request));
        if (!method.type().clientStreams()) {
            if (requestsReceived > 1) {
                throw new StatusException(Status.of(Status.Code.INTERNAL).withDescription("too many requests: " + method.fullName() + " takes one"));
            }
            singleRequest = request;
            return;
        }
        serial.execute(() -> {
            StreamObserver<Req> o = requestObserver;
            if (o == null || isDone()) {
                return;
            }
            try {
                o.onNext(request);
            } catch (Throwable t) {
                closeWith(Status.fromThrowable(t), t);
            }
        });
    }

    @Override
    public void onPeerHalfClose() {
        if (isDone()) {
            return;
        }
        try {
            decoder.finish();
        } catch (StatusException e) {
            closeWith(e.status(), e);
            return;
        }
        if (blog != null) {
            blog.clientHalfClose();
        }
        if (method.type().clientStreams()) {
            serial.execute(() -> {
                StreamObserver<Req> o = requestObserver;
                if (o == null || isDone() || requestsCompleted) {
                    return;
                }
                requestsCompleted = true;
                try {
                    o.onCompleted();
                } catch (Throwable t) {
                    closeWith(Status.fromThrowable(t), t);
                }
            });
            return;
        }
        if (requestsReceived == 0) {
            closeWith(Status.of(Status.Code.INTERNAL).withDescription("the client finished without sending a request"), null);
            return;
        }
        serial.execute(this::invokeSingleRequestHandler);
    }

    @SuppressWarnings("unchecked")
    private void invokeSingleRequestHandler() {
        if (isDone()) {
            return;
        }
        try {
            if (method.type() == MethodDescriptor.MethodType.UNARY) {
                CompletionStage<Resp> result = ((GrpcServer.UnaryHandler<Req, Resp>) handler).call(singleRequest, this);
                result.whenComplete((resp, err) -> {
                    if (err != null) {
                        closeWith(Status.fromThrowable(err instanceof java.util.concurrent.CompletionException && err.getCause() != null ? err.getCause() : err), err);
                    } else if (send(resp)) {
                        closeWith(Status.OK, null);
                    }
                });
            } else {
                ((GrpcServer.ServerStreamingHandler<Req, Resp>) handler).call(singleRequest, this, new Responses());
            }
        } catch (Throwable t) {
            closeWith(Status.fromThrowable(t), t);
        }
    }

    @Override
    public void onWritabilityChanged(Context ctx) {
        List<Runnable> callbacks;
        synchronized (lock) {
            if (!wasNotWritable) {
                return;
            }
            wasNotWritable = false;
            callbacks = new ArrayList<>(readyCallbacks);
        }
        for (Runnable r : callbacks) {
            serial.execute(() -> safely(r));
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        boolean wasDone;
        synchronized (lock) {
            wasDone = finished;
            finished = true;
        }
        cancelTimer();
        if (!wasDone) {
            Status gone = Status.of(Status.Code.CANCELLED).withDescription("the client cancelled the call or the connection was lost");
            if (blog != null) {
                blog.cancel();
            }
            notifyClose(gone);
            cancel(gone);
        }
    }

    // ---- finishing ----

    private boolean isDone() {
        synchronized (lock) {
            return finished;
        }
    }

    private void deadlineExceeded() {
        if (isDone()) {
            return;
        }
        closeWith(Status.of(Status.Code.DEADLINE_EXCEEDED).withDescription("deadline exceeded after " + timeout.toMillis() + " ms"), null);
    }

    /** Sends the final status once; tells the handler the call is over if it did not end well. */
    private void closeWith(Status status, Throwable cause) {
        synchronized (lock) {
            if (finished) {
                return;
            }
            finished = true;
        }
        cancelTimer();
        notifyClose(status);
        if (!status.isOk() && status.code() == Status.Code.UNKNOWN && cause != null && !(cause instanceof StatusException)) {
            LOG.log(Logger.Level.WARNING, "handler of " + method.fullName() + " failed", cause);
        }
        Headers h = new Headers();
        h.add("grpc-status", Integer.toString(status.code().value()));
        if (status.description() != null && !status.description().isEmpty()) {
            h.add("grpc-message", GrpcFraming.encodeMessage(status.description()));
        }
        if (cause instanceof StatusException se && se.trailers() != null) {
            se.trailers().toHeaders(h);
        }
        trailers.toHeaders(h);
        try {
            flushResponseHeaders();
            if (blog != null) {
                Metadata sent = trailers.copy();
                if (cause instanceof StatusException se && se.trailers() != null) {
                    se.trailers().forEachRaw(sent::putRaw);
                }
                blog.serverTrailer(status, sent, null);
            }
            stream.sendTrailers(h);
        } catch (RuntimeException e) {
            // the stream is already gone; the handler is told below
        }
        if (!status.isOk()) {
            cancel(status);
        }
    }

    /** Runs the cancel callbacks and tells a request observer; once. */
    private void cancel(Status status) {
        List<Runnable> callbacks;
        synchronized (lock) {
            if (cancelled) {
                return;
            }
            cancelled = true;
            callbacks = new ArrayList<>(cancelCallbacks);
        }
        for (Runnable r : callbacks) {
            serial.execute(() -> safely(r));
        }
        StreamObserver<Req> o = requestObserver;
        if (o != null && !requestsCompleted) {
            serial.execute(() -> {
                try {
                    o.onError(status.asException());
                } catch (Throwable ignored) {
                    // nowhere to report
                }
            });
        }
    }

    /** Hands the handler's initial response headers to the transport, once, before anything else is written. */
    private void flushResponseHeaders() {
        synchronized (lock) {
            if (headersFlushed) {
                return;
            }
            headersFlushed = true;
        }
        if (blog != null) {
            blog.serverHeader(responseHeaders, null);
        }
        if (webBody == null && !responseHeaders.isEmpty()) {
            Headers h = new Headers();
            responseHeaders.toHeaders(h);
            stream.addResponseHeaders(h);
        }
    }

    @Override
    public Metadata responseHeaders() {
        return responseHeaders;
    }

    private void notifyListeners(java.util.function.Consumer<ServerInterceptor.Listener> event) {
        if (listeners.isEmpty()) {
            return;
        }
        synchronized (listeners) {
            for (ServerInterceptor.Listener l : listeners) {
                try {
                    event.accept(l);
                } catch (Throwable t) {
                    LOG.log(Logger.Level.WARNING, "interceptor failed", t);
                }
            }
        }
    }

    private void notifyClose(Status status) {
        synchronized (listeners) {
            if (closeNotified) {
                return;
            }
            closeNotified = true;
        }
        if (channelz != null && socketStats != null) {
            channelz.calls.end(status.isOk());
            socketStats.streams.end(status.isOk());
            socketStats.activeStreams.decrementAndGet();
            socketStats.lastActiveNanos = System.nanoTime();
        }
        notifyListeners(l -> l.onClose(status));
    }

    private void cancelTimer() {
        Scheduled t = deadlineTimer;
        if (t != null) {
            t.cancel();
        }
    }

    private static void safely(Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            LOG.log(Logger.Level.WARNING, "gRPC callback failed", t);
        }
    }

    // ---- sending ----

    /** @return false if the message could not be sent (the call has been failed then) */
    private boolean send(Resp value) {
        byte[] framed;
        byte[] payload;
        try {
            payload = method.responseMarshaller().encode(value);
            // plain HTTP/2 without compression: the 5-byte prefix and the body go out as two writes that the stream sends in one frame, so a big
            // body is not copied just to put a prefix in front of it (gRPC-Web wraps and encodes whole messages, so it keeps one buffer)
            framed = !gzipOut && webBody == null && payload.length >= 4096 ? null : GrpcFraming.frame(payload, gzipOut);
        } catch (Exception e) {
            closeWith(Status.of(Status.Code.INTERNAL).withDescription("failed to serialize the response").withCause(e), e);
            return false;
        }
        synchronized (lock) {
            if (finished) {
                return false;
            }
            if (!method.type().serverStreams() && responsesSent >= 1) {
                // fall through to fail outside the lock
            } else {
                responsesSent++;
                if (socketStats != null) {
                    socketStats.messageSent();
                }
                flushResponseHeaders();
                if (blog != null) {
                    blog.serverMessage(payload);
                }
                notifyListeners(l -> l.onResponse(value));
                if (framed != null) {
                    stream.write(ByteBuffer.wrap(framed));
                } else {
                    stream.write(ByteBuffer.wrap(GrpcFraming.prefix(payload.length)));
                    stream.write(ByteBuffer.wrap(payload));
                }
                if (!stream.isWritable()) {
                    wasNotWritable = true;
                }
                return true;
            }
        }
        closeWith(Status.of(Status.Code.INTERNAL).withDescription("too many responses: " + method.fullName() + " returns one"), null);
        return false;
    }

    /** The observer handlers write responses into. */
    private final class Responses implements StreamObserver<Resp> {
        @Override
        public void onNext(Resp value) {
            send(value);
        }

        @Override
        public void onError(Throwable t) {
            closeWith(Status.fromThrowable(t), t);
        }

        @Override
        public void onCompleted() {
            synchronized (lock) {
                if (finished) {
                    return;
                }
            }
            if (!method.type().serverStreams() && responsesSent == 0) {
                closeWith(Status.of(Status.Code.INTERNAL).withDescription("completed without a response"), null);
            } else {
                closeWith(Status.OK, null);
            }
        }
    }

    // ---- CallContext ----

    @Override
    public String method() {
        return method.path();
    }

    @Override
    public Metadata requestMetadata() {
        return requestMetadata;
    }

    @Override
    public Metadata trailers() {
        return trailers;
    }

    @Override
    public SocketAddress remoteAddress() {
        return stream == null ? null : stream.remoteAddress();
    }

    @Override
    public Instant deadline() {
        return deadline;
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }

    @Override
    public void onCancel(Runnable r) {
        boolean now;
        synchronized (lock) {
            now = cancelled;
            if (!now) {
                cancelCallbacks.add(r);
            }
        }
        if (now) {
            serial.execute(() -> safely(r));
        }
    }

    @Override
    public boolean isReady() {
        return stream != null && stream.isWritable();
    }

    @Override
    public void onReady(Runnable r) {
        synchronized (lock) {
            readyCallbacks.add(r);
        }
    }

    @Override
    public void setAutoRead(boolean enabled) {
        if (stream != null) {
            stream.setAutoRead(enabled);
        }
    }

    // ---- ordered execution of handler callbacks ----

    /** Runs tasks one after another in submission order, on the event loop's thread (no executor) or on an executor without overlap. */
    private static final class Serial {
        private final Executor executor;
        private final ArrayDeque<Runnable> queue = new ArrayDeque<>();
        private boolean running;

        Serial(Executor executor) {
            this.executor = executor;
        }

        void execute(Runnable r) {
            if (executor == null) {
                safely(r);
                return;
            }
            synchronized (queue) {
                queue.add(r);
                if (running) {
                    return;
                }
                running = true;
            }
            executor.execute(this::drain);
        }

        private void drain() {
            for (;;) {
                Runnable next;
                synchronized (queue) {
                    next = queue.poll();
                    if (next == null) {
                        running = false;
                        return;
                    }
                }
                safely(next);
            }
        }
    }
}
