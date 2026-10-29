import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Server;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.GrpcServer;
import io.hydrangea.grpc.StreamObserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * gRPC benchmark: the same echo service on Hydrangea and on grpc-java (netty), and the same closed-loop load from either client, so every
 * server sees identical load and every client sees an identical server.
 * <pre>
 *   GrpcBench server hydrangea|java port ioThreads
 *   GrpcBench load   hydrangea|java port scenario inflight channels seconds serverPid
 * </pre>
 * scenarios: unary-16 | unary-4k | unary-64k | stream-100x1k (one call returning 100 messages of 1 KiB; counts messages)
 */
public final class GrpcBench {

    // ---- grpc-java side: the same wire format as GrpcEchoExample.MSG ----
    static final io.grpc.MethodDescriptor.Marshaller<Msg> JM = new io.grpc.MethodDescriptor.Marshaller<>() {
        public InputStream stream(Msg m) {
            try {
                ByteArrayOutputStream o = new ByteArrayOutputStream(m.blob().length + 32);
                CodedOutputStream c = CodedOutputStream.newInstance(o);
                if (!m.text().isEmpty()) c.writeString(1, m.text());
                if (m.n() != 0) c.writeInt32(2, m.n());
                if (m.blob().length > 0) c.writeByteArray(3, m.blob());
                c.flush();
                return new ByteArrayInputStream(o.toByteArray());
            } catch (IOException e) { throw new RuntimeException(e); }
        }
        public Msg parse(InputStream in) {
            try {
                CodedInputStream c = CodedInputStream.newInstance(in);
                String t = ""; int n = 0; byte[] b = new byte[0];
                for (int tag; (tag = c.readTag()) != 0; ) {
                    switch (tag >>> 3) { case 1 -> t = c.readString(); case 2 -> n = c.readInt32(); case 3 -> b = c.readByteArray(); default -> c.skipField(tag); }
                }
                return new Msg(t, n, b);
            } catch (IOException e) { throw new RuntimeException(e); }
        }
    };
    static io.grpc.MethodDescriptor<Msg, Msg> jmd(String name, io.grpc.MethodDescriptor.MethodType t) {
        return io.grpc.MethodDescriptor.<Msg, Msg>newBuilder().setType(t).setFullMethodName("hydrangea.test.Echo/" + name)
                .setRequestMarshaller(JM).setResponseMarshaller(JM).build();
    }
    static final io.grpc.MethodDescriptor<Msg, Msg> J_UNARY = jmd("Unary", io.grpc.MethodDescriptor.MethodType.UNARY);
    static final io.grpc.MethodDescriptor<Msg, Msg> J_STREAM = jmd("ServerStream", io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING);

    // ---- servers ----
    public static void main(String[] a) throws Exception {
        if (a[0].equals("server")) {
            server(a[1], Integer.parseInt(a[2]), Integer.parseInt(a[3]));
        } else {
            try { load(a[1], Integer.parseInt(a[2]), a[3], Integer.parseInt(a[4]), Integer.parseInt(a[5]), Integer.parseInt(a[6]), Long.parseLong(a[7])); } catch (Throwable t) { t.printStackTrace(); System.exit(1); }
        }
    }

    static void server(String impl, int port, int threads) throws Exception {
        if (impl.equals("hydrangea")) {
            GrpcServer grpc = GrpcEchoExample.builder().build();
            EventLoopGroup group = new EventLoopGroup(threads);
            Server s = grpc.bind(group, new InetSocketAddress(System.getProperty("bind", "127.0.0.1"), port), null);
            System.out.println("READY " + ((InetSocketAddress) s.localAddress()).getPort() + " pid=" + ProcessHandle.current().pid());
        } else {
            var loop = new io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup(threads);
            io.grpc.Server s = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forAddress(new InetSocketAddress(System.getProperty("bind", "127.0.0.1"), port))
                    .bossEventLoopGroup(loop).workerEventLoopGroup(loop)
                    .channelType(io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel.class)
                    .directExecutor()          // handlers on the event loop, like Hydrangea's: the configuration that favours grpc-java
                    .addService(io.grpc.ServerServiceDefinition.builder("hydrangea.test.Echo")
                            .addMethod(J_UNARY, io.grpc.stub.ServerCalls.<Msg, Msg>asyncUnaryCall((req, obs) -> {
                                obs.onNext(new Msg("echo:" + req.text(), req.n(), req.blob()));
                                obs.onCompleted();
                            }))
                            .addMethod(J_STREAM, io.grpc.stub.ServerCalls.<Msg, Msg>asyncServerStreamingCall((req, obs) -> {
                                for (int i = 0; i < req.n(); i++) obs.onNext(new Msg(req.text() + "-" + i, i, req.blob()));
                                obs.onCompleted();
                            }))
                            .build())
                    .build().start();
            System.out.println("READY " + s.getPort() + " pid=" + ProcessHandle.current().pid());
        }
        Thread.currentThread().join();
    }

    // ---- load ----
    static final AtomicLongArray HIST = new AtomicLongArray(2048);
    static final double LN = Math.log(1.01);
    static final AtomicLong OK = new AtomicLong(), ERR = new AtomicLong(), MSGS = new AtomicLong();
    static volatile boolean measuring;
    static volatile boolean stop;

    static void record(long nanos) {
        if (!measuring) return;
        long us = Math.max(1, nanos / 1000);
        HIST.incrementAndGet(Math.min(2047, (int) (Math.log(us) / LN)));
    }

    static long percentile(double p) {
        long total = 0;
        for (int i = 0; i < HIST.length(); i++) total += HIST.get(i);
        long want = (long) Math.ceil(total * p), seen = 0;
        for (int i = 0; i < HIST.length(); i++) {
            seen += HIST.get(i);
            if (seen >= want) return Math.round(Math.exp((i + 1) * LN));
        }
        return -1;
    }

    /** CPU time of another process via ps (ProcessHandle.info() is empty for other processes on macOS); ps resolves 10 ms. */
    static long cpuNanos(long pid) throws Exception {
        Process p = new ProcessBuilder("ps", "-o", "time=", "-p", Long.toString(pid)).start();
        String t = new String(p.getInputStream().readAllBytes()).trim();
        p.waitFor();
        double secs = 0;
        for (String part : t.split(":")) secs = secs * 60 + Double.parseDouble(part);
        return (long) (secs * 1e9);
    }

    interface Op { CompletableFuture<Void> run(); }

    static void load(String impl, int port, String scenario, int inflight, int channels, int seconds, long serverPid) throws Exception {
        boolean stream = scenario.startsWith("stream");
        int blob = switch (scenario) { case "unary-16" -> 0; case "unary-4k" -> 4096; case "unary-64k" -> 65536; default -> 1024; };
        Msg req = stream ? new Msg("s", 100, new byte[blob]) : new Msg("hello", 0, new byte[blob]);
        Op[] ops = new Op[channels];
        AutoCloseable[] closers = new AutoCloseable[channels];
        for (int c = 0; c < channels; c++) {
            if (impl.equals("hydrangea")) {
                GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port).maxConnections(1).build();
                closers[c] = ch;
                CallOptions opts = CallOptions.DEFAULT;
                ops[c] = stream ? () -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    ch.serverStreaming(GrpcEchoExample.SERVER_STREAM, req, opts, new StreamObserver<Msg>() {
                        public void onNext(Msg m) { if (measuring) MSGS.incrementAndGet(); }
                        public void onError(Throwable t) { f.completeExceptionally(t); }
                        public void onCompleted() { f.complete(null); }
                    });
                    return f;
                } : () -> ch.unary(GrpcEchoExample.UNARY, req, opts).thenApply(r -> null);
            } else {
                io.grpc.ManagedChannel ch = io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress("127.0.0.1", port)
                        .usePlaintext().directExecutor().build();
                closers[c] = () -> ch.shutdownNow();
                ops[c] = stream ? () -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    io.grpc.stub.ClientCalls.asyncServerStreamingCall(ch.newCall(J_STREAM, io.grpc.CallOptions.DEFAULT), req, new io.grpc.stub.StreamObserver<Msg>() {
                        public void onNext(Msg m) { if (measuring) MSGS.incrementAndGet(); }
                        public void onError(Throwable t) { f.completeExceptionally(t); }
                        public void onCompleted() { f.complete(null); }
                    });
                    return f;
                } : () -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    io.grpc.stub.ClientCalls.asyncUnaryCall(ch.newCall(J_UNARY, io.grpc.CallOptions.DEFAULT), req, new io.grpc.stub.StreamObserver<Msg>() {
                        public void onNext(Msg m) {}
                        public void onError(Throwable t) { f.completeExceptionally(t); }
                        public void onCompleted() { f.complete(null); }
                    });
                    return f;
                };
            }
        }
        int slotsPerChannel = Math.max(1, inflight / channels);
        CountDownLatch done = new CountDownLatch(slotsPerChannel * channels);
        // warm up for 8 s, then measure
        for (int c = 0; c < channels; c++) {
            Op op = ops[c];
            for (int s = 0; s < slotsPerChannel; s++) {
                new Object() {
                    void next() {
                        if (stop) { done.countDown(); return; }
                        long t0 = System.nanoTime();
                        CompletableFuture<Void> f;
                        try { f = op.run(); } catch (Throwable t) { ERR.incrementAndGet(); done.countDown(); return; }
                        f.whenComplete((r, e) -> {
                            if (e != null) { if (measuring) ERR.incrementAndGet(); }
                            else if (measuring) { OK.incrementAndGet(); record(System.nanoTime() - t0); }
                            next();
                        });
                    }
                }.next();
            }
        }
        Thread.sleep(8000);
        var osBean = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory.getOperatingSystemMXBean();
        long ccpu0 = osBean.getProcessCpuTime();
        long cpu0 = cpuNanos(serverPid);
        long t0 = System.nanoTime();
        measuring = true;
        Thread.sleep(seconds * 1000L);
        measuring = false;
        long elapsed = System.nanoTime() - t0;
        long cpu1 = cpuNanos(serverPid);
        long ccpu1 = osBean.getProcessCpuTime();
        long ok = OK.get(), err = ERR.get(), msgs = MSGS.get();
        stop = true;
        done.await(10, TimeUnit.SECONDS);
        double secs = elapsed / 1e9;
        double units = stream ? msgs : ok;
        System.out.printf("RESULT impl=%s scenario=%s inflight=%d channels=%d rps=%.0f %s p50=%d p99=%d p999=%d serverCpuUsPerOp=%.1f clientCpuUsPerOp=%.1f errors=%d%n",
                impl, scenario, inflight, channels, ok / secs, stream ? String.format("msgs/s=%.0f", msgs / secs) : "",
                percentile(0.5), percentile(0.99), percentile(0.999), (cpu1 - cpu0) / 1000.0 / Math.max(1, units), (ccpu1 - ccpu0) / 1000.0 / Math.max(1, units), err);
        for (AutoCloseable c : closers) try { c.close(); } catch (Exception ignored) {}
        System.exit(0);
    }
}
