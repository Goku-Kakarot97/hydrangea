import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import io.grpc.*;
import io.grpc.stub.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** The echo.proto service on grpc-java (an independent implementation), for Hydrangea's client to talk to. Usage: GrpcJavaServer port */
public class GrpcJavaServer {
    record Msg(String text, int n, byte[] blob) { Msg(String t) { this(t, 0, new byte[0]); } Msg(String t, int n) { this(t, n, new byte[0]); } }
    static final MethodDescriptor.Marshaller<Msg> M = new MethodDescriptor.Marshaller<>() {
        public InputStream stream(Msg m) {
            try { ByteArrayOutputStream o = new ByteArrayOutputStream(); CodedOutputStream c = CodedOutputStream.newInstance(o);
                if (!m.text().isEmpty()) c.writeString(1, m.text()); if (m.n() != 0) c.writeInt32(2, m.n()); if (m.blob().length > 0) c.writeByteArray(3, m.blob());
                c.flush(); return new ByteArrayInputStream(o.toByteArray()); } catch (IOException e) { throw new RuntimeException(e); }
        }
        public Msg parse(InputStream in) {
            try { CodedInputStream c = CodedInputStream.newInstance(in); String t = ""; int n = 0; byte[] b = new byte[0];
                for (int tag; (tag = c.readTag()) != 0; ) { switch (tag >>> 3) { case 1 -> t = c.readString(); case 2 -> n = c.readInt32(); case 3 -> b = c.readByteArray(); default -> c.skipField(tag); } }
                return new Msg(t, n, b); } catch (IOException e) { throw new RuntimeException(e); }
        }
    };
    static MethodDescriptor<Msg, Msg> md(String name, MethodDescriptor.MethodType t) {
        return MethodDescriptor.<Msg, Msg>newBuilder().setType(t).setFullMethodName("hydrangea.test.Echo/" + name).setRequestMarshaller(M).setResponseMarshaller(M).build();
    }
    static final AtomicInteger cancelled = new AtomicInteger();
    static final Metadata.Key<String> X_TEST = Metadata.Key.of("x-test", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> X_TRAILER = Metadata.Key.of("x-trailer", Metadata.ASCII_STRING_MARSHALLER);
    static final ScheduledExecutorService timer = Executors.newScheduledThreadPool(2, r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });

    public static void main(String[] a) throws Exception {
        ServerServiceDefinition svc = ServerServiceDefinition.builder("hydrangea.test.Echo")
            .addMethod(md("Unary", MethodDescriptor.MethodType.UNARY), ServerCalls.<Msg, Msg>asyncUnaryCall((req, obs) -> {
                String t = req.text();
                ServerCallStreamObserver<Msg> so = (ServerCallStreamObserver<Msg>) obs;
                if (t.startsWith("FAIL:")) { String[] p = t.split(":", 3); obs.onError(Status.fromCodeValue(Integer.parseInt(p[1])).withDescription(p.length > 2 ? p[2] : "").asRuntimeException()); return; }
                if (t.equals("THROW")) throw new IllegalStateException("secret internal detail");
                if (t.equals("CANCELLED?")) { obs.onNext(new Msg(Integer.toString(cancelled.get()))); obs.onCompleted(); return; }
                if (t.startsWith("SLOW:")) { so.setOnCancelHandler(cancelled::incrementAndGet); timer.schedule(() -> { if (!so.isCancelled()) { obs.onNext(new Msg("slow done")); obs.onCompleted(); } }, Long.parseLong(t.substring(5)), TimeUnit.MILLISECONDS); return; }
                if (t.equals("META")) { obs.onNext(new Msg(String.valueOf(CURRENT_TEST.get()))); obs.onCompleted(); return; }
                if (t.startsWith("BIG:")) { obs.onNext(new Msg("big", 0, new byte[Integer.parseInt(t.substring(4))])); obs.onCompleted(); return; }
                obs.onNext(new Msg("echo:" + t, req.n(), req.blob())); obs.onCompleted();
            }))
            .addMethod(md("ServerStream", MethodDescriptor.MethodType.SERVER_STREAMING), ServerCalls.<Msg, Msg>asyncServerStreamingCall((req, obs) -> {
                ServerCallStreamObserver<Msg> so = (ServerCallStreamObserver<Msg>) obs; String t = req.text();
                if (t.equals("ENDLESS")) { int[] i = {0}; var f = timer.scheduleAtFixedRate(() -> { if (!so.isCancelled() && so.isReady()) obs.onNext(new Msg("tick", i[0]++)); }, 10, 10, TimeUnit.MILLISECONDS); so.setOnCancelHandler(() -> { cancelled.incrementAndGet(); f.cancel(false); }); return; }
                if (t.startsWith("FAILAFTER:")) { int k = Integer.parseInt(t.substring(10)); for (int i = 0; i < k; i++) obs.onNext(new Msg("m", i)); obs.onError(Status.INTERNAL.withDescription("failed after " + k).asRuntimeException()); return; }
                for (int i = 0; i < Math.max(0, req.n()); i++) obs.onNext(new Msg(t + "-" + i, i, req.blob()));
                obs.onCompleted();
            }))
            .addMethod(md("ClientStream", MethodDescriptor.MethodType.CLIENT_STREAMING), ServerCalls.<Msg, Msg>asyncClientStreamingCall(obs -> new StreamObserver<Msg>() {
                final List<String> texts = new ArrayList<>();
                public void onNext(Msg m) { texts.add(m.text()); }
                public void onError(Throwable t) { cancelled.incrementAndGet(); }
                public void onCompleted() { obs.onNext(new Msg(String.join(",", texts), texts.size())); obs.onCompleted(); }
            }))
            .addMethod(md("Bidi", MethodDescriptor.MethodType.BIDI_STREAMING), ServerCalls.<Msg, Msg>asyncBidiStreamingCall(obs -> new StreamObserver<Msg>() {
                public void onNext(Msg m) { obs.onNext(new Msg("echo:" + m.text(), m.n(), m.blob())); }
                public void onError(Throwable t) { cancelled.incrementAndGet(); }
                public void onCompleted() { obs.onCompleted(); }
            }))
            .build();
        ServerInterceptor meta = new ServerInterceptor() {
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String v = headers.get(X_TEST);
                Context ctx = Context.current().withValue(CURRENT_TEST, v);
                ServerCall<ReqT, RespT> wrapped = new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    public void close(Status s, Metadata trailers) { if (v != null) trailers.put(X_TRAILER, "yes"); super.close(s, trailers); }
                };
                return Contexts.interceptCall(ctx, wrapped, headers, next);
            }
        };
        Server server = io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder.forPort(Integer.parseInt(a[0])).maxInboundMessageSize(16 << 20).addService(ServerInterceptors.intercept(svc, meta)).build().start();
        System.out.println("READY " + server.getPort());
        server.awaitTermination();
    }
    static final Context.Key<String> CURRENT_TEST = Context.key("x-test");
}
