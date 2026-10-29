import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static io.hydrangea.examples.GrpcEchoExample.*;

/** Hydrangea's gRPC client against any server that implements echo.proto (Hydrangea, grpc-java, grpc-js). Usage: GrpcInteropClient port [hasHealth=true] */
public class GrpcInteropClient {
    static int failures = 0;

    static void check(String what, boolean ok, Object detail) {
        System.out.println((ok ? "  ok   " : "  FAIL ") + what + (ok ? "" : "  -> " + detail));
        if (!ok) failures++;
    }

    static StatusException failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(15, TimeUnit.SECONDS);
            return null;
        } catch (ExecutionException e) {
            return (StatusException) e.getCause();
        }
    }

    public static void main(String[] a) throws Exception {
        int port = Integer.parseInt(a[0]);
        boolean cancelStats = a.length < 2 || Boolean.parseBoolean(a[1]);
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + port).maxInboundMessageBytes(16 << 20).build()) {
            System.out.println("Hydrangea client -> 127.0.0.1:" + port);
            Msg r = ch.unary(UNARY, new Msg("hello", 7, new byte[]{1, 2, 3}), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
            check("unary echo", r.text().equals("echo:hello") && r.n() == 7 && Arrays.equals(r.blob(), new byte[]{1, 2, 3}), r);

            StatusException e = failure(ch.unary(UNARY, new Msg("FAIL:5:no such thing, 100% sure é"), CallOptions.DEFAULT));
            check("error status carries code and message", e != null && e.status().code() == Status.Code.NOT_FOUND && "no such thing, 100% sure é".equals(e.status().description()), e == null ? null : e.status());
            e = failure(ch.unary(UNARY, new Msg("THROW"), CallOptions.DEFAULT));
            check("handler exception is UNKNOWN and its message does not leak", e != null && e.status().code() == Status.Code.UNKNOWN && !String.valueOf(e.status().description()).contains("secret"), e == null ? null : e.status());

            Metadata md = new Metadata().put("x-test", "from-hydrangea");
            CompletableFuture<Msg> meta = ch.unary(UNARY, new Msg("META"), CallOptions.DEFAULT.withMetadata(md));
            check("request metadata reaches the handler", meta.get(10, TimeUnit.SECONDS).text().equals("from-hydrangea"), meta);
            Metadata[] seenTrailers = new Metadata[1];
            CountDownLatch closed = new CountDownLatch(1);
            ClientCall<Msg, Msg> tc = ch.newCall(UNARY, CallOptions.DEFAULT.withMetadata(md), new ClientCall.Listener<>() {
                public void onClose(Status s, Metadata t) { seenTrailers[0] = t; closed.countDown(); }
            });
            tc.send(new Msg("META")); tc.halfClose(); closed.await(10, TimeUnit.SECONDS);
            check("trailing metadata reaches the client", seenTrailers[0] != null && "yes".equals(seenTrailers[0].get("x-trailer")), seenTrailers[0]);

            long t0 = System.nanoTime();
            e = failure(ch.unary(UNARY, new Msg("SLOW:3000"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(200))));
            long took = (System.nanoTime() - t0) / 1_000_000;
            check("deadline -> DEADLINE_EXCEEDED, and promptly", e != null && e.status().code() == Status.Code.DEADLINE_EXCEEDED && took < 1500, e == null ? null : e.status() + " after " + took + " ms");
            if (cancelStats) {
                Thread.sleep(300);
                check("the server saw that call cancelled", Integer.parseInt(ch.unary(UNARY, new Msg("CANCELLED?"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text()) >= 1, "");
            }

            List<String> rows = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> ss = new CompletableFuture<>();
            ch.serverStreaming(SERVER_STREAM, new Msg("row", 5), CallOptions.DEFAULT, observer(m -> rows.add(m.text()), ss));
            ss.get(10, TimeUnit.SECONDS);
            check("server streaming", rows.equals(List.of("row-0", "row-1", "row-2", "row-3", "row-4")), rows);
            List<Integer> got = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> failing = new CompletableFuture<>();
            ch.serverStreaming(SERVER_STREAM, new Msg("FAILAFTER:2"), CallOptions.DEFAULT, observer(m -> got.add(m.n()), failing));
            e = failure(failing);
            check("a stream that fails after two messages", got.size() == 2 && e != null && e.status().code() == Status.Code.INTERNAL && "failed after 2".equals(e.status().description()), got + " " + (e == null ? null : e.status()));

            CompletableFuture<Msg> csResult = new CompletableFuture<>();
            ClientCall<Msg, Msg> cs = ch.clientStreaming(CLIENT_STREAM, CallOptions.DEFAULT, single(csResult));
            for (String s : new String[]{"a", "b", "c"}) cs.send(new Msg(s));
            cs.halfClose();
            Msg csr = csResult.get(10, TimeUnit.SECONDS);
            check("client streaming", csr.text().equals("a,b,c") && csr.n() == 3, csr);

            List<String> echoes = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> bidiDone = new CompletableFuture<>();
            ClientCall<Msg, Msg> bidi = ch.bidiStreaming(BIDI, CallOptions.DEFAULT, observer(m -> echoes.add(m.text()), bidiDone));
            for (int i = 0; i < 100; i++) { bidi.send(new Msg("m" + i)); }
            bidi.halfClose();
            bidiDone.get(10, TimeUnit.SECONDS);
            check("bidirectional streaming (100 round trips)", echoes.size() == 100 && echoes.get(99).equals("echo:m99"), echoes.size());

            check("a 3 MB response", ch.unary(UNARY, new Msg("BIG:3000000"), CallOptions.DEFAULT).get(20, TimeUnit.SECONDS).blob().length == 3_000_000, "");
            check("a 3 MB request", ch.unary(UNARY, new Msg("echo", 0, new byte[3_000_000]), CallOptions.DEFAULT).get(20, TimeUnit.SECONDS).blob().length == 3_000_000, "");
            Msg gz = ch.unary(UNARY, new Msg("compressed", 0, new byte[200_000]), CallOptions.DEFAULT.withCompression("gzip")).get(20, TimeUnit.SECONDS);
            check("gzip-compressed request", gz.text().equals("echo:compressed") && gz.blob().length == 200_000, gz.text());

            e = failure(ch.unary(MethodDescriptor.of("hydrangea.test.Echo", "NoSuchMethod", MethodDescriptor.MethodType.UNARY, MSG, MSG), new Msg("x"), CallOptions.DEFAULT));
            check("unknown method is UNIMPLEMENTED", e != null && e.status().code() == Status.Code.UNIMPLEMENTED, e == null ? null : e.status());

            CountDownLatch three = new CountDownLatch(3);
            CompletableFuture<Status> endless = new CompletableFuture<>();
            ClientCall<Msg, Msg> ec = ch.newCall(SERVER_STREAM, CallOptions.DEFAULT, new ClientCall.Listener<>() {
                public void onMessage(Msg m) { three.countDown(); }
                public void onClose(Status s, Metadata t) { endless.complete(s); }
            });
            ec.send(new Msg("ENDLESS")); ec.halfClose();
            check("an endless stream delivers messages", three.await(5, TimeUnit.SECONDS), three.getCount());
            int before = cancelStats ? Integer.parseInt(ch.unary(UNARY, new Msg("CANCELLED?"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text()) : 0;
            ec.cancel("enough");
            check("cancel -> the listener gets CANCELLED", endless.get(5, TimeUnit.SECONDS).code() == Status.Code.CANCELLED, "");
            if (cancelStats) {
                Thread.sleep(300);
                int after = Integer.parseInt(ch.unary(UNARY, new Msg("CANCELLED?"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS).text());
                check("...and the server's handler was told", after == before + 1, before + " -> " + after);
            }
        }
        System.out.println(failures == 0 ? "ALL OK" : failures + " FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }

    static StreamObserver<Msg> observer(java.util.function.Consumer<Msg> onNext, CompletableFuture<Void> done) {
        return new StreamObserver<>() {
            public void onNext(Msg m) { onNext.accept(m); }
            public void onError(Throwable t) { done.completeExceptionally(t); }
            public void onCompleted() { done.complete(null); }
        };
    }

    static StreamObserver<Msg> single(CompletableFuture<Msg> out) {
        return new StreamObserver<>() {
            public void onNext(Msg m) { out.complete(m); }
            public void onError(Throwable t) { out.completeExceptionally(t); }
            public void onCompleted() {}
        };
    }
}
