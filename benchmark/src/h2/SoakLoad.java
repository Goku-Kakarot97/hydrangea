package h2;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.ClientCall;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.grpc.Status;
import io.hydrangea.grpc.StatusException;
import io.hydrangea.grpc.StreamObserver;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mixed gRPC + HTTP/2 load for a soak test, with a verdict on the client side: every verified workload must finish with 0 errors, and the
 * outcome of every cancel / deadline / refusal must be one of the expected ones.
 * Usage: SoakLoad host port minutes
 * <p>
 * Workloads (each in its own threads, running until the time is up): unary echoes of 16 B / 4 KiB / 64 KiB, server streaming (100 x 1 KiB),
 * bidirectional ping-pong, client streaming, HTTP/2 requests through the HTTP client, channel churn (new channel per few calls), a cancel
 * storm (calls cancelled at random moments, deadlines that expire, requests refused before they start), and rude raw clients that send garbage,
 * truncate frames or vanish with a reset.
 */
public final class SoakLoad {

    static final class Counter {
        final String name;
        final AtomicLong ok = new AtomicLong(), errors = new AtomicLong();
        final Map<String, AtomicLong> detail = new ConcurrentHashMap<>();
        Counter(String name) { this.name = name; }
        void ok() { ok.incrementAndGet(); }
        void error(String what) {
            errors.incrementAndGet();
            detail.computeIfAbsent(what, k -> new AtomicLong()).incrementAndGet();
        }
        void note(String what) { detail.computeIfAbsent(what, k -> new AtomicLong()).incrementAndGet(); }
    }

    static final List<Counter> COUNTERS = new ArrayList<>();
    static volatile boolean stop;

    static Counter counter(String name) {
        Counter c = new Counter(name);
        COUNTERS.add(c);
        return c;
    }

    static byte[] blob(Random r, int n) {
        byte[] b = new byte[n];
        r.nextBytes(b);
        return b;
    }

    static String describe(Throwable t) {
        return describeRaw(t).replaceAll("\\d+", "N");
    }

    static String describeRaw(Throwable t) {
        Throwable c = t instanceof ExecutionException || t instanceof java.util.concurrent.CompletionException ? t.getCause() : t;
        return c instanceof StatusException se ? se.status().code().name() + (se.status().description() == null ? "" : " (" + se.status().description() + ")") : c.getClass().getSimpleName() + ": " + c.getMessage();
    }

    static Status.Code codeOf(Throwable t) {
        Throwable c = t instanceof ExecutionException || t instanceof java.util.concurrent.CompletionException ? t.getCause() : t;
        return c instanceof StatusException se ? se.status().code() : null;
    }

    static Thread thread(String name, Runnable body) {
        Thread t = new Thread(() -> {
            while (!stop) {
                try {
                    body.run();
                } catch (Throwable e) {
                    System.out.println("WORKER-CRASH " + name + " " + e);
                    try { Thread.sleep(100); } catch (InterruptedException ignored) { }
                }
            }
        }, name);
        t.start();
        return t;
    }

    public static void main(String[] a) throws Exception {
        String host = a[0];
        int port = Integer.parseInt(a[1]);
        long endAt = System.nanoTime() + Long.parseLong(a[2]) * 60_000_000_000L;
        String target = "http://" + host + ":" + port;
        EventLoopGroup group = new EventLoopGroup(2);
        GrpcChannel main = GrpcChannel.forTarget(target).eventLoopGroup(group).maxConnections(2).build();
        HttpClient http = new HttpClient(group, HttpClientConfig.defaults().withMaxConnectionsPerHost(2)
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)));
        List<Thread> threads = new ArrayList<>();

        Counter unary = counter("grpc unary echo 16 B / 4 KiB / 64 KiB (8 loops)");
        for (int i = 0; i < 8; i++) {
            Random r = new Random(i);
            threads.add(thread("unary-" + i, () -> {
                int size = new int[]{0, 4096, 65536}[r.nextInt(3)];
                byte[] b = blob(r, size);
                String text = "u" + r.nextInt();
                try {
                    Msg reply = main.unary(GrpcEchoExample.UNARY, new Msg(text, 7, b), CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(20))).get(30, TimeUnit.SECONDS);
                    if (reply.text().equals("echo:" + text) && reply.n() == 7 && Arrays.equals(reply.blob(), b)) unary.ok();
                    else unary.error("wrong reply");
                } catch (Exception e) {
                    unary.error(describe(e));
                }
            }));
        }

        Counter stream = counter("grpc server stream 100 x 1 KiB (4 loops)");
        for (int i = 0; i < 4; i++) {
            Random r = new Random(100 + i);
            threads.add(thread("stream-" + i, () -> {
                byte[] b = blob(r, 1024);
                List<Msg> got = new ArrayList<>();
                CompletableFuture<Void> done = new CompletableFuture<>();
                main.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("s", 100, b), CallOptions.DEFAULT, new StreamObserver<>() {
                    public void onNext(Msg m) { got.add(m); }
                    public void onError(Throwable t) { done.completeExceptionally(t); }
                    public void onCompleted() { done.complete(null); }
                });
                try {
                    done.get(30, TimeUnit.SECONDS);
                    boolean good = got.size() == 100;
                    for (int k = 0; good && k < got.size(); k++) good = got.get(k).n() == k && Arrays.equals(got.get(k).blob(), b);
                    if (good) stream.ok(); else stream.error("wrong stream");
                } catch (Exception e) {
                    stream.error(describe(e));
                }
            }));
        }

        Counter bidi = counter("grpc bidi ping-pong 100 messages (4 loops)");
        for (int i = 0; i < 4; i++) {
            Random r = new Random(200 + i);
            threads.add(thread("bidi-" + i, () -> {
                int[] next = {0};
                CompletableFuture<Void> done = new CompletableFuture<>();
                ClientCall<Msg, Msg>[] call = new ClientCall[1];
                call[0] = main.bidiStreaming(GrpcEchoExample.BIDI, CallOptions.DEFAULT, new StreamObserver<Msg>() {
                    public void onNext(Msg m) {
                        if (!m.text().equals("echo:p" + next[0])) { done.completeExceptionally(new IllegalStateException("out of order: " + m.text())); return; }
                        next[0]++;
                        if (next[0] < 100) call[0].send(new Msg("p" + next[0], next[0], blob(r, 200)));
                        else call[0].halfClose();
                    }
                    public void onError(Throwable t) { done.completeExceptionally(t); }
                    public void onCompleted() { done.complete(null); }
                });
                call[0].send(new Msg("p0", 0, blob(r, 200)));
                try {
                    done.get(30, TimeUnit.SECONDS);
                    if (next[0] == 100) bidi.ok(); else bidi.error("short: " + next[0]);
                } catch (Exception e) {
                    bidi.error(describe(e));
                    call[0].cancel("soak: giving up");
                }
            }));
        }

        Counter clientStream = counter("grpc client stream 50 messages (2 loops)");
        for (int i = 0; i < 2; i++) {
            threads.add(thread("cstream-" + i, () -> {
                CompletableFuture<Msg> reply = new CompletableFuture<>();
                ClientCall<Msg, Msg> call = main.clientStreaming(GrpcEchoExample.CLIENT_STREAM, CallOptions.DEFAULT, new StreamObserver<>() {
                    public void onNext(Msg m) { reply.complete(m); }
                    public void onError(Throwable t) { reply.completeExceptionally(t); }
                    public void onCompleted() { }
                });
                for (int k = 0; k < 50; k++) call.send(new Msg("c" + k));
                call.halfClose();
                try {
                    Msg m = reply.get(30, TimeUnit.SECONDS);
                    if (m.n() == 50) clientStream.ok(); else clientStream.error("count " + m.n());
                } catch (Exception e) {
                    clientStream.error(describe(e));
                }
            }));
        }

        Counter h2 = counter("http/2 GET /plain, POST /echo 1-100 KiB, GET /download (6 loops)");
        for (int i = 0; i < 6; i++) {
            Random r = new Random(300 + i);
            threads.add(thread("h2-" + i, () -> {
                try {
                    int kind = r.nextInt(3);
                    ClientResponse resp;
                    if (kind == 0) {
                        resp = http.send(ClientRequest.get(target + "/plain")).get(30, TimeUnit.SECONDS);
                        if (resp.status() == 200 && resp.bodyAsString().equals("Hello, World!")) h2.ok(); else h2.error("plain " + resp.status());
                    } else if (kind == 1) {
                        byte[] body = blob(r, 1024 + r.nextInt(100 * 1024));
                        resp = http.send(ClientRequest.post(target + "/echo").body(body)).get(30, TimeUnit.SECONDS);
                        if (resp.status() == 200 && Arrays.equals(resp.body(), body)) h2.ok(); else h2.error("echo " + resp.status());
                    } else {
                        int n = 1 + r.nextInt(1_000_000);
                        resp = http.send(ClientRequest.get(target + "/download/" + n)).get(30, TimeUnit.SECONDS);
                        if (resp.status() == 200 && resp.body().length == n) h2.ok(); else h2.error("download " + resp.status());
                    }
                } catch (Exception e) {
                    h2.error(describe(e));
                }
            }));
        }

        Counter churn = counter("channel churn: new channel, 5 calls, close (2 loops)");
        for (int i = 0; i < 2; i++) {
            threads.add(thread("churn-" + i, () -> {
                try (GrpcChannel ch = GrpcChannel.forTarget(target).eventLoopGroup(group).build()) {
                    for (int k = 0; k < 5; k++) {
                        Msg m = ch.unary(GrpcEchoExample.UNARY, new Msg("churn" + k), CallOptions.DEFAULT).get(20, TimeUnit.SECONDS);
                        if (!m.text().equals("echo:churn" + k)) { churn.error("wrong reply"); return; }
                    }
                    churn.ok();
                } catch (Exception e) {
                    churn.error(describe(e));
                }
            }));
        }

        // outcomes that are fine here: the call ended the way we made it end. Anything else is an error.
        Counter cancels = counter("cancel storm: cancel at a random moment, expiring deadlines, refused requests (8 loops)");
        for (int i = 0; i < 8; i++) {
            Random r = new Random(400 + i);
            threads.add(thread("cancel-" + i, () -> {
                int kind = r.nextInt(5);
                try {
                    switch (kind) {
                        case 0 -> {   // cancel a slow unary
                            CompletableFuture<Msg> f = main.unary(GrpcEchoExample.UNARY, new Msg("SLOW:3000"), CallOptions.DEFAULT);
                            Thread.sleep(r.nextInt(15));
                            f.cancel(true);
                            cancels.note("unary cancelled");
                        }
                        case 1 -> {   // cancel an endless server stream
                            CompletableFuture<Void> closed = new CompletableFuture<>();
                            ClientCall<Msg, Msg> call = main.serverStreaming(GrpcEchoExample.SERVER_STREAM, new Msg("ENDLESS"), CallOptions.DEFAULT, new StreamObserver<>() {
                                public void onNext(Msg m) { }
                                public void onError(Throwable t) { closed.complete(null); }
                                public void onCompleted() { closed.complete(null); }
                            });
                            Thread.sleep(r.nextInt(60));
                            call.cancel("soak");
                            closed.get(10, TimeUnit.SECONDS);
                            cancels.note("stream cancelled");
                        }
                        case 2 -> {   // a deadline that expires
                            try {
                                main.unary(GrpcEchoExample.UNARY, new Msg("SLOW:500"), CallOptions.DEFAULT.withDeadline(Duration.ofMillis(1 + r.nextInt(8)))).get(10, TimeUnit.SECONDS);
                                cancels.error("deadline did not expire");
                            } catch (ExecutionException e) {
                                if (codeOf(e) == Status.Code.DEADLINE_EXCEEDED) cancels.note("deadline exceeded");
                                else cancels.error("deadline case: " + describe(e));
                            }
                        }
                        case 3 -> {   // refused by the client before it starts, then a normal call that must work
                            try {
                                main.unary(GrpcEchoExample.UNARY, new Msg("x", 0, new byte[500]), CallOptions.DEFAULT.withMaxRequestBytes(100)).get(10, TimeUnit.SECONDS);
                                cancels.error("oversize request was sent");
                            } catch (ExecutionException e) {
                                if (codeOf(e) != Status.Code.RESOURCE_EXHAUSTED) cancels.error("refusal case: " + describe(e));
                            }
                            Msg m = main.unary(GrpcEchoExample.UNARY, new Msg("after-refusal"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
                            if (m.text().equals("echo:after-refusal")) cancels.note("refused, then ok"); else cancels.error("wrong reply after refusal");
                        }
                        default -> {   // an HTTP/2 request cancelled at a random moment
                            CompletableFuture<ClientResponse> f = http.send(ClientRequest.get(target + "/download/3000000"));
                            Thread.sleep(r.nextInt(10));
                            f.cancel(true);
                            cancels.note("http cancelled");
                        }
                    }
                    cancels.ok();
                } catch (Exception e) {
                    cancels.error(describe(e));
                }
            }));
        }

        Counter rude = counter("rude raw clients: garbage, truncated frames, resets (2 loops)");
        byte[] preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        for (int i = 0; i < 2; i++) {
            Random r = new Random(500 + i);
            threads.add(thread("rude-" + i, () -> {
                try (Socket s = new Socket()) {
                    s.connect(new InetSocketAddress(host, port), 5000);
                    s.setSoTimeout(2000);
                    OutputStream out = s.getOutputStream();
                    switch (r.nextInt(5)) {
                        case 0 -> { }   // connect and leave
                        case 1 -> { out.write(preface); out.write(new byte[]{0, 0, 0, 4, 0, 0, 0, 0, 0}); }   // empty SETTINGS, then gone
                        case 2 -> { out.write(preface); out.write(blob(r, 1 + r.nextInt(2000))); }   // garbage
                        case 3 -> { out.write(preface); out.write(new byte[]{0, 0, 40, 1, 4, 0, 0, 0, 1, (byte) 0x82}); }   // a HEADERS frame cut short
                        default -> { s.setSoLinger(true, 0); out.write(preface); out.write(new byte[]{0, 0, 0, 4, 0, 0, 0, 0, 0}); }   // RST on close
                    }
                    out.flush();
                    try { Thread.sleep(r.nextInt(30)); } catch (InterruptedException ignored) { }
                    rude.ok();
                } catch (Exception e) {
                    rude.note("rude client io: " + e.getClass().getSimpleName());
                }
                try { Thread.sleep(20); } catch (InterruptedException ignored) { }
            }));
        }

        long started = System.nanoTime();
        long nextReport = started + 60_000_000_000L;
        while (System.nanoTime() < endAt) {
            Thread.sleep(1000);
            if (System.nanoTime() >= nextReport) {
                nextReport += 60_000_000_000L;
                StringBuilder sb = new StringBuilder("PROGRESS t=" + (System.nanoTime() - started) / 1_000_000_000L);
                for (Counter c : COUNTERS) sb.append(' ').append(c.name.split(" ")[0]).append(c.name.split(" ")[1]).append('=').append(c.ok.get()).append('/').append(c.errors.get());
                System.out.println(sb);
                System.out.flush();
            }
        }
        stop = true;
        for (Thread t : threads) t.join(40_000);
        long totalErrors = 0;
        for (Counter c : COUNTERS) {
            System.out.println("SUMMARY ok=" + c.ok.get() + " errors=" + c.errors.get() + " | " + c.name + (c.detail.isEmpty() ? "" : " | " + c.detail));
            totalErrors += c.errors.get();
        }
        System.out.println("LOAD-DONE errors=" + totalErrors);
        main.close();
        http.close();
        group.close();
        System.exit(totalErrors == 0 ? 0 : 1);
    }
}
