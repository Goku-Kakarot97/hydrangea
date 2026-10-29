package io.hydrangea;

import io.hydrangea.Http2Test.Frame;
import io.hydrangea.Http2Test.Peer;
import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.RequestBody;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** The HTTP/2 client against a scripted server that speaks raw frames, to provoke what a real server rarely does. */
class Http2ClientProtocolTest {

    private static final int DATA = 0, HEADERS = 1, RST_STREAM = 3, SETTINGS = 4, PUSH_PROMISE = 5, PING = 6, GOAWAY = 7, WINDOW_UPDATE = 8;
    private static final int END_STREAM = 1, ACK = 1, END_HEADERS = 4;

    private ServerSocket listener;
    private EventLoopGroup group;
    private HttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        group = new EventLoopGroup(2);
        client = new HttpClient(group, HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)));
    }

    @AfterEach
    void tearDown() throws IOException {
        client.close();
        group.close();
        listener.close();
    }

    private String url(String path) {
        return "http://127.0.0.1:" + listener.getLocalPort() + path;
    }

    /** Accepts one connection, plays the server's half of the handshake with the given SETTINGS (id, value pairs). */
    private Peer accept(int... serverSettings) throws Exception {
        listener.setSoTimeout(10_000);
        Peer p = new Peer(listener.accept(), false, true);
        p.settings(serverSettings);
        for (;;) { // only the client's SETTINGS matter here; whatever else arrives (WINDOW_UPDATE, the first request) stays for the test
            Frame f = p.next();
            if (f.type() == SETTINGS && (f.flags() & ACK) == 0) {
                p.send(SETTINGS, ACK, 0, new byte[0]);
                break;
            }
        }
        return p;
    }

    /** The next HEADERS frame from the client, decoded as "name: value" lines. */
    private List<String> request(Peer p) throws Exception {
        Frame f = p.nextInteresting();
        assertEquals(HEADERS, f.type(), "expected a request");
        List<String> hs = new ArrayList<>();
        p.dec.decode(f.payload(), (n, v) -> hs.add(n + ": " + v));
        hs.add("#stream " + f.sid());
        hs.add("#end " + ((f.flags() & END_STREAM) != 0));
        return hs;
    }

    private static int streamOf(List<String> req) {
        return Integer.parseInt(req.stream().filter(h -> h.startsWith("#stream ")).findFirst().orElseThrow().substring(8));
    }

    private void reply(Peer p, int sid, int status, String body, String... extraHeaders) throws IOException {
        String[] hs = new String[4 + extraHeaders.length];
        hs[0] = ":status";
        hs[1] = Integer.toString(status);
        hs[2] = "content-length";
        hs[3] = Integer.toString(body.length());
        System.arraycopy(extraHeaders, 0, hs, 4, extraHeaders.length);
        p.headers(sid, body.isEmpty(), true, hs);
        if (!body.isEmpty()) {
            p.data(sid, true, body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private ClientResponse await(CompletableFuture<ClientResponse> f) throws Exception {
        return f.get(10, TimeUnit.SECONDS);
    }

    // ---- what the client says ----

    @Test
    void opensWithSettingsThatDisablePushAndAHeaderBlockInCanonicalForm() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/x/y?q=1"));
        Peer p = accept();
        List<String> req = request(p);
        assertTrue(req.contains(":method: GET"), req.toString());
        assertTrue(req.contains(":scheme: http"));
        assertTrue(req.contains(":path: /x/y?q=1"));
        assertTrue(req.stream().anyMatch(h -> h.startsWith(":authority: 127.0.0.1:")), req.toString());
        assertTrue(req.contains("#end true"), "a GET carries no body");
        assertTrue(req.stream().noneMatch(h -> h.startsWith("connection") || h.startsWith("host")), req.toString());
        reply(p, 1, 200, "ok");
        assertEquals("ok", await(f).bodyAsString());
    }

    // ---- GOAWAY, refused streams: retry only what was never processed ----

    @Test
    void goawayMakesTheUnprocessedRequestRetryOnAnotherConnection() throws Exception {
        CompletableFuture<ClientResponse> a = client.get(url("/a"));
        CompletableFuture<ClientResponse> b = client.get(url("/b"));
        Peer first = accept();
        List<String> r1 = request(first);
        List<String> r2 = request(first);
        assertEquals(1, streamOf(r1));
        assertEquals(3, streamOf(r2));
        first.send(GOAWAY, 0, 0, ByteBuffer.allocate(8).putInt(1).putInt(0).array());  // processed up to stream 1 only
        reply(first, 1, 200, "answer-1");
        assertEquals("answer-1", await(a).bodyAsString());
        Peer second = accept();                      // stream 3 comes again, on a new connection
        List<String> retry = request(second);
        assertTrue(retry.stream().anyMatch(h -> h.equals(":path: /b")), retry.toString());
        assertEquals(1, streamOf(retry));
        reply(second, 1, 200, "answer-2");
        assertEquals("answer-2", await(b).bodyAsString());
    }

    @Test
    void aRefusedStreamIsRetriedBecauseItWasNeverProcessed() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/r"));
        Peer first = accept();
        request(first);
        first.rst(1, 7);  // REFUSED_STREAM
        Peer second = accept();
        request(second);
        reply(second, 1, 200, "second time");
        assertEquals("second time", await(f).bodyAsString());
    }

    @Test
    void aPostBeyondTheGoawayMarkIsRetriedToo() throws Exception {
        // for HTTP/1.1 a POST is never repeated; here the server said it did not look at the request at all, which makes it safe
        CompletableFuture<ClientResponse> get = client.get(url("/warm"));
        Peer first = accept();
        request(first);
        reply(first, 1, 200, "warm");
        await(get);
        CompletableFuture<ClientResponse> post = client.send(ClientRequest.post(url("/p")).body("payload"));
        request(first);
        first.send(GOAWAY, 0, 0, ByteBuffer.allocate(8).putInt(1).putInt(0).array());   // stream 3 was not processed
        Peer second = accept();
        List<String> again = request(second);
        assertTrue(again.contains(":method: POST"), again.toString());
        Frame body = second.nextInteresting();
        assertEquals(DATA, body.type());
        assertEquals("payload", new String(body.payload()));
        reply(second, 1, 201, "created");
        assertEquals(201, await(post).status());
    }

    @Test
    void aRequestIsRetriedOnlyOnce() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/again"));
        Peer first = accept();
        request(first);
        first.rst(1, 7);
        Peer second = accept();
        request(second);
        second.rst(1, 7);
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IOException.class, e.getCause());
    }

    // ---- flow control ----

    @Test
    void doesNotSendMoreThanTheServersWindowAllows() throws Exception {
        CompletableFuture<ClientResponse> warm = client.get(url("/warm"));
        Peer p = accept(4, 1000);  // SETTINGS_INITIAL_WINDOW_SIZE = 1000: in force once the client has read it, so warm up first
        request(p);
        reply(p, 1, 200, "w");
        await(warm);
        CompletableFuture<ClientResponse> f = client.send(ClientRequest.post(url("/up")).body(new byte[50_000]));
        request(p);
        long total = 0;
        boolean end = false;
        while (!end) {
            long burst = 0;
            Frame fr;
            while ((fr = p.pollOrNull(300)) != null) {
                if (fr.type() == DATA) {
                    burst += fr.payload().length;
                    end = (fr.flags() & END_STREAM) != 0;
                }
            }
            assertTrue(burst <= 1000, "sent " + burst + " bytes into a 1000-byte window");
            total += burst;
            if (!end) {
                p.windowUpdate(3, 1000);
                p.windowUpdate(0, 1000);
            }
        }
        assertEquals(50_000, total);
        reply(p, 3, 200, "stored");
        assertEquals("stored", await(f).bodyAsString());
    }

    @Test
    void doesNotSendMoreThanTheConnectionWindowAllows() throws Exception {
        // the stream windows are huge, the connection window is the default 65535: only it limits the upload
        CompletableFuture<ClientResponse> warm = client.get(url("/warm"));
        Peer p = accept(4, 1 << 24);   // announced before the upload starts, so it is in force for it
        request(p);
        reply(p, 1, 200, "w");
        await(warm);
        CompletableFuture<ClientResponse> f = client.send(ClientRequest.post(url("/up")).body(new byte[300_000]));
        request(p);
        long total = 0;
        Frame fr;
        while ((fr = p.pollOrNull(500)) != null) {
            if (fr.type() == DATA) {
                total += fr.payload().length;
            }
        }
        assertEquals(65535, total, "exactly the connection window, then wait");
        p.windowUpdate(0, 400_000);
        boolean end = false;
        while (!end && (fr = p.pollOrNull(2000)) != null) {
            if (fr.type() == DATA) {
                total += fr.payload().length;
                end = (fr.flags() & END_STREAM) != 0;
            }
        }
        assertEquals(300_000, total);
        reply(p, 3, 200, "ok");
        assertEquals("ok", await(f).bodyAsString());
    }

    @Test
    void respectsTheServersLimitOnConcurrentStreams() throws Exception {
        CompletableFuture<ClientResponse> warm = client.get(url("/warm"));
        Peer p = accept(3, 1);   // SETTINGS_MAX_CONCURRENT_STREAMS = 1
        request(p);
        reply(p, 1, 200, "w");
        await(warm);
        List<CompletableFuture<ClientResponse>> burst = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            burst.add(client.get(url("/n" + i)));
        }
        List<String> r = request(p);
        assertEquals(3, streamOf(r));
        Frame extra = p.pollOrNull(400);
        assertTrue(extra == null || extra.type() != HEADERS, "a second stream was opened while the first was running");
        reply(p, 3, 200, "one");
        r = request(p);
        assertEquals(5, streamOf(r));
        assertNull(p.pollOrNull(300) == null ? null : (p.frames.isEmpty() ? null : p.frames.peek()));
        reply(p, 5, 200, "two");
        request(p);
        reply(p, 7, 200, "three");
        for (var f : burst) {
            assertEquals(200, await(f).status());
        }
    }

    @Test
    void resetsAStreamWhenTheServerSendsMoreThanTheStreamWindow() throws Exception {
        HttpClient tiny = new HttpClient(group, HttpClientConfig.defaults()
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true).withWindows(1000, 1 << 20)));  // 1000-byte stream windows
        try {
            CompletableFuture<ClientResponse> f = tiny.get(url("/dos"));
            Peer p = accept();
            request(p);
            p.headers(1, false, true, ":status", "200");
            p.data(1, false, new byte[5000]);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, e.getCause());
            Frame rst;
            do {
                rst = p.next();
            } while (rst.type() != RST_STREAM);
            assertEquals(3, rst.errorCode(), "FLOW_CONTROL_ERROR");
        } finally {
            tiny.close();
        }
    }

    // ---- response rules ----

    @Test
    void malformedResponseHeadersFailTheRequest() throws Exception {
        String[][] bad = {
                {":status", "200", "Upper-Case", "x"},                    // upper case field name
                {":status", "200", "connection", "close"},                // connection-specific
                {":status", "20", "x", "y"},                              // not a three digit status
                {"x", "y"},                                                // no :status
                {":status", "200", ":status", "200"},                     // duplicate pseudo-header
                {":status", "200", "content-length", "abc"},
        };
        for (String[] headers : bad) {
            CompletableFuture<ClientResponse> f = client.get(url("/bad"));
            Peer p = accept();
            request(p);
            p.headers(1, true, true, headers);
            ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS), String.join(" ", headers));
            assertInstanceOf(IOException.class, e.getCause());
            Frame rst;
            do {
                rst = p.next();
            } while (rst.type() != RST_STREAM && rst.type() != GOAWAY);
            assertEquals(1, rst.errorCode(), "PROTOCOL_ERROR for " + String.join(" ", headers));
            p.close();
        }
    }

    @Test
    void aBodyShorterThanContentLengthIsAnError() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/short"));
        Peer p = accept();
        request(p);
        p.headers(1, false, true, ":status", "200", "content-length", "10");
        p.data(1, true, new byte[5]);
        assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
    }

    @Test
    void trailersAreAcceptedAndHeadResponsesHaveNoBody() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/t"));
        Peer p = accept();
        request(p);
        p.headers(1, false, true, ":status", "200");
        p.data(1, false, "body".getBytes());
        p.headers(1, true, true, "x-checksum", "abc");     // trailers end the stream
        assertEquals("body", await(f).bodyAsString());

        CompletableFuture<ClientResponse> head = client.send(ClientRequest.head(url("/h")));
        request(p);
        p.headers(3, true, true, ":status", "200", "content-length", "12345");
        ClientResponse r = await(head);
        assertEquals(0, r.body().length);
        assertEquals("12345", r.header("content-length"));
    }

    @Test
    void interimResponsesAreSkipped() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/i"));
        Peer p = accept();
        request(p);
        p.headers(1, false, true, ":status", "103", "link", "</style.css>; rel=preload");
        reply(p, 1, 200, "final");
        assertEquals("final", await(f).bodyAsString());
    }

    @Test
    void expectContinueWaitsForTheGoAhead() throws Exception {
        RequestBody body = RequestBody.producer(5, false, sink -> {
            sink.write(ByteBuffer.wrap("hello".getBytes()));
            sink.finish();
        });
        CompletableFuture<ClientResponse> f = client.send(ClientRequest.post(url("/c")).header("Expect", "100-continue").body(body));
        Peer p = accept();
        request(p);
        Frame early = p.pollOrNull(300);
        assertTrue(early == null || early.type() != DATA, "the body must wait for 100 Continue");
        p.headers(1, false, true, ":status", "100");
        Frame data = p.nextInteresting();
        assertEquals(DATA, data.type());
        assertEquals("hello", new String(data.payload()));
        reply(p, 1, 200, "thanks");
        assertEquals("thanks", await(f).bodyAsString());
    }

    @Test
    void aFinalAnswerInsteadOfTheGoAheadMeansTheBodyIsNeverSent() throws Exception {
        AtomicBodyCounter counter = new AtomicBodyCounter();
        RequestBody body = RequestBody.producer(1_000_000, false, sink -> {
            counter.started = true;
            sink.finish();
        });
        CompletableFuture<ClientResponse> f = client.send(ClientRequest.post(url("/c")).header("Expect", "100-continue").body(body));
        Peer p = accept();
        request(p);
        reply(p, 1, 417, "no thanks");
        assertEquals(417, await(f).status());
        Thread.sleep(1300);   // past the client's own 1 s patience
        assertFalse(counter.started, "the producer must not run after the server refused");
    }

    static final class AtomicBodyCounter {
        volatile boolean started;
    }

    // ---- what the client must refuse ----

    @Test
    void serverPushIsAProtocolError() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/push"));
        Peer p = accept();
        request(p);
        p.send(PUSH_PROMISE, END_HEADERS, 1, new byte[]{0, 0, 0, 2, (byte) 0x82});
        assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        Frame g;
        do {
            g = p.next();
        } while (g.type() != GOAWAY);
        assertEquals(1, g.errorCode());
    }

    @Test
    void pingsAreAnswered() throws Exception {
        CompletableFuture<ClientResponse> f = client.get(url("/ping"));
        Peer p = accept();
        request(p);
        byte[] payload = {9, 8, 7, 6, 5, 4, 3, 2};
        p.send(PING, 0, 0, payload);
        Frame ack;
        do {
            ack = p.next();
        } while (ack.type() != PING);
        assertEquals(ACK, ack.flags());
        assertArrayEquals(payload, ack.payload());
        reply(p, 1, 200, "pong");
        assertEquals("pong", await(f).bodyAsString());
    }

    @Test
    void aPausedResponseHoldsBackTheServer() throws Exception {
        HttpClient small = new HttpClient(group, HttpClientConfig.defaults()
                .withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true).withWindows(65535, 1 << 20)));
        try {
            List<Integer> sizes = new ArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            BodyControl[] control = new BodyControl[1];
            small.stream(ClientRequest.get(url("/s")), new BodyListener() {
                @Override
                public void onData(ByteBuffer d, BodyControl c) {
                    sizes.add(d.remaining());
                    control[0] = c;
                    if (sizes.size() == 1) {
                        c.pause();
                    }
                }

                @Override
                public void onComplete() {
                    done.complete(null);
                }

                @Override
                public void onError(Throwable e) {
                    done.completeExceptionally(e);
                }
            });
            Peer p = accept();
            request(p);
            p.headers(1, false, true, ":status", "200");
            p.data(1, false, new byte[16384]);
            Thread.sleep(200);
            p.data(1, false, new byte[16384]);
            p.data(1, false, new byte[16384]);
            p.data(1, false, new byte[16383]);       // 65535 bytes in all: exactly the stream window
            Thread.sleep(300);
            assertEquals(1, sizes.size(), "nothing is delivered while paused");
            Frame f;
            while ((f = p.pollOrNull(200)) != null) {
                assertFalse(f.type() == WINDOW_UPDATE && f.sid() == 1, "no stream credit while paused");
            }
            control[0].resume();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (sizes.stream().mapToInt(Integer::intValue).sum() < 65535 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(65535, sizes.stream().mapToInt(Integer::intValue).sum());
            p.data(1, true, new byte[0]);
            done.get(10, TimeUnit.SECONDS);
        } finally {
            small.close();
        }
    }
}
