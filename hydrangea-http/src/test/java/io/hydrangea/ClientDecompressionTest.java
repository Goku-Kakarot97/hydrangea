package io.hydrangea;

import io.hydrangea.http.BodyControl;
import io.hydrangea.http.BodyListener;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.ClientResponse;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.Http2Config;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.*;

/** Transparent gzip/deflate decoding in the client, over HTTP/1.1 and HTTP/2. */
class ClientDecompressionTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Server server;
    private final byte[] text = sample(200_000);
    private final byte[] zeros = new byte[40 * 1024 * 1024];

    @BeforeEach
    void setUp() throws Exception {
        serverGroup = new EventLoopGroup(2);
        clientGroup = new EventLoopGroup(2);
        HttpRouter router = new HttpRouter()
                .get("/plain", r -> HttpResponse.status(200).header("Content-Type", "text/plain").body(text))
                .get("/gz", r -> HttpResponse.status(200).header("Content-Encoding", "gzip").body(gzip(text)))
                .get("/deflate", r -> HttpResponse.status(200).header("Content-Encoding", "deflate").body(zlib(text)))
                .get("/zeros", r -> HttpResponse.status(200).header("Content-Encoding", "gzip").body(gzip(zeros)))
                .get("/bad", r -> HttpResponse.status(200).header("Content-Encoding", "gzip").body("this is not gzip".getBytes()))
                .get("/truncated", r -> {
                    byte[] g = gzip(text);
                    return HttpResponse.status(200).header("Content-Encoding", "gzip").body(Arrays.copyOf(g, g.length - 30));
                })
                .get("/empty-gz", r -> HttpResponse.status(200).header("Content-Encoding", "gzip"))
                .get("/head-gz", r -> HttpResponse.status(200).header("Content-Encoding", "gzip").body(gzip(text)))
                .get("/ae", r -> HttpResponse.ok(String.valueOf(r.request().headers().get("accept-encoding"))));
        server = HttpServer.bind(serverGroup, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), null,
                new HttpConfig(16 * 1024, 1024 * 1024).withHttp2(Http2Config.defaults()), router);
    }

    @AfterEach
    void tearDown() {
        clientGroup.close();
        serverGroup.close();
    }

    private static byte[] sample(int n) {
        byte[] b = new byte[n];
        Random r = new Random(99);
        for (int i = 0; i < n; i++) {
            b[i] = (byte) (r.nextInt(8) == 0 ? r.nextInt(256) : 'a' + i % 7);
        }
        return b;
    }

    private static byte[] gzip(byte[] d) throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(o)) {
            g.write(d);
        }
        return o.toByteArray();
    }

    private static byte[] zlib(byte[] d) throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        try (DeflaterOutputStream g = new DeflaterOutputStream(o)) {
            g.write(d);
        }
        return o.toByteArray();
    }

    private String url(String path) {
        return "http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort() + path;
    }

    private HttpClient client(boolean http2, boolean decompress) {
        HttpClientConfig cfg = HttpClientConfig.defaults().withDecompression(decompress).withMaxResponseBytes(64 * 1024 * 1024);
        if (http2) {
            cfg = cfg.withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        }
        return new HttpClient(clientGroup, cfg);
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void decodesGzipAndDeflateAndHidesTheEncoding(boolean h2) throws Exception {
        try (HttpClient c = client(h2, true)) {
            for (String path : new String[]{"/gz", "/deflate", "/plain"}) {
                ClientResponse r = c.get(url(path)).get(10, TimeUnit.SECONDS);
                assertArrayEquals(text, r.body(), path);
            }
            ClientResponse gz = c.get(url("/gz")).get(10, TimeUnit.SECONDS);
            assertNull(gz.header("content-encoding"), "the body is no longer encoded");
            assertNull(gz.header("content-length"), "...so the wire length does not describe it");
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void asksForCompressionOnlyWhenEnabledAndOnlyDecodesWhatItAskedFor(boolean h2) throws Exception {
        try (HttpClient on = client(h2, true); HttpClient off = client(h2, false)) {
            assertEquals("gzip, deflate", on.get(url("/ae")).get(10, TimeUnit.SECONDS).bodyAsString());
            assertEquals("null", off.get(url("/ae")).get(10, TimeUnit.SECONDS).bodyAsString());
            // disabled: the bytes are delivered as they came, labelled
            ClientResponse raw = off.get(url("/gz")).get(10, TimeUnit.SECONDS);
            assertEquals("gzip", raw.header("content-encoding"));
            assertArrayEquals(gzip(text), raw.body());
            // enabled, but the caller chose its own Accept-Encoding: it asked for the encoding, so it gets it
            ClientResponse own = on.send(ClientRequest.get(url("/gz")).header("Accept-Encoding", "gzip")).get(10, TimeUnit.SECONDS);
            assertEquals("gzip", own.header("content-encoding"));
            assertArrayEquals(gzip(text), own.body());
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void aDecompressionBombIsStoppedAndTheClientKeepsWorking(boolean h2) throws Exception {
        HttpClientConfig cfg = HttpClientConfig.defaults().withDecompression(true).withMaxResponseBytes(1024 * 1024);
        if (h2) {
            cfg = cfg.withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true));
        }
        try (HttpClient c = new HttpClient(clientGroup, cfg)) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> c.get(url("/zeros")).get(20, TimeUnit.SECONDS));
            assertTrue(e.getCause().getMessage().contains("exceeds the limit"), e.getCause().toString());
            assertEquals(text.length, c.get(url("/plain")).get(10, TimeUnit.SECONDS).body().length, "a later request is fine");
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void corruptAndTruncatedBodiesFail(boolean h2) throws Exception {
        try (HttpClient c = client(h2, true)) {
            assertThrows(ExecutionException.class, () -> c.get(url("/bad")).get(10, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> c.get(url("/truncated")).get(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void emptyBodiesAndHeadRequestsAreNotTreatedAsCompressedData(boolean h2) throws Exception {
        try (HttpClient c = client(h2, true)) {
            assertEquals(0, c.get(url("/empty-gz")).get(10, TimeUnit.SECONDS).body().length);
            ClientResponse head = c.send(ClientRequest.head(url("/head-gz"))).get(10, TimeUnit.SECONDS);
            assertEquals(0, head.body().length);
            assertEquals("gzip", head.header("content-encoding"), "no body to decode: the headers stay as sent");
        }
    }

    @ParameterizedTest(name = "http2={0}")
    @ValueSource(booleans = {false, true})
    void aStreamedDecodedBodyHonoursPauseEvenWhenOneChunkExpandsToMany(boolean h2) throws Exception {
        try (HttpClient c = client(h2, true)) {
            AtomicLong received = new AtomicLong();
            AtomicLong atPause = new AtomicLong(-1);
            AtomicInteger calls = new AtomicInteger();
            AtomicReference<BodyControl> control = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> error = new AtomicReference<>();
            c.stream(ClientRequest.get(url("/zeros")), new BodyListener() {
                @Override
                public void onData(ByteBuffer d, BodyControl ctl) {
                    calls.incrementAndGet();
                    received.addAndGet(d.remaining());
                    control.set(ctl);
                    if (atPause.get() < 0) {
                        atPause.set(received.get());
                        ctl.pause();     // the rest of what this compressed chunk expands to must wait
                    }
                }

                @Override
                public void onComplete() {
                    done.countDown();
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                    done.countDown();
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (atPause.get() < 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            Thread.sleep(500);
            assertEquals(atPause.get(), received.get(), "nothing is delivered while paused");
            assertEquals(1, calls.get());
            assertEquals(1, done.getCount(), "and the stream does not end behind the listener's back");
            control.get().resume();
            assertTrue(done.await(30, TimeUnit.SECONDS), "completes after resume");
            assertNull(error.get());
            assertEquals(zeros.length, received.get());
        }
    }
}
