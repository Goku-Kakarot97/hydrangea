package io.hydrangea.fuzz;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.CallOptions;
import io.hydrangea.grpc.GrpcChannel;
import io.hydrangea.http.ClientRequest;
import io.hydrangea.http.Http2ClientConfig;
import io.hydrangea.http.HttpClient;
import io.hydrangea.http.HttpClientConfig;
import io.hydrangea.sse.SseClient;
import io.hydrangea.sse.SseClientListener;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static io.hydrangea.fuzz.ProtocolFuzzTest.cat;
import static io.hydrangea.fuzz.ProtocolFuzzTest.frame;
import static io.hydrangea.fuzz.ProtocolFuzzTest.grpcMessage;
import static io.hydrangea.fuzz.ProtocolFuzzTest.hpack;
import static io.hydrangea.fuzz.ProtocolFuzzTest.text;
import static org.junit.jupiter.api.Assertions.*;

/** A hostile server answering our clients: they must fail the call cleanly (never hang, never log an internal error, never leak the connection). */
class ClientFuzzTest {

    static EventLoopGroup group;
    static ServerSocket listener;
    static volatile byte[] payload = new byte[0];
    static final List<LogRecord> LOGGED = new CopyOnWriteArrayList<>();
    static Handler logHandler;
    static Thread acceptor;

    @BeforeAll
    static void start() throws Exception {
        logHandler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue() && r.getLoggerName() != null && r.getLoggerName().startsWith("io.hydrangea")) {
                    LOGGED.add(r);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        java.util.logging.Logger.getLogger("").addHandler(logHandler);
        group = new EventLoopGroup(2);
        listener = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        acceptor = new Thread(() -> {
            while (!listener.isClosed()) {
                try (Socket s = listener.accept()) {
                    s.setSoTimeout(30);
                    s.getOutputStream().write(payload);
                    s.getOutputStream().flush();
                    try {
                        s.getInputStream().read(new byte[4096]);   // let the client's request arrive before the close, which would reset it
                    } catch (IOException ignored) {
                    }
                    Thread.sleep(15);
                } catch (Exception e) {
                    // the client went away
                }
            }
        }, "hostile-server");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    @AfterAll
    static void stop() throws Exception {
        java.util.logging.Logger.getLogger("").removeHandler(logHandler);
        listener.close();
        group.close();
    }

    static String url(String path) {
        return "http://127.0.0.1:" + listener.getLocalPort() + path;
    }

    private static int iterations() {
        return Math.min(Fuzz.ITERATIONS, Integer.getInteger("fuzz.protocol.iterations", 150));
    }

    private interface Call {
        void run() throws Throwable;
    }

    // The longest legitimate wait is the 5 s connect timeout: a redirect to a numeric host like 127999999999999999999999 is read as an IPv4 address by the
    // JDK (79.255.255.255), which nobody answers.
    /** Serves the input to one client call; a failed call is fine, a hung one or a logged internal error is not. */
    private static void against(byte[] hostile, Call call) throws Throwable {
        int logged = LOGGED.size();
        payload = hostile;
        try {
            call.run();
        } catch (TimeoutException hang) {
            throw new AssertionError("the call never finished");
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.CancellationException failedCleanly) {
            // an error reported to the caller is the right outcome
        }
        if (LOGGED.size() > logged) {
            LogRecord r = LOGGED.get(logged);
            throw new AssertionError("the client logged: " + r.getMessage() + " " + r.getThrown() + (r.getThrown() != null && r.getThrown().getStackTrace().length > 0
                    ? " at " + r.getThrown().getStackTrace()[0] : ""));
        }
    }

    private static void check(List<Fuzz.Finding> findings) {
        assertTrue(findings.isEmpty(), () -> findings.size() + " finding(s):\n" + String.join("\n", findings.stream().map(Fuzz.Finding::toString).toList()));
    }

    private static void noLeaks() throws Exception {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while ((group.metrics().snapshot().connectionsOpened() != group.metrics().snapshot().connectionsClosed()) && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        var m = group.metrics().snapshot();
        assertEquals(m.connectionsOpened(), m.connectionsClosed(), "connections left open by the client");
    }

    @Test
    void http1Responses() throws Throwable {
        List<byte[]> corpus = new ArrayList<>(List.of(
                text("HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"),
                text("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\n\r\n"),
                text("HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 3\r\n\r\n\u001f\u008b\b"),
                text("HTTP/1.1 301 Moved\r\nLocation: http://127.0.0.1:1/x\r\nContent-Length: 0\r\n\r\n"),
                text("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nConnection: close\r\n\r\nuntil close"),
                text("HTTP/1.1 200 OK\r\nSet-Cookie: a=b; Domain=com; Path=/\r\nSet-Cookie: x=y; Max-Age=-1\r\nContent-Length: 0\r\n\r\n"),
                text("HTTP/1.1 101 Switching Protocols\r\nUpgrade: h2c\r\nConnection: Upgrade\r\n\r\n"),
                text("HTTP/1.1 200 OK\r\nContent-Length: 99999999999999999999\r\n\r\n")));
        HttpClient c = new HttpClient(group, HttpClientConfig.defaults());
        try {
            check(Fuzz.run("client http/1.1", corpus, in -> against(in, () -> c.send(ClientRequest.get(url("/x"))).get(8, TimeUnit.SECONDS)), Set.of(), 9000, iterations()));
        } finally {
            c.close();
        }
        noLeaks();
    }

    private static final byte[] SERVER_SETTINGS = frame(4, 0, 0, new byte[0]);

    @Test
    void http2Frames() throws Throwable {
        byte[] ok = hpack(":status", "200", "content-type", "text/plain", "content-length", "5");
        List<byte[]> corpus = new ArrayList<>(List.of(
                cat(SERVER_SETTINGS, frame(1, 4, 1, ok), frame(0, 1, 1, text("hello"))),
                cat(SERVER_SETTINGS, frame(1, 5, 1, hpack(":status", "204"))),
                cat(SERVER_SETTINGS, frame(1, 4, 1, hpack(":status", "103", "link", "</a>; rel=preload")), frame(1, 5, 1, ok)),
                cat(SERVER_SETTINGS, frame(5, 4, 1, cat(new byte[]{0, 0, 0, 2}, hpack(":method", "GET", ":scheme", "http", ":path", "/p", ":authority", "x"))),
                        frame(1, 5, 1, ok)),
                cat(SERVER_SETTINGS, frame(7, 0, 0, new byte[]{0, 0, 0, 0, 0, 0, 0, 0}), frame(1, 5, 1, ok)),
                cat(SERVER_SETTINGS, frame(3, 0, 1, new byte[]{0, 0, 0, 8})),
                cat(SERVER_SETTINGS, frame(6, 0, 0, new byte[8]), frame(8, 0, 0, new byte[]{0, 0, 0, 1}), frame(1, 4, 1, ok), frame(9, 4, 1, new byte[]{(byte) 0x88})),
                cat(frame(4, 0, 0, new byte[]{0, 3, 0, 0, 0, 0, 0, 4, 0x7f, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0, 5, 0, 0, 0, 1}))));
        HttpClient c = new HttpClient(group, HttpClientConfig.defaults().withHttp2(Http2ClientConfig.defaults().withCleartextPriorKnowledge(true)));
        try {
            check(Fuzz.run("client http/2", corpus, in -> against(in, () -> c.send(ClientRequest.get(url("/x"))).get(8, TimeUnit.SECONDS)), Set.of(), 9000, iterations()));
        } finally {
            c.close();
        }
        noLeaks();
    }

    @Test
    void grpcResponses() throws Throwable {
        byte[] hdr = hpack(":status", "200", "content-type", "application/grpc");
        byte[] trailers = hpack("grpc-status", "0");
        List<byte[]> corpus = new ArrayList<>();
        corpus.add(cat(SERVER_SETTINGS, frame(1, 4, 1, hdr), frame(0, 0, 1, grpcMessage(new Msg("echo:x"))), frame(1, 5, 1, trailers)));
        corpus.add(cat(SERVER_SETTINGS, frame(1, 5, 1, hpack(":status", "200", "content-type", "application/grpc", "grpc-status", "14", "grpc-message", "gone%20away"))));
        corpus.add(cat(SERVER_SETTINGS, frame(1, 4, 1, hdr), frame(0, 0, 1, new byte[]{1, 0, 0, 0, 3, 1, 2, 3}), frame(1, 5, 1, trailers)));
        corpus.add(cat(SERVER_SETTINGS, frame(1, 4, 1, hdr), frame(0, 0, 1, new byte[]{0, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 1}), frame(1, 5, 1, trailers)));
        corpus.add(cat(SERVER_SETTINGS, frame(1, 4, 1, hpack(":status", "200", "content-type", "application/grpc", "grpc-status-details-bin", "AAAA", "x-bin", "!!!")),
                frame(1, 5, 1, hpack("grpc-status", "99999"))));
        GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + listener.getLocalPort()).eventLoopGroup(group).build();
        try {
            check(Fuzz.run("client grpc", corpus,
                    in -> against(in, () -> ch.unary(GrpcEchoExample.UNARY, new Msg("x"), CallOptions.DEFAULT.withDeadline(Duration.ofSeconds(3))).get(5, TimeUnit.SECONDS)),
                    Set.of(), 6000, iterations()));
        } finally {
            ch.close();
        }
        noLeaks();
    }

    @Test
    void webSocketAndEventStreamResponses() throws Throwable {
        List<byte[]> ws = new ArrayList<>(List.of(
                text("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n\u0081\u0005hello"),
                text("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\nÁ\u0003abc"),
                text("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"),
                text("HTTP/1.1 101 x\r\n\r\n\u0088\u0002\u0003è")));
        check(Fuzz.run("client websocket", ws, in -> against(in, () -> {
            CompletableFuture<WebSocket> f = WebSocketClient.connect(group, "ws://127.0.0.1:" + listener.getLocalPort() + "/", new WebSocketListener() {
            });
            try {
                f.get(2, TimeUnit.SECONDS).close(1000, "bye");
            } finally {
                Thread.sleep(30);
            }
        }), Set.of(), 5000, iterations()));
        noLeaks();

        List<byte[]> sse = new ArrayList<>(List.of(
                text("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata: a\n\nevent: x\ndata: b\nid: 7\nretry: 100\n\n"),
                text("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nï»¿data: a\r\ndata: b\r\rdata:\n\n:comment\n"),
                text("HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>"),
                text("HTTP/1.1 204 No Content\r\n\r\n")));
        HttpClient c = new HttpClient(group, HttpClientConfig.defaults());
        try {
            check(Fuzz.run("client sse", sse, in -> against(in, () -> {
                var sub = SseClient.subscribe(c, url("/events"), new SseClientListener() {
                });
                try {
                    Thread.sleep(60);
                } finally {
                    sub.close();
                }
            }), Set.of(), 5000, iterations()));
        } finally {
            c.close();
        }
        noLeaks();
    }
}
