package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.http.HttpConfig;
import io.hydrangea.http.HttpResponse;
import io.hydrangea.http.HttpRouter;
import io.hydrangea.http.HttpServer;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketConfig;
import io.hydrangea.websocket.WebSocketListener;
import io.hydrangea.websocket.WebSocketUpgrade;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A chat room: everything a client sends is broadcast to everybody. Open http://localhost:8080/ in two browser tabs.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.WebSocketChatExample [port]
 * </pre>
 * WebSocket endpoints are ordinary routes: the router decides the path, {@link WebSocketUpgrade#accept} performs the
 * handshake and the connection then speaks WebSocket. Same-host Origin checking is on by default (see
 * {@link WebSocketConfig#allowAnyOrigin()} to relax it).
 */
public final class WebSocketChatExample {

    private static final String PAGE = """
            <!doctype html><meta charset=utf-8><title>Hydrangea chat</title>
            <body style="font:16px system-ui;max-width:40rem;margin:2rem auto">
            <h3>Hydrangea chat</h3><pre id=log style="height:16rem;overflow:auto;border:1px solid #888;padding:.5rem"></pre>
            <form id=f><input id=m autofocus style="width:75%"> <button>Send</button></form>
            <script>
              const log = document.getElementById('log'), add = t => { log.textContent += t + '\\n'; log.scrollTop = log.scrollHeight; };
              const ws = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/chat');
              ws.onopen = () => add('* connected'); ws.onclose = e => add('* closed ' + e.code);
              ws.onmessage = e => add(e.data);
              f.onsubmit = e => { e.preventDefault(); if (m.value) ws.send(m.value); m.value = ''; };
            </script>""";

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        EventLoopGroup group = new EventLoopGroup();
        Set<WebSocket> room = ConcurrentHashMap.newKeySet();

        WebSocketConfig config = WebSocketConfig.defaults()
                .withMaxMessageBytes(8 * 1024)               // a chat line, not a file upload
                .withPingInterval(Duration.ofSeconds(20));   // detects dead peers and keeps proxies from timing the socket out

        HttpRouter router = new HttpRouter()
                .get("/", r -> HttpResponse.status(200).header("Content-Type", "text/html; charset=utf-8")
                        .body(PAGE.getBytes(StandardCharsets.UTF_8)))
                .get("/chat", r -> WebSocketUpgrade.accept(r.request(), config, () -> new WebSocketListener() {
                    private String name;

                    @Override
                    public void onOpen(WebSocket ws) {
                        name = "guest-" + Integer.toHexString(System.identityHashCode(ws) & 0xffff);
                        room.add(ws);
                        broadcast("* " + name + " joined (" + room.size() + " online)");
                    }

                    @Override
                    public void onText(WebSocket ws, String text) {
                        broadcast(name + ": " + text);
                    }

                    @Override
                    public void onClose(WebSocket ws, int code, String reason) {
                        room.remove(ws);
                        broadcast("* " + name + " left");
                    }

                    private void broadcast(String line) {
                        for (WebSocket peer : room) {
                            if (peer.isWritable()) { // a slow reader misses lines instead of making us queue without bound
                                peer.sendText(line);
                            }
                        }
                    }
                }));

        Server server = HttpServer.bind(group, new InetSocketAddress(port), Options.defaults(), null, HttpConfig.defaults(), router);
        System.out.println("chat on http://localhost:" + ((InetSocketAddress) server.localAddress()).getPort() + "/");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            group.close();
        }));
        Thread.currentThread().join();
    }
}
