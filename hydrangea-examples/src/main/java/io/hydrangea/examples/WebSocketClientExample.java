package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.websocket.WebSocket;
import io.hydrangea.websocket.WebSocketClient;
import io.hydrangea.websocket.WebSocketListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Talks to {@link WebSocketChatExample}: {@code java -cp target/classes io.hydrangea.examples.WebSocketClientExample [ws://localhost:8080/chat]}
 */
public final class WebSocketClientExample {

    public static void main(String[] args) throws Exception {
        String url = args.length > 0 ? args[0] : "ws://localhost:8080/chat";
        CountDownLatch closed = new CountDownLatch(1);
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            WebSocket ws = WebSocketClient.connect(group, url, new WebSocketListener() {
                @Override
                public void onText(WebSocket ws, String text) {
                    System.out.println("< " + text);
                }

                @Override
                public void onError(WebSocket ws, Throwable error) {
                    System.out.println("! " + error);
                }

                @Override
                public void onClose(WebSocket ws, int code, String reason) {
                    System.out.println("closed " + code + " " + reason);
                    closed.countDown();
                }
            }).get(10, TimeUnit.SECONDS);

            ws.sendText("hello from the Hydrangea client");
            Thread.sleep(500);
            ws.close(1000, "bye").get(10, TimeUnit.SECONDS);
            closed.await(5, TimeUnit.SECONDS);
        }
    }
}
