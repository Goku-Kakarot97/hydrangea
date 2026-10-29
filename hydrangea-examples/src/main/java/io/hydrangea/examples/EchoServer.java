package io.hydrangea.examples;

import io.hydrangea.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/** Minimal echo server: {@code java -cp target/classes io.hydrangea.examples.EchoServer [port]}. */
public final class EchoServer {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 7000;
        var group = new EventLoopGroup();
        Server server = Server.bind(group, new InetSocketAddress(port), Options.defaults(), () -> new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                ByteBuffer copy = ByteBuffer.allocate(data.remaining()).put(data).flip();
                c.write(copy);
                // a slow reader must not make us buffer unboundedly: stop reading until it catches up
                if (!c.isWritable()) {
                    c.setAutoRead(false);
                }
            }

            @Override
            public void onWritabilityChanged(Connection c) {
                if (c.isWritable()) {
                    c.setAutoRead(true);
                }
            }
        });
        System.out.println("echo listening on " + server.localAddress());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            group.close();
        }));
    }
}
