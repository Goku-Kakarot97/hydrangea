package servers;

import io.hydrangea.BufferPool;
import io.hydrangea.Connection;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Handler;
import io.hydrangea.Options;
import io.hydrangea.Server;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Echo server using Hydrangea the idiomatic way, with the same backpressure behaviour as the XNIO one
 * (stop reading while the peer is not draining). Usage: HydrangeaEchoServer port ioThreads [pool]
 */
public final class HydrangeaEchoServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);
        BufferPool pool = args.length > 2 && args[2].equals("pool") ? new BufferPool(true, 4096) : null;
        EventLoopGroup group = new EventLoopGroup(threads);
        Server.bind(group, new InetSocketAddress("127.0.0.1", port), Options.defaults().withBacklog(4096), () -> new Handler() {
            @Override
            public void onRead(Connection c, ByteBuffer data) {
                if (pool == null) {
                    c.write(ByteBuffer.allocate(data.remaining()).put(data).flip());
                } else {
                    ByteBuffer copy = pool.acquire(data.remaining());
                    copy.put(data).flip();
                    c.write(copy, () -> pool.release(copy));
                }
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
        System.out.println("READY");
        System.out.flush();
        Thread.currentThread().join();
    }
}
