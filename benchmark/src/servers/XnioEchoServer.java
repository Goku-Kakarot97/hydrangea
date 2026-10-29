package servers;

import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.StreamConnection;
import org.xnio.conduits.ConduitStreamSinkChannel;
import org.xnio.conduits.ConduitStreamSourceChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Echo server written the way XNIO documents it: accept listener, one read listener + write listener per
 * connection, reads paused while a write is pending (natural backpressure).
 * Usage: XnioEchoServer port ioThreads
 */
public final class XnioEchoServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);
        Xnio xnio = Xnio.getInstance();
        XnioWorker worker = xnio.createWorker(OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, threads)
                .set(Options.WORKER_TASK_CORE_THREADS, 1)
                .set(Options.WORKER_TASK_MAX_THREADS, 1)
                .getMap());
        AcceptingChannel<StreamConnection> server = worker.createStreamConnectionServer(
                new InetSocketAddress("127.0.0.1", port),
                ch -> {
                    try {
                        StreamConnection c;
                        while ((c = ch.accept()) != null) {
                            new Echo(c).start();
                        }
                    } catch (IOException e) {
                        // accept failure: ignore, benchmark counts client-side errors
                    }
                },
                OptionMap.create(Options.REUSE_ADDRESSES, true, Options.BACKLOG, 4096));
        server.resumeAccepts();
        System.out.println("READY");
        System.out.flush();
        Thread.currentThread().join();
    }

    private static final class Echo {
        private final StreamConnection conn;
        private final ConduitStreamSourceChannel in;
        private final ConduitStreamSinkChannel out;
        private final ByteBuffer buf = ByteBuffer.allocateDirect(64 * 1024);

        Echo(StreamConnection conn) {
            this.conn = conn;
            this.in = conn.getSourceChannel();
            this.out = conn.getSinkChannel();
        }

        void start() throws IOException {
            conn.setOption(Options.TCP_NODELAY, true);
            in.getReadSetter().set((ChannelListener<ConduitStreamSourceChannel>) c -> onReadable());
            out.getWriteSetter().set((ChannelListener<ConduitStreamSinkChannel>) c -> onWritable());
            in.resumeReads();
        }

        private void onReadable() {
            try {
                for (;;) {
                    buf.clear();
                    int n = in.read(buf);
                    if (n == 0) {
                        return;
                    }
                    if (n < 0) {
                        IoUtils.safeClose(conn);
                        return;
                    }
                    buf.flip();
                    if (!drain()) {
                        in.suspendReads();
                        out.resumeWrites();
                        return;
                    }
                }
            } catch (IOException e) {
                IoUtils.safeClose(conn);
            }
        }

        private void onWritable() {
            try {
                if (drain()) {
                    out.suspendWrites();
                    in.resumeReads();
                }
            } catch (IOException e) {
                IoUtils.safeClose(conn);
            }
        }

        /** @return true when the buffer was fully written */
        private boolean drain() throws IOException {
            while (buf.hasRemaining()) {
                if (out.write(buf) == 0) {
                    return false;
                }
            }
            return true;
        }
    }
}
