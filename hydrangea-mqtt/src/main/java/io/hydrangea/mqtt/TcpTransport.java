package io.hydrangea.mqtt;

import io.hydrangea.Client;
import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.pipeline.Stage;
import io.hydrangea.tls.TlsStage;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/** The default transport: a TCP connection on the client's own event loop, with TLS when the configuration has an SSLContext. */
final class TcpTransport implements MqttTransport {

    @Override
    public void connect(EventLoopGroup group, EventLoop home, MqttConfig config, Events events) {
        // resolving the name can block: not on the loop
        Thread.startVirtualThread(() -> {
            InetSocketAddress addr;
            try {
                addr = new InetSocketAddress(config.host(), config.port());
                if (addr.isUnresolved()) {
                    throw new java.net.UnknownHostException(config.host());
                }
            } catch (Exception e) {
                events.closed(e);
                return;
            }
            Stage stage = new TcpStage(events);
            Pipeline pipeline = config.sslContext() == null
                    ? Pipeline.of(stage)
                    : Pipeline.of(TlsStage.client(config.sslContext(), config.host(), config.port()), stage);
            Options options = Options.defaults().withKeepAlive(true);
            if (config.connectTimeout() != null) {
                options = options.withConnectTimeout(config.connectTimeout());
            }
            Client.connect(home, addr, options, pipeline).whenComplete((conn, err) -> {
                if (err != null) {
                    events.closed(err);
                }
            });
        });
    }

    private static final class TcpStage implements Stage {
        private final Events events;

        TcpStage(Events events) {
            this.events = events;
        }

        @Override
        public void onActive(Context ctx) {
            events.connected(new TcpLink(ctx));
        }

        @Override
        public void onRead(Context ctx, Object msg) {
            events.data((ByteBuffer) msg);
        }

        @Override
        public void onClose(Context ctx, Throwable cause) {
            events.closed(cause != null ? cause : new java.io.IOException("the connection was closed"));
            ctx.fireClose(cause);
        }
    }

    private static final class TcpLink implements Link {
        private final Context ctx;

        TcpLink(Context ctx) {
            this.ctx = ctx;
        }

        @Override
        public void write(ByteBuffer bytes) throws Exception {
            ctx.write(bytes);
        }

        @Override
        public void close() {
            ctx.connection().close();
        }

        @Override
        public void closeAfterFlush() {
            try {
                ctx.closeAfterFlush();
            } catch (Exception e) {
                ctx.connection().close();
            }
        }

        @Override
        public boolean isWritable() {
            return ctx.connection().isWritable();
        }

        @Override
        public CompletableFuture<Void> whenWritable() {
            return ctx.connection().whenWritable();
        }
    }
}
