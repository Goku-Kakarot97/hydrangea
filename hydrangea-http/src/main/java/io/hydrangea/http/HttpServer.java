package io.hydrangea.http;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.Options;
import io.hydrangea.Server;
import io.hydrangea.pipeline.Pipeline;
import io.hydrangea.tls.TlsStage;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;

/** Convenience wrapper: {@link Server} + optional TLS + {@link HttpServerStage}. */
public final class HttpServer {

    private HttpServer() {
    }

    public static Server bind(EventLoopGroup group, SocketAddress address, HttpHandler handler) throws IOException {
        return bind(group, address, Options.defaults(), null, HttpConfig.defaults(), handler);
    }

    /** @param tls when non-null the server speaks HTTPS */
    public static Server bind(EventLoopGroup group, SocketAddress address, Options options, SSLContext tls,
                              HttpConfig config, HttpHandler handler) throws IOException {
        List<String> alpn = config.http2() != null ? List.of("h2", "http/1.1") : null;
        return Server.bind(group, address, options, () -> {
            if (tls == null) {
                return Pipeline.of(new HttpServerStage(config, handler));
            }
            if (alpn == null) {
                return Pipeline.of(TlsStage.server(tls), new HttpServerStage(config, handler));
            }
            TlsStage stage = TlsStage.server(tls, alpn);
            return Pipeline.of(stage, new HttpServerStage(config, handler, stage::applicationProtocol));
        });
    }
}
