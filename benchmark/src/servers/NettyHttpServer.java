package servers;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * The comparison target: a typical Netty HTTP server (HttpServerCodec + HttpObjectAggregator) with the same routes
 * as {@link HydrangeaHttpServer}, plus WebSocket echo. Usage: NettyHttpServer port ioThreads [bindAddress]
 */
public final class NettyHttpServer {

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        int threads = Integer.parseInt(args[1]);
        String bind = args.length > 2 ? args[2] : "127.0.0.1";
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup(threads);
        byte[] zeros = new byte[64 * 1024 * 1024];
        new ServerBootstrap().group(boss, workers).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 4096)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(80 * 1024 * 1024));
                        ch.pipeline().addLast(new WebSocketServerProtocolHandler("/ws", null, false, 1 << 20));
                        ch.pipeline().addLast(new Routes(zeros));
                        ch.pipeline().addLast(new SimpleChannelInboundHandler<TextWebSocketFrame>() {
                            @Override
                            protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
                                ctx.writeAndFlush(new TextWebSocketFrame(frame.text()));
                            }
                        });
                    }
                }).bind(new InetSocketAddress(bind, port)).sync();
        System.out.println("READY");
        System.out.flush();
        Thread.currentThread().join();
    }

    private static final class Routes extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final byte[] zeros;

        Routes(byte[] zeros) {
            super(false); // we decide when the request is released
            this.zeros = zeros;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            try {
                String uri = req.uri();
                int q = uri.indexOf('?');
                String path = q < 0 ? uri : uri.substring(0, q);
                String query = q < 0 ? "" : uri.substring(q + 1);
                byte[] body;
                String type = "text/plain";
                int status = 200;
                if (path.equals("/plain")) {
                    body = "Hello, World!".getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/json")) {
                    body = HydrangeaHttpServer.JSON;
                    type = "application/json";
                } else if (path.startsWith("/users/")) {
                    String qv = "-";
                    for (String kv : query.split("&")) {
                        if (kv.startsWith("q=")) qv = kv.substring(2);
                    }
                    body = ("user " + path.substring(7) + " q=" + qv).getBytes(StandardCharsets.UTF_8);
                } else if (path.equals("/echo") && req.method() == HttpMethod.POST) {
                    body = new byte[req.content().readableBytes()];
                    req.content().readBytes(body);
                    type = "application/octet-stream";
                } else if (path.startsWith("/download/")) {
                    int n = Integer.parseInt(path.substring(10));
                    body = n == zeros.length ? zeros : java.util.Arrays.copyOf(zeros, n);
                    type = "application/octet-stream";
                } else if (path.equals("/health")) {
                    body = "ok".getBytes(StandardCharsets.UTF_8);
                } else {
                    body = "Not Found".getBytes(StandardCharsets.UTF_8);
                    status = 404;
                }
                FullHttpResponse resp = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(status), Unpooled.wrappedBuffer(body));
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, type).setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
                boolean keepAlive = HttpUtil.isKeepAlive(req);
                if (keepAlive) {
                    resp.headers().set(HttpHeaderNames.CONNECTION, "keep-alive");
                    ctx.writeAndFlush(resp);
                } else {
                    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
                }
            } finally {
                req.release();
            }
        }
    }
}
