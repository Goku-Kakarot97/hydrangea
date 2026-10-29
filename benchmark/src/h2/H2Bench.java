package h2;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http2.*;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * HTTP/2 (cleartext, prior knowledge) benchmark with a Netty client as the load generator for every server, so the servers see identical load.
 * <pre>
 *   H2Bench server netty port ioThreads
 *   H2Bench load port scenario connections streamsPerConnection seconds serverPid loadThreads
 * </pre>
 * scenarios: plain (GET, 13 B) | echo1k (POST 1 KiB, echoed) | download1m (GET, 1 MiB)
 */
public final class H2Bench {

    static final byte[] HELLO = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    static final int WINDOW = 1 << 20;

    public static void main(String[] a) throws Exception {
        if (a[0].equals("server")) {
            server(Integer.parseInt(a[2]), Integer.parseInt(a[3]));
        } else {
            try {
                load(Integer.parseInt(a[1]), a[2], Integer.parseInt(a[3]), Integer.parseInt(a[4]), Integer.parseInt(a[5]), Long.parseLong(a[6]), Integer.parseInt(a[7]));
            } catch (Throwable t) {
                t.printStackTrace();
                System.exit(1);
            }
        }
    }

    // ---- Netty HTTP/2 server ----
    @ChannelHandler.Sharable
    static final class ServerStream extends ChannelDuplexHandler {
        static final byte[] MEGABYTE = new byte[1 << 20];
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof Http2HeadersFrame h) {
                CharSequence path = h.headers().path();
                ctx.channel().attr(io.netty.util.AttributeKey.<CharSequence>valueOf("path")).set(path);
                if (h.isEndStream()) respond(ctx, path, null);
            } else if (msg instanceof Http2DataFrame d) {
                ByteBuf acc = ctx.channel().attr(io.netty.util.AttributeKey.<ByteBuf>valueOf("body")).get();
                if (acc == null) {
                    acc = Unpooled.buffer();
                    ctx.channel().attr(io.netty.util.AttributeKey.<ByteBuf>valueOf("body")).set(acc);
                }
                acc.writeBytes(d.content());
                int n = d.initialFlowControlledBytes();
                boolean end = d.isEndStream();
                d.release();
                ctx.write(new DefaultHttp2WindowUpdateFrame(n).stream(((Http2StreamChannel) ctx.channel()).stream()));
                if (end) respond(ctx, ctx.channel().attr(io.netty.util.AttributeKey.<CharSequence>valueOf("path")).get(), acc);
            } else {
                io.netty.util.ReferenceCountUtil.release(msg);
            }
        }

        private void respond(ChannelHandlerContext ctx, CharSequence path, ByteBuf body) {
            byte[] out;
            String p = path.toString();
            if (p.startsWith("/download/")) out = MEGABYTE;
            else if (p.equals("/echo")) { out = new byte[body.readableBytes()]; body.readBytes(out); }
            else out = HELLO;
            ctx.write(new DefaultHttp2HeadersFrame(new DefaultHttp2Headers().status("200").set("content-type", "application/octet-stream"), false));
            ctx.writeAndFlush(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(out), true));
        }
    }

    static void server(int port, int threads) throws Exception {
        EventLoopGroup loops = new NioEventLoopGroup(threads);
        new ServerBootstrap().group(loops).channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 4096).childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(Http2FrameCodecBuilder.forServer()
                                .initialSettings(Http2Settings.defaultSettings().initialWindowSize(WINDOW)).build());
                        ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                            protected void initChannel(Channel c) { c.pipeline().addLast(new ServerStream()); }
                        }));
                    }
                }).bind("127.0.0.1", port).sync();
        System.out.println("READY " + port + " pid=" + ProcessHandle.current().pid());
        Thread.currentThread().join();
    }

    // ---- load ----
    static final AtomicLongArray HIST = new AtomicLongArray(2048);
    static final double LN = Math.log(1.01);
    static final AtomicLong OK = new AtomicLong(), ERR = new AtomicLong(), BYTES = new AtomicLong();
    static volatile boolean measuring, stop;

    static long percentile(double p) {
        long total = 0;
        for (int i = 0; i < HIST.length(); i++) total += HIST.get(i);
        long want = (long) Math.ceil(total * p), seen = 0;
        for (int i = 0; i < HIST.length(); i++) { seen += HIST.get(i); if (seen >= want) return Math.round(Math.exp((i + 1) * LN)); }
        return -1;
    }

    static long cpuNanos(long pid) throws Exception {
        Process p = new ProcessBuilder("ps", "-o", "time=", "-p", Long.toString(pid)).start();
        String t = new String(p.getInputStream().readAllBytes()).trim();
        p.waitFor();
        double secs = 0;
        for (String part : t.split(":")) secs = secs * 60 + Double.parseDouble(part);
        return (long) (secs * 1e9);
    }

    static final class Conn {
        Channel channel;
        Http2StreamChannelBootstrap streams;
    }

    static void load(int port, String scenario, int conns, int perConn, int seconds, long serverPid, int threads) throws Exception {
        boolean post = scenario.equals("echo1k");
        int expected = scenario.equals("download1m") ? 1 << 20 : post ? 1024 : HELLO.length;
        String path = scenario.equals("download1m") ? "/download/1048576" : post ? "/echo" : "/plain";
        byte[] payload = new byte[1024];
        EventLoopGroup loops = new NioEventLoopGroup(threads);
        Conn[] cs = new Conn[conns];
        for (int i = 0; i < conns; i++) {
            Conn c = cs[i] = new Conn();
            Channel ch = new Bootstrap().group(loops).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        protected void initChannel(SocketChannel x) {
                            x.pipeline().addLast(Http2FrameCodecBuilder.forClient()
                                    .initialSettings(Http2Settings.defaultSettings().initialWindowSize(WINDOW)).build());
                            x.pipeline().addLast(new Http2MultiplexHandler(new ChannelInboundHandlerAdapter()));
                        }
                    }).connect("127.0.0.1", port).sync().channel();
            c.channel = ch;
            c.streams = new Http2StreamChannelBootstrap(ch);
            Http2FrameCodec codec = ch.pipeline().get(Http2FrameCodec.class);
            ch.eventLoop().submit(() -> {
                try { codec.connection().local().flowController().incrementWindowSize(codec.connection().connectionStream(), WINDOW - 65535); } catch (Exception e) { throw new RuntimeException(e); }
                ch.flush();
            }).sync();
        }
        CountDownLatch done = new CountDownLatch(conns * perConn);
        for (Conn c : cs) {
            for (int s = 0; s < perConn; s++) {
                new Object() {
                    void next() {
                        if (stop) { done.countDown(); return; }
                        long t0 = System.nanoTime();
                        c.streams.open().addListener(f -> {
                            if (!f.isSuccess()) { if (measuring) ERR.incrementAndGet(); done.countDown(); return; }
                            Http2StreamChannel st = (Http2StreamChannel) f.getNow();
                            st.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                                int got; boolean ok = true;
                                @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                    boolean end = false;
                                    if (msg instanceof Http2HeadersFrame h) { ok &= "200".contentEquals(h.headers().status()); end = h.isEndStream(); }
                                    else if (msg instanceof Http2DataFrame d) { got += d.content().readableBytes(); end = d.isEndStream(); ctx.write(new DefaultHttp2WindowUpdateFrame(d.initialFlowControlledBytes()).stream(st.stream())); d.release(); ctx.flush(); }
                                    else io.netty.util.ReferenceCountUtil.release(msg);
                                    if (end) {
                                        if (measuring) {
                                            if (ok && got == expected) {
                                                OK.incrementAndGet(); BYTES.addAndGet(got);
                                                long us = Math.max(1, (System.nanoTime() - t0) / 1000);
                                                HIST.incrementAndGet(Math.min(2047, (int) (Math.log(us) / LN)));
                                            } else ERR.incrementAndGet();
                                        }
                                        next();
                                    }
                                }
                                @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable t) { if (measuring) ERR.incrementAndGet(); ctx.close(); }
                            });
                            Http2Headers h = new DefaultHttp2Headers().method(post ? "POST" : "GET").scheme("http").authority("127.0.0.1:" + port).path(path);
                            st.write(new DefaultHttp2HeadersFrame(h, !post));
                            if (post) st.write(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(payload), true));
                            st.flush();
                        });
                    }
                }.next();
            }
        }
        Thread.sleep(8000);
        long cpu0 = cpuNanos(serverPid);
        long t0 = System.nanoTime();
        measuring = true;
        Thread.sleep(seconds * 1000L);
        measuring = false;
        long elapsed = System.nanoTime() - t0;
        long cpu1 = cpuNanos(serverPid);
        long ok = OK.get(), err = ERR.get(), bytes = BYTES.get();
        stop = true;
        double secs = elapsed / 1e9;
        System.out.printf("RESULT scenario=%s conns=%d streams=%d rps=%.0f MB/s=%.0f p50=%d p99=%d p999=%d serverCpuUsPerReq=%.1f errors=%d%n",
                scenario, conns, perConn, ok / secs, bytes / secs / 1e6, percentile(0.5), percentile(0.99), percentile(0.999), (cpu1 - cpu0) / 1000.0 / Math.max(1, ok), err);
        System.exit(0);
    }
}
