package io.hydrangea.http;

import io.hydrangea.codec.Accumulator;
import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * First stage of a connection that goes through an HTTP proxy to an origin ({@code https://}, or cleartext HTTP/2 by prior knowledge): it
 * asks the proxy for a tunnel ({@code CONNECT host:port}) and passes the connection on to the next stage only once the proxy has said 2xx.
 * Everything after that, the TLS handshake included, is between the client and the origin; the proxy just relays bytes.
 * <p>
 * A refusal (407 for missing credentials, 403, 502 ...) fails the connection with a message that names the proxy's answer; the failure is
 * handed to {@code onFailure} so the request that opened the connection is told why, not just that the channel closed.
 */
final class ProxyTunnelStage implements Stage {

    private static final int MAX_RESPONSE_HEAD = 16 * 1024;

    private final String authority;          // host:port of the origin
    private final String authorization;      // Proxy-Authorization value, or null
    private final java.util.function.Consumer<IOException> onFailure;
    private final Accumulator acc = new Accumulator();
    private boolean established;
    private boolean failed;

    ProxyTunnelStage(String host, int port, String authorization, java.util.function.Consumer<IOException> onFailure) {
        this.authority = host + ":" + port;
        this.authorization = authorization;
        this.onFailure = onFailure;
    }

    @Override
    public void onActive(Context ctx) throws Exception {
        StringBuilder sb = new StringBuilder(128);
        sb.append("CONNECT ").append(authority).append(" HTTP/1.1\r\nHost: ").append(authority).append("\r\n");
        if (authorization != null) {
            sb.append("Proxy-Authorization: ").append(authorization).append("\r\n");
        }
        sb.append("\r\n");
        ctx.write(ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.ISO_8859_1)));
        // the next stages hear about the connection only when the tunnel is up
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        if (established) {
            ctx.fireRead(msg);
            return;
        }
        if (failed) {
            return;
        }
        acc.append((ByteBuffer) msg);
        ByteBuffer in = acc.buffer();
        int end = indexOfHeadEnd(in);
        if (end < 0) {
            if (in.remaining() > MAX_RESPONSE_HEAD) {
                fail(ctx, new IOException("proxy answered CONNECT with an oversized response head"));
            }
            return;
        }
        byte[] raw = new byte[end - in.position()];
        in.get(raw);
        in.position(in.position() + 4);
        String head = new String(raw, StandardCharsets.ISO_8859_1);
        String statusLine = head.contains("\r\n") ? head.substring(0, head.indexOf("\r\n")) : head;
        String[] parts = statusLine.split(" ", 3);
        int status;
        try {
            if (parts.length < 2 || !parts[0].startsWith("HTTP/1.")) {
                throw new NumberFormatException();
            }
            status = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            fail(ctx, new IOException("proxy sent a malformed answer to CONNECT: " + printable(statusLine)));
            return;
        }
        if (status < 200 || status > 299) {
            fail(ctx, new IOException("proxy refused CONNECT " + authority + ": " + printable(statusLine)
                    + (status == 407 ? " (proxy authentication required: configure credentials)" : "")));
            return;
        }
        established = true;
        ctx.fireActive();
        if (in.hasRemaining()) { // bytes the origin sent right behind the proxy's answer
            ByteBuffer rest = ByteBuffer.allocate(in.remaining()).put(in).flip();
            ctx.fireRead(rest);
        }
        acc.release();
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        if (!established && !failed) {
            fail(null, new IOException("proxy closed the connection before the tunnel was established", cause));
        }
        acc.release();
        ctx.fireClose(cause);
    }

    private void fail(Context ctx, IOException error) {
        if (failed) {
            return;
        }
        failed = true;
        onFailure.accept(error);
        if (ctx != null) {
            ctx.connection().close();
        }
    }

    private static int indexOfHeadEnd(ByteBuffer in) {
        for (int i = in.position(); i + 3 < in.limit(); i++) {
            if (in.get(i) == '\r' && in.get(i + 1) == '\n' && in.get(i + 2) == '\r' && in.get(i + 3) == '\n') {
                return i;
            }
        }
        return -1;
    }

    private static String printable(String s) {
        String cut = s.length() > 100 ? s.substring(0, 100) + "..." : s;
        return cut.replaceAll("[^\\x20-\\x7e]", "?");
    }
}
