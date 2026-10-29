package io.hydrangea.grpc;

import io.hydrangea.Connection;
import io.hydrangea.EventLoop;
import io.hydrangea.http.Headers;
import io.hydrangea.http.Http2Stream;
import io.hydrangea.http.HttpRequest;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;

/**
 * gRPC-Web (what browsers can speak): the same calls over plain HTTP/1.1 or HTTP/2 requests, with the status in a last body frame instead of
 * HTTP trailers. Both modes: binary ({@code application/grpc-web[+proto]}) and base64 text ({@code application/grpc-web-text[+proto]}).
 * Like gRPC-Web itself, server-side this serves unary and server-streaming methods: the request is one message.
 */
final class GrpcWeb {
    private GrpcWeb() {
    }

    /** True for gRPC-Web content types of either mode. */
    static boolean isWeb(HttpRequest r) {
        String ct = r.headers().get("content-type");
        if (ct == null) {
            return false;
        }
        String t = ct.toLowerCase(Locale.ROOT);
        return t.equals("application/grpc-web") || t.startsWith("application/grpc-web+") || t.startsWith("application/grpc-web;") || isTextType(t);
    }

    static boolean isWebText(HttpRequest r) {
        String ct = r.headers().get("content-type");
        return ct != null && isTextType(ct.toLowerCase(Locale.ROOT));
    }

    private static boolean isTextType(String t) {
        return t.equals("application/grpc-web-text") || t.startsWith("application/grpc-web-text+") || t.startsWith("application/grpc-web-text;");
    }

    /**
     * Decodes a base64 request body. Browsers may send several padded base64 pieces back to back, which one decoder rejects, so each piece
     * (up to and including its padding) is decoded on its own. Whitespace is ignored.
     */
    static byte[] decodeText(byte[] body) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        StringBuilder piece = new StringBuilder();
        for (byte b : body) {
            char c = (char) (b & 0xff);
            if (c == ' ' || c == '\r' || c == '\n' || c == '\t') {
                continue;
            }
            piece.append(c);
            if (c == '=' && (piece.length() % 4 == 0)) {
                out.writeBytes(java.util.Base64.getDecoder().decode(piece.toString()));
                piece.setLength(0);
            }
        }
        if (piece.length() > 0) {
            out.writeBytes(java.util.Base64.getDecoder().decode(piece.toString()));   // throws IllegalArgumentException when malformed
        }
        return out.toByteArray();
    }

    /** The response content type: the request's, minus parameters. */
    static String responseContentType(HttpRequest r) {
        String ct = r.headers().get("content-type").toLowerCase(Locale.ROOT);
        int semi = ct.indexOf(';');
        return semi < 0 ? ct.trim() : ct.substring(0, semi).trim();
    }

    /** The trailer block of gRPC-Web: a frame with the high bit of the flag byte set, holding HTTP/1-style header lines. */
    static byte[] trailerFrame(Headers trailers) {
        StringBuilder sb = new StringBuilder();
        trailers.forEach((n, v) -> sb.append(n).append(": ").append(v).append("\r\n"));
        byte[] body = sb.toString().getBytes(StandardCharsets.ISO_8859_1);
        ByteBuffer b = ByteBuffer.allocate(5 + body.length);
        b.put((byte) 0x80).putInt(body.length).put(body);
        return b.array();
    }

    /**
     * Presents a connection as the {@link Http2Stream} {@code ServerCall} writes to: messages go out as body bytes (chunk-framed on
     * HTTP/1.1), and "trailers" become the last body frame, after which the response ends.
     */
    static final class WebStream implements Http2Stream {
        private final Connection c;
        private final boolean chunked;
        private final Headers requestHeaders;
        private final boolean text;
        private byte[] carry = new byte[0];   // text mode: bytes not yet a multiple of 3, held back like Envoy does so the base64 pieces concatenate
        private boolean ended;

        WebStream(Connection c, boolean chunked, boolean text, Headers requestHeaders) {
            this.c = c;
            this.chunked = chunked;
            this.text = text;
            this.requestHeaders = requestHeaders;
        }

        /** Text mode: the base64 of everything that can be encoded without padding (or everything, when {@code last}); the rest is carried. */
        private byte[] encodeText(byte[] data, boolean last) {
            byte[] all = new byte[carry.length + data.length];
            System.arraycopy(carry, 0, all, 0, carry.length);
            System.arraycopy(data, 0, all, carry.length, data.length);
            int take = last ? all.length : all.length - all.length % 3;
            carry = java.util.Arrays.copyOfRange(all, take, all.length);
            return java.util.Base64.getEncoder().encode(java.util.Arrays.copyOfRange(all, 0, take));
        }

        private ByteBuffer chunk(byte[] data) {
            if (!chunked) {
                return ByteBuffer.wrap(data);
            }
            byte[] head = (Integer.toHexString(data.length) + "\r\n").getBytes(StandardCharsets.US_ASCII);
            ByteBuffer b = ByteBuffer.allocate(head.length + data.length + 2);
            b.put(head).put(data).put((byte) '\r').put((byte) '\n');
            b.flip();
            return b;
        }

        @Override
        public void write(ByteBuffer data) {
            byte[] bytes = new byte[data.remaining()];
            data.get(bytes);
            synchronized (this) {
                if (ended) {
                    return;
                }
                if (text) {
                    bytes = encodeText(bytes, false);
                    if (bytes.length == 0) {
                        return;
                    }
                }
                c.write(chunk(bytes));
            }
        }

        @Override
        public void sendTrailers(Headers trailers) {
            synchronized (this) {
                if (ended) {
                    return;
                }
                ended = true;
                byte[] last = trailerFrame(trailers);
                c.write(chunk(text ? encodeText(last, true) : last));
                if (chunked) {
                    c.write(ByteBuffer.wrap("0\r\n\r\n".getBytes(StandardCharsets.US_ASCII)));
                }
            }
            c.closeAfterFlush();
        }

        @Override
        public int streamId() {
            return c instanceof Http2Stream s ? s.streamId() : 0;
        }

        @Override
        public Headers requestHeaders() {
            return requestHeaders;
        }

        @Override
        public EventLoop eventLoop() {
            return c.eventLoop();
        }

        @Override
        public SocketAddress remoteAddress() {
            return c.remoteAddress();
        }

        @Override
        public SocketAddress localAddress() {
            return c.localAddress();
        }

        @Override
        public boolean isOpen() {
            return c.isOpen();
        }

        @Override
        public boolean isWritable() {
            return c.isWritable();
        }

        @Override
        public long pendingBytes() {
            return c.pendingBytes();
        }

        @Override
        public CompletableFuture<Void> closeFuture() {
            return c.closeFuture();
        }

        @Override
        public void write(ByteBuffer data, Runnable done) {
            write(data);
            if (done != null) {
                done.run();
            }
        }

        @Override
        public void setAutoRead(boolean enabled) {
            c.setAutoRead(enabled);
        }

        @Override
        public void close() {
            c.close();
        }

        @Override
        public void closeAfterFlush() {
            c.closeAfterFlush();
        }

        @Override
        public void closeAfterFlush(Duration linger) {
            c.closeAfterFlush(linger);
        }
    }
}
