package io.hydrangea.postgres;

import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * The first step of PostgreSQL's TLS: before anything else the client sends an {@code SSLRequest} and the server answers one byte,
 * {@code S} (go ahead and start TLS) or {@code N} (no TLS here). This stage sits next to the socket, below the {@code TlsStage}: it holds back
 * the activation so that the TLS handshake starts only after the {@code S}, then passes everything through.
 * <p>
 * The answer must be alone in what the server sent. Anything after it would be plain text that the TLS layer would take for its own, which lets
 * a man in the middle slip data into the session (CVE-2021-23222), so extra bytes close the connection.
 */
final class SslRequestStage implements Stage {

    private final Consumer<PgException> refused;
    private final Consumer<Throwable> closedBeforeLogin;
    private boolean passing;

    SslRequestStage(Consumer<PgException> refused, Consumer<Throwable> closedBeforeLogin) {
        this.refused = refused;
        this.closedBeforeLogin = closedBeforeLogin;
    }

    @Override
    public void onActive(Context ctx) throws Exception {
        ByteBuffer b = ByteBuffer.allocate(8);
        b.putInt(8).putInt(80877103).flip();           // length 8, request code 1234.5679
        ctx.write(b);
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        if (passing) {
            ctx.fireRead(msg);
            return;
        }
        ByteBuffer in = (ByteBuffer) msg;
        if (in.remaining() != 1) {
            throw new PgException("protocol error: the answer to SSLRequest is not a single byte", false);
        }
        byte answer = in.get();
        if (answer == 'S') {
            passing = true;
            ctx.fireActive();                          // now the TlsStage starts its handshake
        } else if (answer == 'N') {
            refused.accept(new PgException("the server does not support TLS", false));
            ctx.connection().close();
        } else {
            throw new PgException("protocol error: unexpected answer '" + (char) answer + "' to SSLRequest", false);
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        closedBeforeLogin.accept(cause);     // the client ignores it unless it was still connecting (the TLS handshake had not finished)
        ctx.fireClose(cause);
    }
}
