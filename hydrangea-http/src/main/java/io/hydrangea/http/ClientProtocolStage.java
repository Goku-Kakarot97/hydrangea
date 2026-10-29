package io.hydrangea.http;

import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.function.Supplier;

/**
 * Last stage of a client connection when HTTP/2 is enabled: once the connection is active (after the TLS handshake) it looks at the
 * negotiated protocol and hands everything to the HTTP/1.1 or the HTTP/2 stage. Requests submitted before that wait here.
 */
final class ClientProtocolStage implements Stage {

    private final HttpClient.PooledConn owner;
    private final Supplier<String> alpn; // the TLS handshake's result; null on cleartext, where HTTP/2 is by prior knowledge or upgrade
    private final boolean upgradeOffer;  // cleartext without prior knowledge: start as HTTP/1.1 and offer "Upgrade: h2c" on the first request
    private Context ctx;
    private Stage delegate;
    private HttpClient.Exchange pending;
    private boolean h2;
    private boolean offerSettled;

    ClientProtocolStage(HttpClient.PooledConn owner, Supplier<String> alpn, boolean upgradeOffer) {
        this.owner = owner;
        this.alpn = alpn;
        this.upgradeOffer = upgradeOffer;
    }

    @Override
    public void onActive(Context ctx) throws Exception {
        this.ctx = ctx;
        h2 = !upgradeOffer && (alpn == null || "h2".equals(alpn.get()));
        delegate = h2 ? owner.h2stage : owner.stage;
        owner.h2 = h2;
        if (h2) {
            owner.pool.markH2(owner);
        }
        if (!h2) {
            owner.h2Active.set(0); // the slot held for the first request only matters on HTTP/2
        }
        delegate.onActive(ctx);
        HttpClient.Exchange first = pending;
        pending = null;
        if (first != null) {
            submitFirst(first);
        }
        if (!upgradeOffer) {
            owner.pool.protocolKnown(owner, h2);
        }
    }

    /** The request this connection was opened for. */
    void submitFirst(HttpClient.Exchange ex) {
        if (!upgradeOffer || offerSettled) {
            ex.offerUpgrade = false; // a retry on a connection that already answered the offer must not repeat it
        }
        if (delegate == null) {
            pending = ex;
        } else if (upgradeOffer && !offerSettled) {
            if (HttpClient.hasStreamedBody(ex.req)) { // the whole request must be out before the switch: no offer for streamed bodies
                settle(false);
            } else {
                ex.offerUpgrade = true;
                owner.h2Active.set(1);   // the slot for this request if the server says yes
            }
            owner.stage.submit(ex);
        } else if (h2) {
            owner.h2stage.submit(ex); // its slot was reserved when the connection was created
        } else {
            owner.stage.submit(ex);
        }
    }

    /** The server said 101: this connection is HTTP/2 from now on, and {@code ex} (already sent as HTTP/1.1) is its stream 1. */
    void switchToHttp2(HttpClient.Exchange ex, ByteBuffer leftover) {
        offerSettled = true;
        h2 = true;
        delegate = owner.h2stage;
        owner.h2 = true;
        owner.pool.markH2(owner);
        ex.offerUpgrade = false;
        try {
            owner.h2stage.onActive(ctx);
            owner.h2stage.adoptUpgraded(ex);
            if (leftover.hasRemaining()) {
                owner.h2stage.onRead(ctx, leftover);
            }
        } catch (Exception e) {
            ctx.connection().close();
            return;
        }
        owner.pool.protocolKnown(owner, true);
    }

    /** The server answered the offer with an ordinary response: the host speaks HTTP/1.1 (here at least). */
    void upgradeDeclined() {
        settle(false);
    }

    private void settle(boolean isH2) {
        if (offerSettled) {
            return;
        }
        offerSettled = true;
        owner.h2Active.set(0);
        owner.pool.protocolKnown(owner, isH2);
    }

    /** A further request on an HTTP/2 connection whose slot the pool already reserved. */
    void submitReserved(HttpClient.Exchange ex) {
        owner.h2stage.submit(ex);
    }

    @Override
    public void onRead(Context ctx, Object msg) throws Exception {
        if (delegate == null) {
            ctx.connection().close();
            return;
        }
        delegate.onRead(ctx, (ByteBuffer) msg);
    }

    @Override
    public void onWritabilityChanged(Context ctx) throws Exception {
        if (delegate != null) {
            delegate.onWritabilityChanged(ctx);
        }
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        if (delegate != null) {
            delegate.onClose(ctx, cause);
        } else {
            failPending();
        }
    }

    /** From the connection's closeFuture: covers a close before activation, when onClose is not delivered. */
    void connectionClosed() {
        failPending();
        owner.stage.connectionClosed();
        owner.h2stage.connectionClosed();
    }

    private void failPending() {
        HttpClient.Exchange ex = pending;
        pending = null;
        if (ex != null) {
            owner.failed(ex, new ClosedChannelException(), false);
        }
    }
}
