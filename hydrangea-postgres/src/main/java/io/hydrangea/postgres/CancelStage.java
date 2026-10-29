package io.hydrangea.postgres;

import io.hydrangea.pipeline.Context;
import io.hydrangea.pipeline.Stage;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * A connection made only to ask the server to cancel what another connection is running: it sends the 16-byte {@code CancelRequest} and closes. The server
 * answers nothing (it closes too), and cannot say whether the cancel hit anything: the target may have finished already.
 */
final class CancelStage implements Stage {

    private final int pid;
    private final int secret;
    private final Consumer<Throwable> done;

    CancelStage(int pid, int secret, Consumer<Throwable> done) {
        this.pid = pid;
        this.secret = secret;
        this.done = done;
    }

    @Override
    public void onActive(Context ctx) throws Exception {
        ctx.write(ByteBuffer.wrap(Wire.cancelRequest(pid, secret)));
        ctx.closeAfterFlush();
        done.accept(null);          // written: that is all a client can know
    }

    @Override
    public void onRead(Context ctx, Object msg) {
        // the server has nothing to say
    }

    @Override
    public void onClose(Context ctx, Throwable cause) {
        ctx.fireClose(cause);
    }
}
