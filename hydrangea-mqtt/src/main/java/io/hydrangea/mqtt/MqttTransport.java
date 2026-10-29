package io.hydrangea.mqtt;

import io.hydrangea.EventLoop;
import io.hydrangea.EventLoopGroup;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * How MQTT bytes get to the broker: plain TCP (optionally with TLS) by default, or another carrier such as WebSocket ({@link MqttWebSocket}). A transport opens one connection per call to {@link #connect}, and tells the client what happens
 * on it through {@link Events}. The client does the protocol; the transport only moves bytes.
 */
public interface MqttTransport {

    /**
     * Starts opening a connection. Exactly one of these follows, possibly later or on another thread: {@link Events#connected} (ready to send CONNECT),
     * then any number of {@link Events#data}, then {@link Events#closed} (once; also when the attempt fails before it connected).
     *
     * @param home the event loop that owns the client's state; a transport may use it for its own connection or not
     */
    void connect(EventLoopGroup group, EventLoop home, MqttConfig config, Events events);

    /** The client's side of one connection. All methods are thread-safe; the client moves the work onto its loop itself. Calls from one thread keep their order. */
    interface Events {
        /** The connection is up. */
        void connected(Link link);

        /** Bytes arrived (any amount: part of a packet, several packets). The buffer is only valid during the call. */
        void data(ByteBuffer bytes);

        /** The connection is over, or never came up; {@code cause} may be null for an orderly close. */
        void closed(Throwable cause);
    }

    /** One open connection, used by the client on its loop. */
    interface Link {
        /** Queues bytes for sending; the transport copies or takes ownership as it needs. */
        void write(ByteBuffer bytes) throws Exception;

        /** Closes now, dropping what is unsent. */
        void close();

        /** Sends what is queued, then closes. */
        void closeAfterFlush();

        boolean isWritable();

        /** Completes when {@link #isWritable()} is true; fails if the connection closes first. */
        CompletableFuture<Void> whenWritable();
    }
}
