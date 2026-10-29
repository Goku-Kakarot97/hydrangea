package io.hydrangea.http;

/**
 * Implemented by a stage that took over an HTTP/2 stream (see {@link HttpHandler#wantsStream}) and wants to hear that the client
 * finished sending (END_STREAM) <em>without</em> the stream being treated as closed: the response is still ours to send. A taken-over
 * stage that does not implement this sees the end of the client's side as the connection closing, which is right for a WebSocket and wrong
 * for a request/response protocol such as gRPC.
 */
public interface HalfCloseListener {

    /** The client will send no more; runs on the connection's event loop. */
    void onPeerHalfClose();
}
