package io.hydrangea.grpc;

/** Something calls can be started on: a {@link GrpcChannel}, or what a {@link ClientInterceptor} hands on to the next one. */
public interface Channel {
    <Req, Resp> ClientCall<Req, Resp> newCall(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener);
}
