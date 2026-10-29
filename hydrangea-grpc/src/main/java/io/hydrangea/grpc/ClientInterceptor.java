package io.hydrangea.grpc;

/**
 * Wraps every call a {@link GrpcChannel} starts: add headers (a token), log, time, trace, or wrap the call or its listener.
 * <pre>{@code
 * GrpcChannel.forTarget(url).intercept(new ClientInterceptor() {
 *     public <Req, Resp> ClientCall<Req, Resp> intercept(MethodDescriptor<Req, Resp> m, CallOptions o, ClientCall.Listener<Resp> l, Channel next) {
 *         return next.newCall(m, o.withMetadata(o.metadata().copy().put("authorization", "Bearer " + token())), l);
 *     }
 * })
 * }</pre>
 * The first interceptor added is the outermost. Interceptors see one call per logical call: retries happen below them.
 */
public interface ClientInterceptor {
    <Req, Resp> ClientCall<Req, Resp> intercept(MethodDescriptor<Req, Resp> method, CallOptions options, ClientCall.Listener<Resp> listener, Channel next);
}
