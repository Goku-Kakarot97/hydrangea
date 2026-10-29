package io.hydrangea.grpc;

/**
 * One RPC method: its full name ({@code package.Service/Method}), the shape of the call, and how to marshal its messages.
 */
public record MethodDescriptor<Req, Resp>(String fullName, MethodType type, Marshaller<Req> requestMarshaller, Marshaller<Resp> responseMarshaller) {

    public enum MethodType {
        UNARY, CLIENT_STREAMING, SERVER_STREAMING, BIDI_STREAMING;

        public boolean clientStreams() {
            return this == CLIENT_STREAMING || this == BIDI_STREAMING;
        }

        public boolean serverStreams() {
            return this == SERVER_STREAMING || this == BIDI_STREAMING;
        }
    }

    public MethodDescriptor {
        int slash = fullName.indexOf('/');
        if (slash <= 0 || slash == fullName.length() - 1 || fullName.indexOf('/', slash + 1) >= 0) {
            throw new IllegalArgumentException("a method's full name is package.Service/Method: " + fullName);
        }
    }

    public static <Req, Resp> MethodDescriptor<Req, Resp> of(String service, String method, MethodType type, Marshaller<Req> request,
                                                             Marshaller<Resp> response) {
        return new MethodDescriptor<>(service + "/" + method, type, request, response);
    }

    public String serviceName() {
        return fullName.substring(0, fullName.indexOf('/'));
    }

    public String methodName() {
        return fullName.substring(fullName.indexOf('/') + 1);
    }

    /** The HTTP/2 {@code :path}. */
    public String path() {
        return "/" + fullName;
    }
}
