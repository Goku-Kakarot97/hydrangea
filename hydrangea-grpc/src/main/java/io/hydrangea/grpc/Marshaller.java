package io.hydrangea.grpc;

/**
 * Turns messages into bytes and back; the bytes of one gRPC message. With protobuf-java: {@code Marshaller.of(Msg::toByteArray, Msg::parseFrom)}.
 * Without it, {@link io.hydrangea.grpc.proto.ProtoWriter} and {@link io.hydrangea.grpc.proto.ProtoReader} handle the wire format by hand.
 */
public interface Marshaller<T> {

    /** The message as bytes. The array is sent as it is, without a copy (large messages): it must not be changed afterwards. */
    byte[] encode(T value) throws Exception;

    /** @throws Exception if the bytes are not a valid message; the call then fails with INTERNAL */
    T decode(byte[] bytes) throws Exception;

    @FunctionalInterface
    interface Encoder<T> {
        byte[] encode(T value) throws Exception;
    }

    @FunctionalInterface
    interface Decoder<T> {
        T decode(byte[] bytes) throws Exception;
    }

    static <T> Marshaller<T> of(Encoder<T> encoder, Decoder<T> decoder) {
        return new Marshaller<>() {
            @Override
            public byte[] encode(T value) throws Exception {
                return encoder.encode(value);
            }

            @Override
            public T decode(byte[] bytes) throws Exception {
                return decoder.decode(bytes);
            }
        };
    }

    /** Messages that are their own bytes. */
    static Marshaller<byte[]> bytes() {
        return of(b -> b, b -> b);
    }
}
