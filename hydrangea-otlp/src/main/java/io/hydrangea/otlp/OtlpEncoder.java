package io.hydrangea.otlp;

import io.hydrangea.grpc.proto.ProtoWriter;
import io.hydrangea.trace.Span;

import java.util.List;
import java.util.Map;

/**
 * Writes an OTLP {@code ExportTraceServiceRequest} (opentelemetry-proto, trace v1) by hand with {@link ProtoWriter}. The field numbers
 * are those of the {@code .proto} files; the protocol is stable, so they do not change.
 * <pre>
 * ExportTraceServiceRequest { repeated ResourceSpans resource_spans = 1; }
 * ResourceSpans   { Resource resource = 1; repeated ScopeSpans scope_spans = 2; }
 * Resource        { repeated KeyValue attributes = 1; }
 * ScopeSpans      { InstrumentationScope scope = 1; repeated Span spans = 2; }
 * Span            { bytes trace_id = 1; bytes span_id = 2; bytes parent_span_id = 4; string name = 5; SpanKind kind = 6;
 *                   fixed64 start_time_unix_nano = 7; fixed64 end_time_unix_nano = 8; repeated KeyValue attributes = 9;
 *                   repeated Event events = 11; Status status = 15; fixed32 flags = 16; }
 * </pre>
 */
final class OtlpEncoder {

    static final String SCOPE_NAME = "io.hydrangea";
    private static final int FLAGS_SAMPLED = 0x01;

    private OtlpEncoder() {
    }

    /** The {@code Resource} message: the attributes every span of this process shares ({@code service.name} ...). Encoded once. */
    static byte[] resource(Map<String, String> attributes) {
        ProtoWriter w = new ProtoWriter();
        for (Map.Entry<String, String> e : attributes.entrySet()) {
            w.message(1, keyValue(e.getKey(), e.getValue()));
        }
        return w.toByteArray();
    }

    /** One request carrying {@code spans} under {@code resource}. */
    static byte[] request(byte[] resource, List<OtlpSpan> spans) {
        ProtoWriter scope = new ProtoWriter().message(1, new ProtoWriter().string(1, SCOPE_NAME).toByteArray());
        for (OtlpSpan s : spans) {
            scope.message(2, span(s));
        }
        byte[] resourceSpans = new ProtoWriter().message(1, resource).message(2, scope.toByteArray()).toByteArray();
        return new ProtoWriter().message(1, resourceSpans).toByteArray();
    }

    static byte[] span(OtlpSpan s) {
        ProtoWriter w = new ProtoWriter()
                .bytes(1, hex(s.context().traceId()))
                .bytes(2, hex(s.context().spanId()));
        if (s.parentSpanId() != null) {
            w.bytes(4, hex(s.parentSpanId()));
        }
        w.string(5, s.name())
                .varint(6, kind(s.kind()))
                .fixed64(7, s.startEpochNanos())
                .fixed64(8, s.endEpochNanos());
        for (OtlpSpan.Attribute a : s.attributes()) {
            w.message(9, keyValue(a.key(), a.value()));
        }
        Throwable exception = s.exception();
        if (exception != null) {
            w.message(11, exceptionEvent(exception, s.endEpochNanos()));
        }
        if (s.failed()) {
            ProtoWriter status = new ProtoWriter();
            if (s.errorDescription() != null) {
                status.string(2, s.errorDescription());
            }
            w.message(15, status.varint(3, 2).toByteArray());   // StatusCode.STATUS_CODE_ERROR
        }
        return w.fixed32(16, FLAGS_SAMPLED).toByteArray();
    }

    /** The semantic-conventions {@code exception} event: type, message, stack trace. */
    private static byte[] exceptionEvent(Throwable t, long timeNanos) {
        ProtoWriter w = new ProtoWriter().fixed64(1, timeNanos).string(2, "exception")
                .message(3, keyValue("exception.type", t.getClass().getName()));
        if (t.getMessage() != null) {
            w.message(3, keyValue("exception.message", t.getMessage()));
        }
        return w.message(3, keyValue("exception.stacktrace", OtlpSpan.stackTrace(t))).toByteArray();
    }

    /** {@code KeyValue { string key = 1; AnyValue value = 2; }} with {@code AnyValue { string 1 | bool 2 | int 3 | double 4 }}. */
    static byte[] keyValue(String key, Object value) {
        ProtoWriter any = new ProtoWriter();
        if (value instanceof Boolean b) {
            any.bool(2, b);
        } else if (value instanceof Long || value instanceof Integer) {
            any.varint(3, ((Number) value).longValue());
        } else if (value instanceof Double || value instanceof Float) {
            any.doubleValue(4, ((Number) value).doubleValue());
        } else {
            any.string(1, String.valueOf(value));
        }
        return new ProtoWriter().string(1, key).message(2, any.toByteArray()).toByteArray();
    }

    /** {@code SpanKind}: UNSPECIFIED 0, INTERNAL 1, SERVER 2, CLIENT 3, PRODUCER 4, CONSUMER 5. */
    static int kind(Span.Kind kind) {
        return switch (kind) {
            case INTERNAL -> 1;
            case SERVER -> 2;
            case CLIENT -> 3;
            case PRODUCER -> 4;
            case CONSUMER -> 5;
        };
    }

    static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) ((Character.digit(s.charAt(2 * i), 16) << 4) | Character.digit(s.charAt(2 * i + 1), 16));
        }
        return out;
    }
}
