package io.hydrangea.otlp;

import io.hydrangea.grpc.proto.ProtoReader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Reads back what {@link OtlpEncoder} wrote, field numbers from the same .proto files, so the tests check the wire format itself. */
final class OtlpDecoder {

    record Decoded(Map<String, Object> resource, String scopeName, List<DecodedSpan> spans) {
    }

    record DecodedSpan(String traceId, String spanId, String parentSpanId, String name, int kind, long start, long end,
                       Map<String, Object> attributes, int statusCode, String statusMessage, List<Map<String, Object>> events, int flags) {
    }

    private OtlpDecoder() {
    }

    static Decoded decode(byte[] request) {
        Map<String, Object> resource = new LinkedHashMap<>();
        String scopeName = null;
        List<DecodedSpan> spans = new ArrayList<>();
        ProtoReader r = new ProtoReader(request);
        while (r.next()) {
            if (r.field() != 1) {
                r.skip();
                continue;
            }
            ProtoReader rs = r.message();
            while (rs.next()) {
                switch (rs.field()) {
                    case 1 -> {
                        ProtoReader res = rs.message();
                        while (res.next()) {
                            if (res.field() == 1) {
                                keyValue(res.message(), resource);
                            } else {
                                res.skip();
                            }
                        }
                    }
                    case 2 -> {
                        ProtoReader ss = rs.message();
                        while (ss.next()) {
                            switch (ss.field()) {
                                case 1 -> {
                                    ProtoReader scope = ss.message();
                                    while (scope.next()) {
                                        if (scope.field() == 1) {
                                            scopeName = scope.string();
                                        } else {
                                            scope.skip();
                                        }
                                    }
                                }
                                case 2 -> spans.add(span(ss.message()));
                                default -> ss.skip();
                            }
                        }
                    }
                    default -> rs.skip();
                }
            }
        }
        return new Decoded(resource, scopeName, spans);
    }

    private static DecodedSpan span(ProtoReader s) {
        String traceId = null, spanId = null, parent = null, name = null, statusMessage = null;
        int kind = 0, statusCode = 0, flags = 0;
        long start = 0, end = 0;
        Map<String, Object> attributes = new LinkedHashMap<>();
        List<Map<String, Object>> events = new ArrayList<>();
        while (s.next()) {
            switch (s.field()) {
                case 1 -> traceId = hex(s.bytes());
                case 2 -> spanId = hex(s.bytes());
                case 4 -> parent = hex(s.bytes());
                case 5 -> name = s.string();
                case 6 -> kind = (int) s.varint();
                case 7 -> start = s.fixed64();
                case 8 -> end = s.fixed64();
                case 9 -> keyValue(s.message(), attributes);
                case 11 -> {
                    Map<String, Object> event = new LinkedHashMap<>();
                    ProtoReader e = s.message();
                    while (e.next()) {
                        switch (e.field()) {
                            case 1 -> event.put("time", e.fixed64());
                            case 2 -> event.put("name", e.string());
                            case 3 -> keyValue(e.message(), event);
                            default -> e.skip();
                        }
                    }
                    events.add(event);
                }
                case 15 -> {
                    ProtoReader st = s.message();
                    while (st.next()) {
                        switch (st.field()) {
                            case 2 -> statusMessage = st.string();
                            case 3 -> statusCode = (int) st.varint();
                            default -> st.skip();
                        }
                    }
                }
                case 16 -> flags = s.fixed32();
                default -> s.skip();
            }
        }
        return new DecodedSpan(traceId, spanId, parent, name, kind, start, end, attributes, statusCode, statusMessage, events, flags);
    }

    private static void keyValue(ProtoReader kv, Map<String, Object> into) {
        String key = null;
        Object value = null;
        while (kv.next()) {
            switch (kv.field()) {
                case 1 -> key = kv.string();
                case 2 -> {
                    ProtoReader any = kv.message();
                    while (any.next()) {
                        switch (any.field()) {
                            case 1 -> value = any.string();
                            case 2 -> value = any.bool();
                            case 3 -> value = any.varint();
                            case 4 -> value = any.doubleValue();
                            default -> any.skip();
                        }
                    }
                }
                default -> kv.skip();
            }
        }
        into.put(key, value);
    }

    static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder(b.length * 2);
        for (byte x : b) {
            sb.append(Character.forDigit((x >> 4) & 0xf, 16)).append(Character.forDigit(x & 0xf, 16));
        }
        return sb.toString();
    }
}
