package io.hydrangea.grpc;

import io.hydrangea.fuzz.Fuzz;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hostile bytes into the parsers that read data from a peer or a control plane: they may reject it, never crash, hang or balloon. */
class DecoderFuzzTest {

    private static final Set<Class<? extends Throwable>> BAD_INPUT = Set.of(StatusException.class, IllegalArgumentException.class);

    private static void expectNoFindings(List<Fuzz.Finding> findings) {
        assertTrue(findings.isEmpty(), () -> findings.size() + " finding(s):\n" + String.join("\n", findings.stream().map(Fuzz.Finding::toString).toList()));
    }

    private static byte[] gzip(byte[] b) throws Exception {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        try (GZIPOutputStream g = new GZIPOutputStream(o)) {
            g.write(b);
        }
        return o.toByteArray();
    }

    private static byte[] frame(int flag, byte[] payload) {
        return ByteBuffer.allocate(5 + payload.length).put((byte) flag).putInt(payload.length).put(payload).array();
    }

    @Test
    void grpcMessageFraming() throws Exception {
        List<byte[]> corpus = List.of(frame(0, "hello".getBytes()), frame(1, gzip("hello hello hello".getBytes())), frame(0, new byte[0]),
                new byte[]{0, 0, 0, 0, 5, 1, 2});
        for (boolean gzipOk : new boolean[]{false, true}) {
            expectNoFindings(Fuzz.run("GrpcFraming.Decoder gzip=" + gzipOk, corpus, in -> {
                GrpcFraming.Decoder d = new GrpcFraming.Decoder(1 << 20, gzipOk);
                Random r = new Random(in.length);
                int at = 0;
                while (at < in.length) {   // fed in random pieces, as the network delivers it
                    int n = Math.min(in.length - at, 1 + r.nextInt(12));
                    d.feed(ByteBuffer.wrap(in, at, n), m -> { });
                    at += n;
                }
                d.finish();
            }, BAD_INPUT, 2_000));
        }
    }

    @Test
    void aGzipBombInAMessageIsStoppedByTheSizeLimit() throws Exception {
        byte[] bomb = gzip(new byte[200 * 1024 * 1024]);   // 200 MiB of zeros, a few hundred KiB compressed
        GrpcFraming.Decoder d = new GrpcFraming.Decoder(4 * 1024 * 1024, true);
        long t0 = System.nanoTime();
        try {
            d.feed(ByteBuffer.wrap(frame(1, bomb)), m -> assertTrue(m.length <= 4 * 1024 * 1024, "decompressed message of " + m.length + " bytes"));
        } catch (StatusException expected) {
            // rejected: fine
        }
        assertTrue((System.nanoTime() - t0) / 1_000_000 < 3_000, "the bomb took too long to refuse");
    }

    @Test
    void jsonAndServiceConfig() {
        List<byte[]> corpus = new ArrayList<>();
        for (String s : new String[]{
                "{}", "[1,2,{\"a\":[true,false,null]}]", "\"\\u00e9\\n\"", "-1.5e10", "{\"a\":{\"b\":{\"c\":[[[[]]]]}}}",
                "{\"loadBalancingConfig\":[{\"round_robin\":{}}],\"methodConfig\":[{\"name\":[{\"service\":\"pkg.S\",\"method\":\"M\"}],\"timeout\":\"2s\","
                        + "\"maxRequestMessageBytes\":1000,\"retryPolicy\":{\"maxAttempts\":3,\"initialBackoff\":\"0.1s\",\"maxBackoff\":\"1s\","
                        + "\"backoffMultiplier\":2,\"retryableStatusCodes\":[\"UNAVAILABLE\"]}}],\"retryThrottling\":{\"maxTokens\":10,\"tokenRatio\":0.1}}",
                "{\"methodConfig\":[{\"name\":[{}],\"hedgingPolicy\":{\"maxAttempts\":2,\"hedgingDelay\":\"0.5s\",\"nonFatalStatusCodes\":[\"UNAVAILABLE\"]}}]}"}) {
            corpus.add(s.getBytes(StandardCharsets.UTF_8));
        }
        expectNoFindings(Fuzz.run("Json.parse", corpus, in -> Json.parse(new String(in, StandardCharsets.UTF_8)), Set.of(IllegalArgumentException.class), 1_000));
        expectNoFindings(Fuzz.run("ServiceConfig.parse", corpus, in -> ServiceConfig.parse(new String(in, StandardCharsets.UTF_8)),
                Set.of(IllegalArgumentException.class), 1_000));
        expectNoFindings(Fuzz.run("XdsBootstrap.parse", corpus, in -> XdsBootstrap.parse(new String(in, StandardCharsets.UTF_8)),
                Set.of(IllegalArgumentException.class), 1_000));
    }

    @Test
    void xdsResourcesFromAControlPlane() {
        // a few well-formed protobuf messages as seeds: field 1 strings, nested messages, varints
        List<byte[]> corpus = List.of(
                new byte[]{0x0a, 0x03, 'f', 'o', 'o'},
                new byte[]{0x0a, 0x03, 'f', 'o', 'o', 0x12, 0x05, 0x0a, 0x03, 'b', 'a', 'r'},
                new byte[]{0x0a, 0x01, 'c', 0x12, 0x0e, 0x0a, 0x0c, 0x0a, 0x0a, 0x0a, 0x08, 0x08, 0x01, 0x12, 0x04, 0x0a, 0x02, 0x08, 0x50},
                new byte[]{0x08, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x0f});
        Set<Class<? extends Throwable>> ok = Set.of(XdsDecoder.Invalid.class, IllegalArgumentException.class);
        expectNoFindings(Fuzz.run("XdsDecoder.listener", corpus, XdsDecoder::listener, ok, 1_000));
        expectNoFindings(Fuzz.run("XdsDecoder.routeConfig", corpus, XdsDecoder::routeConfig, ok, 1_000));
        expectNoFindings(Fuzz.run("XdsDecoder.cluster", corpus, XdsDecoder::cluster, ok, 1_000));
        expectNoFindings(Fuzz.run("XdsDecoder.clusterLoadAssignment", corpus, XdsDecoder::clusterLoadAssignment, ok, 1_000));
    }

    @Test
    void grpcWebTextBodies() {
        List<byte[]> corpus = List.of("AAAAAAU=".getBytes(), "AAAAAAUAAAAA".getBytes(), "gAAAABBncnBjLXN0YXR1czogMA0K".getBytes());
        expectNoFindings(Fuzz.run("GrpcWeb.decodeText", corpus, GrpcWeb::decodeText, Set.of(IllegalArgumentException.class, StatusException.class), 1_000));
    }
}
