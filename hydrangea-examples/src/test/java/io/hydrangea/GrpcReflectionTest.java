package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.grpc.*;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class GrpcReflectionTest {

    private EventLoopGroup group;
    private Server server;
    private GrpcChannel channel;

    // The wire messages of grpc.reflection.v1, written out by hand so the test does not share code with the server.
    private static final Marshaller<byte[]> RAW = Marshaller.of(b -> b, b -> b);
    private static final MethodDescriptor<byte[], byte[]> INFO = MethodDescriptor.of("grpc.reflection.v1.ServerReflection", "ServerReflectionInfo",
            MethodDescriptor.MethodType.BIDI_STREAMING, RAW, RAW);
    private static final MethodDescriptor<byte[], byte[]> INFO_ALPHA = MethodDescriptor.of("grpc.reflection.v1alpha.ServerReflection", "ServerReflectionInfo",
            MethodDescriptor.MethodType.BIDI_STREAMING, RAW, RAW);

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        byte[] descriptors = getClass().getResourceAsStream("/echo.pb").readAllBytes();
        server = GrpcEchoExample.builder().addService(new HealthService()).addService(new ReflectionService().addDescriptorSet(descriptors)).build()
                .bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        channel = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).build();
    }

    @AfterEach
    void tearDown() {
        channel.close();
        group.close();
    }

    private List<byte[]> ask(MethodDescriptor<byte[], byte[]> method, byte[]... requests) throws Exception {
        List<byte[]> replies = new ArrayList<>();
        CompletableFuture<Void> done = new CompletableFuture<>();
        ClientCall<byte[], byte[]> call = channel.bidiStreaming(method, CallOptions.DEFAULT, new StreamObserver<>() {
            public void onNext(byte[] m) { replies.add(m); }
            public void onError(Throwable t) { done.completeExceptionally(t); }
            public void onCompleted() { done.complete(null); }
        });
        for (byte[] r : requests) call.send(r);
        call.halfClose();
        done.get(10, TimeUnit.SECONDS);
        return replies;
    }

    private static byte[] listServices() {
        return new ProtoWriter().string(7, "*").toByteArray();
    }

    /** The oneof member of a ServerReflectionResponse: its field number and bytes. */
    private record Member(int field, byte[] bytes, byte[] original, String host) { }

    private static Member parse(byte[] response) {
        ProtoReader r = new ProtoReader(response);
        int field = 0;
        byte[] bytes = null, original = null;
        String host = "";
        while (r.next()) {
            switch (r.field()) {
                case 1 -> host = r.string();
                case 2 -> original = r.bytes();
                case 4, 5, 6, 7 -> { field = r.field(); bytes = r.bytes(); }
                default -> r.skip();
            }
        }
        return new Member(field, bytes, original, host);
    }

    private static List<String> serviceNames(Member m) {
        List<String> names = new ArrayList<>();
        ProtoReader r = new ProtoReader(m.bytes());
        while (r.next()) {
            ProtoReader svc = r.message();
            while (svc.next()) {
                if (svc.field() == 1) names.add(svc.string()); else svc.skip();
            }
        }
        return names;
    }

    @Test
    void listsEveryRegisteredService() throws Exception {
        Member m = parse(ask(INFO, listServices()).get(0));
        assertEquals(6, m.field());
        assertEquals(List.of("grpc.health.v1.Health", "grpc.reflection.v1.ServerReflection", "grpc.reflection.v1alpha.ServerReflection", "hydrangea.test.Echo"), serviceNames(m));
    }

    @Test
    void theOlderAlphaServiceAnswersToo() throws Exception {
        assertEquals(6, parse(ask(INFO_ALPHA, listServices()).get(0)).field());
    }

    @Test
    void theResponseEchoesTheRequestAndAnswersEachInOrder() throws Exception {
        byte[] list = new ProtoWriter().string(1, "myhost").string(7, "*").toByteArray();
        byte[] byName = new ProtoWriter().string(3, "no_such_file.proto").toByteArray();
        List<byte[]> replies = ask(INFO, list, byName, list);
        assertEquals(3, replies.size());
        Member first = parse(replies.get(0));
        assertArrayEquals(list, first.original());
        assertEquals("myhost", first.host());
        assertEquals(7, parse(replies.get(1)).field(), "the second request failed with an error response, not a failed call");
        assertEquals(6, parse(replies.get(2)).field());
    }

    @Test
    void describesASymbolWithItsFileDescriptor() throws Exception {
        Member m = parse(ask(INFO, new ProtoWriter().string(4, "hydrangea.test.Echo").toByteArray()).get(0));
        assertEquals(4, m.field());
        ProtoReader fds = new ProtoReader(m.bytes());
        assertTrue(fds.next());
        byte[] file = fds.bytes();
        ProtoReader f = new ProtoReader(file);
        String pkg = null;
        boolean hasService = false;
        while (f.next()) {
            if (f.field() == 2) pkg = f.string(); else if (f.field() == 6) { hasService = true; f.skip(); } else f.skip();
        }
        assertEquals("hydrangea.test", pkg);
        assertTrue(hasService);
        // the same file is found through a message, a method and by file name
        for (String symbol : new String[]{"hydrangea.test.EchoRequest", "hydrangea.test.Echo.Unary"}) {
            assertEquals(4, parse(ask(INFO, new ProtoWriter().string(4, symbol).toByteArray()).get(0)).field(), symbol);
        }
        assertEquals(4, parse(ask(INFO, new ProtoWriter().string(3, "hydrangea_test.proto").toByteArray()).get(0)).field());
    }

    @Test
    void unknownSymbolsAreAnErrorResponseWithNotFound() throws Exception {
        Member m = parse(ask(INFO, new ProtoWriter().string(4, "no.such.Thing").toByteArray()).get(0));
        assertEquals(7, m.field());
        ProtoReader e = new ProtoReader(m.bytes());
        int code = -1;
        String message = "";
        while (e.next()) {
            if (e.field() == 1) code = (int) e.varint(); else if (e.field() == 2) message = e.string(); else e.skip();
        }
        assertEquals(5, code);
        assertTrue(message.contains("no.such.Thing"));
    }

    @Test
    void aFileIsFollowedByTheFilesItImports() throws Exception {
        byte[] a = new ProtoWriter().string(1, "a.proto").string(2, "pa").message(4, new ProtoWriter().string(1, "A").toByteArray()).toByteArray();
        byte[] c = new ProtoWriter().string(1, "c.proto").string(2, "pc").message(4, new ProtoWriter().string(1, "C").toByteArray()).toByteArray();
        byte[] b = new ProtoWriter().string(1, "b.proto").string(2, "pb").string(3, "a.proto").string(3, "c.proto")
                .message(4, new ProtoWriter().string(1, "B").message(3, new ProtoWriter().string(1, "Inner").toByteArray()).toByteArray()).toByteArray();
        byte[] set = new ProtoWriter().message(1, a).message(1, b).message(1, c).toByteArray();
        try (GrpcChannel ch = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) server.localAddress()).getPort()).build()) {
            // use a separate server so the descriptor set is exactly the three files
            Server s2 = GrpcServer.builder().addService(new ReflectionService().addDescriptorSet(set)).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
            GrpcChannel c2 = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) s2.localAddress()).getPort()).build();
            List<byte[]> replies = new ArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            ClientCall<byte[], byte[]> call = c2.bidiStreaming(INFO, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(byte[] m) { replies.add(m); }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            call.send(new ProtoWriter().string(4, "pb.B.Inner").toByteArray());
            call.halfClose();
            done.get(10, TimeUnit.SECONDS);
            Member m = parse(replies.get(0));
            assertEquals(4, m.field());
            List<String> names = new ArrayList<>();
            ProtoReader r = new ProtoReader(m.bytes());
            while (r.next()) {
                ProtoReader f = new ProtoReader(r.bytes());
                while (f.next()) {
                    if (f.field() == 1) names.add(f.string()); else f.skip();
                }
            }
            assertEquals(List.of("b.proto", "a.proto", "c.proto"), names, "the file, then its imports, each once");
            c2.close();
            s2.close();
        }
    }

    @Test
    void extensionsAreFoundByExtendeeAndNumber() throws Exception {
        byte[] base = new ProtoWriter().string(1, "base.proto").string(2, "pb").message(4, new ProtoWriter().string(1, "Base").toByteArray()).toByteArray();
        byte[] topLevel = new ProtoWriter().string(1, "tag").string(2, ".pb.Base").varint(3, 100).toByteArray();
        byte[] inMessage = new ProtoWriter().string(1, "note").string(2, ".pb.Base").varint(3, 101).toByteArray();
        byte[] ext = new ProtoWriter().string(1, "ext.proto").string(2, "pe").string(3, "base.proto")
                .message(4, new ProtoWriter().string(1, "Holder").message(6, inMessage).toByteArray())
                .message(7, topLevel).toByteArray();
        byte[] set = new ProtoWriter().message(1, base).message(1, ext).toByteArray();
        Server s2 = GrpcServer.builder().addService(new ReflectionService().addDescriptorSet(set)).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        try (GrpcChannel c2 = GrpcChannel.forTarget("http://127.0.0.1:" + ((InetSocketAddress) s2.localAddress()).getPort()).build()) {
            byte[] byNumber = new ProtoWriter().message(5, new ProtoWriter().string(1, "pb.Base").varint(2, 101).toByteArray()).toByteArray();
            byte[] missing = new ProtoWriter().message(5, new ProtoWriter().string(1, "pb.Base").varint(2, 999).toByteArray()).toByteArray();
            byte[] all = new ProtoWriter().string(6, "pb.Base").toByteArray();
            byte[] symbol = new ProtoWriter().string(4, "pe.tag").toByteArray();
            List<byte[]> replies = new ArrayList<>();
            CompletableFuture<Void> done = new CompletableFuture<>();
            ClientCall<byte[], byte[]> call = c2.bidiStreaming(INFO, CallOptions.DEFAULT, new StreamObserver<>() {
                public void onNext(byte[] m) { replies.add(m); }
                public void onError(Throwable t) { done.completeExceptionally(t); }
                public void onCompleted() { done.complete(null); }
            });
            for (byte[] q : new byte[][]{byNumber, missing, all, symbol}) call.send(q);
            call.halfClose();
            done.get(10, TimeUnit.SECONDS);

            Member found = parse(replies.get(0));
            assertEquals(4, found.field(), "the file declaring pb.Base extension 101 (inside a message) is returned");
            ProtoReader fds = new ProtoReader(found.bytes());
            assertTrue(fds.next());
            assertEquals("ext.proto", firstString(fds.bytes(), 1));

            assertEquals(7, parse(replies.get(1)).field(), "an unknown extension number is an error response");

            Member numbers = parse(replies.get(2));
            assertEquals(5, numbers.field());
            ProtoReader nr = new ProtoReader(numbers.bytes());
            List<Long> got = new ArrayList<>();
            String base2 = "";
            while (nr.next()) {
                if (nr.field() == 1) base2 = nr.string(); else if (nr.field() == 2) got.add(nr.varint()); else nr.skip();
            }
            assertEquals("pb.Base", base2);
            assertEquals(List.of(100L, 101L), got);

            assertEquals(4, parse(replies.get(3)).field(), "an extension's own name is a symbol too");
        } finally {
            s2.close();
        }
    }

    private static String firstString(byte[] message, int field) {
        ProtoReader r = new ProtoReader(message);
        while (r.next()) {
            if (r.field() == field) return r.string();
            r.skip();
        }
        return null;
    }

    @Test
    void aGarbageDescriptorSetIsRejectedWhenAdded() {
        assertThrows(IllegalArgumentException.class, () -> new ReflectionService().addDescriptorSet(new byte[]{0x0a, 0x7f, 1, 2}));
    }
}
