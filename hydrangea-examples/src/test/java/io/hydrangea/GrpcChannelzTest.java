package io.hydrangea;

import io.hydrangea.examples.GrpcEchoExample;
import io.hydrangea.examples.GrpcEchoExample.Msg;
import io.hydrangea.grpc.*;
import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.hydrangea.examples.GrpcEchoExample.UNARY;
import static org.junit.jupiter.api.Assertions.*;

/** Channelz, read with hand-written decoders that use the field numbers of channelz.proto (also checked against grpc-java's stubs by interop/). */
class GrpcChannelzTest {

    private static final Marshaller<byte[]> RAW = Marshaller.of(b -> b, b -> b);
    private EventLoopGroup group;
    private Server server;
    private GrpcChannel client;     // reads channelz
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        group = new EventLoopGroup(2);
        server = GrpcEchoExample.builder().addService(new ChannelzService()).build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        port = ((InetSocketAddress) server.localAddress()).getPort();
        client = GrpcChannel.forTarget("http://127.0.0.1:" + port).build();
    }

    @AfterEach
    void tearDown() {
        client.close();
        group.close();
    }

    private byte[] call(String method, byte[] request) throws Exception {
        MethodDescriptor<byte[], byte[]> m = MethodDescriptor.of("grpc.channelz.v1.Channelz", method, MethodDescriptor.MethodType.UNARY, RAW, RAW);
        return client.unary(m, request, CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
    }

    private static List<byte[]> repeated(byte[] msg, int field) {
        List<byte[]> out = new ArrayList<>();
        ProtoReader r = new ProtoReader(msg);
        while (r.next()) {
            if (r.field() == field) out.add(r.bytes()); else r.skip();
        }
        return out;
    }

    private static long varint(byte[] msg, int field) {
        ProtoReader r = new ProtoReader(msg);
        while (r.next()) {
            if (r.field() == field) return r.varint(); else r.skip();
        }
        return 0;
    }

    private static byte[] sub(byte[] msg, int field) {
        List<byte[]> l = repeated(msg, field);
        return l.isEmpty() ? null : l.get(0);
    }

    private static String string(byte[] msg, int field) {
        ProtoReader r = new ProtoReader(msg);
        while (r.next()) {
            if (r.field() == field) return r.string(); else r.skip();
        }
        return null;
    }

    private byte[] myServer() throws Exception {
        for (byte[] s : repeated(call("GetServers", new byte[0]), 1)) {
            if (String.valueOf(string(sub(s, 1), 6)).endsWith(":" + port)) return s;
        }
        fail("this test's server is not listed");
        return null;
    }

    private byte[] myChannel() throws Exception {
        for (byte[] c : repeated(call("GetTopChannels", new byte[0]), 1)) {
            if (String.valueOf(string(sub(c, 1), 2)).endsWith(":" + port)) return c;
        }
        fail("this test's channel is not listed");
        return null;
    }

    @Test
    void theServerCountsStartedSucceededAndFailedCalls() throws Exception {
        for (int i = 0; i < 4; i++) client.unary(UNARY, new Msg("ok"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class, () -> client.unary(UNARY, new Msg("FAIL:5:x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
        byte[] data = sub(myServer(), 2);
        assertEquals(4, varint(data, 3), "succeeded");
        assertEquals(1, varint(data, 4), "failed");
        assertTrue(varint(data, 2) >= 5, "started (the GetServers call itself may still be running)");
        assertTrue(varint(sub(data, 5), 1) > 1_700_000_000L, "last call started timestamp");
    }

    @Test
    void theClientChannelCountsItsCallsAndReportsItsSubchannel() throws Exception {
        for (int i = 0; i < 3; i++) client.unary(UNARY, new Msg("ok"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
        byte[] ch = myChannel();
        byte[] data = sub(ch, 2);
        assertEquals(3, varint(data, 5), "three echoes succeeded; the GetTopChannels call that reads this is still running");
        assertEquals(0, varint(data, 6));
        assertEquals(3, varint(sub(data, 1), 1), "state READY = 3");
        List<byte[]> subs = repeated(ch, 4);
        assertEquals(1, subs.size());
        long subId = varint(subs.get(0), 7);
        byte[] subchannel = sub(call("GetSubchannel", new ProtoWriter().varint(1, subId).toByteArray()), 1);
        assertTrue(varint(sub(subchannel, 2), 4) >= 3, "attempts counted on the subchannel");
        assertTrue(varint(sub(subchannel, 2), 5) >= 3, "and the finished ones as succeeded");
        assertEquals(0, varint(sub(subchannel, 2), 6));
        byte[] socketRef = sub(subchannel, 5);
        byte[] socket = sub(call("GetSocket", new ProtoWriter().varint(1, varint(socketRef, 3)).toByteArray()), 1);
        assertTrue(varint(sub(socket, 2), 1) >= 3, "streams started on the socket");
        assertNotNull(sub(sub(socket, 4), 1), "remote address is a TCP/IP address");
        assertEquals(port, varint(sub(sub(socket, 4), 1), 2));
    }

    @Test
    void serverSocketsListTheConnectionsThatCarriedCalls() throws Exception {
        client.unary(UNARY, new Msg("ok"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
        long serverId = varint(sub(myServer(), 1), 5);
        byte[] resp = call("GetServerSockets", new ProtoWriter().varint(1, serverId).toByteArray());
        List<byte[]> refs = repeated(resp, 1);
        assertEquals(1, refs.size(), "one client connection");
        assertEquals(1, varint(resp, 2), "end = true");
        byte[] socket = sub(call("GetSocket", new ProtoWriter().varint(1, varint(refs.get(0), 3)).toByteArray()), 1);
        assertTrue(varint(sub(socket, 2), 1) >= 2);
        assertTrue(varint(sub(socket, 2), 5) >= 1, "messages received");
    }

    @Test
    void aChannelToADeadAddressIsInTransientFailure() throws Exception {
        int dead;
        try (ServerSocket tmp = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            dead = tmp.getLocalPort();
        }
        try (GrpcChannel bad = GrpcChannel.forTarget("http://127.0.0.1:" + dead).build()) {
            assertThrows(ExecutionException.class, () -> bad.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS));
            byte[] found = null;
            for (byte[] c : repeated(call("GetTopChannels", new byte[0]), 1)) {
                if (String.valueOf(string(sub(c, 1), 2)).endsWith(":" + dead)) found = c;
            }
            assertNotNull(found);
            assertEquals(4, varint(sub(sub(found, 2), 1), 1), "TRANSIENT_FAILURE = 4");
            assertEquals(1, varint(sub(found, 2), 6), "the call failed");
        }
    }

    @Test
    void closedChannelsAndStoppedServersDisappear() throws Exception {
        GrpcChannel temp = GrpcChannel.forTarget("http://127.0.0.1:" + port + "/").build();
        temp.unary(UNARY, new Msg("x"), CallOptions.DEFAULT).get(10, TimeUnit.SECONDS);
        long id = -1;
        for (byte[] c : repeated(call("GetTopChannels", new byte[0]), 1)) {
            if (c != null && varint(sub(c, 2), 5) == 1) id = varint(sub(c, 1), 1);
        }
        assertTrue(id > 0);
        byte[] ok = call("GetChannel", new ProtoWriter().varint(1, id).toByteArray());
        assertNotNull(sub(ok, 1));
        temp.close();
        ExecutionException e = assertThrows(ExecutionException.class, () -> call("GetChannel", new ProtoWriter().varint(1, varintOf(ok)).toByteArray()));
        assertEquals(Status.Code.NOT_FOUND, ((StatusException) e.getCause()).status().code());

        Server extra = GrpcEchoExample.builder().build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        int extraPort = ((InetSocketAddress) extra.localAddress()).getPort();
        assertTrue(serversListing().contains(":" + extraPort));
        extra.close();
        assertFalse(serversListing().contains(":" + extraPort), "a stopped server is no longer listed");
    }

    private static long varintOf(byte[] getChannelResponse) {
        return varint(sub(sub(getChannelResponse, 1), 1), 1);
    }

    private String serversListing() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (byte[] s : repeated(call("GetServers", new byte[0]), 1)) sb.append(string(sub(s, 1), 6)).append(' ');
        return sb.toString();
    }

    @Test
    void paginationAndUnknownIdsFollowTheProtocol() throws Exception {
        // start past every id: empty and finished
        byte[] none = call("GetServers", new ProtoWriter().varint(1, 1_000_000_000L).toByteArray());
        assertEquals(0, repeated(none, 1).size());
        assertEquals(1, varint(none, 2));
        // a page of one: not finished if more exist
        Server second = GrpcEchoExample.builder().build().bind(group, new InetSocketAddress("127.0.0.1", 0), null);
        byte[] page = call("GetServers", new ProtoWriter().varint(2, 1).toByteArray());
        assertEquals(1, repeated(page, 1).size());
        assertEquals(0, varint(page, 2), "more servers follow");
        second.close();
        for (String m : new String[]{"GetServer", "GetChannel", "GetSubchannel", "GetSocket"}) {
            ExecutionException e = assertThrows(ExecutionException.class, () -> call(m, new ProtoWriter().varint(1, 987654321L).toByteArray()), m);
            assertEquals(Status.Code.NOT_FOUND, ((StatusException) e.getCause()).status().code(), m);
        }
    }
}
