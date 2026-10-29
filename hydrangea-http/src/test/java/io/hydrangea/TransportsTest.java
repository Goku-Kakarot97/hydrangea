package io.hydrangea;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.MembershipKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class TransportsTest {

    private EventLoopGroup group;

    @BeforeEach
    void setUp() {
        group = new EventLoopGroup(2);
    }

    @AfterEach
    void tearDown() {
        group.close();
    }

    private static ByteBuffer bytes(String s) {
        return ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    // ---- closing releases the port immediately ----

    @Test
    void closedServerReleasesItsPortAtOnce() throws Exception {
        int failures = 0;
        for (int i = 0; i < 300; i++) {
            Server s = Server.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), () -> new Handler() {});
            InetSocketAddress addr = (InetSocketAddress) s.localAddress();
            s.close();
            // 1. nothing may still be listening
            try (var probe = java.nio.channels.SocketChannel.open()) {
                probe.configureBlocking(true);
                probe.connect(addr);
                failures++; // connected to a "closed" server
            } catch (IOException expected) {
                // connection refused: the port is free
            }
        }
        assertEquals(0, failures, "a closed server must not accept connections afterwards");
    }

    @Test
    void closedServerPortCanBeBoundAgainRightAway() throws Exception {
        Server first = Server.bind(group, new InetSocketAddress("127.0.0.1", 0), Options.defaults(), () -> new Handler() {});
        int port = ((InetSocketAddress) first.localAddress()).getPort();
        first.close();
        for (int i = 0; i < 100; i++) {
            Server again = Server.bind(group, new InetSocketAddress("127.0.0.1", port), Options.defaults(), () -> new Handler() {});
            again.close();
        }
    }

    @Test
    void closedUdpSocketReleasesItsPortAtOnce() throws Exception {
        UdpSocket first = UdpSocket.bind(group, new InetSocketAddress("127.0.0.1", 0), (s, f, d) -> {});
        int port = ((InetSocketAddress) first.localAddress()).getPort();
        first.close();
        for (int i = 0; i < 100; i++) {
            UdpSocket again = UdpSocket.bind(group, new InetSocketAddress("127.0.0.1", port), (s, f, d) -> {});
            again.close();
        }
    }

    // ---- Unix domain sockets ----

    @Test
    void unixDomainEcho() throws Exception {
        Path dir = Files.createTempDirectory("hyd"); // keep the path short: sun_path is ~104 bytes on macOS
        Path sock = dir.resolve("s.sock");
        try {
            Server server = Server.bind(group, UnixDomainSocketAddress.of(sock), Options.defaults(), () -> new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer d) {
                    c.write(ByteBuffer.allocate(d.remaining()).put(d).flip());
                }
            });
            assertTrue(Files.exists(sock));
            assertEquals(UnixDomainSocketAddress.of(sock), server.localAddress());

            var got = new CompletableFuture<String>();
            Connection c = Client.connect(group, UnixDomainSocketAddress.of(sock), Options.defaults(), new Handler() {
                @Override
                public void onRead(Connection c, ByteBuffer d) {
                    got.complete(StandardCharsets.UTF_8.decode(d).toString());
                }
            }).get(5, TimeUnit.SECONDS);
            c.write(bytes("over unix"));
            assertEquals("over unix", got.get(5, TimeUnit.SECONDS));
            assertInstanceOf(UnixDomainSocketAddress.class, c.remoteAddress());

            server.close();
            assertFalse(Files.exists(sock), "socket file removed on close");
        } finally {
            Files.deleteIfExists(sock);
            Files.deleteIfExists(dir);
        }
    }

    @Test
    void unixConnectToMissingSocketFails() throws Exception {
        Path missing = Path.of(System.getProperty("java.io.tmpdir"), "hyd-missing-" + System.nanoTime());
        var f = Client.connect(group, UnixDomainSocketAddress.of(missing), Options.defaults(), new Handler() {});
        var ex = assertThrows(java.util.concurrent.ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
        assertInstanceOf(IOException.class, ex.getCause());
    }

    @Test
    void http_over_unix_socket() throws Exception {
        Path dir = Files.createTempDirectory("hyd");
        Path sock = dir.resolve("h.sock");
        try (Server server = io.hydrangea.http.HttpServer.bind(group, UnixDomainSocketAddress.of(sock), Options.defaults(), null,
                io.hydrangea.http.HttpConfig.defaults(),
                io.hydrangea.http.HttpHandler.sync(r -> io.hydrangea.http.HttpResponse.ok("via unix")))) {
            try (var ch = java.nio.channels.SocketChannel.open(UnixDomainSocketAddress.of(sock))) {
                ch.write(bytes("GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n"));
                String resp = new String(java.nio.channels.Channels.newInputStream(ch).readAllBytes(), StandardCharsets.UTF_8);
                assertTrue(resp.startsWith("HTTP/1.1 200") && resp.endsWith("via unix"), resp);
            }
        } finally {
            Files.deleteIfExists(sock);
            Files.deleteIfExists(dir);
        }
    }

    // ---- multicast ----

    private static NetworkInterface multicastInterface() throws IOException {
        for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (ni.isUp() && ni.supportsMulticast() && ni.inetAddresses().anyMatch(a -> a instanceof java.net.Inet4Address)) {
                return ni;
            }
        }
        return null;
    }

    @Test
    void multicastDeliversToAllMembers() throws Exception {
        NetworkInterface ni = multicastInterface();
        Assumptions.assumeTrue(ni != null, "no multicast-capable IPv4 interface");
        InetAddress mcast = InetAddress.getByName("239.255.42.99");
        UdpSocket.Config cfg = new UdpSocket.Config(1024, true, StandardProtocolFamily.INET);

        var r1 = new CompletableFuture<String>();
        var r2 = new CompletableFuture<String>();
        UdpSocket a = UdpSocket.bind(group, new InetSocketAddress(0), cfg, (s, f, d) -> r1.complete(StandardCharsets.UTF_8.decode(d).toString()));
        int port = ((InetSocketAddress) a.localAddress()).getPort();
        UdpSocket b = UdpSocket.bind(group, new InetSocketAddress(port), cfg, (s, f, d) -> r2.complete(StandardCharsets.UTF_8.decode(d).toString()));
        UdpSocket sender = UdpSocket.bind(group, new InetSocketAddress(0), new UdpSocket.Config(1024, false, StandardProtocolFamily.INET), (s, f, d) -> {});
        MembershipKey ka = null;
        MembershipKey kb = null;
        try {
            ka = a.joinGroup(mcast, ni);
            kb = b.joinGroup(mcast, ni);
            sender.setOption(StandardSocketOptions.IP_MULTICAST_IF, ni);
            sender.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
            sender.setOption(StandardSocketOptions.IP_MULTICAST_TTL, 1);
            for (int i = 0; i < 5 && !(r1.isDone() && r2.isDone()); i++) { // UDP: retry a few times
                sender.send(bytes("announce"), new InetSocketAddress(mcast, port));
                try {
                    CompletableFuture.allOf(r1, r2).get(500, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                    // retry
                }
            }
            Assumptions.assumeTrue(r1.isDone() || r2.isDone(), "multicast loopback not delivered in this environment (" + ni.getName() + ")");
            assertEquals("announce", r1.getNow("a missing"));
            assertEquals("announce", r2.getNow("b missing"));

            kb.drop(); // leaving the group stops delivery to b only
            var r3 = new CompletableFuture<String>();
            assertFalse(kb.isValid());
            assertTrue(ka.isValid());
        } finally {
            a.close();
            b.close();
            sender.close();
        }
    }
}
