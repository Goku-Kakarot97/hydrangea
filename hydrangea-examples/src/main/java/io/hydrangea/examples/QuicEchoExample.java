package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.quic.QuicConfig;
import io.hydrangea.quic.QuicConnection;
import io.hydrangea.quic.QuicHandler;
import io.hydrangea.quic.QuicServer;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;

/**
 * Echoes every stream of every QUIC connection back to the peer (ALPN "echo").
 * <pre>
 * java -cp target/classes io.hydrangea.examples.QuicEchoExample port keystore.p12 [password] [alias]
 * </pre>
 */
public final class QuicEchoExample {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(args[0]);
        char[] password = (args.length > 2 ? args[2] : "changeit").toCharArray();
        String alias = args.length > 3 ? args[3] : "srv";
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(Path.of(args[1]))) {
            store.load(in, password);
        }
        EventLoopGroup group = new EventLoopGroup(1);
        QuicServer server = QuicServer.bind(group, new InetSocketAddress("127.0.0.1", port), QuicConfig.fromKeyStore(store, alias, password, List.of("echo")), new QuicHandler() {
            @Override
            public void onOpen(QuicConnection c) {
                System.out.println("open " + c.remoteAddress() + " alpn=" + c.alpn() + " sni=" + c.serverName());
            }

            @Override
            public void onStreamData(QuicConnection c, long streamId, byte[] data, boolean fin) {
                if (new String(data, java.nio.charset.StandardCharsets.ISO_8859_1).equals("CLOSE")) {
                    c.close(42, "bye");   // lets a test see an application close from the server
                    return;
                }
                c.send(streamId, data, fin);
            }

            @Override
            public void onClose(QuicConnection c, long code, boolean application, String reason) {
                System.out.println("close " + c.remoteAddress() + " code=" + code + " app=" + application + " " + reason);
            }
        });
        System.out.println("READY " + ((InetSocketAddress) server.localAddress()).getPort());
        Thread.currentThread().join();
    }
}
