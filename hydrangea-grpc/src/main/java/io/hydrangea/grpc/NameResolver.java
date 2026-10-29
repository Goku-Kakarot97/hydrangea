package io.hydrangea.grpc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns the channel's target host into the addresses of the servers behind it. Called off the event loops (it may block), at start and then
 * every {@link GrpcChannel.Builder#resolveInterval refresh interval}; the channel keeps its connections to addresses that are still listed.
 */
@FunctionalInterface
public interface NameResolver {

    List<InetSocketAddress> resolve(String host, int port) throws IOException;

    /** DNS: every A/AAAA record of the host (what makes client-side round-robin over a headless Kubernetes service work). The default. */
    static NameResolver dns() {
        return (host, port) -> {
            String h = host.startsWith("[") ? host.substring(1, host.length() - 1) : host;
            List<InetSocketAddress> out = new ArrayList<>();
            for (InetAddress a : InetAddress.getAllByName(h)) {
                out.add(new InetSocketAddress(a, port));
            }
            return out;
        };
    }

    /** A fixed list, whatever the target says. */
    static NameResolver fixed(InetSocketAddress... addresses) {
        List<InetSocketAddress> list = List.of(addresses);
        return (host, port) -> list;
    }
}
