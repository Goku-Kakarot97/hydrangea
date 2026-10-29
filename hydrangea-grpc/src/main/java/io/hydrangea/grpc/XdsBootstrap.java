package io.hydrangea.grpc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * The bootstrap an xDS client starts from: where the management server is and who this client is. The JSON is the one other gRPC
 * implementations read ({@code GRPC_XDS_BOOTSTRAP} names a file, {@code GRPC_XDS_BOOTSTRAP_CONFIG} holds the text):
 * <pre>{@code
 * {"xds_servers": [{"server_uri": "xds.example.com:18000", "channel_creds": [{"type": "insecure"}]}],
 *  "node": {"id": "client-1", "cluster": "web", "locality": {"region": "eu", "zone": "a"}}}
 * }</pre>
 * Only an {@code insecure} channel to the management server is supported, and only the first server is used.
 */
public record XdsBootstrap(String serverUri, String nodeId, String nodeCluster, String region, String zone, String subZone) {

    @SuppressWarnings("unchecked")
    public static XdsBootstrap parse(String json) {
        Object root = Json.parse(json);
        if (!(root instanceof Map<?, ?> top)) {
            throw new IllegalArgumentException("an xDS bootstrap is a JSON object");
        }
        if (!(top.get("xds_servers") instanceof List<?> servers) || servers.isEmpty() || !(servers.get(0) instanceof Map<?, ?> server)) {
            throw new IllegalArgumentException("the bootstrap needs a non-empty xds_servers list");
        }
        if (!(server.get("server_uri") instanceof String uri) || uri.isEmpty()) {
            throw new IllegalArgumentException("xds_servers[0].server_uri is missing");
        }
        boolean insecure = false;
        if (server.get("channel_creds") instanceof List<?> creds) {
            for (Object c : creds) {
                if (c instanceof Map<?, ?> m && "insecure".equals(m.get("type"))) {
                    insecure = true;
                }
            }
        }
        if (!insecure) {
            throw new IllegalArgumentException("only channel_creds of type \"insecure\" are supported for the xDS server");
        }
        String id = "", cluster = "", region = "", zone = "", subZone = "";
        if (top.get("node") instanceof Map<?, ?> node) {
            id = node.get("id") instanceof String s ? s : "";
            cluster = node.get("cluster") instanceof String s ? s : "";
            if (node.get("locality") instanceof Map<?, ?> loc) {
                region = loc.get("region") instanceof String s ? s : "";
                zone = loc.get("zone") instanceof String s ? s : "";
                subZone = loc.get("sub_zone") instanceof String s ? s : "";
            }
        }
        return new XdsBootstrap(uri, id, cluster, region, zone, subZone);
    }

    /** From {@code GRPC_XDS_BOOTSTRAP_CONFIG} or the file named by {@code GRPC_XDS_BOOTSTRAP}. @throws IllegalStateException if neither is set */
    public static XdsBootstrap fromEnvironment() {
        String inline = System.getenv("GRPC_XDS_BOOTSTRAP_CONFIG");
        if (inline != null && !inline.isBlank()) {
            return parse(inline);
        }
        String file = System.getenv("GRPC_XDS_BOOTSTRAP");
        if (file == null || file.isBlank()) {
            throw new IllegalStateException("an xds:/// target needs a bootstrap: set GRPC_XDS_BOOTSTRAP (a file) or GRPC_XDS_BOOTSTRAP_CONFIG, or call xdsBootstrap(..)");
        }
        try {
            return parse(Files.readString(Path.of(file)));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read the xDS bootstrap " + file + ": " + e.getMessage(), e);
        }
    }
}
