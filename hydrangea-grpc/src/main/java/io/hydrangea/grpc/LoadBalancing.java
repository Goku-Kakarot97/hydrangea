package io.hydrangea.grpc;

/** How a {@link GrpcChannel} spreads calls over the addresses its {@link NameResolver} returned. */
public enum LoadBalancing {
    /** All calls go to the first healthy address; the next one only when it fails. The default. */
    PICK_FIRST,
    /** Calls take the healthy addresses in turn. */
    ROUND_ROBIN
}
