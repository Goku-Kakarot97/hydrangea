package io.hydrangea.grpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Credentials sent with every call, fetched per call (a token that expires, a signed request). The call starts as soon as the metadata
 * is available; messages sent before that are held. A failed future fails the call with UNAUTHENTICATED... or the status you give it.
 * <pre>{@code
 * GrpcChannel.forTarget("https://api.example.com").callCredentials(CallCredentials.bearer(() -> tokens.current())).build()
 * }</pre>
 * To avoid sending secrets in the clear, a channel refuses credentials on an {@code http://} target unless told otherwise
 * ({@link GrpcChannel.Builder#allowCredentialsOverPlaintext}).
 */
@FunctionalInterface
public interface CallCredentials {

    /**
     * @param methodPath the method being called, {@code /package.Service/Method}
     * @param authority  the server's {@code host[:port]}
     * @return metadata to add to the call; a failed stage fails the call
     */
    CompletionStage<Metadata> metadata(String methodPath, String authority);

    /** {@code authorization: Bearer <token>} from a (possibly blocking-free, cheap) supplier called for each call. */
    static CallCredentials bearer(Supplier<String> token) {
        return (path, authority) -> {
            try {
                return CompletableFuture.completedFuture(new Metadata().put("authorization", "Bearer " + token.get()));
            } catch (RuntimeException e) {
                return CompletableFuture.failedFuture(e);
            }
        };
    }
}
