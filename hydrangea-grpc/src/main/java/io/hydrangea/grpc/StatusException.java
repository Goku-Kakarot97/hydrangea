package io.hydrangea.grpc;

/** A call that ended with a non-OK {@link Status}; throw it from a handler to answer with that status. Carries optional trailing metadata. */
public final class StatusException extends RuntimeException {

    private final Status status;
    private final Metadata trailers;

    public StatusException(Status status) {
        this(status, null);
    }

    public StatusException(Status status, Metadata trailers) {
        super(status.toString(), status.cause(), false, false);
        this.status = status;
        this.trailers = trailers;
    }

    public Status status() {
        return status;
    }

    /** Metadata that came with the status (or goes with it), or null. */
    public Metadata trailers() {
        return trailers;
    }
}
