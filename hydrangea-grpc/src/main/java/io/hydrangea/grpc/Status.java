package io.hydrangea.grpc;

/**
 * The outcome of a gRPC call: one of the 17 canonical codes, an optional human readable description, and (locally only) the cause. It travels
 * as the {@code grpc-status} and {@code grpc-message} trailers.
 */
public record Status(Code code, String description, Throwable cause) {

    public enum Code {
        OK(0), CANCELLED(1), UNKNOWN(2), INVALID_ARGUMENT(3), DEADLINE_EXCEEDED(4), NOT_FOUND(5), ALREADY_EXISTS(6), PERMISSION_DENIED(7),
        RESOURCE_EXHAUSTED(8), FAILED_PRECONDITION(9), ABORTED(10), OUT_OF_RANGE(11), UNIMPLEMENTED(12), INTERNAL(13), UNAVAILABLE(14),
        DATA_LOSS(15), UNAUTHENTICATED(16);

        private final int value;

        Code(int value) {
            this.value = value;
        }

        public int value() {
            return value;
        }

        /** The code for a wire value; anything unknown is {@link #UNKNOWN}, as the protocol asks. */
        public static Code fromValue(int v) {
            return v >= 0 && v < values().length ? values()[v] : UNKNOWN;
        }

        public Status toStatus() {
            return new Status(this, null, null);
        }
    }

    public static final Status OK = Code.OK.toStatus();

    public static Status of(Code code) {
        return code.toStatus();
    }

    public boolean isOk() {
        return code == Code.OK;
    }

    public Status withDescription(String description) {
        return new Status(code, description, cause);
    }

    public Status withCause(Throwable cause) {
        return new Status(code, description, cause);
    }

    public StatusException asException() {
        return new StatusException(this);
    }

    /** The status an exception stands for: its own if it is a {@link StatusException}, otherwise UNKNOWN (the message is not sent to the peer). */
    public static Status fromThrowable(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof StatusException se) {
                return se.status();
            }
            if (c.getCause() == c) {
                break;
            }
        }
        return Code.UNKNOWN.toStatus().withCause(t);
    }

    @Override
    public String toString() {
        return code + (description != null ? ": " + description : "");
    }
}
