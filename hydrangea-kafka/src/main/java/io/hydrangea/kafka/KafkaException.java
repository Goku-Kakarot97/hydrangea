package io.hydrangea.kafka;

/** A failure of a Kafka request: an error code from the broker ({@link #code()}, 0 if it did not come from one) or a local problem. */
public final class KafkaException extends RuntimeException {

    private final int code;
    private final boolean retriable;

    public KafkaException(String message, boolean retriable) {
        this(message, 0, retriable, null);
    }

    public KafkaException(String message, boolean retriable, Throwable cause) {
        this(message, 0, retriable, cause);
    }

    KafkaException(String message, int code, boolean retriable, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.retriable = retriable;
    }

    /** The broker's error code (see the Kafka protocol guide), or 0. */
    public int code() {
        return code;
    }

    /** Whether trying again can succeed: leader moved, broker not available, request timed out... */
    public boolean retriable() {
        return retriable;
    }

    static final int UNKNOWN_SERVER_ERROR = -1, OFFSET_OUT_OF_RANGE = 1, CORRUPT_MESSAGE = 2, UNKNOWN_TOPIC_OR_PARTITION = 3, LEADER_NOT_AVAILABLE = 5,
            NOT_LEADER_OR_FOLLOWER = 6, REQUEST_TIMED_OUT = 7, MESSAGE_TOO_LARGE = 10, COORDINATOR_LOAD_IN_PROGRESS = 14, COORDINATOR_NOT_AVAILABLE = 15,
            NOT_COORDINATOR = 16, NOT_ENOUGH_REPLICAS = 19, NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20, ILLEGAL_GENERATION = 22, UNKNOWN_MEMBER_ID = 25,
            REBALANCE_IN_PROGRESS = 27, TOPIC_AUTHORIZATION_FAILED = 29, GROUP_AUTHORIZATION_FAILED = 30, TOPIC_ALREADY_EXISTS = 36,
            UNSUPPORTED_VERSION = 35, SASL_AUTHENTICATION_FAILED = 58, FENCED_INSTANCE_ID = 82, MEMBER_ID_REQUIRED = 79, NETWORK_EXCEPTION = 13,
            OUT_OF_ORDER_SEQUENCE_NUMBER = 45, DUPLICATE_SEQUENCE_NUMBER = 46, INVALID_PRODUCER_EPOCH = 47, UNKNOWN_PRODUCER_ID = 59, UNKNOWN_TOPIC_ID = 100;

    static String name(int code) {
        return switch (code) {
            case -1 -> "UNKNOWN_SERVER_ERROR";
            case 1 -> "OFFSET_OUT_OF_RANGE";
            case 2 -> "CORRUPT_MESSAGE";
            case 3 -> "UNKNOWN_TOPIC_OR_PARTITION";
            case 5 -> "LEADER_NOT_AVAILABLE";
            case 6 -> "NOT_LEADER_OR_FOLLOWER";
            case 7 -> "REQUEST_TIMED_OUT";
            case 10 -> "MESSAGE_TOO_LARGE";
            case 13 -> "NETWORK_EXCEPTION";
            case 14 -> "COORDINATOR_LOAD_IN_PROGRESS";
            case 15 -> "COORDINATOR_NOT_AVAILABLE";
            case 16 -> "NOT_COORDINATOR";
            case 19 -> "NOT_ENOUGH_REPLICAS";
            case 20 -> "NOT_ENOUGH_REPLICAS_AFTER_APPEND";
            case 22 -> "ILLEGAL_GENERATION";
            case 25 -> "UNKNOWN_MEMBER_ID";
            case 27 -> "REBALANCE_IN_PROGRESS";
            case 29 -> "TOPIC_AUTHORIZATION_FAILED";
            case 30 -> "GROUP_AUTHORIZATION_FAILED";
            case 35 -> "UNSUPPORTED_VERSION";
            case 36 -> "TOPIC_ALREADY_EXISTS";
            case 45 -> "OUT_OF_ORDER_SEQUENCE_NUMBER";
            case 46 -> "DUPLICATE_SEQUENCE_NUMBER";
            case 47 -> "INVALID_PRODUCER_EPOCH";
            case 58 -> "SASL_AUTHENTICATION_FAILED";
            case 59 -> "UNKNOWN_PRODUCER_ID";
            case 79 -> "MEMBER_ID_REQUIRED";
            case 82 -> "FENCED_INSTANCE_ID";
            default -> "ERROR_" + code;
        };
    }

    static boolean retriableCode(int code) {
        return switch (code) {
            case 3, 5, 6, 7, 13, 14, 15, 16, 19, 20 -> true;
            default -> false;
        };
    }

    static KafkaException of(int code, String what) {
        return new KafkaException(what + ": " + name(code) + " (" + code + ")", code, retriableCode(code), null);
    }
}
