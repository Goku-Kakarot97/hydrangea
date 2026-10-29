package io.hydrangea.mqtt;

/** MQTT delivery guarantee. */
public enum QoS {
    /** Fire and forget: delivered at most once, no acknowledgement. */
    AT_MOST_ONCE(0),
    /** Acknowledged: delivered at least once (a duplicate is possible after a lost acknowledgement). */
    AT_LEAST_ONCE(1),
    /** Four-way handshake: delivered exactly once. */
    EXACTLY_ONCE(2);

    private final int level;

    QoS(int level) {
        this.level = level;
    }

    public int level() {
        return level;
    }

    public static QoS of(int level) {
        return switch (level) {
            case 0 -> AT_MOST_ONCE;
            case 1 -> AT_LEAST_ONCE;
            case 2 -> EXACTLY_ONCE;
            default -> throw new IllegalArgumentException("QoS " + level);
        };
    }
}
