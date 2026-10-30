package io.hydrangea.mongodb;

/** The BSON timestamp type (0x11): what the oplog and change streams use, not a date. {@code seconds} since the epoch and an {@code increment} within that second. */
public record BsonTimestamp(int seconds, int increment) implements Comparable<BsonTimestamp> {

    @Override
    public int compareTo(BsonTimestamp o) {
        int c = Integer.compareUnsigned(seconds, o.seconds);
        return c != 0 ? c : Integer.compareUnsigned(increment, o.increment);
    }
}
