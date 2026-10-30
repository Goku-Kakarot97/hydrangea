package io.hydrangea.mongodb;

/** A BSON regular expression (0x0B): the pattern and its option letters ({@code i}, {@code m}, {@code s}, {@code x}...). A {@link java.util.regex.Pattern} is written as one too. */
public record BsonRegex(String pattern, String options) {

    public BsonRegex {
        if (pattern == null) {
            throw new IllegalArgumentException("pattern is required");
        }
        options = options == null ? "" : options;
    }

    public static BsonRegex of(String pattern) {
        return new BsonRegex(pattern, "");
    }
}
