package io.hydrangea.redis;

/**
 * An error reply the server sent as a <em>value</em>: inside an array (the results of a transaction, where one command may fail and the others not) it
 * is delivered as this. A command whose own reply is an error fails its future with a {@link RedisException} instead.
 *
 * @param message the whole line, for example {@code WRONGTYPE Operation against a key holding the wrong kind of value}
 */
public record RedisError(String message) {

    /** The first word, upper case by convention: ERR, WRONGTYPE, NOSCRIPT, MOVED, NOAUTH... */
    public String code() {
        int sp = message.indexOf(' ');
        return sp < 0 ? message : message.substring(0, sp);
    }
}
