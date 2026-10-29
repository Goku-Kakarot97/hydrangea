package io.hydrangea.kafka;

/** A record header: a key and an optional value. */
public record Header(String key, byte[] value) {
}
