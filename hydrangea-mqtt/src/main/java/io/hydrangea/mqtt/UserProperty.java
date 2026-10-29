package io.hydrangea.mqtt;

/** An MQTT 5 user property: a name and a value the application defines; a message may carry several, even with the same name. */
public record UserProperty(String name, String value) {
    public UserProperty {
        if (name == null || value == null) {
            throw new IllegalArgumentException("name and value are required");
        }
    }
}
