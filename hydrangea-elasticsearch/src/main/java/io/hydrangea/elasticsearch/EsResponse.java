package io.hydrangea.elasticsearch;

import io.hydrangea.http.Headers;

/**
 * The answer to a {@link EsClient#request raw request}: the status, the headers and the body parsed when it is JSON ({@link #json}; the text otherwise, in
 * {@link #text}). Error statuses have already been turned into {@link EsException} by the time a caller sees one of these.
 */
public record EsResponse(int status, Headers headers, Object json, String text) {

    /** {@link Json#at} on the body. */
    public Object at(Object... path) {
        return Json.at(json, path);
    }

    public boolean isJson() {
        return text == null;
    }
}
