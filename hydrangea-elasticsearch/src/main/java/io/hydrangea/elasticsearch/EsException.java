package io.hydrangea.elasticsearch;

import java.util.List;
import java.util.Map;

/**
 * What a request fails with. Either the cluster answered with an error status ({@link #status} 4xx/5xx, {@link #type} and {@link #reason} from its {@code error}
 * object, the whole answer in {@link #body}), or no node answered at all ({@link #status} 0, the last transport failure as the cause).
 */
public final class EsException extends RuntimeException {

    private final int status;
    private final String type;
    private final Object body;

    public EsException(int status, String type, String reason, Object body, Throwable cause) {
        super(reason, cause);
        this.status = status;
        this.type = type;
        this.body = body;
    }

    /** From an error answer of the cluster. */
    static EsException of(int status, Object body) {
        Object error = Json.at(body, "error");
        String type = null;
        String reason;
        if (error instanceof Map<?, ?>) {
            type = Json.str(Json.at(error, "type"));
            reason = Json.str(Json.at(error, "reason"));
            Object cause = Json.at(error, "caused_by", "reason");
            if (cause != null) {
                reason = reason + " (caused by: " + cause + ")";
            }
            List<Object> roots = Json.list(Json.at(error, "root_cause"));
            if (type == null && !roots.isEmpty()) {
                type = Json.str(Json.at(roots.get(0), "type"));
            }
        } else {
            reason = Json.str(error);
        }
        if (reason == null) {
            reason = status == 404 ? "not found" : "HTTP " + status;
        }
        return new EsException(status, type, "HTTP " + status + (type != null ? " " + type : "") + ": " + reason, body, null);
    }

    /** No node answered. */
    static EsException transport(String what, Throwable cause) {
        return new EsException(0, null, what, null, cause);
    }

    /** The HTTP status, or 0 when no node answered. */
    public int status() {
        return status;
    }

    /** The {@code error.type} of the answer ({@code index_not_found_exception}, {@code version_conflict_engine_exception}, ...), or null. */
    public String type() {
        return type;
    }

    /** The parsed answer (a {@code Map}), a string if it was not JSON, or null for a transport failure. */
    public Object body() {
        return body;
    }

    public boolean isNotFound() {
        return status == 404;
    }

    /** 409: the document changed since the given {@code if_seq_no}/{@code if_primary_term}, or {@code create} of an id that exists. */
    public boolean isConflict() {
        return status == 409;
    }

    /** 429 or 503, or no node answered: worth another try after a pause. */
    public boolean isRetriable() {
        return status == 0 || status == 429 || status == 503;
    }
}
