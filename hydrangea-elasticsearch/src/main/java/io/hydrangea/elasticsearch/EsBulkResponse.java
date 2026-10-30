package io.hydrangea.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The answer to a bulk request: one {@link Item} per action, in order. A bulk request as a whole succeeds (HTTP 200) even when some actions failed: look at
 * {@link #errors} and {@link #failures}.
 *
 * @param took   server-side milliseconds
 * @param errors whether any item failed
 * @param items  one per action, in the order they were sent
 */
public record EsBulkResponse(long took, boolean errors, List<Item> items) {

    /**
     * One action's outcome.
     *
     * @param action  {@code index}, {@code create}, {@code update} or {@code delete}
     * @param index   the index
     * @param id      the id (the one the cluster made up, when none was given)
     * @param status  the HTTP status of this action: 200/201 ok, 404 (an update or delete of a missing id), 409 (conflict), 429 (rejected under load: retry), ...
     * @param result  {@code created}, {@code updated}, {@code deleted}, {@code not_found}, {@code noop}; null when the action failed
     * @param error   the {@code error} object when the action failed, else null
     * @param version the document's version after the write, 0 when it failed
     * @param seqNo   {@code _seq_no} after the write
     */
    public record Item(String action, String index, String id, int status, String result, Map<String, Object> error, long version, long seqNo) {

        static Item of(Object entry) {
            Map<String, Object> m = Json.map(entry);
            if (m.isEmpty()) {
                return new Item("?", null, null, 0, null, Map.of("reason", "unreadable item"), 0, 0);
            }
            String action = m.keySet().iterator().next();
            Object body = m.get(action);
            Object error = Json.at(body, "error");
            return new Item(action, Json.str(Json.at(body, "_index")), Json.str(Json.at(body, "_id")), (int) Json.lng(Json.at(body, "status")),
                    Json.str(Json.at(body, "result")), error == null ? null : error instanceof Map ? Json.map(error) : Map.of("reason", Json.str(error)),
                    Json.lng(Json.at(body, "_version")), Json.lng(Json.at(body, "_seq_no")));
        }

        public boolean failed() {
            return error != null;
        }

        /** {@code error.type}, or null. */
        public String errorType() {
            return error == null ? null : Json.str(error.get("type"));
        }

        public String errorReason() {
            return error == null ? null : Json.str(error.get("reason"));
        }

        /** 429: the cluster's write queue was full. The same action sent again later will usually go through. */
        public boolean rejected() {
            return status == 429;
        }

        /** This item as an exception, for callers who want one per failed action. */
        public EsException toException() {
            return new EsException(status, errorType(), "bulk " + action + " " + index + "/" + id + ": HTTP " + status + (errorType() != null ? " " + errorType() : "") + ": " + errorReason(), error, null);
        }
    }

    static EsBulkResponse of(Object body) {
        List<Object> list = Json.list(Json.at(body, "items"));
        List<Item> items = new ArrayList<>(list.size());
        for (Object o : list) {
            items.add(Item.of(o));
        }
        return new EsBulkResponse(Json.lng(Json.at(body, "took")), Json.bool(Json.at(body, "errors")), List.copyOf(items));
    }

    /** The items that failed. */
    public List<Item> failures() {
        List<Item> f = new ArrayList<>();
        for (Item i : items) {
            if (i.failed()) {
                f.add(i);
            }
        }
        return f;
    }
}
