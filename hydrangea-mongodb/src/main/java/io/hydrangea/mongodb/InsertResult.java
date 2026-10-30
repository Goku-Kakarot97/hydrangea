package io.hydrangea.mongodb;

import java.util.List;

/** The {@code _id}s of inserted documents, in the order they were sent (the ones given, or the {@link ObjectId}s made up for documents without one). */
public record InsertResult(List<Object> ids) {

    public InsertResult {
        ids = List.copyOf(ids);
    }

    /** The first (for {@code insertOne}: the only) id. */
    public Object id() {
        return ids.isEmpty() ? null : ids.get(0);
    }

    public int count() {
        return ids.size();
    }
}
