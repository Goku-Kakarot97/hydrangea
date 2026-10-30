package io.hydrangea.mongodb;

/**
 * The outcome of an update or replace.
 *
 * @param matched    documents the filter matched
 * @param modified   documents actually changed (a no-op update matches without modifying)
 * @param upsertedId the {@code _id} of the document an upsert inserted, or null
 */
public record UpdateResult(long matched, long modified, Object upsertedId) {

    public boolean upserted() {
        return upsertedId != null;
    }
}
