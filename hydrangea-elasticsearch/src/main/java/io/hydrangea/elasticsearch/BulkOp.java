package io.hydrangea.elasticsearch;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One action of a bulk request: {@link #index}, {@link #create}, {@link #update}, {@link #delete}. {@link #with} adds a parameter to the action line ({@code routing},
 * {@code if_seq_no}, {@code if_primary_term}, {@code version}, {@code pipeline}, ...).
 *
 * @param action {@code index}, {@code create}, {@code update} or {@code delete}
 * @param index  the index
 * @param id     the id, or null to let the cluster make one up ({@code index} and {@code create} only)
 * @param meta   the other fields of the action line
 * @param source the document line, or null for a delete
 */
public record BulkOp(String action, String index, String id, Map<String, Object> meta, Object source) {

    public BulkOp {
        if (index == null || index.isEmpty()) {
            throw new IllegalArgumentException("index is required");
        }
        if (id != null && id.isEmpty()) {
            throw new IllegalArgumentException("an id is not empty (null for none)");
        }
        meta = meta == null ? Map.of() : Map.copyOf(meta);
    }

    /** Writes the document, replacing an existing one with this id. */
    public static BulkOp index(String index, String id, Object document) {
        return new BulkOp("index", index, id, null, document);
    }

    /** Writes the document; fails with 409 when the id exists. */
    public static BulkOp create(String index, String id, Object document) {
        return new BulkOp("create", index, id, null, document);
    }

    /** Merges {@code partialDocument} into the document (a {@code doc} update); fails with 404 when it does not exist. */
    public static BulkOp update(String index, String id, Object partialDocument) {
        return new BulkOp("update", index, id, null, Map.of("doc", partialDocument));
    }

    /** An update with the whole update body ({@code script}, {@code upsert}, {@code doc_as_upsert}, ...). */
    public static BulkOp updateWith(String index, String id, Map<String, Object> updateBody) {
        return new BulkOp("update", index, id, null, updateBody);
    }

    public static BulkOp delete(String index, String id) {
        return new BulkOp("delete", index, id, null, null);
    }

    public BulkOp with(String key, Object value) {
        Map<String, Object> m = new LinkedHashMap<>(meta);
        m.put(key, value);
        return new BulkOp(action, index, id, m, source);
    }

    public BulkOp withRouting(String routing) {
        return with("routing", routing);
    }

    /** Optimistic concurrency: the write applies only if the document is still at this {@code _seq_no}/{@code _primary_term}, else the item fails with 409. */
    public BulkOp withIfSeqNo(long seqNo, long primaryTerm) {
        return with("if_seq_no", seqNo).with("if_primary_term", primaryTerm);
    }

    /** The NDJSON lines of this action. */
    void writeTo(StringBuilder sb) {
        Map<String, Object> line = new LinkedHashMap<>(4 + meta.size());
        line.put("_index", index);
        if (id != null) {
            line.put("_id", id);
        }
        line.putAll(meta);
        sb.append('{');
        Json.writeString(sb, action);
        sb.append(':');
        Json.write(sb, line, 1);
        sb.append("}\n");
        if (!action.equals("delete")) {
            Json.write(sb, source, 0);
            sb.append('\n');
        }
    }

    /** The NDJSON size of this action, for the indexer's byte limit. */
    int approximateBytes() {
        StringBuilder sb = new StringBuilder(128);
        writeTo(sb);
        return sb.length();
    }
}
