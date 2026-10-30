package io.hydrangea.elasticsearch;

/**
 * The answer to an index, create, update or delete of one document.
 *
 * @param index       the index
 * @param id          the id (the one the cluster made up, for an index without id)
 * @param result      {@code created}, {@code updated}, {@code deleted}, {@code not_found} (a delete of a missing id) or {@code noop} (an update that changed nothing)
 * @param version     the document's version after the write
 * @param seqNo       {@code _seq_no} after the write
 * @param primaryTerm {@code _primary_term}
 */
public record EsWriteResponse(String index, String id, String result, long version, long seqNo, long primaryTerm) {

    static EsWriteResponse of(Object body) {
        return new EsWriteResponse(Json.str(Json.at(body, "_index")), Json.str(Json.at(body, "_id")), Json.str(Json.at(body, "result")),
                Json.lng(Json.at(body, "_version")), Json.lng(Json.at(body, "_seq_no")), Json.lng(Json.at(body, "_primary_term")));
    }

    public boolean created() {
        return "created".equals(result);
    }

    public boolean deleted() {
        return "deleted".equals(result);
    }
}
