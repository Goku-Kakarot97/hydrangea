package io.hydrangea.elasticsearch;

import java.util.Map;

/**
 * A document as {@link EsClient#get} returns it. {@link #found} false means there is no such id (the other fields are then empty: no exception, a missing document
 * is an ordinary answer).
 *
 * @param index       the index
 * @param id          the id
 * @param found       whether the document exists
 * @param version     {@code _version}, 0 when not found
 * @param seqNo       {@code _seq_no}: with {@link #primaryTerm}, what an optimistic-concurrency write ({@code if_seq_no}, {@code if_primary_term}) takes
 * @param primaryTerm {@code _primary_term}
 * @param source      the document, empty when not found or excluded
 */
public record EsDocument(String index, String id, boolean found, long version, long seqNo, long primaryTerm, Map<String, Object> source) {

    static EsDocument of(Object body) {
        return new EsDocument(Json.str(Json.at(body, "_index")), Json.str(Json.at(body, "_id")), Json.bool(Json.at(body, "found")), Json.lng(Json.at(body, "_version")),
                Json.lng(Json.at(body, "_seq_no")), Json.lng(Json.at(body, "_primary_term")), Json.map(Json.at(body, "_source")));
    }

    /** A field of the source by path. */
    public Object field(String... path) {
        return Json.at(source, (Object[]) path);
    }

    /** The source as a record ({@link Json#to}); null when not found. */
    public <T> T source(Class<T> type) {
        return found ? Json.to(type, source) : null;
    }
}
