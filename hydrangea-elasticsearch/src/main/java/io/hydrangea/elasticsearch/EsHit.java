package io.hydrangea.elasticsearch;

import java.util.List;
import java.util.Map;

/**
 * One search hit.
 *
 * @param index  the index the document lives in
 * @param id     its id
 * @param score  the relevance score, or NaN when the search did not compute one (sorted searches)
 * @param source the document ({@code _source}), empty when the search excluded it
 * @param sort   the sort values ({@code sort}), empty when the search had no sort; what {@code search_after} takes
 * @param raw    the whole hit object: {@code highlight}, {@code fields}, {@code inner_hits}, ...
 */
public record EsHit(String index, String id, double score, Map<String, Object> source, List<Object> sort, Map<String, Object> raw) {

    static EsHit of(Object hit) {
        Object score = Json.at(hit, "_score");
        return new EsHit(Json.str(Json.at(hit, "_index")), Json.str(Json.at(hit, "_id")), score == null ? Double.NaN : Json.dbl(score),
                Json.map(Json.at(hit, "_source")), Json.list(Json.at(hit, "sort")), Json.map(hit));
    }

    /** A field of the source by path: {@code hit.field("address", "city")}. */
    public Object field(String... path) {
        return Json.at(source, (Object[]) path);
    }

    /** The source as a record ({@link Json#to}). */
    public <T> T source(Class<T> type) {
        return Json.to(type, source);
    }
}
