package io.hydrangea.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The answer to a search.
 *
 * @param took          server-side milliseconds
 * @param timedOut      the search stopped at its {@code timeout} with partial results
 * @param total         the number of matching documents, exact when {@link #totalRelation} is {@code eq}, a lower bound when it is {@code gte} (the default stops counting at 10 000;
 *                      send {@code "track_total_hits": true} for an exact count)
 * @param totalRelation {@code eq} or {@code gte}
 * @param maxScore      the best score, or NaN
 * @param hits          the page of hits
 * @param aggregations  the {@code aggregations} object, empty when the search had none
 * @param scrollId      the scroll id when the search opened a scroll, else null
 * @param raw           the whole answer: {@code suggest}, {@code _shards}, {@code pit_id}, ...
 */
public record EsSearchResponse(long took, boolean timedOut, long total, String totalRelation, double maxScore, List<EsHit> hits, Map<String, Object> aggregations,
                               String scrollId, Map<String, Object> raw) {

    static EsSearchResponse of(Object body) {
        Object hitsObj = Json.at(body, "hits");
        Object total = Json.at(hitsObj, "total");
        long count;
        String relation;
        if (total instanceof Map<?, ?>) {                          // 7+: {"value": 12, "relation": "eq"}
            count = Json.lng(Json.at(total, "value"));
            relation = Json.str(Json.at(total, "relation"));
        } else {                                                   // rest_total_hits_as_int, or 6.x
            count = Json.lng(total);
            relation = "eq";
        }
        List<Object> list = Json.list(Json.at(hitsObj, "hits"));
        List<EsHit> hits = new ArrayList<>(list.size());
        for (Object h : list) {
            hits.add(EsHit.of(h));
        }
        Object max = Json.at(hitsObj, "max_score");
        return new EsSearchResponse(Json.lng(Json.at(body, "took")), Json.bool(Json.at(body, "timed_out")), count, relation == null ? "eq" : relation,
                max == null ? Double.NaN : Json.dbl(max), List.copyOf(hits), Json.map(Json.at(body, "aggregations")), Json.str(Json.at(body, "_scroll_id")), Json.map(body));
    }

    /** The sources of the hits as records ({@link Json#to}), in order. */
    public <T> List<T> sources(Class<T> type) {
        List<T> out = new ArrayList<>(hits.size());
        for (EsHit h : hits) {
            out.add(h.source(type));
        }
        return out;
    }

    /** The sort values of the last hit: what the next page's {@code search_after} takes; null when there are no hits. */
    public List<Object> lastSort() {
        return hits.isEmpty() ? null : hits.get(hits.size() - 1).sort();
    }

    /** One aggregation's result by name ({@code aggregations.<name>}), empty if absent. */
    public Map<String, Object> aggregation(String name) {
        return Json.map(aggregations.get(name));
    }

    /** The buckets of a bucket aggregation ({@code terms}, {@code histogram}, ...): each a map with {@code key} and {@code doc_count}. */
    public List<Object> buckets(String aggregation) {
        return Json.list(Json.at(aggregations, aggregation, "buckets"));
    }
}
