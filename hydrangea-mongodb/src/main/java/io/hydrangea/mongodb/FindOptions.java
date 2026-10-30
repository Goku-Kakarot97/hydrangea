package io.hydrangea.mongodb;

import java.time.Duration;
import java.util.Map;

/**
 * Options of a {@code find}: {@code projection} ({@code {name: 1, _id: 0}}), {@code sort} ({@code {price: -1}}), {@code skip}, {@code limit} (0 for none), {@code batchSize}
 * (documents per round trip, 0 for the server's default of 101 then 16 MB), {@code maxTime} (the server stops the query after it), {@code hint} (an index name or key document).
 */
public record FindOptions(Map<String, Object> projection, Map<String, Object> sort, long skip, long limit, int batchSize, Duration maxTime, Object hint) {

    public FindOptions {
        if (skip < 0 || limit < 0 || batchSize < 0) {
            throw new IllegalArgumentException("skip, limit and batchSize are not negative");
        }
    }

    public static FindOptions defaults() {
        return new FindOptions(null, null, 0, 0, 0, null, null);
    }

    public FindOptions withProjection(Map<String, Object> p) {
        return new FindOptions(p, sort, skip, limit, batchSize, maxTime, hint);
    }

    public FindOptions withSort(Map<String, Object> s) {
        return new FindOptions(projection, s, skip, limit, batchSize, maxTime, hint);
    }

    public FindOptions withSkip(long n) {
        return new FindOptions(projection, sort, n, limit, batchSize, maxTime, hint);
    }

    public FindOptions withLimit(long n) {
        return new FindOptions(projection, sort, skip, n, batchSize, maxTime, hint);
    }

    public FindOptions withBatchSize(int n) {
        return new FindOptions(projection, sort, skip, limit, n, maxTime, hint);
    }

    public FindOptions withMaxTime(Duration d) {
        return new FindOptions(projection, sort, skip, limit, batchSize, d, hint);
    }

    public FindOptions withHint(Object h) {
        return new FindOptions(projection, sort, skip, limit, batchSize, maxTime, h);
    }

    /** Adds the options to a {@code find} command. */
    void applyTo(Map<String, Object> find) {
        if (projection != null) find.put("projection", projection);
        if (sort != null) find.put("sort", sort);
        if (skip > 0) find.put("skip", skip);
        if (limit > 0) find.put("limit", limit);
        if (batchSize > 0) find.put("batchSize", batchSize);
        if (maxTime != null) find.put("maxTimeMS", maxTime.toMillis());
        if (hint != null) find.put("hint", hint);
    }
}
