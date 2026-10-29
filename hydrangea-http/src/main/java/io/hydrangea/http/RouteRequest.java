package io.hydrangea.http;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What a route sees: the raw {@link HttpRequest} plus path variables and request parameters.
 * <ul>
 *   <li><b>Path variables</b> come from the matched pattern, e.g. {@code /users/{id}}. Already percent-decoded.</li>
 *   <li><b>Request parameters</b> are the query string followed by, for
 *       {@code application/x-www-form-urlencoded} bodies, the form fields. A name may repeat
 *       ({@code ?tag=a&tag=b}); {@link #param} returns the first value, {@link #params} all of them.</li>
 * </ul>
 * Typed accessors answer <b>400 Bad Request</b> (via {@link HttpStatusException}) when a value is missing or not
 * a number, so routes do not need their own validation boilerplate.
 */
public final class RouteRequest {

    /** Upper bound on distinct request parameters accepted per request. */
    static final int MAX_PARAMS = 1000;

    private final HttpRequest request;
    private final Map<String, String> pathVariables;
    private Map<String, List<String>> params; // lazy

    RouteRequest(HttpRequest request, Map<String, String> pathVariables) {
        this.request = request;
        this.pathVariables = pathVariables;
    }

    public HttpRequest request() {
        return request;
    }

    public String method() {
        return request.method();
    }

    public String path() {
        return request.path();
    }

    public String header(String name) {
        return request.headers().get(name);
    }

    public byte[] body() {
        return request.body();
    }

    public String bodyAsString() {
        return request.bodyAsString();
    }

    // ---- path variables ----

    /** @return the decoded value of {@code {name}} in the route pattern; never null for a declared variable */
    public String pathVariable(String name) {
        String v = pathVariables.get(name);
        if (v == null) {
            throw new IllegalArgumentException("route has no path variable {" + name + "}");
        }
        return v;
    }

    public int pathInt(String name) {
        return parseInt("path variable '" + name + "'", pathVariable(name));
    }

    public long pathLong(String name) {
        return parseLong("path variable '" + name + "'", pathVariable(name));
    }

    public Map<String, String> pathVariables() {
        return Collections.unmodifiableMap(pathVariables);
    }

    // ---- request parameters ----

    /** First value of the parameter, or null if absent. */
    public String param(String name) {
        List<String> v = paramMap().get(name);
        return v == null ? null : v.get(0);
    }

    public String param(String name, String defaultValue) {
        String v = param(name);
        return v == null ? defaultValue : v;
    }

    /** First value, or 400 if the parameter is absent. */
    public String requiredParam(String name) {
        String v = param(name);
        if (v == null) {
            throw new HttpStatusException(400, "Missing required parameter '" + name + "'");
        }
        return v;
    }

    /** All values of a repeated parameter; empty list if absent. */
    public List<String> params(String name) {
        return paramMap().getOrDefault(name, List.of());
    }

    public int paramInt(String name, int defaultValue) {
        String v = param(name);
        return v == null ? defaultValue : parseInt("parameter '" + name + "'", v);
    }

    public long paramLong(String name, long defaultValue) {
        String v = param(name);
        return v == null ? defaultValue : parseLong("parameter '" + name + "'", v);
    }

    /** {@code true}/{@code false} (case-insensitive); anything else is a 400. A bare {@code ?flag} counts as true. */
    public boolean paramBool(String name, boolean defaultValue) {
        String v = param(name);
        if (v == null) {
            return defaultValue;
        }
        if (v.isEmpty() || v.equalsIgnoreCase("true")) {
            return true;
        }
        if (v.equalsIgnoreCase("false")) {
            return false;
        }
        throw new HttpStatusException(400, "Parameter '" + name + "' must be true or false");
    }

    public Map<String, List<String>> paramMap() {
        if (params == null) {
            Map<String, List<String>> m = new LinkedHashMap<>();
            parseInto(m, request.query());
            String type = request.headers().get("content-type");
            if (type != null && type.regionMatches(true, 0, "application/x-www-form-urlencoded", 0, 33)) {
                parseInto(m, new String(request.body(), java.nio.charset.StandardCharsets.ISO_8859_1));
            }
            params = Collections.unmodifiableMap(m);
        }
        return params;
    }

    private static void parseInto(Map<String, List<String>> into, String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return;
        }
        int pos = 0;
        while (pos <= encoded.length()) {
            int amp = encoded.indexOf('&', pos);
            int end = amp < 0 ? encoded.length() : amp;
            if (end > pos) { // skip empty pairs like "a=1&&b=2"
                int eq = encoded.indexOf('=', pos);
                String rawName = eq >= 0 && eq < end ? encoded.substring(pos, eq) : encoded.substring(pos, end);
                String rawValue = eq >= 0 && eq < end ? encoded.substring(eq + 1, end) : "";
                String name = Url.decode(rawName, true);
                List<String> values = into.get(name);
                if (values == null) {
                    if (into.size() >= MAX_PARAMS) {
                        throw new HttpStatusException(400, "Too many parameters");
                    }
                    values = new ArrayList<>(1);
                    into.put(name, values);
                }
                values.add(Url.decode(rawValue, true));
            }
            if (amp < 0) {
                break;
            }
            pos = amp + 1;
        }
    }

    private static int parseInt(String what, String v) {
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new HttpStatusException(400, what + " must be an integer");
        }
    }

    private static long parseLong(String what, String v) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new HttpStatusException(400, what + " must be an integer");
        }
    }
}
