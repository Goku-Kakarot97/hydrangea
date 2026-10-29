package io.hydrangea.http;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A cookie store in memory following RFC 6265 (and the stricter habits of its revision): {@code Set-Cookie} parsing with the lenient date
 * algorithm, {@code Max-Age} before {@code Expires}, domain and path matching, host-only cookies, {@code Secure} cookies kept to https
 * (and not accepted from http), default paths, replacement of a cookie by one of the same name/domain/path, expiry, and limits (4096 bytes
 * per cookie, 50 per domain, 3000 in all; the oldest are dropped first).
 * <p>
 * <b>Public suffixes.</b> A page on {@code a.example.co.uk} must not be able to set a cookie for {@code co.uk}: that would be sent to every
 * site under it. Stopping that needs the Public Suffix List, which is far too large to embed. The default check refuses {@code Domain}
 * values with no dot and a short built-in list of common two-label suffixes and hosting domains; it is <em>not complete</em>. Pass a
 * predicate backed by the real list (for example from a library that ships it) to the constructor if cookies matter for your threat model.
 * Cookies without a {@code Domain} attribute (host-only) are never affected.
 * <p>
 * Not implemented: {@code SameSite} (stored, not applied: a library client has no "site" it is navigating from), cookie prefixes.
 */
public final class InMemoryCookieJar implements CookieJar {

    private static final int MAX_COOKIE_BYTES = 4096;
    private static final int MAX_ATTRIBUTE_BYTES = 1024;
    private static final int MAX_PER_DOMAIN = 50;
    private static final int MAX_TOTAL = 3000;
    private static final Duration MAX_LIFETIME = Duration.ofDays(400);

    /** Two-label public suffixes and shared-hosting domains that are very common; a small subset of the real Public Suffix List. */
    private static final Set<String> COMMON_SUFFIXES = Set.of(
            "co.uk", "org.uk", "gov.uk", "ac.uk", "me.uk", "ltd.uk", "plc.uk", "net.uk", "sch.uk",
            "com.au", "net.au", "org.au", "edu.au", "gov.au", "co.nz", "org.nz", "net.nz", "govt.nz",
            "co.jp", "ne.jp", "or.jp", "ac.jp", "go.jp", "com.cn", "net.cn", "org.cn", "gov.cn", "edu.cn",
            "com.br", "net.br", "org.br", "gov.br", "com.vn", "net.vn", "org.vn", "edu.vn", "gov.vn",
            "co.in", "net.in", "org.in", "gov.in", "co.za", "org.za", "com.sg", "edu.sg", "com.hk", "com.tw", "com.mx", "com.ar",
            "com.tr", "co.kr", "or.kr", "co.id", "co.th", "co.il",
            "github.io", "gitlab.io", "herokuapp.com", "appspot.com", "blogspot.com", "cloudfront.net", "azurewebsites.net",
            "vercel.app", "netlify.app", "pages.dev", "web.app", "firebaseapp.com", "s3.amazonaws.com");

    private record Cookie(String name, String value, String domain, boolean hostOnly, String path, boolean secure, boolean httpOnly,
                          String sameSite, Instant expires, long created) {

        boolean expired(Instant now) {
            return expires != null && !expires.isAfter(now);
        }
    }

    private final Predicate<String> publicSuffix;
    private final Map<String, Cookie> cookies = new HashMap<>(); // key: name, domain, path (RFC 6265 section 5.3 step 11)
    private long sequence;

    /** With the built-in (incomplete) public suffix check. */
    public InMemoryCookieJar() {
        this(InMemoryCookieJar::builtInPublicSuffix);
    }

    /** @param publicSuffix true if the (lower case, dot separated) domain is a public suffix such as {@code com} or {@code co.uk} */
    public InMemoryCookieJar(Predicate<String> publicSuffix) {
        this.publicSuffix = publicSuffix;
    }

    private static boolean builtInPublicSuffix(String domain) {
        return domain.indexOf('.') < 0 || COMMON_SUFFIXES.contains(domain);
    }

    /** Number of cookies currently stored (expired ones may still count until the next request). */
    public synchronized int size() {
        return cookies.size();
    }

    public synchronized void clear() {
        cookies.clear();
    }

    // ---- storing ----

    @Override
    public void store(URI requestUri, List<String> setCookieHeaders) {
        String host = host(requestUri);
        if (host == null) {
            return;
        }
        boolean https = "https".equalsIgnoreCase(requestUri.getScheme());
        for (String header : setCookieHeaders) {
            Cookie c = parse(header, host, https, requestUri.getRawPath());
            if (c != null) {
                put(c, https);
            }
        }
    }

    private Cookie parse(String header, String host, boolean https, String requestPath) {
        if (header == null || header.length() > MAX_COOKIE_BYTES + MAX_ATTRIBUTE_BYTES * 8) {
            return null;
        }
        String[] parts = header.split(";", -1);
        String pair = parts[0];
        int eq = pair.indexOf('=');
        if (eq < 0) {
            return null;
        }
        String name = pair.substring(0, eq).trim();
        String value = pair.substring(eq + 1).trim();
        if (name.isEmpty() || name.length() + value.length() > MAX_COOKIE_BYTES || hasControl(name) || hasControl(value)) {
            return null;
        }
        String domain = null;
        String path = null;
        boolean secure = false, httpOnly = false;
        String sameSite = null;
        Long maxAge = null;
        Instant expires = null;
        for (int i = 1; i < parts.length; i++) {
            String attr = parts[i].trim();
            if (attr.isEmpty() || attr.length() > MAX_ATTRIBUTE_BYTES) {
                continue;
            }
            int e = attr.indexOf('=');
            String an = (e < 0 ? attr : attr.substring(0, e)).trim().toLowerCase(Locale.ROOT);
            String av = e < 0 ? "" : attr.substring(e + 1).trim();
            switch (an) {
                case "expires" -> {
                    Instant d = parseDate(av);
                    if (d != null) expires = d;
                }
                case "max-age" -> {
                    if (av.matches("-?[0-9]{1,18}")) maxAge = Long.parseLong(av);
                }
                case "domain" -> {
                    if (!av.isEmpty()) domain = (av.startsWith(".") ? av.substring(1) : av).toLowerCase(Locale.ROOT);
                }
                case "path" -> path = av.startsWith("/") ? av : null;
                case "secure" -> secure = true;
                case "httponly" -> httpOnly = true;
                case "samesite" -> sameSite = av;
                default -> { }
            }
        }
        if (secure && !https) {
            return null; // an insecure origin may not set (or replace) a Secure cookie
        }
        boolean hostOnly = true;
        String cookieDomain = host;
        if (domain != null && !domain.isEmpty()) {
            if (publicSuffix.test(domain)) {
                if (!domain.equals(host)) {
                    return null; // a cookie for "co.uk" from some site below it
                }
            } else {
                if (!domainMatches(host, domain)) {
                    return null;
                }
                hostOnly = false;
                cookieDomain = domain;
            }
        }
        if (path == null) {
            path = defaultPath(requestPath);
        }
        Instant now = Instant.now();
        Instant expiry = null;
        if (maxAge != null) {
            expiry = maxAge <= 0 ? Instant.EPOCH : now.plusSeconds(Math.min(maxAge, MAX_LIFETIME.toSeconds()));
        } else if (expires != null) {
            Instant cap = now.plus(MAX_LIFETIME);
            expiry = expires.isAfter(cap) ? cap : expires;
        }
        synchronized (this) {
            return new Cookie(name, value, cookieDomain, hostOnly, path, secure, httpOnly, sameSite, expiry, sequence++);
        }
    }

    private static String key(Cookie c) {
        return c.name() + '\0' + c.domain() + '\0' + c.path();
    }

    private synchronized void put(Cookie c, boolean fromHttps) {
        String key = key(c);
        Instant now = Instant.now();
        Cookie existing = cookies.get(key);
        if (existing != null && existing.secure() && !fromHttps && !existing.expired(now)) {
            return; // "leave secure cookies alone": an insecure response may not replace or delete a Secure cookie
        }
        if (c.expired(now)) { // Max-Age=0 / a date in the past: delete
            cookies.remove(key);
            return;
        }
        Cookie old = cookies.get(key);
        cookies.put(key, old == null ? c : new Cookie(c.name(), c.value(), c.domain(), c.hostOnly(), c.path(), c.secure(), c.httpOnly(),
                c.sameSite(), c.expires(), old.created())); // a replaced cookie keeps its creation order
        evict(c.domain(), now);
    }

    /** Drops expired cookies, then the oldest of an over-full domain, then the oldest overall. */
    private void evict(String domain, Instant now) {
        cookies.values().removeIf(x -> x.expired(now));
        List<Cookie> sameDomain = new ArrayList<>();
        for (Cookie x : cookies.values()) {
            if (x.domain().equals(domain)) sameDomain.add(x);
        }
        sameDomain.sort(Comparator.comparingLong(Cookie::created));
        for (int i = 0; sameDomain.size() - i > MAX_PER_DOMAIN; i++) {
            Cookie victim = sameDomain.get(i);
            cookies.remove(key(victim));
        }
        if (cookies.size() > MAX_TOTAL) {
            List<Cookie> all = new ArrayList<>(cookies.values());
            all.sort(Comparator.comparingLong(Cookie::created));
            for (int i = 0; all.size() - i > MAX_TOTAL; i++) {
                Cookie victim = all.get(i);
                cookies.remove(key(victim));
            }
        }
    }

    // ---- sending ----

    @Override
    public synchronized String cookieHeader(URI uri) {
        String host = host(uri);
        if (host == null) {
            return null;
        }
        boolean https = "https".equalsIgnoreCase(uri.getScheme());
        String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
        Instant now = Instant.now();
        List<Cookie> matching = new ArrayList<>();
        cookies.values().removeIf(c -> c.expired(now));
        for (Cookie c : cookies.values()) {
            boolean domainOk = c.hostOnly() ? host.equals(c.domain()) : domainMatches(host, c.domain());
            if (domainOk && pathMatches(path, c.path()) && (!c.secure() || https)) {
                matching.add(c);
            }
        }
        if (matching.isEmpty()) {
            return null;
        }
        matching.sort(Comparator.comparingInt((Cookie c) -> -c.path().length()).thenComparingLong(Cookie::created));
        StringBuilder sb = new StringBuilder();
        for (Cookie c : matching) {
            if (sb.length() > 0) sb.append("; ");
            sb.append(c.name()).append('=').append(c.value());
        }
        return sb.toString();
    }

    // ---- RFC 6265 algorithms ----

    private static String host(URI uri) {
        String h = uri.getHost();
        if (h == null) {
            return null;
        }
        h = h.toLowerCase(Locale.ROOT);
        return h.startsWith("[") && h.endsWith("]") ? h.substring(1, h.length() - 1) : h;
    }

    private static boolean isIpLiteral(String host) {
        return host.indexOf(':') >= 0 || host.matches("[0-9]{1,3}(\\.[0-9]{1,3}){3}");
    }

    /** RFC 6265 section 5.1.3. */
    static boolean domainMatches(String host, String domain) {
        if (host.equals(domain)) {
            return true;
        }
        return host.endsWith("." + domain) && !isIpLiteral(host);
    }

    /** RFC 6265 section 5.1.4. */
    static boolean pathMatches(String requestPath, String cookiePath) {
        if (requestPath.equals(cookiePath)) {
            return true;
        }
        if (requestPath.startsWith(cookiePath)) {
            return cookiePath.endsWith("/") || requestPath.charAt(cookiePath.length()) == '/';
        }
        return false;
    }

    /** RFC 6265 section 5.1.4: the directory of the request path. */
    static String defaultPath(String requestPath) {
        if (requestPath == null || requestPath.isEmpty() || requestPath.charAt(0) != '/') {
            return "/";
        }
        int last = requestPath.lastIndexOf('/');
        return last == 0 ? "/" : requestPath.substring(0, last);
    }

    private static boolean hasControl(String s) {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < 0x20 && c != '\t' || c == 0x7f) {
                return true;
            }
        }
        return false;
    }

    private static final String[] MONTHS = {"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};

    /** RFC 6265 section 5.1.1: the forgiving date parser (any delimiters, any order of day/month/year/time). */
    static Instant parseDate(String s) {
        int hour = -1, minute = -1, second = -1, day = -1, month = -1, year = -1;
        for (String token : s.split("[\\x09\\x20-\\x2f\\x3b-\\x40\\x5b-\\x60\\x7b-\\x7e]+")) {
            if (token.isEmpty()) {
                continue;
            }
            if (hour < 0 && token.matches("[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.*")) {
                String[] t = token.split("[^0-9]", 4);
                hour = Integer.parseInt(t[0]);
                minute = Integer.parseInt(t[1]);
                second = Integer.parseInt(t[2]);
            } else if (day < 0 && token.matches("[0-9]{1,2}([^0-9].*)?")) {
                day = Integer.parseInt(token.replaceAll("^([0-9]{1,2}).*$", "$1"));
            } else if (month < 0 && token.length() >= 3) {
                String m = token.substring(0, 3).toLowerCase(Locale.ROOT);
                for (int i = 0; i < 12; i++) {
                    if (MONTHS[i].equals(m)) {
                        month = i + 1;
                        break;
                    }
                }
            } else if (year < 0 && token.matches("[0-9]{2,4}([^0-9].*)?")) {
                year = Integer.parseInt(token.replaceAll("^([0-9]{2,4}).*$", "$1"));
            }
        }
        if (hour < 0 || day < 0 || month < 0 || year < 0) {
            return null;
        }
        if (year >= 70 && year <= 99) {
            year += 1900;
        } else if (year >= 0 && year <= 69) {
            year += 2000;
        }
        if (day < 1 || day > 31 || year < 1601 || hour > 23 || minute > 59 || second > 59) {
            return null;
        }
        try {
            return java.time.LocalDateTime.of(year, month, day, hour, minute, second).toInstant(java.time.ZoneOffset.UTC);
        } catch (java.time.DateTimeException e) {
            return null; // 31 February
        }
    }
}
