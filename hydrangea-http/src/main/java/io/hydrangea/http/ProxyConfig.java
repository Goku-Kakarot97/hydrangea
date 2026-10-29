package io.hydrangea.http;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Forward proxies for {@link HttpClient}. Requests for {@code http://} URLs go to the proxy in absolute form
 * ({@code GET http://host/path HTTP/1.1}); {@code https://} URLs (and HTTP/2 prior-knowledge cleartext) are tunnelled with
 * {@code CONNECT host:port}, so the TLS handshake and certificate check are with the origin, not the proxy. The origin's name is
 * never resolved locally when a proxy is used. The proxy itself is reached over plain TCP (HTTP proxies; no SOCKS, no TLS to the proxy).
 * <p>
 * Credentials are sent preemptively as {@code Proxy-Authorization: Basic}; a proxy that answers 407 fails the request.
 *
 * @param http    proxy for {@code http://} URLs, or null for a direct connection
 * @param https   proxy for {@code https://} URLs, or null
 * @param noProxy hosts that bypass the proxy: {@code *} (everything), {@code example.com} or {@code .example.com} or {@code *.example.com}
 *                (that domain and its subdomains), or an exact host / IP literal
 */
public record ProxyConfig(Endpoint http, Endpoint https, List<String> noProxy) {

    /** Where a proxy listens and, optionally, who to be at it. */
    public record Endpoint(String host, int port, String username, String password) {
        public Endpoint {
            if (host == null || host.isEmpty() || port < 1 || port > 65535) {
                throw new IllegalArgumentException("invalid proxy address " + host + ":" + port);
            }
        }

        /** The {@code Proxy-Authorization} value, or null without credentials. */
        String authorization() {
            if (username == null) {
                return null;
            }
            String raw = username + ":" + (password == null ? "" : password);
            return "Basic " + java.util.Base64.getEncoder().encodeToString(raw.getBytes(StandardCharsets.UTF_8));
        }
    }

    public ProxyConfig {
        noProxy = List.copyOf(noProxy);
    }

    /** The same proxy for http and https. */
    public static ProxyConfig all(String host, int port) {
        Endpoint e = new Endpoint(host, port, null, null);
        return new ProxyConfig(e, e, List.of());
    }

    public ProxyConfig withCredentials(String username, String password) {
        return new ProxyConfig(http == null ? null : new Endpoint(http.host(), http.port(), username, password),
                https == null ? null : new Endpoint(https.host(), https.port(), username, password), noProxy);
    }

    public ProxyConfig withNoProxy(String... patterns) {
        List<String> all = new ArrayList<>(noProxy);
        all.addAll(List.of(patterns));
        return new ProxyConfig(http, https, all);
    }

    /**
     * The conventional environment variables: {@code HTTP_PROXY}, {@code HTTPS_PROXY} and {@code NO_PROXY} (lower case names work too).
     * Values may be {@code http://user:pass@host:port} or {@code host:port}. Returns null if neither proxy variable is set.
     */
    public static ProxyConfig fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    static ProxyConfig fromEnvironment(java.util.function.Function<String, String> env) {
        Endpoint http = parseEndpoint(first(env, "HTTP_PROXY", "http_proxy"));
        Endpoint https = parseEndpoint(first(env, "HTTPS_PROXY", "https_proxy"));
        if (http == null && https == null) {
            return null;
        }
        String no = first(env, "NO_PROXY", "no_proxy");
        List<String> patterns = new ArrayList<>();
        if (no != null) {
            for (String p : no.split(",")) {
                if (!p.isBlank()) {
                    patterns.add(p.trim());
                }
            }
        }
        return new ProxyConfig(http, https, patterns);
    }

    private static String first(java.util.function.Function<String, String> env, String a, String b) {
        String v = env.apply(a);
        return v != null && !v.isBlank() ? v : env.apply(b);
    }

    private static Endpoint parseEndpoint(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String v = value.trim();
        if (!v.contains("://")) {
            v = "http://" + v;
        }
        URI u = URI.create(v);
        if (!"http".equalsIgnoreCase(u.getScheme())) {
            throw new IllegalArgumentException("only http:// proxies are supported, got " + u.getScheme() + "://");
        }
        String user = null, pass = null;
        if (u.getRawUserInfo() != null) {
            String ui = u.getRawUserInfo();
            int c = ui.indexOf(':');
            user = URLDecoder.decode(c < 0 ? ui : ui.substring(0, c), StandardCharsets.UTF_8);
            pass = c < 0 ? "" : URLDecoder.decode(ui.substring(c + 1), StandardCharsets.UTF_8);
        }
        return new Endpoint(u.getHost(), u.getPort() != -1 ? u.getPort() : 80, user, pass);
    }

    /** The proxy to use for {@code uri}, or null for a direct connection. */
    Endpoint forUri(URI uri) {
        Endpoint e = "https".equalsIgnoreCase(uri.getScheme()) ? https : http;
        if (e == null || bypasses(uri.getHost())) {
            return null;
        }
        return e;
    }

    private boolean bypasses(String host) {
        if (host == null) {
            return true;
        }
        String h = host.toLowerCase(Locale.ROOT);
        if (h.startsWith("[") && h.endsWith("]")) {
            h = h.substring(1, h.length() - 1);
        }
        for (String raw : noProxy) {
            String p = raw.toLowerCase(Locale.ROOT).trim();
            if (p.startsWith("[") && p.endsWith("]")) {
                p = p.substring(1, p.length() - 1);
            }
            if (p.equals("*")) {
                return true;
            }
            if (p.startsWith("*.")) {
                p = p.substring(1);
            }
            if (p.startsWith(".")) {
                if (h.endsWith(p) || h.equals(p.substring(1))) {
                    return true;
                }
            } else if (h.equals(p) || h.endsWith("." + p)) {
                return true;
            }
        }
        return false;
    }
}
