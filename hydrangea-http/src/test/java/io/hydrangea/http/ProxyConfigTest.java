package io.hydrangea.http;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProxyConfigTest {

    @Test
    void environmentVariablesAreUnderstood() {
        Map<String, String> env = Map.of("HTTP_PROXY", "http://bob:s%40cret@proxy.corp:3128", "https_proxy", "proxy.corp:3129", "NO_PROXY", "localhost, .internal ,10.0.0.5");
        ProxyConfig c = ProxyConfig.fromEnvironment(env::get);
        assertEquals("proxy.corp", c.http().host());
        assertEquals(3128, c.http().port());
        assertEquals("bob", c.http().username());
        assertEquals("s@cret", c.http().password());
        assertEquals(3129, c.https().port());
        assertNull(c.https().username());
        assertEquals(java.util.List.of("localhost", ".internal", "10.0.0.5"), c.noProxy());
        assertNull(ProxyConfig.fromEnvironment(Map.<String, String>of()::get));
        assertThrows(IllegalArgumentException.class, () -> ProxyConfig.fromEnvironment(Map.of("HTTP_PROXY", "https://secure-proxy:443")::get));
    }

    @Test
    void theRightProxyIsChosenPerScheme() {
        ProxyConfig c = new ProxyConfig(new ProxyConfig.Endpoint("a", 1, null, null), null, java.util.List.of());
        assertEquals("a", c.forUri(URI.create("http://example.com/")).host());
        assertNull(c.forUri(URI.create("https://example.com/")));
    }

    @Test
    void noProxyPatterns() {
        ProxyConfig c = ProxyConfig.all("p", 1).withNoProxy("example.com", ".internal", "*.corp.net", "127.0.0.1", "[::1]");
        for (String bypassed : new String[]{"http://example.com/", "http://www.example.com/", "http://a.b.example.com/", "http://x.internal/", "http://internal/",
                "http://svc.corp.net/", "http://127.0.0.1:8080/", "http://[::1]/", "http://EXAMPLE.COM/"}) {
            assertNull(c.forUri(URI.create(bypassed)), bypassed);
        }
        for (String proxied : new String[]{"http://notexample.com/", "http://example.org/", "http://corp.net.evil.com/", "http://127.0.0.2/"}) {
            assertNotNull(c.forUri(URI.create(proxied)), proxied);
        }
        assertNull(ProxyConfig.all("p", 1).withNoProxy("*").forUri(URI.create("http://anything/")));
    }

    @Test
    void basicCredentialsAreEncodedAsUtf8() {
        String v = new ProxyConfig.Endpoint("h", 1, "alice", "päss").authorization();
        assertEquals("Basic " + java.util.Base64.getEncoder().encodeToString("alice:päss".getBytes(java.nio.charset.StandardCharsets.UTF_8)), v);
        assertNull(new ProxyConfig.Endpoint("h", 1, null, null).authorization());
    }
}
