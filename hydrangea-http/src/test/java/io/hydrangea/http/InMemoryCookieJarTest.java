package io.hydrangea.http;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InMemoryCookieJarTest {

    private static URI u(String s) {
        return URI.create(s);
    }

    @Test
    void aSimpleCookieComesBackOnTheSameHostOnly() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/a/b"), List.of("sid=abc123"));
        assertEquals("sid=abc123", jar.cookieHeader(u("http://example.com/a/b")));
        assertNull(jar.cookieHeader(u("http://example.com/")), "the default path of /a/b is /a, which does not cover /");
        assertNull(jar.cookieHeader(u("http://other.com/a/b")));
        assertNull(jar.cookieHeader(u("http://www.example.com/a/b")), "a host-only cookie is not sent to subdomains");
    }

    @Test
    void theDefaultPathIsTheDirectoryOfTheRequestPath() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/a/b/page"), List.of("p=1"));
        assertEquals("p=1", jar.cookieHeader(u("http://example.com/a/b/other")));
        assertEquals("p=1", jar.cookieHeader(u("http://example.com/a/b/c/d")));
        assertNull(jar.cookieHeader(u("http://example.com/a/x")));
        assertNull(jar.cookieHeader(u("http://example.com/a/bc")), "/a/b must not match /a/bc");
        assertEquals("/", InMemoryCookieJar.defaultPath("/x"));
        assertEquals("/", InMemoryCookieJar.defaultPath(null));
    }

    @Test
    void explicitPathsAndTheOrderLongestPathFirst() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("a=root; Path=/", "b=deep; Path=/x/y", "c=mid; Path=/x"));
        assertEquals("b=deep; c=mid; a=root", jar.cookieHeader(u("http://example.com/x/y/z")));
        assertEquals("c=mid; a=root", jar.cookieHeader(u("http://example.com/x/q")));
        assertEquals("a=root", jar.cookieHeader(u("http://example.com/other")));
    }

    @Test
    void domainCookiesReachSubdomainsButOnlyIfTheServerMayClaimThem() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://www.example.com/"), List.of("shared=1; Domain=example.com", "mine=1"));
        assertEquals("shared=1", jar.cookieHeader(u("http://api.example.com/")));
        assertEquals("shared=1", jar.cookieHeader(u("http://example.com/")));
        assertEquals("shared=1; mine=1", jar.cookieHeader(u("http://www.example.com/")), "equal paths: creation order");
        assertNull(jar.cookieHeader(u("http://notexample.com/")), "suffix match must be at a label boundary");
        jar.store(u("http://www.example.com/"), List.of("evil=1; Domain=other.com"));
        assertNull(jar.cookieHeader(u("http://other.com/")), "a server cannot set a cookie for a domain it is not in");
        jar.store(u("http://www.example.com/"), List.of("leading=1; Domain=.example.com"));
        assertTrue(jar.cookieHeader(u("http://x.example.com/")).contains("leading=1"), "a leading dot is ignored");
    }

    @Test
    void publicSuffixesCannotBeClaimed() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://a.example.com/"), List.of("super=1; Domain=com"));
        jar.store(u("http://a.example.co.uk/"), List.of("super2=1; Domain=co.uk"));
        jar.store(u("http://someone.github.io/"), List.of("super3=1; Domain=github.io"));
        assertEquals(0, jar.size());
        assertNull(jar.cookieHeader(u("http://other.com/")));
        // a host that is itself the suffix (localhost) may set a cookie for itself
        jar.store(u("http://localhost/"), List.of("dev=1; Domain=localhost"));
        assertEquals("dev=1", jar.cookieHeader(u("http://localhost/")));
        // a stricter list can be plugged in
        InMemoryCookieJar strict = new InMemoryCookieJar(d -> d.equals("example.com") || d.indexOf('.') < 0);
        strict.store(u("http://a.example.com/"), List.of("x=1; Domain=example.com"));
        assertEquals(0, strict.size());
    }

    @Test
    void ipAddressesGetNoSubdomainMatching() {
        assertFalse(InMemoryCookieJar.domainMatches("10.1.2.3", "2.3"));
        assertTrue(InMemoryCookieJar.domainMatches("10.1.2.3", "10.1.2.3"));
        assertTrue(InMemoryCookieJar.domainMatches("a.b.example.com", "example.com"));
    }

    @Test
    void secureCookiesStayOnHttpsAndCannotBeSetOrReplacedFromHttp() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("https://example.com/"), List.of("s=1; Secure"));
        assertEquals("s=1", jar.cookieHeader(u("https://example.com/")));
        assertNull(jar.cookieHeader(u("http://example.com/")));
        jar.store(u("http://example.com/"), List.of("s=2"));                        // an insecure response must not replace it
        jar.store(u("http://example.com/"), List.of("s=3; Max-Age=0"));             // ...nor delete it
        assertEquals("s=1", jar.cookieHeader(u("https://example.com/")));
        jar.store(u("http://example.com/"), List.of("t=1; Secure"));                // ...nor create a Secure one
        assertEquals("s=1", jar.cookieHeader(u("https://example.com/")));
    }

    @Test
    void maxAgeExpiresAndBeatsExpires() throws Exception {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("gone=1; Max-Age=0", "kept=1; Max-Age=3600", "mixed=1; Max-Age=3600; Expires=Thu, 01 Jan 1970 00:00:01 GMT",
                "past=1; Expires=Wed, 09 Jun 2021 10:18:14 GMT"));
        assertEquals("kept=1; mixed=1", jar.cookieHeader(u("http://example.com/")));
        jar.store(u("http://example.com/"), List.of("kept=1; Max-Age=0"));          // a later Max-Age=0 deletes it
        assertEquals("mixed=1", jar.cookieHeader(u("http://example.com/")));
    }

    @Test
    void shortLivedCookiesReallyExpire() throws Exception {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("brief=1; Max-Age=1"));
        assertEquals("brief=1", jar.cookieHeader(u("http://example.com/")));
        Thread.sleep(1300);
        assertNull(jar.cookieHeader(u("http://example.com/")));
    }

    @Test
    void aNewCookieWithTheSameNameDomainAndPathReplacesTheOldOne() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("a=1", "b=1"));
        jar.store(u("http://example.com/"), List.of("a=2"));
        assertEquals("a=2; b=1", jar.cookieHeader(u("http://example.com/")), "the replaced cookie keeps its place in the order");
        jar.store(u("http://example.com/x/"), List.of("a=3; Path=/x"));
        assertEquals("a=3; a=2; b=1", jar.cookieHeader(u("http://example.com/x/y")));
    }

    @Test
    void malformedAndOversizedCookiesAreIgnored() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("novalue", "=nameless", "bad\u0001ctl=1", "huge=" + "x".repeat(5000), "ok=1"));
        assertEquals("ok=1", jar.cookieHeader(u("http://example.com/")));
    }

    @Test
    void theLimitPerDomainDropsTheOldestCookies() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        for (int i = 0; i < 60; i++) {
            jar.store(u("http://example.com/"), List.of("c" + i + "=v"));
        }
        assertEquals(50, jar.size());
        String header = jar.cookieHeader(u("http://example.com/"));
        assertFalse(header.contains("c0=") || header.contains("c9="), "the oldest are gone");
        assertTrue(header.contains("c59=v"));
    }

    @Test
    void theLenientDateParserAcceptsTheFormatsServersSend() {
        Instant expected = Instant.parse("2025-10-21T07:28:00Z");
        for (String d : new String[]{"Tue, 21 Oct 2025 07:28:00 GMT", "Tuesday, 21-Oct-25 07:28:00 GMT", "Tue Oct 21 07:28:00 2025", "21 Oct 2025 07:28:00",
                "Tue, 21-Oct-2025 07:28:00 UTC", "21-oct-25 7:28:0"}) {
            assertEquals(expected, InMemoryCookieJar.parseDate(d), d);
        }
        assertEquals(Instant.parse("1999-01-02T03:04:05Z"), InMemoryCookieJar.parseDate("Sat, 02 Jan 99 03:04:05 GMT"), "two digit years 70-99 are 19xx");
        assertNull(InMemoryCookieJar.parseDate("garbage"));
        assertNull(InMemoryCookieJar.parseDate("31 Feb 2025 00:00:00"));
        assertNull(InMemoryCookieJar.parseDate("21 Oct 2025 25:00:00"));
    }

    @Test
    void lifetimeIsCapped() {
        InMemoryCookieJar jar = new InMemoryCookieJar();
        jar.store(u("http://example.com/"), List.of("forever=1; Max-Age=999999999999"));
        assertEquals("forever=1", jar.cookieHeader(u("http://example.com/")));
    }
}
