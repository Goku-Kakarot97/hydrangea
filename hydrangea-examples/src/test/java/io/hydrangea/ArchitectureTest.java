package io.hydrangea;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The shape of the code base, as a test: which package may use which, and that the library depends on nothing but the JDK.
 * <p>
 * The layering is what lets the packages become independent modules (and new protocol clients be added) without surprises: a package
 * that quietly starts using one above it would make that impossible. Only real code counts: comments, javadoc and string literals are ignored,
 * so a {@code {@link}} to a package above is fine. When you add a package, add a line to {@link #ALLOWED}; the test refuses to guess.
 * <p>
 * "core" is the classes directly in {@code io.hydrangea}. {@code examples} may use everything and nothing may use it.
 */
class ArchitectureTest {

    /** package -> the packages it may use (besides itself). Keep this acyclic: it is the module graph. */
    private static final Map<String, Set<String>> ALLOWED = Map.ofEntries(
            Map.entry("core", Set.of()),
            Map.entry("codec", Set.of("core")),
            Map.entry("pipeline", Set.of("core", "codec")),
            Map.entry("tls", Set.of("core", "codec", "pipeline")),
            Map.entry("quic", Set.of("core")),
            Map.entry("http2", Set.of()),                       // HPACK / QPACK: pure functions on bytes
            Map.entry("mqtt", Set.of("core", "codec", "pipeline", "tls", "http", "websocket")),   // http/websocket: only MqttWebSocket
            Map.entry("amqp", Set.of("core", "codec", "pipeline", "tls")),
            Map.entry("redis", Set.of("core", "codec", "pipeline", "tls")),
            Map.entry("kafka", Set.of("core", "codec", "pipeline", "tls")),
            Map.entry("sql", Set.of()),                         // the interfaces the database clients share: JDK only
            Map.entry("postgres", Set.of("core", "codec", "pipeline", "tls", "sql")),
            Map.entry("mysql", Set.of("core", "codec", "pipeline", "tls", "sql")),
            Map.entry("micrometer", Set.of("core")),             // the one module allowed to import a third-party library (io.micrometer)
            Map.entry("http", Set.of("core", "codec", "pipeline", "tls", "http2", "quic")),
            Map.entry("websocket", Set.of("core", "codec", "pipeline", "tls", "http")),
            Map.entry("sse", Set.of("core", "pipeline", "http")),
            Map.entry("grpc", Set.of("core", "pipeline", "http")),
            Map.entry("fuzz", Set.of()),                        // test helpers (core's test sources): JDK only
            Map.entry("testing", Set.of()),
            Map.entry("examples", Set.of("core", "codec", "pipeline", "tls", "quic", "http2", "amqp", "mqtt", "redis", "kafka", "postgres", "mysql", "sql", "http", "websocket", "sse", "grpc")));

    private static final Pattern PACKAGE_REF = Pattern.compile("\\bio\\.hydrangea\\.([a-z][a-z0-9]*)\\.");
    private static final Pattern CORE_REF = Pattern.compile("\\bio\\.hydrangea\\.[A-Z]");
    private static final Pattern IMPORT = Pattern.compile("^\\s*import\\s+(?:static\\s+)?([\\w.]+)", Pattern.MULTILINE);

    /** Every {@code src/main/java} of the project: every module's. */
    static List<Path> sourceRoots() throws IOException {
        Path top = Path.of("").toAbsolutePath();
        while (top.getParent() != null && Files.exists(top.getParent().resolve("pom.xml"))) {
            top = top.getParent();
        }
        List<Path> roots = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(top, 4)) {
            walk.filter(p -> p.endsWith(Path.of("src", "main", "java")) && !p.toString().contains("/target/")
                    && !p.toString().contains("/benchmark/") && !p.toString().contains("/interop/")).forEach(roots::add);
        }
        return roots;
    }

    /** The source without comments (javadoc included) and with string / char literals emptied. */
    static String code(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            if (c == '/' && i + 1 < n && src.charAt(i + 1) == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                out.append(' ');
            } else if (src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                i = end < 0 ? n : end + 3;
                out.append("\"\"");
            } else if (c == '"' || c == '\'') {
                int j = i + 1;
                while (j < n && src.charAt(j) != c) {
                    j += src.charAt(j) == '\\' ? 2 : 1;
                }
                i = j + 1;
                out.append(c).append(c);
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** "core" for io.hydrangea.Foo, otherwise the first package segment under io.hydrangea. */
    static String packageOf(Path root, Path file) {
        Path rel = root.resolve("io").resolve("hydrangea").relativize(file);
        return rel.getNameCount() == 1 ? "core" : rel.getName(0).toString();
    }

    private record Source(String pkg, Path file, String code) {
    }

    private static List<Source> sources() throws IOException {
        List<Source> all = new ArrayList<>();
        for (Path root : sourceRoots()) {
            try (Stream<Path> walk = Files.walk(root.resolve("io").resolve("hydrangea"))) {
                for (Path f : (Iterable<Path>) walk.filter(p -> p.toString().endsWith(".java"))::iterator) {
                    if (f.getFileName().toString().equals("package-info.java") || f.getFileName().toString().equals("module-info.java")) {
                        continue;
                    }
                    all.add(new Source(packageOf(root, f), f, code(Files.readString(f))));
                }
            }
        }
        return all;
    }

    @Test
    void thereIsSomethingToCheck() throws IOException {
        assertTrue(sources().size() > 100, "found " + sources().size() + " source files: is the test looking in the right place?");
    }

    @Test
    void everyPackageHasARule() throws IOException {
        Set<String> unknown = new TreeSet<>();
        for (Source s : sources()) {
            if (!ALLOWED.containsKey(s.pkg())) {
                unknown.add(s.pkg());
            }
        }
        assertTrue(unknown.isEmpty(), "new package(s) " + unknown + ": decide what they may use and add them to ArchitectureTest.ALLOWED");
    }

    @Test
    void packagesOnlyUseWhatTheirLayerAllows() throws IOException {
        Map<String, Set<String>> violations = new TreeMap<>();
        for (Source s : sources()) {
            Set<String> allowed = ALLOWED.getOrDefault(s.pkg(), Set.of());
            Set<String> used = new TreeSet<>();
            Matcher m = PACKAGE_REF.matcher(s.code());
            while (m.find()) {
                used.add(m.group(1));
            }
            if (CORE_REF.matcher(s.code()).find()) {
                used.add("core");
            }
            used.remove(s.pkg());
            for (String u : used) {
                if (!allowed.contains(u)) {
                    violations.computeIfAbsent(s.pkg() + " must not use " + u, k -> new TreeSet<>()).add(s.file().getFileName().toString());
                }
            }
        }
        assertEquals(Map.of(), violations, "layering violations (package -> files)");
    }

    @Test
    void theLibraryDependsOnNothingButTheJdk() throws IOException {
        Map<String, Set<String>> foreign = new TreeMap<>();
        for (Source s : sources()) {
            Matcher m = IMPORT.matcher(s.code());
            while (m.find()) {
                String name = m.group(1);
                boolean jdk = name.startsWith("java.") || name.startsWith("javax.") || name.startsWith("jdk.") || name.startsWith("io.hydrangea.");
                boolean bridge = s.pkg().equals("micrometer") && name.startsWith("io.micrometer.");
                if (!jdk && !bridge) {
                    foreign.computeIfAbsent(name, k -> new TreeSet<>()).add(s.file().getFileName().toString());
                }
            }
        }
        assertEquals(Map.of(), foreign, "imports from outside the JDK (the library has no dependencies; tests and benchmarks may)");
    }

    @Test
    void theRulesThemselvesHaveNoCycle() {
        // a cycle in the allowed graph would make the packages impossible to split into modules
        for (String start : ALLOWED.keySet()) {
            assertTrue(!reaches(start, start, new TreeSet<>()), start + " can reach itself through ALLOWED");
        }
        assertTrue(ALLOWED.get("core").isEmpty(), "core uses nothing");
        for (Map.Entry<String, Set<String>> e : ALLOWED.entrySet()) {
            if (!e.getKey().equals("examples")) {
                assertTrue(!e.getValue().contains("examples"), "nothing may use examples");
            }
            assertTrue(!e.getValue().contains("fuzz") && !e.getValue().contains("testing"), "main code does not use the test helpers");
        }
    }

    private static boolean reaches(String from, String target, Set<String> seen) {
        for (String next : ALLOWED.getOrDefault(from, Set.of())) {
            if (next.equals(target) || (seen.add(next) && reaches(next, target, seen))) {
                return true;
            }
        }
        return false;
    }
}
