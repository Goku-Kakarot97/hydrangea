package io.hydrangea.log;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Properties;
import java.util.TreeSet;

/**
 * The configuration file, {@code hydrangea-log.properties}:
 * <pre>
 * level=INFO                      # root level
 * level.io.hydrangea.http=DEBUG   # a prefix
 * async=true
 * buffer=65536                    # events in the ring buffer
 * full=block                      # or drop
 * format=line                     # or json
 * zone=UTC                        # timestamps; the system zone for line, UTC for json by default
 * short=false                     # line format: only the last segment of the logger name
 * console=out                     # or err, or off
 * file=/var/log/app.log
 * file.maxBytes=104857600
 * file.maxFiles=10
 * </pre>
 * Every key is also a system property with the prefix {@code hydrangea.log.} ({@code -Dhydrangea.log.level=DEBUG}), which wins over
 * the file; {@code -Dhydrangea.log.config=/path/file.properties} names another file. The values are data: no class names, no URLs.
 * A bad value is reported on {@code System.err} and the default used; an unknown key likewise.
 */
final class LogProperties {

    static final String RESOURCE = "hydrangea-log.properties";
    static final String PREFIX = "hydrangea.log.";

    private LogProperties() {
    }

    static LogConfig load() {
        Properties p = new Properties();
        String path = System.getProperty(PREFIX + "config");
        if (path != null) {
            try (InputStream in = Files.newInputStream(Path.of(path))) {
                p.load(in);
            } catch (IOException e) {
                Log.warn("cannot read " + path + ": " + e);
            }
        } else {
            InputStream in = resource();
            if (in != null) {
                try (in) {
                    p.load(in);
                } catch (IOException e) {
                    Log.warn("cannot read " + RESOURCE + ": " + e);
                }
            }
        }
        Properties system = System.getProperties();
        for (String key : system.stringPropertyNames()) {
            if (key.startsWith(PREFIX) && !key.equals(PREFIX + "config")) {
                p.setProperty(key.substring(PREFIX.length()), system.getProperty(key));
            }
        }
        return parse(p);
    }

    private static InputStream resource() {
        ClassLoader cl = Log.class.getClassLoader();
        InputStream in = cl == null ? ClassLoader.getSystemResourceAsStream(RESOURCE) : cl.getResourceAsStream(RESOURCE);
        if (in == null) {
            ClassLoader context = Thread.currentThread().getContextClassLoader();
            if (context != null && context != cl) {
                in = context.getResourceAsStream(RESOURCE);
            }
        }
        return in;
    }

    static LogConfig parse(Properties p) {
        LogConfig c = new LogConfig();
        boolean json = "json".equalsIgnoreCase(p.getProperty("format", "line"));
        if (!json && !"line".equalsIgnoreCase(p.getProperty("format", "line"))) {
            Log.warn("format must be line or json, not " + p.getProperty("format"));
        }
        ZoneId zone = null;
        String z = p.getProperty("zone");
        if (z != null) {
            try {
                zone = ZoneId.of(z.trim());
            } catch (RuntimeException e) {
                Log.warn("bad zone " + z);
            }
        }
        boolean shortNames = bool(p, "short", false);
        c.format(json ? (zone == null ? LogFormat.json() : new JsonFormat(zone))
                : (zone == null && !shortNames ? LogFormat.line() : new LineFormat(zone == null ? ZoneId.systemDefault() : zone, shortNames)));
        c.async(bool(p, "async", true));
        c.buffer((int) Math.min(Integer.MAX_VALUE, integer(p, "buffer", c.buffer, 2)));
        String full = p.getProperty("full", "block").trim();
        if (full.equalsIgnoreCase("drop")) {
            c.dropWhenFull(true);
        } else if (!full.equalsIgnoreCase("block")) {
            Log.warn("full must be block or drop, not " + full);
        }
        String console = p.getProperty("console", "out").trim();
        switch (console.toLowerCase()) {
            case "out" -> c.console();
            case "err" -> c.stderr();
            case "off" -> { }
            default -> {
                Log.warn("console must be out, err or off, not " + console);
                c.console();
            }
        }
        String file = p.getProperty("file");
        if (file != null && !file.isBlank()) {
            c.file(Path.of(file.trim()), integer(p, "file.maxBytes", FileAppender.DEFAULT_MAX_BYTES, 0), (int) integer(p, "file.maxFiles", FileAppender.DEFAULT_MAX_FILES, 0));
        }
        for (String key : new TreeSet<>(p.stringPropertyNames())) {
            String v = p.getProperty(key);
            if (key.equals("level")) {
                level(c, null, v);
            } else if (key.startsWith("level.")) {
                level(c, key.substring("level.".length()), v);
            } else if (!key.equals("format") && !key.equals("zone") && !key.equals("short") && !key.equals("async") && !key.equals("buffer")
                    && !key.equals("full") && !key.equals("console") && !key.equals("file") && !key.equals("file.maxBytes") && !key.equals("file.maxFiles")) {
                Log.warn("unknown key " + key);
            }
        }
        return c;
    }

    private static void level(LogConfig c, String logger, String value) {
        try {
            Level l = Level.parse(value);
            if (logger == null) {
                c.level(l);
            } else {
                c.level(logger, l);
            }
        } catch (IllegalArgumentException e) {
            Log.warn(e.getMessage());
        }
    }

    private static boolean bool(Properties p, String key, boolean dflt) {
        String v = p.getProperty(key);
        if (v == null) {
            return dflt;
        }
        v = v.trim();
        if (v.equalsIgnoreCase("true")) {
            return true;
        }
        if (v.equalsIgnoreCase("false")) {
            return false;
        }
        Log.warn(key + " must be true or false, not " + v);
        return dflt;
    }

    private static long integer(Properties p, String key, long dflt, long min) {
        String v = p.getProperty(key);
        if (v == null) {
            return dflt;
        }
        try {
            long n = Long.parseLong(v.trim());
            if (n < min) {
                Log.warn(key + " must be at least " + min);
                return dflt;
            }
            return n;
        } catch (NumberFormatException e) {
            Log.warn(key + " must be a number, not " + v);
            return dflt;
        }
    }
}
