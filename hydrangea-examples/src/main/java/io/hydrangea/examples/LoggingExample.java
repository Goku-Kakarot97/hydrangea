package io.hydrangea.examples;

import io.hydrangea.log.Appender;
import io.hydrangea.log.Level;
import io.hydrangea.log.Log;
import io.hydrangea.log.LogContext;
import io.hydrangea.log.LogFormat;
import io.hydrangea.log.LogStage;
import io.hydrangea.log.Logger;
import io.hydrangea.log.Registration;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * hydrangea-log: placeholders, structured fields, context, a chain of stages (redact, console, file), handlers registered per level,
 * and the framework's own System.Logger output arriving in the same chain.
 * <pre>
 * java -cp target/classes io.hydrangea.examples.LoggingExample
 * </pre>
 */
public final class LoggingExample {

    private static final Logger log = Logger.get(LoggingExample.class);

    public static void main(String[] args) throws Exception {
        Path file = Files.createTempFile("hydrangea-example", ".log");
        Log.configure()
                .level(Level.INFO)
                .level("io.hydrangea.examples", Level.DEBUG)                  // this package: DEBUG; everything else: INFO
                .stage(LogStage.map(e -> e.withMessage(e.message().replaceAll("card=\\d+", "card=****"))))   // a redaction stage
                .console()                                                   // text to stdout
                .format(LogFormat.json())
                .file(file, 0, 0)                                            // JSON to a file (the format set just before applies)
                .apply();

        // handlers by level: ERROR and above, and exactly WARN
        Registration alerts = Log.on(Level.ERROR, (e, next) -> System.out.println("ALERT " + e.logger() + ": " + e.message()));
        Log.onExactly(Level.WARN, LogStage.each(e -> System.out.println("(a warning was counted)")));

        log.debug("debug is on for this package");
        log.info("paid with card={} amount={}", 4111111111111111L, 42);       // redacted by the stage
        try (LogContext.Scope ignored = LogContext.with("request", "r-1")) {
            log.at(Level.INFO).kv("user", "ann").kv("ms", 17).log("handled");
            log.warn("slow query {}", "SELECT ...");
        }
        log.error("payment failed", new IllegalStateException("gateway down"));
        alerts.cancel();
        log.error("no alert for this one");

        System.getLogger("io.hydrangea.Server").log(System.Logger.Level.INFO, "the framework's own logging ({0}) lands here too", "System.Logger");
        Logger.get("io.hydrangea.quiet").level(Level.OFF);
        Logger.get("io.hydrangea.quiet").error("never written");

        Log.flush();
        System.out.println("\nJSON file " + file + ":");
        Files.readAllLines(file).forEach(System.out::println);
        System.out.println("\nstats: " + Log.stats());
        Files.delete(file);
    }
}
