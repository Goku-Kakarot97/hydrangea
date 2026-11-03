package io.hydrangea.log;

import java.util.ResourceBundle;

/**
 * Makes this library the backend of {@link System.Logger}, the JDK's own logging facade: anything that logs through
 * {@code System.getLogger(...)} (every Hydrangea module does) lands in the chain configured here, with no change to that code.
 * Picked up automatically through {@code META-INF/services/java.lang.System$LoggerFinder} when {@code hydrangea-log} is on the
 * class path; the JDK takes the first finder it sees, so keep only one such backend on the class path.
 * <p>
 * {@code {0}}-style parameters are substituted as plain text ({@link java.text.MessageFormat} patterns such as {@code {0,number}}
 * are not interpreted).
 */
public final class HydrangeaLoggerFinder extends System.LoggerFinder {

    @Override
    public System.Logger getLogger(String name, Module module) {
        return new Bridge(Log.logger(name));
    }

    static Level map(System.Logger.Level level) {
        return switch (level) {
            case ALL, TRACE -> Level.TRACE;
            case DEBUG -> Level.DEBUG;
            case INFO -> Level.INFO;
            case WARNING -> Level.WARN;
            case ERROR -> Level.ERROR;
            case OFF -> Level.OFF;
        };
    }

    static final class Bridge implements System.Logger {
        private final Logger logger;

        Bridge(Logger logger) {
            this.logger = logger;
        }

        @Override
        public String getName() {
            return logger.name();
        }

        @Override
        public boolean isLoggable(System.Logger.Level level) {
            return logger.isEnabled(map(level));
        }

        @Override
        public void log(System.Logger.Level level, ResourceBundle bundle, String msg, Throwable thrown) {
            io.hydrangea.log.Level l = map(level);
            if (logger.isEnabled(l)) {
                logger.emit(l, localize(bundle, msg), thrown, null, 0);
            }
        }

        @Override
        public void log(System.Logger.Level level, ResourceBundle bundle, String format, Object... params) {
            io.hydrangea.log.Level l = map(level);
            if (logger.isEnabled(l)) {
                logger.emit(l, Messages.renderIndexed(localize(bundle, format), params), null, null, 0);
            }
        }

        private static String localize(ResourceBundle bundle, String key) {
            if (bundle != null && key != null && bundle.containsKey(key)) {
                try {
                    return bundle.getString(key);
                } catch (RuntimeException ignored) {
                    // not a string: use the key
                }
            }
            return key;
        }
    }
}
