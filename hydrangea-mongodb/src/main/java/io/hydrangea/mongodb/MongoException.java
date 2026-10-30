package io.hydrangea.mongodb;

import java.util.List;
import java.util.Map;

/**
 * What a command fails with. Either the server answered with an error ({@link #code}, {@link #codeName}, its message; {@link #fromServer} true), or a write command
 * succeeded as a whole while some of its writes failed ({@link #writeErrors}: {@code index}, {@code code}, {@code errmsg} each), or there was no answer
 * ({@link #code} 0: connection lost, timeout, client closed; {@link #retriable} says whether the command may be sent again without having run twice).
 */
public final class MongoException extends RuntimeException {

    private final int code;
    private final String codeName;
    private final boolean fromServer;
    private final boolean retriable;
    private final boolean timeout;
    private final List<Map<String, Object>> writeErrors;
    private final Map<String, Object> reply;
    private final List<String> labels;
    private final boolean lost;

    MongoException(int code, String codeName, String message, boolean fromServer, boolean retriable, boolean timeout, List<Map<String, Object>> writeErrors, Map<String, Object> reply, Throwable cause) {
        this(code, codeName, message, fromServer, retriable, timeout, writeErrors, reply, cause, false);
    }

    private MongoException(int code, String codeName, String message, boolean fromServer, boolean retriable, boolean timeout, List<Map<String, Object>> writeErrors, Map<String, Object> reply, Throwable cause, boolean lost) {
        super(message, cause);
        this.code = code;
        this.codeName = codeName;
        this.fromServer = fromServer;
        this.retriable = retriable;
        this.timeout = timeout;
        this.writeErrors = writeErrors == null ? List.of() : List.copyOf(writeErrors);
        this.reply = reply;
        this.lost = lost;
        List<String> l = new java.util.ArrayList<>();
        if (reply != null && reply.get("errorLabels") instanceof List<?> ls) {
            for (Object o : ls) {
                l.add(String.valueOf(o));
            }
        }
        this.labels = List.copyOf(l);
    }

    /** No answer from the server. */
    static MongoException local(String message, boolean retriable, Throwable cause) {
        return new MongoException(0, null, message, false, retriable, false, null, null, cause, false);
    }

    /** The connection could not be made or was lost: the server may be gone, or no longer the primary. */
    static MongoException lost(String message, Throwable cause) {
        return new MongoException(0, null, message, false, true, false, null, null, cause, true);
    }

    static MongoException timeout(String message) {
        return new MongoException(0, null, message, false, false, true, null, null, null);
    }

    /** The server's {@code ok: 0} answer. */
    static MongoException of(Map<String, Object> reply) {
        int code = reply.get("code") instanceof Number n ? n.intValue() : 0;
        String codeName = reply.get("codeName") instanceof String s ? s : null;
        String msg = reply.get("errmsg") instanceof String s ? s : "command failed";
        return new MongoException(code, codeName, (codeName != null ? codeName + " (" + code + "): " : code != 0 ? "(" + code + "): " : "") + msg, true,
                isRetriableCode(code, msg), code == 50, null, reply, null);
    }

    /** A write command that ran, with {@code writeErrors} (and maybe a {@code writeConcernError}). */
    @SuppressWarnings("unchecked")
    static MongoException writes(Map<String, Object> reply) {
        List<Map<String, Object>> errors = reply.get("writeErrors") instanceof List<?> l ? (List<Map<String, Object>>) l : List.of();
        Map<String, Object> first = !errors.isEmpty() ? errors.get(0) : reply.get("writeConcernError") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
        int code = first.get("code") instanceof Number n ? n.intValue() : 0;
        String codeName = first.get("codeName") instanceof String s ? s : null;
        String msg = first.get("errmsg") instanceof String s ? s : "write failed";
        String where = errors.size() > 1 ? errors.size() + " writes failed, the first: " : errors.size() == 1 ? "write " + first.get("index") + " failed: " : "write concern: ";
        return new MongoException(code, codeName, where + (codeName != null ? codeName + " (" + code + "): " : "(" + code + "): ") + msg, true, false, false, errors, reply, null);
    }

    private static boolean isRetriableCode(int code, String msg) {
        return switch (code) {
            case 6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436 -> true;    // HostUnreachable, HostNotFound, NetworkTimeout, ShutdownInProgress, PrimarySteppedDown, ExceededTimeLimit, SocketException, NotWritablePrimary, InterruptedAtShutdown, InterruptedDueToReplStateChange, NotPrimaryNoSecondaryOk, NotPrimaryOrSecondary
            default -> msg != null && (msg.contains("not master") || msg.contains("node is recovering"));
        };
    }

    /** The server's error code, or 0 when the server did not answer. */
    public int code() {
        return code;
    }

    public String codeName() {
        return codeName;
    }

    /** True when the server answered with the error. */
    public boolean fromServer() {
        return fromServer;
    }

    /** The command may be sent again: it did not run (not connected, too many pending) or the error is a transient server-side one (primary stepped down...). */
    public boolean retriable() {
        return retriable;
    }

    public boolean isTimeout() {
        return timeout;
    }

    /** 11000: a unique index refused the write. */
    public boolean isDuplicateKey() {
        return code == 11000 || code == 11001;
    }

    /** For a write command: the failed writes ({@code index} into the sent list, {@code code}, {@code errmsg}); empty otherwise. */
    public List<Map<String, Object>> writeErrors() {
        return writeErrors;
    }

    /** The whole answer, or null. */
    public Map<String, Object> reply() {
        return reply;
    }

    /** The server's {@code errorLabels}: {@code TransientTransactionError}, {@code UnknownTransactionCommitResult}, {@code RetryableWriteError}, ... */
    public List<String> labels() {
        return labels;
    }

    public boolean hasLabel(String label) {
        return labels.contains(label);
    }

    /** The connection to the server could not be made or was lost before an answer. */
    public boolean connectionLost() {
        return lost;
    }

    /** The server is not (or no longer) the primary: the client looks for the new one. */
    public boolean notPrimary() {
        return switch (code) {
            case 10107, 13435, 13436, 11600, 11602, 189, 91 -> true;     // NotWritablePrimary, NotPrimaryNoSecondaryOk, NotPrimaryOrSecondary, InterruptedAtShutdown, InterruptedDueToReplStateChange, PrimarySteppedDown, ShutdownInProgress
            default -> false;
        };
    }
}
