package io.hydrangea.mysql;

/**
 * A MySQL failure: an error the server sent (with its error number and SQLSTATE), or a failure of the connection or of this client.
 * {@link #retriable()} says that nothing was executed (or that running it again is the normal cure: a deadlock, a lock wait timeout).
 */
public class MyException extends io.hydrangea.sql.SqlException {

    private final int errorCode;
    private final String sqlState;
    private final boolean retriable;
    private boolean timeout;

    public MyException(String message, boolean retriable) {
        this(message, 0, null, retriable, null);
    }

    public MyException(String message, boolean retriable, Throwable cause) {
        this(message, 0, null, retriable, cause);
    }

    public MyException(String message, int errorCode, String sqlState, boolean retriable, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.sqlState = sqlState;
        this.retriable = retriable;
    }

    static MyException timeout(String message) {
        MyException e = new MyException(message, false);
        e.timeout = true;
        return e;
    }

    /** True when the call gave up waiting (the time limit ran out); the statement may still have run, or may have been killed. */
    @Override
    public boolean isTimeout() {
        return timeout;
    }

    /** The server's error number (1062 duplicate key...), 0 when the failure is not an error the server sent. */
    public int errorCode() {
        return errorCode;
    }

    @Override
    public String sqlState() {
        return sqlState;
    }

    @Override
    public boolean retriable() {
        return retriable;
    }

    /** ER_LOCK_DEADLOCK (1213) and ER_LOCK_WAIT_TIMEOUT (1205): the transaction was rolled back (or the statement given up), run it again. */
    @Override
    public boolean isRetriableServerError() {
        return errorCode == 1213 || errorCode == 1205;
    }

    /** Error 1062 (ER_DUP_ENTRY). */
    @Override
    public boolean isDuplicateKey() {
        return errorCode == 1062;
    }
}
