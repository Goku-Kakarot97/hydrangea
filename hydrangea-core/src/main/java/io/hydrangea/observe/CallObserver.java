package io.hydrangea.observe;

/**
 * The two ends of one unit of work this library serves or performs, reported to whoever wants to time it: a request handled by a
 * server, a request sent by a client, a message produced, a statement run. hydrangea records no spans and exports nothing itself;
 * a framework on top implements this, starts its own span (or timer, or log line) in {@link #started} and ends it in
 * {@link Observation#ended}, which fires when the answer is really on the wire (a server's response handed to the socket, a client's
 * response received), not when the handler returned.
 * <p>
 * Every call runs on the event loop that does the work, so the implementation must not block. {@code started} may return
 * {@code null} for a call it does not care about: then nothing else is called for it.
 *
 * @param <Q> what starts the work: {@code HttpRequest}, {@code ClientRequest}, a statement, a record ...
 * @param <A> what ends it: {@code HttpResponse}, {@code ClientResponse}, a result, record metadata ...; {@code null} on failure
 */
@FunctionalInterface
public interface CallObserver<Q, A> {

    /** The work has started and its request is known. Returns the observation that gets the rest of the calls, or {@code null}. */
    Observation<A> started(Q request);

    /** One observed call. */
    interface Observation<A> {

        /** Servers: the application's handler is about to be invoked on this thread. Put a span or log context in place here. */
        default void handling() {
        }

        /** Servers: the handler returned on this thread (its future may still be pending). Undo what {@link #handling} did. */
        default void handled() {
        }

        /**
         * The work is over: {@code answer} is on the wire or in hand, or {@code error} says why not (then {@code answer} may be
         * {@code null}, or the error response that was sent instead). Called once, on the event loop.
         */
        void ended(A answer, Throwable error);
    }

    /** Observes nothing. */
    @SuppressWarnings("unchecked")
    static <Q, A> CallObserver<Q, A> none() {
        return (CallObserver<Q, A>) NONE;
    }

    CallObserver<Object, Object> NONE = request -> null;
}
