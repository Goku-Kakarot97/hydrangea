package io.hydrangea.http;

import io.hydrangea.log.Logger;
import io.hydrangea.observe.CallObserver;

/** The server stages' side of {@link HttpConfig#withObserver}: start an observation, carry it on the request, end it once. Never throws. */
final class Observed {

    private static final Logger LOG = Logger.get(Observed.class);

    private Observed() {
    }

    /** The request, carrying the observation the observer returned for it (or unchanged when there is no observer or it declined). */
    static HttpRequest start(CallObserver<HttpRequest, HttpResponse> observer, HttpRequest request) {
        if (observer == null) {
            return request;
        }
        try {
            CallObserver.Observation<HttpResponse> o = observer.started(request);
            return o == null ? request : request.withAttachment(o);
        } catch (Throwable t) {
            LOG.warn("observer failed for " + request.method() + " " + request.target(), t);
            return request;
        }
    }

    @SuppressWarnings("unchecked")
    static CallObserver.Observation<HttpResponse> of(HttpRequest request) {
        return request != null && request.attachment() instanceof CallObserver.Observation<?> o ? (CallObserver.Observation<HttpResponse>) o : null;
    }

    static void handling(HttpRequest request) {
        CallObserver.Observation<HttpResponse> o = of(request);
        if (o != null) {
            try {
                o.handling();
            } catch (Throwable t) {
                LOG.warn("observer handling() failed", t);
            }
        }
    }

    static void handled(HttpRequest request) {
        CallObserver.Observation<HttpResponse> o = of(request);
        if (o != null) {
            try {
                o.handled();
            } catch (Throwable t) {
                LOG.warn("observer handled() failed", t);
            }
        }
    }

    static void end(CallObserver.Observation<HttpResponse> o, HttpResponse response, Throwable error) {
        if (o != null) {
            try {
                o.ended(response, error);
            } catch (Throwable t) {
                LOG.warn("observer ended() failed", t);
            }
        }
    }

    /** What to run once the response bytes have left: the end of the observation, or null when there is none. */
    static Runnable ender(HttpRequest request, HttpResponse response, Throwable error) {
        CallObserver.Observation<HttpResponse> o = of(request);
        return o == null ? null : () -> end(o, response, error);
    }
}
