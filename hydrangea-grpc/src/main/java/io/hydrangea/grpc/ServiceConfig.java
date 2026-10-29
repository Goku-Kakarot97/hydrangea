package io.hydrangea.grpc;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A gRPC service config (the JSON a service owner publishes, gRFC A2/A6): per-method timeouts, retry or hedging policies and message size
 * limits, the load-balancing policy and retry throttling. Unknown fields are ignored, as the format requires; wrong values are rejected.
 * <pre>{@code
 * GrpcChannel.forTarget(url).serviceConfig("""
 *   {"loadBalancingConfig": [{"round_robin": {}}],
 *    "methodConfig": [{
 *      "name": [{"service": "pkg.Orders", "method": "Get"}],
 *      "timeout": "2s",
 *      "retryPolicy": {"maxAttempts": 3, "initialBackoff": "0.1s", "maxBackoff": "1s",
 *                      "backoffMultiplier": 2, "retryableStatusCodes": ["UNAVAILABLE"]}}],
 *    "retryThrottling": {"maxTokens": 10, "tokenRatio": 0.1}}
 * """)
 * }</pre>
 * A method matches the most specific selector: {@code {service, method}}, then {@code {service}}, then {@code {}} (every method).
 */
public final class ServiceConfig {

    /** What the config says about one method; null fields mean "not specified". */
    public record MethodConfig(Duration timeout, RetryPolicy retry, HedgingPolicy hedging, Integer maxRequestMessageBytes, Integer maxResponseMessageBytes) {
    }

    private record Rule(String service, String method, MethodConfig config) {
    }

    private final LoadBalancing balancing;
    private final RetryThrottling throttling;
    private final String healthService;
    private final List<Rule> rules;

    private ServiceConfig(LoadBalancing balancing, RetryThrottling throttling, String healthService, List<Rule> rules) {
        this.balancing = balancing;
        this.throttling = throttling;
        this.healthService = healthService;
        this.rules = rules;
    }

    /** @throws IllegalArgumentException if the text is not JSON or a value is out of range */
    @SuppressWarnings("unchecked")
    public static ServiceConfig parse(String json) {
        Object root = Json.parse(json);
        if (!(root instanceof Map<?, ?> top)) {
            throw new IllegalArgumentException("a service config is a JSON object");
        }
        Map<String, Object> cfg = (Map<String, Object>) top;
        LoadBalancing lb = null;
        Object lbc = cfg.get("loadBalancingConfig");
        if (lbc instanceof List<?> choices) {
            for (Object choice : choices) {   // the first policy we know wins
                if (choice instanceof Map<?, ?> m && !m.isEmpty()) {
                    String name = String.valueOf(m.keySet().iterator().next());
                    if (name.equals("pick_first")) { lb = LoadBalancing.PICK_FIRST; break; }
                    if (name.equals("round_robin")) { lb = LoadBalancing.ROUND_ROBIN; break; }
                }
            }
        } else if (cfg.get("loadBalancingPolicy") instanceof String p) {
            lb = switch (p.toLowerCase(java.util.Locale.ROOT)) {
                case "pick_first" -> LoadBalancing.PICK_FIRST;
                case "round_robin" -> LoadBalancing.ROUND_ROBIN;
                default -> null;
            };
        }
        RetryThrottling throttling = null;
        if (cfg.get("retryThrottling") instanceof Map<?, ?> t) {
            throttling = new RetryThrottling((int) number(t.get("maxTokens"), "retryThrottling.maxTokens"), number(t.get("tokenRatio"), "retryThrottling.tokenRatio"));
        }
        String health = null;
        if (cfg.get("healthCheckConfig") instanceof Map<?, ?> h && h.get("serviceName") instanceof String n) {
            health = n;
        }
        List<Rule> rules = new ArrayList<>();
        if (cfg.get("methodConfig") instanceof List<?> methods) {
            for (Object o : methods) {
                if (!(o instanceof Map<?, ?> mc)) {
                    throw new IllegalArgumentException("methodConfig entries are objects");
                }
                MethodConfig config = methodConfig((Map<String, Object>) mc);
                if (!(mc.get("name") instanceof List<?> names) || names.isEmpty()) {
                    throw new IllegalArgumentException("a methodConfig needs a non-empty name list");
                }
                for (Object n : names) {
                    Map<?, ?> sel = (Map<?, ?>) n;
                    String service = sel.get("service") instanceof String s && !s.isEmpty() ? s : null;
                    String method = sel.get("method") instanceof String s && !s.isEmpty() ? s : null;
                    if (service == null && method != null) {
                        throw new IllegalArgumentException("a name with a method needs a service");
                    }
                    rules.add(new Rule(service, method, config));
                }
            }
        }
        return new ServiceConfig(lb, throttling, health, List.copyOf(rules));
    }

    @SuppressWarnings("unchecked")
    private static MethodConfig methodConfig(Map<String, Object> mc) {
        Duration timeout = mc.get("timeout") == null ? null : duration(mc.get("timeout"), "timeout");
        RetryPolicy retry = null;
        HedgingPolicy hedging = null;
        if (mc.get("retryPolicy") instanceof Map<?, ?> r) {
            Map<String, Object> m = (Map<String, Object>) r;
            Duration initial = duration(m.get("initialBackoff"), "retryPolicy.initialBackoff");
            Duration max = duration(m.get("maxBackoff"), "retryPolicy.maxBackoff");
            Set<Status.Code> codes = codes(m.get("retryableStatusCodes"), "retryPolicy.retryableStatusCodes");
            retry = new RetryPolicy((int) number(m.get("maxAttempts"), "retryPolicy.maxAttempts"), initial, max,
                    number(m.get("backoffMultiplier"), "retryPolicy.backoffMultiplier"), codes);
        }
        if (mc.get("hedgingPolicy") instanceof Map<?, ?> h) {
            Map<String, Object> m = (Map<String, Object>) h;
            Duration delay = m.get("hedgingDelay") == null ? Duration.ZERO : duration(m.get("hedgingDelay"), "hedgingPolicy.hedgingDelay");
            Set<Status.Code> nonFatal = m.get("nonFatalStatusCodes") == null ? Set.of() : codes(m.get("nonFatalStatusCodes"), "hedgingPolicy.nonFatalStatusCodes");
            hedging = new HedgingPolicy((int) number(m.get("maxAttempts"), "hedgingPolicy.maxAttempts"), delay, nonFatal);
        }
        if (retry != null && hedging != null) {
            throw new IllegalArgumentException("a method has a retryPolicy or a hedgingPolicy, not both");
        }
        Integer maxReq = mc.get("maxRequestMessageBytes") == null ? null : (int) number(mc.get("maxRequestMessageBytes"), "maxRequestMessageBytes");
        Integer maxResp = mc.get("maxResponseMessageBytes") == null ? null : (int) number(mc.get("maxResponseMessageBytes"), "maxResponseMessageBytes");
        return new MethodConfig(timeout, retry, hedging, maxReq, maxResp);
    }

    private static double number(Object o, String what) {
        if (o instanceof Double d) {
            return d;
        }
        throw new IllegalArgumentException(what + " must be a number");
    }

    /** A protobuf JSON duration: seconds with an optional fraction, then "s". */
    private static Duration duration(Object o, String what) {
        if (!(o instanceof String s) || !s.endsWith("s")) {
            throw new IllegalArgumentException(what + " must be a duration like \"0.5s\"");
        }
        try {
            double seconds = Double.parseDouble(s.substring(0, s.length() - 1));
            if (seconds < 0 || Double.isNaN(seconds) || seconds > 315_576_000_000.0) {
                throw new IllegalArgumentException(what + " out of range");
            }
            return Duration.ofNanos(Math.round(seconds * 1_000_000_000d));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " must be a duration like \"0.5s\"");
        }
    }

    private static Set<Status.Code> codes(Object o, String what) {
        if (!(o instanceof List<?> l) || l.isEmpty()) {
            throw new IllegalArgumentException(what + " must be a non-empty list");
        }
        java.util.HashSet<Status.Code> out = new java.util.HashSet<>();
        for (Object c : l) {
            if (c instanceof String name) {
                try {
                    out.add(Status.Code.valueOf(name.toUpperCase(java.util.Locale.ROOT)));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(what + ": unknown status code " + name);
                }
            } else if (c instanceof Double d) {
                out.add(Status.Code.fromValue(d.intValue()));
            } else {
                throw new IllegalArgumentException(what + ": status codes are names or numbers");
            }
        }
        return out;
    }

    /** The load-balancing policy the config names, or null. */
    public LoadBalancing loadBalancing() {
        return balancing;
    }

    public RetryThrottling retryThrottling() {
        return throttling;
    }

    public String healthCheckServiceName() {
        return healthService;
    }

    /** The config for {@code /service/method}, from the most specific matching selector; null if none matches. */
    public MethodConfig forMethod(String service, String method) {
        MethodConfig serviceWide = null, everything = null;
        for (Rule r : rules) {
            if (r.service == null) {
                if (everything == null) everything = r.config;
            } else if (r.service.equals(service)) {
                if (r.method == null) {
                    if (serviceWide == null) serviceWide = r.config;
                } else if (r.method.equals(method)) {
                    return r.config;
                }
            }
        }
        return serviceWide != null ? serviceWide : everything;
    }
}
