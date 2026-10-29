package io.hydrangea.redis;

/**
 * Options of {@code SET}. Combine: {@code SetArgs.ex(60).andNx()}.
 *
 * @param exSeconds   expire after this many seconds (EX), or null
 * @param pxMillis    expire after this many milliseconds (PX), or null
 * @param ifAbsent    only set if the key does not exist (NX)
 * @param ifPresent   only set if the key exists (XX)
 * @param keepExpiry  keep the key's time to live (KEEPTTL)
 * @param returnOld   reply with the old value (GET)
 */
public record SetArgs(Long exSeconds, Long pxMillis, boolean ifAbsent, boolean ifPresent, boolean keepExpiry, boolean returnOld) {

    public static final SetArgs NONE = new SetArgs(null, null, false, false, false, false);

    public SetArgs {
        if (ifAbsent && ifPresent) {
            throw new IllegalArgumentException("NX and XX exclude each other");
        }
        if ((exSeconds != null ? 1 : 0) + (pxMillis != null ? 1 : 0) + (keepExpiry ? 1 : 0) > 1) {
            throw new IllegalArgumentException("EX, PX and KEEPTTL exclude each other");
        }
    }

    public static SetArgs ex(long seconds) {
        return new SetArgs(seconds, null, false, false, false, false);
    }

    public static SetArgs px(long millis) {
        return new SetArgs(null, millis, false, false, false, false);
    }

    public static SetArgs nx() {
        return new SetArgs(null, null, true, false, false, false);
    }

    public static SetArgs xx() {
        return new SetArgs(null, null, false, true, false, false);
    }

    public SetArgs andNx() {
        return new SetArgs(exSeconds, pxMillis, true, ifPresent, keepExpiry, returnOld);
    }

    public SetArgs andXx() {
        return new SetArgs(exSeconds, pxMillis, ifAbsent, true, keepExpiry, returnOld);
    }

    public SetArgs andKeepTtl() {
        return new SetArgs(null, null, ifAbsent, ifPresent, true, returnOld);
    }

    public SetArgs andGet() {
        return new SetArgs(exSeconds, pxMillis, ifAbsent, ifPresent, keepExpiry, true);
    }

    /** The arguments after the key and the value. */
    java.util.List<Object> toArgs() {
        java.util.List<Object> a = new java.util.ArrayList<>(4);
        if (exSeconds != null) {
            a.add("EX");
            a.add(exSeconds);
        }
        if (pxMillis != null) {
            a.add("PX");
            a.add(pxMillis);
        }
        if (ifAbsent) {
            a.add("NX");
        }
        if (ifPresent) {
            a.add("XX");
        }
        if (keepExpiry) {
            a.add("KEEPTTL");
        }
        if (returnOld) {
            a.add("GET");
        }
        return a;
    }
}
