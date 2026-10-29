package io.hydrangea.fuzz;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A tiny mutation fuzzer with no dependencies. A target gets inputs derived from a seed corpus (byte flips, insertions, deletions, splices, repeats,
 * boundary values) and from pure noise; it may only fail in the ways it documents. Anything else (NullPointerException, ArrayIndexOutOfBounds, a stack
 * overflow, an out-of-memory error) or taking too long is a finding, reported with the seed and the input in hex so it can be replayed.
 * The number of inputs is {@code -Dfuzz.iterations} (default 3000: a smoke run in the normal test suite; use millions for a real campaign).
 */
public final class Fuzz {
    public static final int ITERATIONS = Integer.getInteger("fuzz.iterations", 3000);
    public static final long SEED = Long.getLong("fuzz.seed", 20261021L);

    @FunctionalInterface
    public interface Target {
        void run(byte[] input) throws Throwable;
    }

    public record Finding(String target, long seed, int iteration, String problem, byte[] input) {
        @Override
        public String toString() {
            return target + " seed=" + seed + " iteration=" + iteration + ": " + problem + "\n  input(" + input.length + ")=" + hex(input);
        }
    }

    private Fuzz() {
    }

    public static String hex(byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(b.length, 400); i++) {
            sb.append(String.format("%02x", b[i]));
        }
        return b.length > 400 ? sb + "..." : sb.toString();
    }

    /**
     * Runs the target on {@link #ITERATIONS} inputs. {@code allowed} are the exception classes the target is documented to throw for bad input.
     *
     * @return the distinct problems found (at most one per exception type and top stack frame)
     */
    public static List<Finding> run(String name, List<byte[]> corpus, Target target, Set<Class<? extends Throwable>> allowed, long maxMillisPerInput) {
        return run(name, corpus, target, allowed, maxMillisPerInput, ITERATIONS);
    }

    public static List<Finding> run(String name, List<byte[]> corpus, Target target, Set<Class<? extends Throwable>> allowed, long maxMillisPerInput,
                                    int iterations) {
        Random r = new Random(SEED ^ name.hashCode());
        List<Finding> found = new ArrayList<>();
        Set<String> seen = new java.util.HashSet<>();
        for (int i = 0; i < iterations; i++) {
            byte[] in = corpus.isEmpty() || r.nextInt(10) == 0 ? noise(r) : mutate(r, corpus.get(r.nextInt(corpus.size())), corpus);
            long t0 = System.nanoTime();
            Throwable failure = null;
            try {
                target.run(in);
            } catch (Throwable t) {
                if (!isAllowed(t, allowed)) {
                    failure = t;
                }
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            if (failure != null) {
                StackTraceElement top = failure.getStackTrace().length > 0 ? failure.getStackTrace()[0] : null;
                String key = failure.getClass().getName() + "@" + top;
                if (seen.add(key)) {
                    found.add(new Finding(name, SEED, i, failure + " at " + top, in));
                }
            } else if (ms > maxMillisPerInput && seen.add("slow")) {
                found.add(new Finding(name, SEED, i, "took " + ms + " ms (limit " + maxMillisPerInput + ")", in));
            }
        }
        return found;
    }

    private static boolean isAllowed(Throwable t, Set<Class<? extends Throwable>> allowed) {
        for (Class<? extends Throwable> c : allowed) {
            if (c.isInstance(t)) {
                return true;
            }
        }
        return false;
    }

    public static byte[] noise(Random r) {
        byte[] b = new byte[r.nextInt(r.nextInt(8) == 0 ? 600 : 40)];
        r.nextBytes(b);
        return b;
    }

    private static final int[] INTERESTING = {0, 1, 0x7f, 0x80, 0xff, 0x20, 0x0d, 0x0a, 0x3a, 0x22, 0x5c, 0x7b, 0x7d, 0x5b, 0x5d};

    public static byte[] mutate(Random r, byte[] seed, List<byte[]> corpus) {
        byte[] b = seed.clone();
        int rounds = 1 + r.nextInt(4);
        for (int k = 0; k < rounds; k++) {
            switch (r.nextInt(8)) {
                case 0 -> { // flip a bit
                    if (b.length > 0) b[r.nextInt(b.length)] ^= (byte) (1 << r.nextInt(8));
                }
                case 1 -> { // set a byte to an interesting value
                    if (b.length > 0) b[r.nextInt(b.length)] = (byte) INTERESTING[r.nextInt(INTERESTING.length)];
                }
                case 2 -> { // random byte
                    if (b.length > 0) b[r.nextInt(b.length)] = (byte) r.nextInt(256);
                }
                case 3 -> { // delete a range
                    if (b.length > 1) {
                        int from = r.nextInt(b.length), len = 1 + r.nextInt(Math.min(8, b.length - from));
                        byte[] n = new byte[b.length - len];
                        System.arraycopy(b, 0, n, 0, from);
                        System.arraycopy(b, from + len, n, from, b.length - from - len);
                        b = n;
                    }
                }
                case 4 -> { // insert random bytes
                    int at = r.nextInt(b.length + 1), len = 1 + r.nextInt(8);
                    byte[] n = new byte[b.length + len];
                    System.arraycopy(b, 0, n, 0, at);
                    for (int i = 0; i < len; i++) n[at + i] = (byte) r.nextInt(256);
                    System.arraycopy(b, at, n, at + len, b.length - at);
                    b = n;
                }
                case 5 -> { // repeat a slice (long runs, deep nesting)
                    if (b.length > 0 && b.length < 4000) {
                        int from = r.nextInt(b.length), len = 1 + r.nextInt(Math.min(16, b.length - from)), times = 2 + r.nextInt(r.nextInt(4) == 0 ? 300 : 20);
                        byte[] n = new byte[b.length + len * (times - 1)];
                        System.arraycopy(b, 0, n, 0, from + len);
                        for (int t = 1; t < times; t++) System.arraycopy(b, from, n, from + len * t, len);
                        System.arraycopy(b, from + len, n, from + len * times, b.length - from - len);
                        b = n;
                    }
                }
                case 6 -> { // splice with another corpus entry
                    byte[] o = corpus.get(r.nextInt(corpus.size()));
                    int cut = r.nextInt(b.length + 1), from = r.nextInt(o.length + 1);
                    byte[] n = new byte[cut + o.length - from];
                    System.arraycopy(b, 0, n, 0, cut);
                    System.arraycopy(o, from, n, cut, o.length - from);
                    b = n;
                }
                default -> { // truncate
                    if (b.length > 0) b = java.util.Arrays.copyOf(b, r.nextInt(b.length));
                }
            }
        }
        return b;
    }
}
