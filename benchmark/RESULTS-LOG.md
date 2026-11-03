# hydrangea-log: cost per call

`java -cp hydrangea-log/target/classes:hydrangea-log/target/test-classes io.hydrangea.log.LogBenchmark` (`LogBenchmark` in the module's test sources). One JVM, 200 000 warm-up calls then 2 000 000 timed calls per row, the clock stopped after `Log.flush()` so the file has every line. The message is `"value {} and {}"` with two arguments (about 80 bytes per line). macOS laptop, Apple silicon, JDK 21, local SSD. One run; expect ±20% between runs and different ratios on other hardware.

| call | ns/call | calls/s |
|---|---|---|
| disabled (logger at WARN, `info` called) | 1 | 744 M |
| async → file, 1 thread | 144 | 6.9 M |
| async → file, 4 threads logging at once | 190 | 5.3 M |
| async → file, `at(INFO).kv("a", 1).kv("b", "two").log("value")` | 176 | 5.7 M |
| async → file, JSON format | 184 | 5.4 M |
| sync → file, 1 thread (every event flushed) | 2 207 | 453 k |
| sync → file, 4 threads | 3 233 | 309 k |
| `System.Logger` bridge, async → file | 121 | 8.2 M |
| `java.util.logging` `FileHandler` + `SimpleFormatter`, 1 thread | 7 697 | 130 k |
| `java.util.logging`, 4 threads | 6 058 | 165 k |

What the async number contains: rendering the message on the caller (a `StringBuilder`, one `String`), one `LogEvent`, one CAS on the ring buffer's tail, and, on the log thread, the line format (cached timestamp prefix, no `String.format`), UTF-8 into a reused `byte[]`, a buffered write, and one flush per batch. The caller is slowed only when the buffer (65 536 events) is full, which here happened constantly: the file is the bottleneck, the producer waits for room, so these are the appender's rates, not the queue's.

Sync mode flushes after every event (one `write` system call each), which is where its 2 µs go; it is for programs that must not lose a line even on `kill -9`.

## Against Log4j 2 and Logback

Same call (`log.info("value {} and {}", 1, 2)`), same machine, each library in its own JVM, the clock including the flush or context stop at the end so every line is on disk (the line count is checked: 2 200 000 with the warm-up). Log4j 2.24.3 with a `File` appender (`bufferedIO`, 64 KB), async through its Disruptor `AsyncLoggerContextSelector` (`immediateFlush=false`) or sync (`immediateFlush=true`); Logback 1.5.18 with a `FileAppender`, async through `AsyncAppender` (queue 65 536, `discardingThreshold=0`, `immediateFlush=false`) or sync (`immediateFlush=true`). Three runs for the async rows (the spread is shown), one for the sync rows. The harness is `Compare.java`, not kept in the repository because it needs those libraries.

| | 1 thread, ns/call | 4 threads, ns/call |
|---|---|---|
| hydrangea-log async | 136–168 | 161–207 |
| Log4j 2 async (Disruptor) | 198–317 | 225–283 |
| Logback AsyncAppender | 301–333 | 372–496 |
| hydrangea-log sync | 2 230 | 3 089 |
| Log4j 2 sync | 2 562 | 3 581 |
| Logback sync | 2 621 | 3 365 |

So: in async mode about 1.5× faster than Log4j 2 with the Disruptor and about 2× faster than Logback, with no dependency; in sync mode all three pay for one `write` per line and are within 15% of each other. The differences are what the design predicts (no `String.format`, no pattern interpretation, one reused buffer, a cheaper queue), not something exotic; Log4j 2's garbage-free mode and Logback's tuning knobs were not explored, and a different message mix (exceptions, many fields, long strings) can change the ratios.

Not measured: Linux, contention with more threads than cores, console output, exceptions with stack traces (dominated by `Throwable.getStackTrace`).
