package io.hydrangea;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class EventLoopTest {

    /**
     * Regression test for a lost wakeup, forced into the exact bad interleaving:
     * <ol>
     *   <li>the loop is inside task A;</li>
     *   <li>another thread calls execute(B): the "wakeup requested" flag becomes true and selector.wakeup() is armed;</li>
     *   <li>A calls selectNow() (as closeAndRelease does), which swallows the armed wakeup;</li>
     *   <li>B runs, the queue is empty, the loop blocks in select().</li>
     * </ol>
     * If the flag is still true now, execute(C) skips wakeup() and C is stranded forever.
     */
    @Test
    void aWakeupSwallowedBySelectNowInsideATaskDoesNotStrandLaterTasks() throws Exception {
        EventLoopGroup group = new EventLoopGroup(1);
        try {
            EventLoop loop = group.next();
            CountDownLatch inTaskA = new CountDownLatch(1);
            CountDownLatch bQueued = new CountDownLatch(1);
            CountDownLatch bRan = new CountDownLatch(1);
            CountDownLatch cRan = new CountDownLatch(1);

            loop.execute(() -> {
                inTaskA.countDown();
                try {
                    assertTrue(bQueued.await(5, TimeUnit.SECONDS));
                    DatagramChannel throwaway = DatagramChannel.open();
                    throwaway.configureBlocking(false);
                    loop.closeAndRelease(throwaway); // runs selectNow() on the loop thread, eating the armed wakeup
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            assertTrue(inTaskA.await(5, TimeUnit.SECONDS));
            loop.execute(bRan::countDown); // from this thread: sets the flag, arms selector.wakeup()
            bQueued.countDown();
            assertTrue(bRan.await(5, TimeUnit.SECONDS));

            Thread.sleep(200); // the loop is now parked in select() with an empty queue
            loop.execute(cRan::countDown);
            assertTrue(cRan.await(3, TimeUnit.SECONDS), "task C was stranded: the loop slept through execute() with work queued");
        } finally {
            group.shutdown(); // not close(): never block the build on a loop that may be stuck
        }
    }

    /** Many threads submitting while another keeps running selectNow() inside tasks: nothing may be stranded. */
    @Test
    void tasksAlwaysRunUnderConcurrentSubmissionAndSelectNow() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            EventLoop loop = group.next();
            int submitters = 4, perSubmitter = 2000;
            AtomicInteger ran = new AtomicInteger();
            CountDownLatch all = new CountDownLatch(submitters * perSubmitter);
            CountDownLatch go = new CountDownLatch(1);
            for (int t = 0; t < submitters; t++) {
                Thread.startVirtualThread(() -> {
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    for (int i = 0; i < perSubmitter; i++) {
                        loop.execute(() -> {
                            ran.incrementAndGet();
                            all.countDown();
                        });
                    }
                });
            }
            Thread closer = Thread.startVirtualThread(() -> {
                try {
                    go.await();
                    for (int i = 0; i < 300; i++) {
                        try (DatagramChannel ch = DatagramChannel.open()) {
                            ch.configureBlocking(false);
                            loop.closeAndRelease(ch);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            go.countDown();
            assertTrue(all.await(20, TimeUnit.SECONDS), "ran " + ran.get() + " of " + submitters * perSubmitter);
            closer.join(20_000);
            CountDownLatch last = new CountDownLatch(1);
            Thread.sleep(50);
            loop.execute(last::countDown);
            assertTrue(last.await(5, TimeUnit.SECONDS), "a task submitted to an idle loop must run");
        }
    }

    @Test
    void closeAndReleaseFreesTheSocketBeforeReturning() throws Exception {
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            EventLoop loop = group.next();
            for (int i = 0; i < 100; i++) {
                DatagramChannel ch = DatagramChannel.open();
                ch.bind(new InetSocketAddress("127.0.0.1", 0));
                int port = ((InetSocketAddress) ch.getLocalAddress()).getPort();
                ch.configureBlocking(false);
                CountDownLatch registered = new CountDownLatch(1);
                loop.execute(() -> {
                    try {
                        loop.register(ch, java.nio.channels.SelectionKey.OP_READ, new SelectionHandle() {
                            public void ready(int ops) { }
                            public void terminate() { }
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    registered.countDown();
                });
                assertTrue(registered.await(5, TimeUnit.SECONDS));
                loop.closeAndRelease(ch);
                try (DatagramChannel again = DatagramChannel.open()) {
                    again.bind(new InetSocketAddress("127.0.0.1", port)); // would throw "Address already in use" if still held
                }
            }
        }
    }

    /**
     * Regression test for a leak found by the soak test: a cancelled timer stayed in the queue until its deadline and kept its
     * task (and through it a closed connection) reachable, so connection churn with a long keep-alive timer exhausted the heap.
     */
    @Test
    void aCancelledTimerDoesNotKeepItsTaskReachable() throws Exception {
        EventLoopGroup group = new EventLoopGroup(1);
        try {
            EventLoop loop = group.next();
            List<java.lang.ref.WeakReference<Object>> refs = new ArrayList<>();
            // a live timer with an earlier deadline sits at the head of the queue, so the cancelled ones behind it are not
            // reached by the loop's "drop cancelled timers at the head" and stay queued, exactly as in a busy server
            loop.schedule(() -> { }, 30, TimeUnit.MINUTES);
            for (int i = 0; i < 3000; i++) { // more than the compaction threshold
                Object owner = new byte[1024];
                refs.add(new java.lang.ref.WeakReference<>(owner));
                loop.schedule(() -> owner.hashCode(), 1, TimeUnit.HOURS).cancel();
            }
            CountDownLatch queued = new CountDownLatch(1);
            loop.execute(queued::countDown); // the schedule() registrations run before this
            assertTrue(queued.await(5, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long alive;
            do {
                System.gc();
                alive = refs.stream().filter(r -> r.get() != null).count();
            } while (alive > 0 && System.nanoTime() < deadline);
            assertEquals(0, alive, "cancelled timers still hold their tasks");
        } finally {
            group.close();
        }
    }
}
