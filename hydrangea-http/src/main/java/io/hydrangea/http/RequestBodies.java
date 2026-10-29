package io.hydrangea.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/** Implementations behind {@link RequestBody}'s factories. */
final class RequestBodies {

    /** Blocking reads happen here, never on an event loop. */
    private static final ExecutorService VIRTUAL = Executors.newVirtualThreadPerTaskExecutor();
    private static final int BUFFER = 64 * 1024;

    private RequestBodies() {
    }

    /** An in-memory body: sent together with the request head in one write. */
    static final class Bytes implements RequestBody {
        private final byte[] data;

        Bytes(byte[] data) {
            this.data = data;
        }

        byte[] data() {
            return data;
        }

        @Override
        public long contentLength() {
            return data.length;
        }

        @Override
        public boolean isReplayable() {
            return true;
        }

        @Override
        public void start(BodySink sink) {
            if (data.length > 0) {
                sink.write(ByteBuffer.wrap(data));
            }
            sink.finish();
        }
    }

    static final class Produced implements RequestBody {
        private final long length;
        private final boolean replayable;
        private final Producer producer;

        Produced(long length, boolean replayable, Producer producer) {
            if (length < -1) {
                throw new IllegalArgumentException("length must be >= 0, or -1 if unknown");
            }
            this.length = length;
            this.replayable = replayable;
            this.producer = producer;
        }

        @Override
        public long contentLength() {
            return length;
        }

        @Override
        public boolean isReplayable() {
            return replayable;
        }

        @Override
        public void start(BodySink sink) {
            VIRTUAL.execute(() -> {
                try {
                    producer.produce(sink);
                    sink.finish(); // idempotent: a producer that already finished is fine
                } catch (Throwable t) {
                    sink.fail(t);
                }
            });
        }
    }

    static RequestBody file(Path path) throws IOException {
        long size = Files.size(path);
        return new Produced(size, true, sink -> {
            try (FileChannel ch = FileChannel.open(path, StandardOpenOption.READ)) {
                if (ch.size() != size) {
                    throw new IOException(path + " changed size from " + size + " to " + ch.size() + " bytes since the request was built");
                }
                pump(ch, size, sink);
            }
        });
    }

    static RequestBody stream(Supplier<? extends InputStream> source, long length, boolean replayable) {
        return new Produced(length, replayable, sink -> {
            try (InputStream in = source.get(); ReadableByteChannel ch = Channels.newChannel(in)) {
                pump(ch, length, sink);
            }
        });
    }

    /** Copies {@code in} to the sink with backpressure: one buffer of memory, whatever the size. */
    private static void pump(ReadableByteChannel in, long length, BodySink sink) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(BUFFER);
        long sent = 0;
        while (length < 0 || sent < length) {
            buf.clear();
            if (length >= 0) {
                buf.limit((int) Math.min(buf.capacity(), length - sent));
            }
            int n = in.read(buf);
            if (n < 0) {
                break;
            }
            if (n == 0) {
                continue;
            }
            buf.flip();
            sink.whenWritable().get();   // blocks this virtual thread while the socket is full
            sink.write(buf).get();       // fails if the request is gone: stops the copy loop
            sent += n;
        }
        if (length >= 0 && sent != length) {
            throw new IOException("the source ended after " + sent + " of the declared " + length + " bytes");
        }
        sink.finish();
    }
}
