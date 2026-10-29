package io.hydrangea.grpc;

import io.hydrangea.grpc.proto.ProtoReader;
import io.hydrangea.grpc.proto.ProtoWriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Server reflection ({@code grpc.reflection.v1} and {@code v1alpha}): lets tools like {@code grpcurl} and Postman list the server's services and
 * learn their message types without having the {@code .proto} files.
 * <p>Listing services needs nothing more. Describing them needs the descriptors, which this library cannot derive from its hand-written
 * marshallers, so you hand over a {@code FileDescriptorSet} made by {@code protoc --descriptor_set_out=app.pb --include_imports app.proto}:
 * <pre>{@code
 * GrpcServer.builder()
 *     .addService(new ReflectionService().addDescriptorSet(Files.readAllBytes(Path.of("app.pb"))))
 * }</pre>
 * Extensions declared in those files (top level or inside messages) can be looked up by extendee and number.
 */
public final class ReflectionService implements GrpcServer.BindableService {

    private static final String V1 = "grpc.reflection.v1.ServerReflection";
    private static final String V1ALPHA = "grpc.reflection.v1alpha.ServerReflection";

    /** One .proto file: its name, the files it imports, every symbol it defines, and its serialized FileDescriptorProto. */
    private record ProtoFile(String name, List<String> dependencies, List<String> symbols, List<Extension> extensions, byte[] descriptor) {
    }

    /** An extension field: the message it extends ("pkg.Type", no leading dot) and its field number. */
    private record Extension(String extendee, int number) {
    }

    private final Map<String, ProtoFile> filesByName = new LinkedHashMap<>();
    private final Map<String, ProtoFile> filesBySymbol = new HashMap<>();
    private final Map<String, Map<Integer, ProtoFile>> extensions = new HashMap<>();
    private volatile List<String> serviceNames = List.of(V1, V1ALPHA);

    /**
     * Adds the files of a serialized {@code FileDescriptorSet}.
     * @throws IllegalArgumentException if the bytes are not a descriptor set
     */
    public synchronized ReflectionService addDescriptorSet(byte[] fileDescriptorSet) {
        try {
            ProtoReader r = new ProtoReader(fileDescriptorSet);
            while (r.next()) {
                if (r.field() == 1) {
                    add(parseFile(r.bytes()));
                } else {
                    r.skip();
                }
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("not a valid FileDescriptorSet", e);
        }
        return this;
    }

    private void add(ProtoFile f) {
        filesByName.put(f.name, f);
        for (String s : f.symbols) {
            filesBySymbol.put(s, f);
        }
        for (Extension e : f.extensions) {
            extensions.computeIfAbsent(e.extendee, k -> new java.util.TreeMap<>()).put(e.number, f);
        }
    }

    private static ProtoFile parseFile(byte[] bytes) {
        String name = "", pkg = "";
        List<String> deps = new ArrayList<>();
        List<String> symbols = new ArrayList<>();
        List<byte[]> messages = new ArrayList<>(), enums = new ArrayList<>(), services = new ArrayList<>(), fileExtensions = new ArrayList<>();
        List<Extension> exts = new ArrayList<>();
        ProtoReader r = new ProtoReader(bytes);
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> pkg = r.string();
                case 3 -> deps.add(r.string());
                case 4 -> messages.add(r.bytes());
                case 5 -> enums.add(r.bytes());
                case 6 -> services.add(r.bytes());
                case 7 -> fileExtensions.add(r.bytes());
                default -> r.skip();
            }
        }
        String prefix = pkg.isEmpty() ? "" : pkg + ".";
        for (byte[] m : messages) {
            collectMessage(prefix, m, symbols, exts);
        }
        for (byte[] x : fileExtensions) {
            collectExtension(prefix, x, symbols, exts);
        }
        for (byte[] e : enums) {
            collectEnum(prefix, e, symbols);
        }
        for (byte[] s : services) {
            ProtoReader sr = new ProtoReader(s);
            String svc = "";
            List<String> methods = new ArrayList<>();
            while (sr.next()) {
                if (sr.field() == 1) {
                    svc = sr.string();
                } else if (sr.field() == 2) {
                    ProtoReader mr = new ProtoReader(sr.bytes());
                    while (mr.next()) {
                        if (mr.field() == 1) methods.add(mr.string()); else mr.skip();
                    }
                } else {
                    sr.skip();
                }
            }
            symbols.add(prefix + svc);
            for (String m : methods) {
                symbols.add(prefix + svc + "." + m);
            }
        }
        return new ProtoFile(name, deps, symbols, exts, bytes);
    }

    private static void collectMessage(String prefix, byte[] message, List<String> out, List<Extension> exts) {
        ProtoReader r = new ProtoReader(message);
        String name = "";
        List<byte[]> nested = new ArrayList<>(), enums = new ArrayList<>(), nestedExtensions = new ArrayList<>();
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 3 -> nested.add(r.bytes());
                case 4 -> enums.add(r.bytes());
                case 6 -> nestedExtensions.add(r.bytes());
                default -> r.skip();
            }
        }
        String full = prefix + name;
        out.add(full);
        for (byte[] n : nested) {
            collectMessage(full + ".", n, out, exts);
        }
        for (byte[] x : nestedExtensions) {
            collectExtension(full + ".", x, out, exts);
        }
        for (byte[] e : enums) {
            collectEnum(full + ".", e, out);
        }
    }

    /** A FieldDescriptorProto of an extension: name = 1, extendee = 2 (".pkg.Type"), number = 3. */
    private static void collectExtension(String prefix, byte[] field, List<String> symbols, List<Extension> exts) {
        ProtoReader r = new ProtoReader(field);
        String name = "", extendee = "";
        int number = 0;
        while (r.next()) {
            switch (r.field()) {
                case 1 -> name = r.string();
                case 2 -> extendee = r.string();
                case 3 -> number = (int) r.varint();
                default -> r.skip();
            }
        }
        symbols.add(prefix + name);
        exts.add(new Extension(extendee.startsWith(".") ? extendee.substring(1) : extendee, number));
    }

    private static void collectEnum(String prefix, byte[] enumBytes, List<String> out) {
        ProtoReader r = new ProtoReader(enumBytes);
        while (r.next()) {
            if (r.field() == 1) {
                out.add(prefix + r.string());
            } else {
                r.skip();
            }
        }
    }

    // ---- the service ----

    @Override
    public void bindTo(GrpcServer.Builder builder) {
        builder.addBuiltListener(server -> {
            Set<String> names = new TreeSet<>(server.serviceNames());
            serviceNames = List.copyOf(names);
        });
        for (String service : new String[]{V1, V1ALPHA}) {
            String pkg = service.substring(0, service.lastIndexOf('.'));
            MethodDescriptor<Request, Response> info = MethodDescriptor.of(pkg + ".ServerReflection", "ServerReflectionInfo",
                    MethodDescriptor.MethodType.BIDI_STREAMING, REQUEST, RESPONSE);
            builder.bidiStreaming(info, (ctx, out) -> new StreamObserver<Request>() {
                @Override
                public void onNext(Request request) {
                    out.onNext(answer(request));
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                    out.onCompleted();
                }
            });
        }
    }

    private Response answer(Request q) {
        switch (q.kind) {
            case 7: // list_services
                return Response.services(q, serviceNames);
            case 3: { // file_by_filename
                ProtoFile f;
                synchronized (this) {
                    f = filesByName.get(q.argument);
                }
                return f == null ? Response.error(q, 5, "file not found: " + q.argument) : Response.files(q, closure(f));
            }
            case 4: { // file_containing_symbol
                ProtoFile f;
                synchronized (this) {
                    f = filesBySymbol.get(q.argument);
                }
                return f == null ? Response.error(q, 5, "symbol not found: " + q.argument) : Response.files(q, closure(f));
            }
            case 5: { // file_containing_extension
                ProtoFile f;
                synchronized (this) {
                    Map<Integer, ProtoFile> byNumber = extensions.get(q.argument);
                    f = byNumber == null ? null : byNumber.get(q.number);
                }
                return f == null ? Response.error(q, 5, "extension not found: " + q.argument + " " + q.number) : Response.files(q, closure(f));
            }
            case 6: { // all_extension_numbers_of_type
                List<Integer> numbers;
                synchronized (this) {
                    Map<Integer, ProtoFile> byNumber = extensions.get(q.argument);
                    numbers = byNumber == null ? List.of() : new ArrayList<>(byNumber.keySet());
                }
                return Response.extensionNumbers(q, numbers);
            }
            default:
                return Response.error(q, 12, "unsupported reflection request");
        }
    }

    /** The file, then everything it imports (transitively) that is known here; each once. */
    private synchronized List<byte[]> closure(ProtoFile start) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<ProtoFile> queue = new ArrayDeque<>();
        queue.add(start);
        List<byte[]> out = new ArrayList<>();
        while (!queue.isEmpty()) {
            ProtoFile f = queue.poll();
            if (!seen.add(f.name)) {
                continue;
            }
            out.add(f.descriptor);
            for (String d : f.dependencies) {
                ProtoFile dep = filesByName.get(d);
                if (dep != null) {
                    queue.add(dep);
                }
            }
        }
        return out;
    }

    // ---- wire messages (grpc/reflection/v1/reflection.proto) ----

    /** ServerReflectionRequest: {@code kind} is the field number of the oneof member that was set. */
    record Request(String host, int kind, String argument, int number, byte[] raw) {
    }

    record Response(String host, byte[] original, byte[] body) {
        static Response services(Request q, List<String> names) {
            ProtoWriter list = new ProtoWriter();
            for (String n : names) {
                list.message(1, new ProtoWriter().string(1, n).toByteArray());
            }
            return new Response(q.host, q.raw, new ProtoWriter().message(6, list.toByteArray()).toByteArray());
        }

        static Response files(Request q, List<byte[]> descriptors) {
            ProtoWriter fd = new ProtoWriter();
            for (byte[] d : descriptors) {
                fd.bytes(1, d);
            }
            return new Response(q.host, q.raw, new ProtoWriter().message(4, fd.toByteArray()).toByteArray());
        }

        static Response extensionNumbers(Request q, List<Integer> numbers) {
            ProtoWriter w = new ProtoWriter().string(1, q.argument);
            for (int n : numbers) {
                w.varint(2, n);
            }
            return new Response(q.host, q.raw, new ProtoWriter().message(5, w.toByteArray()).toByteArray());
        }

        static Response error(Request q, int code, String message) {
            return new Response(q.host, q.raw, new ProtoWriter().message(7, new ProtoWriter().varint(1, code).string(2, message).toByteArray()).toByteArray());
        }
    }

    static final Marshaller<Request> REQUEST = Marshaller.of(
            r -> r.raw,
            bytes -> {
                ProtoReader r = new ProtoReader(bytes);
                String host = "", arg = "";
                int kind = 0, number = 0;
                while (r.next()) {
                    switch (r.field()) {
                        case 1 -> host = r.string();
                        case 3, 4, 7 -> { kind = r.field(); arg = r.string(); }
                        case 5 -> {
                            kind = 5;
                            ProtoReader x = r.message();
                            while (x.next()) {
                                if (x.field() == 1) arg = x.string(); else if (x.field() == 2) number = (int) x.varint(); else x.skip();
                            }
                        }
                        case 6 -> { kind = 6; arg = r.string(); }
                        default -> r.skip();
                    }
                }
                return new Request(host, kind, arg, number, bytes);
            });

    static final Marshaller<Response> RESPONSE = Marshaller.of(
            resp -> {
                ProtoWriter w = new ProtoWriter();
                if (!resp.host.isEmpty()) {
                    w.string(1, resp.host);
                }
                w.message(2, resp.original);
                byte[] out = w.toByteArray();
                byte[] all = new byte[out.length + resp.body.length];
                System.arraycopy(out, 0, all, 0, out.length);
                System.arraycopy(resp.body, 0, all, out.length, resp.body.length);
                return all;
            },
            bytes -> {
                throw new UnsupportedOperationException("the server never reads a reflection response");
            });
}
