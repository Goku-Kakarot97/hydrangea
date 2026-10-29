import io.grpc.binarylog.v1.*;
import java.io.*;
import java.util.*;

/** Reads a Hydrangea binary log with grpc-java's own GrpcLogEntry class: every entry must parse, and each call must have the expected shape. */
public class JavaBinaryLogCheck {
    static int failures;
    static void check(String what, boolean ok, Object detail) { System.out.println((ok ? "  ok   " : "  FAIL ") + what + (ok ? "" : "  -> " + detail)); if (!ok) failures++; }

    public static void main(String[] a) throws Exception {
        List<GrpcLogEntry> entries = new ArrayList<>();
        try (InputStream in = new BufferedInputStream(new FileInputStream(a[0]))) {
            GrpcLogEntry e;
            while ((e = GrpcLogEntry.parseDelimitedFrom(in)) != null) entries.add(e);
        }
        System.out.println(entries.size() + " entries");
        // group by (logger, call id)
        Map<String, List<GrpcLogEntry>> calls = new TreeMap<>();
        for (GrpcLogEntry e : entries) calls.computeIfAbsent(e.getLogger() + "#" + e.getCallId(), k -> new ArrayList<>()).add(e);
        check("server and client each logged 3 calls", calls.size() == 6, calls.keySet());
        for (var c : calls.entrySet()) {
            List<GrpcLogEntry> es = c.getValue();
            boolean seqOk = true;
            for (int i = 0; i < es.size(); i++) seqOk &= es.get(i).getSequenceIdWithinCall() == i + 1;
            check(c.getKey() + ": sequence ids are 1..n", seqOk, es.stream().map(GrpcLogEntry::getSequenceIdWithinCall).toList());
            check(c.getKey() + ": starts with CLIENT_HEADER", es.get(0).getType() == GrpcLogEntry.EventType.EVENT_TYPE_CLIENT_HEADER, es.get(0).getType());
            check(c.getKey() + ": ends with SERVER_TRAILER or CANCEL", es.get(es.size() - 1).getType() == GrpcLogEntry.EventType.EVENT_TYPE_SERVER_TRAILER, es.get(es.size() - 1).getType());
            check(c.getKey() + ": timestamps set", es.stream().allMatch(e -> e.getTimestamp().getSeconds() > 1_700_000_000L), "");
        }
        GrpcLogEntry hdr = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_CLIENT_HEADER && e.getLogger() == GrpcLogEntry.Logger.LOGGER_SERVER).findFirst().get();
        check("method name", hdr.getClientHeader().getMethodName().equals("/hydrangea.test.Echo/Unary"), hdr.getClientHeader());
        check("custom metadata logged", hdr.getClientHeader().getMetadata().getEntryList().stream().anyMatch(m -> m.getKey().equals("x-test") && m.getValue().toStringUtf8().equals("v1")), hdr.getClientHeader().getMetadata());
        check("peer address logged on the server", hdr.getPeer().getType() == Address.Type.TYPE_IPV4 && hdr.getPeer().getIpPort() > 0, hdr.getPeer());
        GrpcLogEntry big = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_CLIENT_MESSAGE).findFirst().get();
        check("message cut at 16 bytes, true length kept, marked truncated", big.getMessage().getData().size() == 16 && big.getMessage().getLength() > 16 && big.getPayloadTruncated(), big.getMessage().getLength() + "/" + big.getMessage().getData().size());
        List<GrpcLogEntry> trailers = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_SERVER_TRAILER && e.getLogger() == GrpcLogEntry.Logger.LOGGER_CLIENT).toList();
        check("client logged three trailers", trailers.size() == 3, trailers.size());
        check("one of them carries NOT_FOUND and its message", trailers.stream().anyMatch(t -> t.getTrailer().getStatusCode() == 5 && t.getTrailer().getStatusMessage().equals("gone")), trailers);
        GrpcLogEntry timeoutHdr = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_CLIENT_HEADER && e.getClientHeader().hasTimeout()).findFirst().get();
        check("deadline logged as a timeout", timeoutHdr.getClientHeader().getTimeout().getSeconds() >= 4, timeoutHdr.getClientHeader().getTimeout());
        long halfCloses = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_CLIENT_HALF_CLOSE).count();
        check("every call has a half close on both sides", halfCloses == 6, halfCloses);
        long serverMsgs = entries.stream().filter(e -> e.getType() == GrpcLogEntry.EventType.EVENT_TYPE_SERVER_MESSAGE && e.getLogger() == GrpcLogEntry.Logger.LOGGER_SERVER).count();
        check("server logged 1 + 3 response messages", serverMsgs == 4, serverMsgs);
        System.out.println(failures == 0 ? "ALL OK" : failures + " FAILED");
        System.exit(failures == 0 ? 0 : 1);
    }
}
