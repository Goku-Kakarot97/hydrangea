package io.hydrangea.postgres;

import io.hydrangea.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The client against a scripted server: what real PostgreSQL does not do on purpose (split reads, garbage, a dying peer). */
class PgWireTest {

    private EventLoopGroup group;
    private ServerSocket server;

    @BeforeEach
    void setUp() throws IOException {
        group = new EventLoopGroup(1);
        server = new ServerSocket(0, 5, java.net.InetAddress.getLoopbackAddress());
    }

    @AfterEach
    void tearDown() throws IOException {
        server.close();
        group.close();
    }

    private PgConfig config() {
        return PgConfig.of("127.0.0.1", server.getLocalPort(), "u", "d").withLimits(100, 4096);
    }

    // ---- message building ----

    private static byte[] msg(char type, byte[] body) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        o.write(type);
        int len = body.length + 4;
        o.write(len >> 24);
        o.write(len >> 16);
        o.write(len >> 8);
        o.write(len);
        o.writeBytes(body);
        return o.toByteArray();
    }

    private static byte[] cat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            o.writeBytes(p);
        }
        return o.toByteArray();
    }

    private static byte[] i32(int v) {
        return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
    }

    private static byte[] i16(int v) {
        return new byte[]{(byte) (v >> 8), (byte) v};
    }

    private static byte[] cstr(String s) {
        return cat(s.getBytes(StandardCharsets.UTF_8), new byte[]{0});
    }

    private static byte[] authOk() {
        return msg('R', i32(0));
    }

    private static byte[] ready() {
        return msg('Z', new byte[]{'I'});
    }

    private static byte[] rowDescription(String name, int oid) {
        return msg('T', cat(i16(1), cstr(name), i32(0), i16(0), i32(oid), i16(4), i32(-1), i16(0)));
    }

    private static byte[] dataRow(String v) {
        byte[] b = v.getBytes(StandardCharsets.UTF_8);
        return msg('D', cat(i16(1), i32(b.length), b));
    }

    private static byte[] commandComplete(String tag) {
        return msg('C', cstr(tag));
    }

    /** One scripted connection: login done, then {@code script} for the first query. */
    private interface Script {
        void run(Socket s, DataInputStream in, OutputStream out) throws Exception;
    }

    private Thread serve(Script script) {
        Thread t = Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                s.setTcpNoDelay(true);
                DataInputStream in = new DataInputStream(s.getInputStream());
                OutputStream out = s.getOutputStream();
                int len = in.readInt();
                in.readFully(new byte[len - 4]);      // the startup message
                out.write(cat(authOk(), msg('S', cat(cstr("server_version"), cstr("16.0"))), msg('K', cat(i32(7), i32(8))), ready()));
                out.flush();
                script.run(s, in, out);
            } catch (Exception e) {
                // the client hung up
            }
        });
        return t;
    }

    /** Reads frontend messages until the Sync of one extended query. */
    private static void readQuery(DataInputStream in) throws IOException {
        for (;;) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            in.readFully(new byte[len - 4]);
            if (type == 'S' || type == 'Q') {
                return;
            }
        }
    }

    private static PgException failure(CompletableFuture<?> f) throws Exception {
        ExecutionException e = assertThrows(ExecutionException.class, () -> f.get(10, TimeUnit.SECONDS));
        return (PgException) e.getCause();
    }

    private PgClient connect() throws Exception {
        return PgClient.connect(group, config()).get(10, TimeUnit.SECONDS);
    }

    // ---- tests ----

    @Test
    void answersArrivingOneByteAtATimeAreAssembled() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            byte[] answer = cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]),
                    dataRow("41"), dataRow("42"), commandComplete("SELECT 2"), ready());
            for (byte b : answer) {
                out.write(b);
                out.flush();
            }
            in.read();
        });
        try (PgClient c = connect()) {
            PgResult r = c.query("SELECT n").get(10, TimeUnit.SECONDS);
            assertEquals(2, r.size());
            assertEquals(42, r.rows().get(1).getInt("n"));
            assertEquals("SELECT 2", r.tag());
        }
    }

    @Test
    void severalAnswersInOneReadAreAllHandled() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            readQuery(in);
            byte[] one = cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]), dataRow("1"), commandComplete("SELECT 1"), ready());
            byte[] two = cat(msg('2', new byte[0]), dataRow("2"), commandComplete("SELECT 1"), ready());
            out.write(cat(one, two));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            var a = c.query("SELECT n");
            var b = c.query("SELECT n");
            assertEquals(1, a.get(10, TimeUnit.SECONDS).first().getInt(0));
            assertEquals(2, b.get(10, TimeUnit.SECONDS).first().getInt(0), "the second used the description of the first");
        }
    }

    @Test
    void anAbsurdMessageLengthClosesTheConnection() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(new byte[]{'D'}, i32(1 << 20)));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            PgException e = failure(c.query("SELECT 1"));
            assertTrue(e.getMessage().contains("protocol error"), e.getMessage());
            assertFalse(c.isOpen());
        }
    }

    @Test
    void aLengthBelowTheMinimumClosesTheConnection() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(new byte[]{'C'}, i32(2)));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            assertTrue(failure(c.query("SELECT 1")).getMessage().contains("protocol error"));
        }
    }

    @Test
    void aRowWithoutADescriptionIsAProtocolError() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(msg('1', new byte[0]), dataRow("1"), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            assertTrue(failure(c.query("SELECT 1")).getMessage().contains("description"));
        }
    }

    @Test
    void aColumnLongerThanItsRowIsAProtocolError() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 25), msg('2', new byte[0]),
                    msg('D', cat(i16(1), i32(500), new byte[]{'x'})), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            assertTrue(failure(c.query("SELECT 1")).getMessage().contains("protocol error"));
        }
    }

    @Test
    void anAnswerWithNothingWaitingIsAProtocolError() throws Exception {
        serve((s, in, out) -> {
            out.write(ready());
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (c.isOpen() && System.nanoTime() < end) {
                Thread.sleep(10);
            }
            assertFalse(c.isOpen());
        }
    }

    @Test
    void anUnknownMessageTypeIsAProtocolError() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(msg('Q', new byte[0]));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            assertTrue(failure(c.query("SELECT 1")).getMessage().contains("unexpected"));
        }
    }

    @Test
    void theServerHangingUpInTheMiddleOfAnAnswerFailsTheQuery() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            byte[] row = dataRow("hello");
            out.write(cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 25), msg('2', new byte[0]), java.util.Arrays.copyOf(row, row.length - 2)));
            out.flush();
            s.close();
        });
        try (PgClient c = connect()) {
            PgException e = failure(c.query("SELECT 1"));
            assertFalse(e.retriable(), "it was sent: it may have run");
            assertFalse(c.isOpen());
        }
    }

    @Test
    void anErrorResponseWithAllItsFields() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(msg('E', cat(new byte[]{'S'}, cstr("ERROR"), new byte[]{'C'}, cstr("42P01"), new byte[]{'M'}, cstr("relation \"x\" does not exist"),
                    new byte[]{'H'}, cstr("create it"), new byte[]{0})), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            PgException e = failure(c.query("SELECT * FROM x"));
            assertEquals("42P01", e.sqlState());
            assertEquals("create it", e.hint());
            assertEquals("relation \"x\" does not exist", e.fields().get('M'));
            assertTrue(c.isOpen(), "an error response is not the end of the connection");
        }
    }

    @Test
    void theServerRefusingTheLoginWithAnErrorFailsConnect() throws Exception {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                in.readFully(new byte[in.readInt() - 4]);
                s.getOutputStream().write(msg('E', cat(new byte[]{'S'}, cstr("FATAL"), new byte[]{'C'}, cstr("28000"), new byte[]{'M'}, cstr("no pg_hba.conf entry"), new byte[]{0})));
                s.getOutputStream().flush();
                in.read();
            } catch (Exception e) {
                // done
            }
        });
        PgException e = failure(PgClient.connect(group, config()));
        assertEquals("28000", e.sqlState());
    }

    @Test
    void anUnsupportedAuthenticationMethodFailsConnect() throws Exception {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                DataInputStream in = new DataInputStream(s.getInputStream());
                in.readFully(new byte[in.readInt() - 4]);
                s.getOutputStream().write(msg('R', i32(7)));      // GSSAPI
                s.getOutputStream().flush();
                in.read();
            } catch (Exception e) {
                // done
            }
        });
        assertTrue(failure(PgClient.connect(group, config())).getMessage().contains("unsupported authentication"));
    }

    @Test
    void aServerThatNeverAnswersTheStartupTimesOut() throws Exception {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                s.getInputStream().read(new byte[1024]);
                Thread.sleep(3000);
            } catch (Exception e) {
                // done
            }
        });
        PgException e = failure(PgClient.connect(group, config().withConnectTimeout(java.time.Duration.ofMillis(300))));
        assertTrue(e.getMessage().contains("no login"), e.getMessage());
    }

    // ---- transactions: BEGIN rides with the first query ----

    /** Reads frontend messages until the Sync, returning their types and, for Parse, the SQL. */
    private static java.util.List<String> readQueryMessages(DataInputStream in) throws IOException {
        java.util.List<String> seen = new java.util.ArrayList<>();
        for (;;) {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);
            if (type == 'P') {
                int nul = 0;
                while (body[nul] != 0) nul++;                                   // statement name
                int start = nul + 1, end = start;
                while (body[end] != 0) end++;
                seen.add("P:" + new String(body, start, end - start, StandardCharsets.UTF_8));
            } else if (type == 'Q') {
                seen.add("Q:" + new String(body, 0, body.length - 1, StandardCharsets.UTF_8));
            } else {
                seen.add(String.valueOf((char) type));
            }
            if (type == 'S' || type == 'Q') {
                return seen;
            }
        }
    }

    @Test
    void theFirstQueryOfATransactionCarriesTheBeginInTheSameBatch() throws Exception {
        java.util.concurrent.atomic.AtomicReference<java.util.List<String>> first = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<java.util.List<String>> second = new java.util.concurrent.atomic.AtomicReference<>();
        serve((s, in, out) -> {
            first.set(readQueryMessages(in));
            // the answer to BEGIN + the query, as PostgreSQL gives it: the BEGIN's ParseComplete, BindComplete and CommandComplete come first
            out.write(cat(msg('1', new byte[0]), msg('2', new byte[0]), commandComplete("BEGIN"),
                    msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]), dataRow("7"), commandComplete("SELECT 1"), ready()));
            out.flush();
            second.set(readQueryMessages(in));
            out.write(cat(msg('2', new byte[0]), dataRow("8"), commandComplete("SELECT 1"), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            c.armBegin().get(10, TimeUnit.SECONDS);
            var a = c.query("SELECT n FROM t");
            var b = c.query("SELECT n FROM t");
            PgResult r = a.get(10, TimeUnit.SECONDS);
            assertEquals(7, r.first().getInt(0));
            assertEquals("SELECT 1", r.tag(), "the BEGIN's command tag is not the query's");
            assertEquals(1, r.size());
            assertEquals(8, b.get(10, TimeUnit.SECONDS).first().getInt(0));
            assertEquals(java.util.List.of("P:BEGIN", "B", "E", "P:SELECT n FROM t", "D", "B", "E", "S"), first.get(), "BEGIN, then the query, one Sync");
            assertEquals(java.util.List.of("B", "E", "S"), second.get(), "only the first query begins");
        }
    }

    @Test
    void aTransactionWithoutQueriesSendsNothing() throws Exception {
        java.util.concurrent.atomic.AtomicReference<java.util.List<String>> next = new java.util.concurrent.atomic.AtomicReference<>();
        serve((s, in, out) -> {
            next.set(readQueryMessages(in));        // the first thing that reaches the server is the later query
            out.write(cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]), dataRow("1"), commandComplete("SELECT 1"), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            c.armBegin().get(10, TimeUnit.SECONDS);
            assertEquals(0, c.endTransaction(true).get(10, TimeUnit.SECONDS).size());
            assertEquals(1, c.query("SELECT 1").get(10, TimeUnit.SECONDS).first().getInt(0));
            assertEquals(java.util.List.of("P:SELECT 1", "D", "B", "E", "S"), next.get(), "no BEGIN, no COMMIT went out, and nothing carried a BEGIN");
        }
    }

    @Test
    void aSimpleQueryAsTheFirstOfATransactionIsPrefixedAndItsBeginResultDropped() throws Exception {
        java.util.concurrent.atomic.AtomicReference<java.util.List<String>> got = new java.util.concurrent.atomic.AtomicReference<>();
        serve((s, in, out) -> {
            got.set(readQueryMessages(in));
            out.write(cat(commandComplete("BEGIN"), rowDescription("n", 23), dataRow("5"), commandComplete("SELECT 1"), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            c.armBegin().get(10, TimeUnit.SECONDS);
            var r = c.execute("SELECT 5 n").get(10, TimeUnit.SECONDS);
            assertEquals(1, r.size());
            assertEquals(5, r.get(0).first().getInt(0));
            assertEquals(java.util.List.of("Q:BEGIN;SELECT 5 n"), got.get());
        }
    }

    @Test
    void aFailingBeginFailsTheQueryItCarried() throws Exception {
        serve((s, in, out) -> {
            readQueryMessages(in);
            // the server rejects the BEGIN and skips the rest up to the Sync
            out.write(cat(msg('E', cat(new byte[]{'S'}, cstr("ERROR"), new byte[]{'C'}, cstr("25006"), new byte[]{'M'}, cstr("read-only"), new byte[]{0})), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            c.armBegin().get(10, TimeUnit.SECONDS);
            assertEquals("25006", failure(c.query("SELECT 1")).sqlState());
            assertTrue(c.isOpen());
        }
    }

    // ---- TLS request ----

    private PgConfig tlsConfig() throws Exception {
        return config().withTls(javax.net.ssl.SSLContext.getDefault());
    }

    /** One connection with nothing done for the client: the script starts from the SSLRequest. */
    private void serveRaw(Script script) {
        Thread.ofVirtual().start(() -> {
            try (Socket s = server.accept()) {
                s.setTcpNoDelay(true);
                script.run(s, new DataInputStream(s.getInputStream()), s.getOutputStream());
            } catch (Exception e) {
                // the client hung up
            }
        });
    }

    private static void expectSslRequest(DataInputStream in) throws IOException {
        assertEquals(8, in.readInt());
        assertEquals(80877103, in.readInt());
    }

    @Test
    void theTlsRequestIsTheFirstThingSentAndAnNoIsAnError() throws Exception {
        serveRaw((s, in, out) -> {
            expectSslRequest(in);
            out.write('N');
            out.flush();
            in.read();
        });
        PgException e = failure(PgClient.connect(group, tlsConfig()));
        assertTrue(e.getMessage().contains("does not support TLS"), e.getMessage());
    }

    @Test
    void bytesAfterTheSAreRefusedSoNothingCanBeSlippedIntoTheTlsSession() throws Exception {
        serveRaw((s, in, out) -> {
            expectSslRequest(in);
            out.write(new byte[]{'S', 'x'});      // as if an attacker had added plain text to the answer
            out.flush();
            in.read();
        });
        assertTrue(failure(PgClient.connect(group, tlsConfig())).getMessage().contains("single byte"));
    }

    @Test
    void anUnexpectedAnswerToTheTlsRequestIsAProtocolError() throws Exception {
        serveRaw((s, in, out) -> {
            expectSslRequest(in);
            out.write('E');
            out.flush();
            in.read();
        });
        assertTrue(failure(PgClient.connect(group, tlsConfig())).getMessage().contains("unexpected answer"));
    }

    @Test
    void afterTheSTheTlsHandshakeStartsAndNoStartupMessageGoesInPlainText() throws Exception {
        java.util.concurrent.atomic.AtomicInteger first = new java.util.concurrent.atomic.AtomicInteger(-1);
        serveRaw((s, in, out) -> {
            expectSslRequest(in);
            out.write('S');
            out.flush();
            first.set(in.readUnsignedByte());      // a TLS record of type handshake (22), not a startup message
            s.close();
        });
        PgException e = failure(PgClient.connect(group, tlsConfig()));
        assertEquals(22, first.get(), "the ClientHello comes next");
        assertTrue(e.getMessage().length() > 0);
    }

    @Test
    void aServerThatNeverAnswersTheTlsRequestTimesOut() throws Exception {
        serveRaw((s, in, out) -> {
            expectSslRequest(in);
            Thread.sleep(3000);
        });
        PgException e = failure(PgClient.connect(group, tlsConfig().withConnectTimeout(java.time.Duration.ofMillis(300))));
        assertTrue(e.getMessage().contains("no login"), e.getMessage());
    }

    // ---- time limits and the cancel request ----

    private static byte[] errorResponse(String code, String message) {
        return msg('E', cat(new byte[]{'S'}, cstr("ERROR"), new byte[]{'C'}, cstr(code), new byte[]{'M'}, cstr(message), new byte[]{0}));
    }

    private static byte[] answerWith(String value) {
        return cat(msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]), dataRow(value), commandComplete("SELECT 1"), ready());
    }

    /** Waits for the next connection to the fake server and reads the 16 bytes of a CancelRequest from it. */
    private CompletableFuture<int[]> acceptCancel(int timeoutMillis) {
        CompletableFuture<int[]> f = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            try {
                server.setSoTimeout(timeoutMillis);
                try (Socket s2 = server.accept()) {
                    DataInputStream in2 = new DataInputStream(s2.getInputStream());
                    f.complete(new int[]{in2.readInt(), in2.readInt(), in2.readInt(), in2.readInt()});
                }
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        });
        return f;
    }

    @Test
    void aTimedOutQueryIsCancelledOnAConnectionOfItsOwnWithThePidAndSecretOfTheLogin() throws Exception {
        CompletableFuture<Void> cancelSeen = new CompletableFuture<>();
        serve((s, in, out) -> {
            readQuery(in);
            cancelSeen.get(10, TimeUnit.SECONDS);
            out.write(cat(errorResponse("57014", "canceling statement due to user request"), ready()));   // the cancel took effect
            out.flush();
            readQuery(in);                                                                                 // and the connection goes on
            out.write(answerWith("1"));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            CompletableFuture<int[]> cancel = acceptCancel(10_000);
            var q = c.query(java.time.Duration.ofMillis(200), "SELECT pg_sleep(30)");
            PgException e = failure(q);
            assertTrue(e.isTimeout());
            assertFalse(e.retriable());
            assertArrayEquals(new int[]{16, 80877102, 7, 8}, cancel.get(10, TimeUnit.SECONDS), "length 16, the cancel request code, the pid and the secret key of BackendKeyData");
            cancelSeen.complete(null);
            assertEquals(1, c.query("SELECT n").get(10, TimeUnit.SECONDS).first().getInt(0), "the dropped error did not upset the connection");
        }
    }

    @Test
    void aQueryThatWaitsBehindAnotherIsCancelledOnlyWhenItsTurnComes() throws Exception {
        CompletableFuture<Void> release = new CompletableFuture<>();
        serve((s, in, out) -> {
            readQuery(in);
            readQuery(in);
            release.get(10, TimeUnit.SECONDS);
            out.write(answerWith("1"));        // the first one ends; now the server would start the second
            out.flush();
            Thread.sleep(300);
            out.write(cat(errorResponse("57014", "canceling statement due to user request"), ready()));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            CompletableFuture<int[]> early = acceptCancel(700);
            var first = c.query("SELECT n");
            var second = c.query(java.time.Duration.ofMillis(200), "SELECT pg_sleep(30)");
            assertTrue(failure(second).isTimeout());
            ExecutionException none = assertThrows(ExecutionException.class, () -> early.get(10, TimeUnit.SECONDS));
            assertTrue(none.getCause() instanceof java.net.SocketTimeoutException, "no cancel while it only waits: it would hit the query in front: " + none.getCause());
            CompletableFuture<int[]> late = acceptCancel(10_000);
            release.complete(null);
            assertEquals(1, first.get(10, TimeUnit.SECONDS).first().getInt(0));
            assertArrayEquals(new int[]{16, 80877102, 7, 8}, late.get(10, TimeUnit.SECONDS), "the cancel goes out when the first is answered and the second is the one running");
        }
    }

    @Test
    void aCancelThatReachedTheNextQueryIsRetriedBecauseItLeftNoTrace() throws Exception {
        CompletableFuture<Void> cancelSeen = new CompletableFuture<>();
        serve((s, in, out) -> {
            readQuery(in);
            readQuery(in);
            cancelSeen.get(10, TimeUnit.SECONDS);
            // the first query had just finished when the cancel arrived: the second one got it (and the connection is idle again, outside a transaction)
            out.write(cat(answerWith("1"), errorResponse("57014", "canceling statement due to user request"), ready()));
            out.flush();
            readQuery(in);                               // the second query, sent again
            out.write(answerWith("2"));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            CompletableFuture<int[]> cancel = acceptCancel(10_000);
            var timedOut = c.query(java.time.Duration.ofMillis(200), "SELECT pg_sleep(30)");
            var next = c.query("SELECT n");
            assertTrue(failure(timedOut).isTimeout());
            cancel.get(10, TimeUnit.SECONDS);
            cancelSeen.complete(null);
            assertEquals(2, next.get(10, TimeUnit.SECONDS).first().getInt(0), "the caller never sees the stray cancel: its query ran again");
        }
    }

    @Test
    void aCancelThatReachedTheNextQueryInsideATransactionIsReportedAsRetriable() throws Exception {
        CompletableFuture<Void> cancelSeen = new CompletableFuture<>();
        serve((s, in, out) -> {
            readQuery(in);
            readQuery(in);
            cancelSeen.get(10, TimeUnit.SECONDS);
            out.write(cat(answerWith("1"), errorResponse("57014", "canceling statement due to user request"), msg('Z', new byte[]{'E'})));   // in a failed transaction
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            CompletableFuture<int[]> cancel = acceptCancel(10_000);
            var timedOut = c.query(java.time.Duration.ofMillis(200), "SELECT pg_sleep(30)");
            var next = c.query("SELECT n");
            assertTrue(failure(timedOut).isTimeout());
            cancel.get(10, TimeUnit.SECONDS);
            cancelSeen.complete(null);
            PgException e = failure(next);
            assertTrue(e.retriable(), "it did not complete, and the transaction is gone: the caller decides");
            assertEquals("57014", e.sqlState());
            assertTrue(e.getMessage().contains("time limit of another query"), e.getMessage());
        }
    }

    @Test
    void aStrayCancelThatHitTheBeginOfATransactionRunsTheQueryAgainWithItsBegin() throws Exception {
        CompletableFuture<Void> cancelSeen = new CompletableFuture<>();
        java.util.List<java.util.List<String>> seen = new java.util.concurrent.CopyOnWriteArrayList<>();
        serve((s, in, out) -> {
            readQuery(in);                                    // the timed-out query
            seen.add(readQueryMessages(in));                  // the transaction's first query, with its BEGIN
            cancelSeen.get(10, TimeUnit.SECONDS);
            // the timed-out query had just finished; the stray cancel then killed the BEGIN of the next one: an error, nothing begun, the connection idle
            out.write(cat(answerWith("1"), errorResponse("57014", "canceling statement due to user request"), ready()));
            out.flush();
            seen.add(readQueryMessages(in));                  // the same query again: it must carry the BEGIN again
            out.write(cat(msg('2', new byte[0]), commandComplete("BEGIN"), msg('1', new byte[0]), msg('t', i16(0)), rowDescription("n", 23), msg('2', new byte[0]), dataRow("5"),
                    commandComplete("SELECT 1"), msg('Z', new byte[]{'T'})));
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            CompletableFuture<int[]> cancel = acceptCancel(10_000);
            var timedOut = c.query(java.time.Duration.ofMillis(200), "SELECT pg_sleep(30)");
            c.armBegin("BEGIN").get(10, TimeUnit.SECONDS);
            var first = c.query("SELECT n");
            assertTrue(failure(timedOut).isTimeout());
            cancel.get(10, TimeUnit.SECONDS);
            cancelSeen.complete(null);
            assertEquals(5, first.get(10, TimeUnit.SECONDS).first().getInt(0));
            assertEquals(2, seen.size());
            assertEquals("P:BEGIN", seen.get(0).get(0), "the first attempt carried the BEGIN");
            assertEquals("P:BEGIN", seen.get(1).get(0), "and so did the second: without it the query would have run outside the transaction");
            assertEquals('T', c.transactionStatus());
        }
    }

    @Test
    void aCancelFromTheServerThatWeDidNotCauseIsAnOrdinaryError() throws Exception {
        serve((s, in, out) -> {
            readQuery(in);
            out.write(cat(errorResponse("57014", "canceling statement due to user request"), ready()));   // an administrator's pg_cancel_backend
            out.flush();
            in.read();
        });
        try (PgClient c = connect()) {
            PgException e = failure(c.query("SELECT n"));
            assertFalse(e.retriable());
            assertFalse(e.isTimeout());
        }
    }
}
