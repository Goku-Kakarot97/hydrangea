package io.hydrangea.postgres;

import java.nio.charset.StandardCharsets;

/** The messages a client sends. */
final class Wire {

    private Wire() {
    }

    /** The first message of a connection: protocol 3.0 and the startup parameters. */
    static byte[] startup(String user, String database, String applicationName) {
        Buf b = new Buf(128);
        b.i32(0).i32(196608);
        b.cstr("user").cstr(user);
        b.cstr("database").cstr(database);
        b.cstr("client_encoding").cstr("UTF8");
        if (applicationName != null) {
            b.cstr("application_name").cstr(applicationName);
        }
        b.b(0);
        int len = b.n;
        b.a[0] = (byte) (len >> 24);
        b.a[1] = (byte) (len >> 16);
        b.a[2] = (byte) (len >> 8);
        b.a[3] = (byte) len;
        return b.toByteArray();
    }

    static void parse(Buf b, String name, String sql, int[] oids) {
        int at = b.begin('P');
        b.cstr(name).cstr(sql);
        b.i16(oids.length);
        for (int oid : oids) {
            b.i32(oid);
        }
        b.end(at);
    }

    /** {@code kind} 'S' describes a prepared statement, 'P' a portal. */
    static void describe(Buf b, char kind, String name) {
        int at = b.begin('D');
        b.b(kind).cstr(name);
        b.end(at);
    }

    static void close(Buf b, char kind, String name) {
        int at = b.begin('C');
        b.b(kind).cstr(name);
        b.end(at);
    }

    /** Binds text parameters (a null value is SQL NULL) and asks for text results. */
    static void bind(Buf b, String portal, String statement, byte[][] values) {
        bind(b, portal, statement, values, null, null);
    }

    /**
     * Binds parameters, each in the format {@code binaryParams} says (null = all text), and asks for the results in the formats of {@code resultFormats}, one per column, 1 = binary
     * (null = all text).
     */
    static void bind(Buf b, String portal, String statement, byte[][] values, boolean[] binaryParams, byte[] resultFormats) {
        int at = b.begin('B');
        b.cstr(portal).cstr(statement);
        if (binaryParams == null) {
            b.i16(0);                   // parameter formats: none given = all text
        } else {
            b.i16(binaryParams.length);
            for (boolean bin : binaryParams) {
                b.i16(bin ? 1 : 0);
            }
        }
        b.i16(values.length);
        for (byte[] v : values) {
            if (v == null) {
                b.i32(-1);
            } else {
                b.i32(v.length).raw(v);
            }
        }
        if (resultFormats == null) {
            b.i16(0);                   // result formats: none given = all text
        } else {
            b.i16(resultFormats.length);
            for (byte f : resultFormats) {
                b.i16(f);
            }
        }
        b.end(at);
    }

    static void execute(Buf b, String portal) {
        execute(b, portal, 0);          // no row limit
    }

    /** Asks for at most {@code maxRows} rows (0 = all); a portal that has more answers {@code PortalSuspended} and waits for the next Execute. */
    static void execute(Buf b, String portal, int maxRows) {
        int at = b.begin('E');
        b.cstr(portal).i32(maxRows);
        b.end(at);
    }

    /** Asks the server to send what it has so far, without ending the cycle the way a Sync does. */
    static void flush(Buf b) {
        int at = b.begin('H');
        b.end(at);
    }

    static void sync(Buf b) {
        int at = b.begin('S');
        b.end(at);
    }

    static void query(Buf b, String sql) {
        int at = b.begin('Q');
        b.cstr(sql);
        b.end(at);
    }

    static void terminate(Buf b) {
        int at = b.begin('X');
        b.end(at);
    }

    /** What a connection of its own sends to cancel the query another connection is running: its backend's process id and secret key. */
    static byte[] cancelRequest(int pid, int secret) {
        Buf b = new Buf(16);
        b.i32(16).i32(80877102).i32(pid).i32(secret);
        return b.toByteArray();
    }

    static void copyData(Buf b, byte[] chunk) {
        int at = b.begin('d');
        b.raw(chunk);
        b.end(at);
    }

    static void copyDone(Buf b) {
        int at = b.begin('c');
        b.end(at);
    }

    static void copyFail(Buf b, String why) {
        int at = b.begin('f');
        b.cstr(why);
        b.end(at);
    }

    /** A password message ('p'): cleartext, MD5 or the SASL responses all use it. */
    static byte[] password(byte[] body) {
        Buf b = new Buf(body.length + 5);
        int at = b.begin('p');
        b.raw(body);
        b.end(at);
        return b.toByteArray();
    }

    static byte[] cstring(String s) {
        return (s + "\0").getBytes(StandardCharsets.UTF_8);
    }
}
