package io.hydrangea.mqtt;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** MQTT 3.1.1 packet encoding, the reader for inbound packets, and topic validation and matching. */
final class Packets {

    static final int CONNECT = 1, CONNACK = 2, PUBLISH = 3, PUBACK = 4, PUBREC = 5, PUBREL = 6, PUBCOMP = 7, SUBSCRIBE = 8, SUBACK = 9,
            UNSUBSCRIBE = 10, UNSUBACK = 11, PINGREQ = 12, PINGRESP = 13, DISCONNECT = 14, AUTH = 15;

    /** The largest value of the Remaining Length field (four bytes of seven bits). */
    static final int MAX_REMAINING = 268_435_455;

    private Packets() {
    }

    // ---- strings and topics ----

    /** UTF-8 for an MQTT string: no lone surrogates, no U+0000, at most 65535 bytes. */
    static byte[] utf8(String s) {
        if (s.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("an MQTT string may not contain U+0000");
        }
        try {
            ByteBuffer b = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).encode(CharBuffer.wrap(s));
            if (b.remaining() > 65535) {
                throw new IllegalArgumentException("an MQTT string is at most 65535 bytes");
            }
            byte[] out = new byte[b.remaining()];
            b.get(out);
            return out;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("not valid UTF-8 (lone surrogate)", e);
        }
    }

    static void checkTopicName(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("a topic name may not be empty");
        }
        if (topic.indexOf('+') >= 0 || topic.indexOf('#') >= 0) {
            throw new IllegalArgumentException("a topic name to publish to may not contain wildcards: " + topic);
        }
        utf8(topic);
    }

    /** '+' and '#' only as whole levels, '#' only last. */
    static void checkTopicFilter(String filter) {
        if (filter == null || filter.isEmpty()) {
            throw new IllegalArgumentException("a topic filter may not be empty");
        }
        utf8(filter);
        String[] levels = filter.split("/", -1);
        for (int i = 0; i < levels.length; i++) {
            String l = levels[i];
            if (l.contains("#") && (!l.equals("#") || i != levels.length - 1)) {
                throw new IllegalArgumentException("'#' must be a whole last level: " + filter);
            }
            if (l.contains("+") && !l.equals("+")) {
                throw new IllegalArgumentException("'+' must be a whole level: " + filter);
            }
        }
    }

    /** Whether {@code topic} matches {@code filter}; a filter that starts with a wildcard never matches a topic that starts with '$'. */
    static boolean matches(String filter, String topic) {
        if (!topic.isEmpty() && topic.charAt(0) == '$' && !filter.isEmpty() && (filter.charAt(0) == '+' || filter.charAt(0) == '#')) {
            return false;
        }
        String[] f = filter.split("/", -1);
        String[] t = topic.split("/", -1);
        for (int i = 0; i < f.length; i++) {
            if (f[i].equals("#")) {
                return true;   // the rest of the topic, possibly nothing: "a/#" matches "a"
            }
            if (i >= t.length || !f[i].equals("+") && !f[i].equals(t[i])) {
                return false;
            }
        }
        return f.length == t.length;
    }

    // ---- writer ----

    /** Builds the body of a packet. */
    static final class Writer {
        private byte[] buf;
        private int n;

        Writer(int capacity) {
            buf = new byte[Math.max(16, capacity)];
        }

        private void room(int more) {
            if (n + more > buf.length) {
                buf = java.util.Arrays.copyOf(buf, Math.max(buf.length * 2, n + more));
            }
        }

        Writer u8(int v) {
            room(1);
            buf[n++] = (byte) v;
            return this;
        }

        Writer u32(long v) {
            room(4);
            buf[n++] = (byte) (v >>> 24);
            buf[n++] = (byte) (v >>> 16);
            buf[n++] = (byte) (v >>> 8);
            buf[n++] = (byte) v;
            return this;
        }

        /** A variable byte integer (7 bits per byte, at most four bytes). */
        Writer vbi(int v) {
            if (v < 0 || v > MAX_REMAINING) throw new IllegalArgumentException("variable byte integer " + v);
            do {
                int d = v & 0x7f;
                v >>>= 7;
                u8(v > 0 ? d | 0x80 : d);
            } while (v > 0);
            return this;
        }

        /** Binary data: a two-byte length and the bytes. */
        Writer bin(byte[] b) {
            if (b.length > 65535) throw new IllegalArgumentException("binary data over 65535 bytes");
            u16(b.length);
            return raw(b, 0, b.length);
        }

        /** A properties block: its size as a variable byte integer, then the encoded properties. */
        Writer props(byte[] encoded) {
            vbi(encoded.length);
            return raw(encoded, 0, encoded.length);
        }

        byte[] toByteArray() {
            return java.util.Arrays.copyOf(buf, n);
        }

        Writer u16(int v) {
            room(2);
            buf[n++] = (byte) (v >>> 8);
            buf[n++] = (byte) v;
            return this;
        }

        Writer str(byte[] utf8) {
            u16(utf8.length);
            return raw(utf8, 0, utf8.length);
        }

        Writer raw(byte[] b, int off, int len) {
            room(len);
            System.arraycopy(b, off, buf, n, len);
            n += len;
            return this;
        }

        int size() {
            return n;
        }

        /** The whole packet: fixed header (type, flags, remaining length) followed by this body. */
        ByteBuffer packet(int type, int flags) {
            if (n > MAX_REMAINING) {
                throw new IllegalArgumentException("packet of " + n + " bytes is over the MQTT limit");
            }
            byte[] out = new byte[1 + 4 + n];
            int p = 0;
            out[p++] = (byte) (type << 4 | flags);
            int len = n;
            do {
                int d = len & 0x7f;
                len >>>= 7;
                out[p++] = (byte) (len > 0 ? d | 0x80 : d);
            } while (len > 0);
            System.arraycopy(buf, 0, out, p, n);
            return ByteBuffer.wrap(out, 0, p + n);
        }
    }

    // ---- encoders ----

    static ByteBuffer connect(MqttConfig c) {
        return connect(c, c.clientId(), null);
    }

    /** @param authData MQTT 5 enhanced authentication: the first data of the exchange (the method comes from the config), may be null */
    static ByteBuffer connect(MqttConfig c, String clientId, byte[] authData) {
        boolean v5 = c.version() == 5;
        Writer w = new Writer(64);
        w.str("MQTT".getBytes(StandardCharsets.US_ASCII)).u8(v5 ? 5 : 4);
        int flags = 0;
        if (c.cleanSession()) flags |= 0x02;
        MqttConfig.Will will = c.will();
        if (will != null) {
            flags |= 0x04 | will.qos().level() << 3 | (will.retain() ? 0x20 : 0);
        }
        if (c.username() != null) flags |= 0x80;
        if (c.password() != null) flags |= 0x40;
        w.u8(flags).u16(c.keepAlive() == null ? 0 : (int) c.keepAlive().toSeconds());
        if (v5) {
            MqttConfig.V5 o = c.v5();
            Props5 p = new Props5();
            if (o.sessionExpirySeconds() > 0) p.sessionExpiry = o.sessionExpirySeconds();
            if (o.receiveMaximum() < 65535) p.receiveMaximum = o.receiveMaximum();
            if (o.maximumPacketSize() > 0) p.maximumPacketSize = o.maximumPacketSize();
            if (o.topicAliasMaximum() > 0) p.topicAliasMaximum = o.topicAliasMaximum();
            p.user.addAll(o.userProperties());
            if (o.authenticator() != null) {
                p.authMethod = o.authenticator().method();
                p.authData = authData;
            }
            w.props(p.encode());
        }
        w.str(utf8(clientId));
        if (will != null) {
            if (v5) {
                Props5 wp = Props5.of(will.properties());
                if (will.delaySeconds() > 0) wp.willDelay = will.delaySeconds();
                w.props(wp.encode());
            }
            w.str(utf8(will.topic())).u16(will.payload().length).raw(will.payload(), 0, will.payload().length);
        }
        if (c.username() != null) w.str(utf8(c.username()));
        if (c.password() != null) {
            byte[] p = c.password().getBytes(StandardCharsets.UTF_8);
            if (p.length > 65535) throw new IllegalArgumentException("password too long");
            w.u16(p.length).raw(p, 0, p.length);
        }
        return w.packet(CONNECT, 0);
    }

    static ByteBuffer publish(String topic, byte[] payload, int qos, boolean retain, boolean dup, int packetId) {
        return publish(topic, payload, qos, retain, dup, packetId, null);
    }

    /** @param props the encoded MQTT 5 properties (without their length), or null for MQTT 3.1.1 */
    static ByteBuffer publish(String topic, byte[] payload, int qos, boolean retain, boolean dup, int packetId, byte[] props) {
        byte[] t = utf8(topic);
        int propsLen = props == null ? 0 : vbiSize(props.length) + props.length;
        long body = 2L + t.length + (qos > 0 ? 2 : 0) + propsLen + payload.length;
        if (body > MAX_REMAINING) {
            throw new IllegalArgumentException("message of " + payload.length + " bytes is over the MQTT limit");
        }
        byte[] out = new byte[(int) (1 + 4 + body)];   // built in place: a payload of many MiB is copied once, not twice
        int p = 0;
        out[p++] = (byte) (PUBLISH << 4 | (dup ? 8 : 0) | qos << 1 | (retain ? 1 : 0));
        p = putVbi(out, p, (int) body);
        out[p++] = (byte) (t.length >>> 8);
        out[p++] = (byte) t.length;
        System.arraycopy(t, 0, out, p, t.length);
        p += t.length;
        if (qos > 0) {
            out[p++] = (byte) (packetId >>> 8);
            out[p++] = (byte) packetId;
        }
        if (props != null) {
            p = putVbi(out, p, props.length);
            System.arraycopy(props, 0, out, p, props.length);
            p += props.length;
        }
        System.arraycopy(payload, 0, out, p, payload.length);
        return ByteBuffer.wrap(out, 0, p + payload.length);
    }

    private static int vbiSize(int v) {
        return v < 128 ? 1 : v < 16384 ? 2 : v < 2097152 ? 3 : 4;
    }

    private static int putVbi(byte[] out, int p, int v) {
        do {
            int d = v & 0x7f;
            v >>>= 7;
            out[p++] = (byte) (v > 0 ? d | 0x80 : d);
        } while (v > 0);
        return p;
    }

    /** PUBACK, PUBREC, PUBREL (flags 2) or PUBCOMP, UNSUBACK-like: a type and a packet identifier. */
    static ByteBuffer ack(int type, int packetId) {
        return new Writer(2).u16(packetId).packet(type, type == PUBREL ? 2 : 0);
    }

    static ByteBuffer subscribe(int packetId, List<String> filters, List<QoS> qos) {
        List<SubscribeOptions> o = new java.util.ArrayList<>();
        for (QoS q : qos) o.add(SubscribeOptions.of(q));
        return subscribe(packetId, filters, o, 0, false);
    }

    /** @param subscriptionId MQTT 5 only: the identifier the broker will attach to matching messages, 0 = none */
    static ByteBuffer subscribe(int packetId, List<String> filters, List<SubscribeOptions> options, int subscriptionId, boolean v5) {
        Writer w = new Writer(64).u16(packetId);
        if (v5) {
            Props5 p = new Props5();
            if (subscriptionId > 0) p.subscriptionIds.add(subscriptionId);
            w.props(p.encode());
        }
        for (int i = 0; i < filters.size(); i++) {
            SubscribeOptions o = options.get(i);
            w.str(utf8(filters.get(i)));
            if (!v5 && !o.isPlain()) {
                throw new IllegalArgumentException("noLocal, retainAsPublished and retainHandling need MQTT 5");
            }
            w.u8(o.qos().level() | (o.noLocal() ? 4 : 0) | (o.retainAsPublished() ? 8 : 0) | o.retainHandling().code() << 4);
        }
        return w.packet(SUBSCRIBE, 2);
    }

    static ByteBuffer unsubscribe(int packetId, List<String> filters) {
        return unsubscribe(packetId, filters, false);
    }

    static ByteBuffer unsubscribe(int packetId, List<String> filters, boolean v5) {
        Writer w = new Writer(64).u16(packetId);
        if (v5) {
            w.vbi(0);
        }
        for (String f : filters) {
            w.str(utf8(f));
        }
        return w.packet(UNSUBSCRIBE, 2);
    }

    /** MQTT 5 AUTH: a reason code (0x00 success, 0x18 continue, 0x19 re-authenticate), the method and the data. */
    static ByteBuffer auth(int reason, String method, byte[] data) {
        Props5 p = new Props5();
        p.authMethod = method;
        p.authData = data;
        return new Writer(32).u8(reason).props(p.encode()).packet(AUTH, 0);
    }

    static ByteBuffer pingreq() {
        return new Writer(0).packet(PINGREQ, 0);
    }

    static ByteBuffer disconnect() {
        return new Writer(0).packet(DISCONNECT, 0);
    }

    // ---- reader ----

    /** Reads the body of one inbound packet; anything malformed is a protocol error. */
    static final class Reader {
        private final ByteBuffer b;

        Reader(ByteBuffer body) {
            this.b = body;
        }

        int u8() {
            need(1);
            return b.get() & 0xff;
        }

        int u16() {
            need(2);
            return b.getShort() & 0xffff;
        }

        long u32() {
            need(4);
            return b.getInt() & 0xffffffffL;
        }

        int vbi() {
            int v = 0;
            for (int k = 0; k < 4; k++) {
                int d = u8();
                v |= (d & 0x7f) << (7 * k);
                if ((d & 0x80) == 0) {
                    return v;
                }
            }
            throw new MqttException("a variable byte integer of more than four bytes", false);
        }

        byte[] bin() {
            int len = u16();
            need(len);
            byte[] out = new byte[len];
            b.get(out);
            return out;
        }

        /** The next {@code len} bytes as a reader of their own (they are consumed here). */
        Reader sub(int len) {
            need(len);
            ByteBuffer s = b.slice(b.position(), len);
            b.position(b.position() + len);
            return new Reader(s);
        }

        String str() {
            int len = u16();
            need(len);
            byte[] raw = new byte[len];
            b.get(raw);
            try {
                String s = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(raw)).toString();
                if (s.indexOf('\0') >= 0) throw new MqttException("a string with U+0000", false);
                return s;
            } catch (CharacterCodingException e) {
                throw new MqttException("a string that is not valid UTF-8", false, e);
            }
        }

        int remaining() {
            return b.remaining();
        }

        byte[] rest() {
            byte[] out = new byte[b.remaining()];
            b.get(out);
            return out;
        }

        void end() {
            if (b.hasRemaining()) throw new MqttException(b.remaining() + " unexpected trailing byte(s) in a packet", false);
        }

        private void need(int n) {
            if (b.remaining() < n) throw new MqttException("a packet is shorter than its fields", false);
        }
    }
}
