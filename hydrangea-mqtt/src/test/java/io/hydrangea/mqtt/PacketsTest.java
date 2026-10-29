package io.hydrangea.mqtt;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PacketsTest {

    private static byte[] bytes(ByteBuffer b) {
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    @Test
    void topicMatchingFollowsTheSpec() {
        String[][] yes = {
                {"a/b", "a/b"}, {"a/+", "a/b"}, {"+/b", "a/b"}, {"+/+", "a/b"}, {"#", "a/b/c"}, {"a/#", "a/b/c"}, {"a/#", "a"},
                {"a/b/#", "a/b"}, {"+", "a"}, {"a//b", "a//b"}, {"a/+/c", "a//c"}, {"+/+", "/a"}, {"/+", "/a"}, {"#", "/"},
                {"$SYS/#", "$SYS/broker"}, {"$SYS/+", "$SYS/x"}, {"a/+/#", "a/b"}, {"a/+/#", "a/b/c/d"}};
        for (String[] c : yes) {
            assertTrue(Packets.matches(c[0], c[1]), c[0] + " should match " + c[1]);
        }
        String[][] no = {
                {"a/b", "a"}, {"a", "a/b"}, {"a/+", "a"}, {"a/+", "a/b/c"}, {"+", "a/b"}, {"a/b", "A/b"}, {"#", "$SYS/x"}, {"+/x", "$SYS/x"},
                {"+/#", "$SYS/x"}, {"a/+/c", "a/c"}, {"a/b/#", "a/c"}, {"a/+", "b/c"}, {"+/+", "a"}};
        for (String[] c : no) {
            assertFalse(Packets.matches(c[0], c[1]), c[0] + " should not match " + c[1]);
        }
    }

    @Test
    void filtersAndTopicNamesAreValidated() {
        for (String ok : new String[] {"a", "a/b", "+", "#", "a/+/b", "a/#", "/", "+/+/#", "$SYS/#", "a//b"}) {
            Packets.checkTopicFilter(ok);
        }
        for (String bad : new String[] {"", "a#", "#/a", "a/#/b", "a+", "+a", "a/b+/c", "a/#b", "\0"}) {
            assertThrows(IllegalArgumentException.class, () -> Packets.checkTopicFilter(bad), "filter " + bad);
        }
        Packets.checkTopicName("a/b");
        Packets.checkTopicName("$SYS/x");
        for (String bad : new String[] {"", "a/+", "#", "a/#", "a\0b"}) {
            assertThrows(IllegalArgumentException.class, () -> Packets.checkTopicName(bad), "topic " + bad);
        }
        assertThrows(IllegalArgumentException.class, () -> Packets.checkTopicName("\ud800"), "a lone surrogate is not UTF-8");
        assertThrows(IllegalArgumentException.class, () -> Packets.checkTopicName("x".repeat(65536)), "over 65535 bytes");
        Packets.checkTopicName("é".repeat(32767)); // exactly 65534 bytes
        assertThrows(IllegalArgumentException.class, () -> Packets.checkTopicName("é".repeat(32768)));
    }

    @Test
    void remainingLengthBoundaries() {
        int[][] cases = {{0, 1}, {127, 1}, {128, 2}, {16383, 2}, {16384, 3}, {2097151, 3}, {2097152, 4}};
        for (int[] c : cases) {
            Packets.Writer w = new Packets.Writer(16);
            byte[] body = new byte[c[0]];
            w.raw(body, 0, body.length);
            ByteBuffer p = w.packet(Packets.PUBLISH, 0);
            assertEquals(1 + c[1] + c[0], p.remaining(), "length " + c[0]);
            int len = 0;
            for (int k = 0; k < c[1]; k++) {
                int d = p.get(1 + k) & 0xff;
                len |= (d & 0x7f) << (7 * k);
                assertEquals(k < c[1] - 1, (d & 0x80) != 0);
            }
            assertEquals(c[0], len);
        }
    }

    @Test
    void connectPacketMatchesTheSpecByteForByte() {
        MqttConfig c = MqttConfig.of("h", 1).withClientId("abc").withKeepAlive(Duration.ofSeconds(30))
                .withCredentials("u", "pw")
                .withWill(new MqttConfig.Will("t/w", new byte[] {1, 2}, QoS.AT_LEAST_ONCE, true));
        byte[] p = bytes(Packets.connect(c));
        byte[] expected = {
                0x10, 31,                                       // CONNECT, remaining length 31
                0, 4, 'M', 'Q', 'T', 'T', 4,                    // protocol name and level 4
                (byte) (0x80 | 0x40 | 0x20 | 0x08 | 0x04 | 0x02), // user, password, will retain, will QoS 1, will, clean session
                0, 30,                                          // keep alive
                0, 3, 'a', 'b', 'c',                            // client id
                0, 3, 't', '/', 'w', 0, 2, 1, 2,                // will topic, will message
                0, 1, 'u', 0, 2, 'p', 'w'};                     // user, password
        assertArrayEquals(expected, p);
    }

    @Test
    void minimalConnectAndTheSmallPackets() {
        byte[] p = bytes(Packets.connect(MqttConfig.of("h", 1).withKeepAlive(null)));
        assertArrayEquals(new byte[] {0x10, 12, 0, 4, 'M', 'Q', 'T', 'T', 4, 2, 0, 0, 0, 0}, p);
        assertArrayEquals(new byte[] {(byte) 0xC0, 0}, bytes(Packets.pingreq()));
        assertArrayEquals(new byte[] {(byte) 0xE0, 0}, bytes(Packets.disconnect()));
        assertArrayEquals(new byte[] {0x40, 2, 0x12, 0x34}, bytes(Packets.ack(Packets.PUBACK, 0x1234)));
        assertArrayEquals(new byte[] {0x62, 2, 0, 5}, bytes(Packets.ack(Packets.PUBREL, 5)), "PUBREL carries the reserved flags 0010");
        assertArrayEquals(new byte[] {(byte) 0x82, 8, 0, 9, 0, 3, 'a', '/', '#', 2}, bytes(Packets.subscribe(9, List.of("a/#"), List.of(QoS.EXACTLY_ONCE))));
        assertArrayEquals(new byte[] {(byte) 0xA2, 7, 0, 9, 0, 3, 'a', '/', '#'}, bytes(Packets.unsubscribe(9, List.of("a/#"))));
    }

    @Test
    void publishPacketCarriesFlagsAndIdentifier() {
        byte[] q0 = bytes(Packets.publish("t", new byte[] {7}, 0, false, false, 0));
        assertArrayEquals(new byte[] {0x30, 4, 0, 1, 't', 7}, q0);
        byte[] q2 = bytes(Packets.publish("t", new byte[0], 2, true, true, 0x0102));
        assertArrayEquals(new byte[] {0x30 | 8 | 4 | 1, 5, 0, 1, 't', 1, 2}, q2);
        byte[] big = bytes(Packets.publish("t", new byte[200], 1, false, false, 1));
        assertEquals(1 + 2 + 2 + 1 + 2 + 200, big.length);
        assertEquals((byte) (200 + 5 | 0x80), big[1]);
    }

    @Test
    void aMessageOverTheProtocolLimitIsRefusedNotTruncated() {
        assertThrows(IllegalArgumentException.class,
                () -> Packets.publish("t", new byte[Packets.MAX_REMAINING], 0, false, false, 0));
    }

    @Test
    void stringsAreStrictUtf8() {
        assertArrayEquals("héllo".getBytes(StandardCharsets.UTF_8), Packets.utf8("héllo"));
        assertThrows(IllegalArgumentException.class, () -> Packets.utf8("a\0b"));
        // inbound: overlong encoding, a surrogate half, a truncated sequence, U+0000
        for (byte[] bad : new byte[][] {{(byte) 0xC0, (byte) 0x80}, {(byte) 0xED, (byte) 0xA0, (byte) 0x80}, {(byte) 0xE2, (byte) 0x82}, {0}}) {
            ByteBuffer b = ByteBuffer.allocate(2 + bad.length).putShort((short) bad.length).put(bad).flip();
            assertThrows(MqttException.class, () -> new Packets.Reader(b).str());
        }
    }

    @Test
    void configRejectsWhatTheSpecForbids() {
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withCleanSession(false), "empty id with a persistent session");
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withCredentials(null, "pw"), "password without user");
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withKeepAlive(Duration.ofSeconds(65536)));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withLimits(10, 5));
        assertThrows(IllegalArgumentException.class, () -> MqttConfig.of("h", 1).withLimits(0, 5));
        assertThrows(IllegalArgumentException.class, () -> new MqttConfig.Will("a/#", new byte[0], QoS.AT_MOST_ONCE, false));
    }
}
