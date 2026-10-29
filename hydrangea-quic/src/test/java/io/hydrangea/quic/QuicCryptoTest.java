package io.hydrangea.quic;

import org.junit.jupiter.api.Test;

import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

/** The test vectors of RFC 9001 appendix A. */
class QuicCryptoTest {
    private static byte[] hex(String s) {
        return HexFormat.of().parseHex(s.replace(" ", ""));
    }

    @Test
    void initialKeysMatchTheRfcVectors() {
        QuicCrypto.Keys[] k = QuicCrypto.initialKeys(hex("8394c8f03e515708"));
        assertArrayEquals(hex("1f369613dd76d5467730efcbe3b1a22d"), k[0].keyBytes());
        assertArrayEquals(hex("fa044b2f42a3fd3b46fb255c"), k[0].iv());
        assertArrayEquals(hex("9f50449e04a0e810283a1e9933adedd2"), k[0].hpBytes());
        assertArrayEquals(hex("cf3a5331653c364c88f0f379b6067e37"), k[1].keyBytes());
        assertArrayEquals(hex("0ac1493ca1905853b0bba03e"), k[1].iv());
        assertArrayEquals(hex("c206b8d9b9f0f37644430b490eeaa314"), k[1].hpBytes());
    }

    @Test
    void headerProtectionMaskMatchesTheRfcVector() {
        QuicCrypto.Keys[] k = QuicCrypto.initialKeys(hex("8394c8f03e515708"));
        byte[] mask = k[0].mask(hex("d1b1c98dd7689fb8ec11d242b123dc9b"), 0);
        assertArrayEquals(hex("437b9aec36"), java.util.Arrays.copyOf(mask, 5));
    }

    @Test
    void sealAndOpenRoundTripAndRejectTampering() {
        QuicCrypto.Keys[] k = QuicCrypto.initialKeys(hex("0001020304050607"));
        byte[] header = hex("c00000000108000102030405060700");
        byte[] ct = k[0].seal(7, header, "hello quic".getBytes(), 0, 10);
        assertEquals(10 + 16, ct.length);
        assertEquals("hello quic", new String(k[0].open(7, header, ct, 0, ct.length)));
        assertNull(k[0].open(8, header, ct, 0, ct.length), "wrong packet number");
        ct[3] ^= 1;
        assertNull(k[0].open(7, header, ct, 0, ct.length), "flipped bit");
    }
}
