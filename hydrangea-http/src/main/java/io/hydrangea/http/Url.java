package io.hydrangea.http;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Percent-decoding shared by path and query parsing. */
final class Url {

    private Url() {
    }

    /**
     * Decodes %XX escapes as UTF-8. Characters below 256 in the input are treated as raw bytes, because the HTTP
     * parser reads the request target as ISO-8859-1 (so unescaped UTF-8 from sloppy clients still decodes correctly).
     *
     * @param plusAsSpace true for query strings and form bodies, false for paths (where '+' is a literal plus)
     * @throws HttpStatusException 400 on a malformed escape or invalid UTF-8
     */
    static String decode(String s, boolean plusAsSpace) {
        boolean plain = true;
        for (int i = 0; i < s.length() && plain; i++) {
            char c = s.charAt(i);
            plain = c != '%' && c != '+' && c < 0x80;
        }
        if (plain) {
            return s;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 >= s.length()) {
                    throw bad();
                }
                int hi = Character.digit(s.charAt(i + 1), 16);
                int lo = Character.digit(s.charAt(i + 2), 16);
                if (hi < 0 || lo < 0) {
                    throw bad();
                }
                out.write(hi << 4 | lo);
                i += 2;
            } else if (c == '+' && plusAsSpace) {
                out.write(' ');
            } else if (c < 0x100) {
                out.write(c);
            } else {
                throw bad();
            }
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(out.toByteArray())).toString();
        } catch (CharacterCodingException e) {
            throw bad();
        }
    }

    private static HttpStatusException bad() {
        return new HttpStatusException(400, "Malformed percent-encoding");
    }
}
