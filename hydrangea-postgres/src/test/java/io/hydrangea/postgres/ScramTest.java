package io.hydrangea.postgres;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScramTest {

    // RFC 7677 section 3
    private static final String NONCE = "rOprNGfwEbeRWgbNEkqO";
    private static final String SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";

    @Test
    void matchesTheRfc7677Exchange() {
        Scram s = new Scram("user", "pencil", NONCE);
        assertEquals("n,,n=user,r=rOprNGfwEbeRWgbNEkqO", s.clientFirst());
        assertEquals("c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
                s.clientFinal(SERVER_FIRST));
        s.verifyServerFinal("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");
    }

    @Test
    void rejectsAWrongServerSignatureANonceThatDoesNotExtendOursAndAServerError() {
        Scram s = new Scram("user", "pencil", NONCE);
        s.clientFinal(SERVER_FIRST);
        assertThrows(IllegalStateException.class, () -> s.verifyServerFinal("v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="));
        assertThrows(IllegalStateException.class, () -> s.verifyServerFinal("e=invalid-proof"));
        assertThrows(IllegalStateException.class,
                () -> new Scram("user", "pencil", NONCE).clientFinal("r=somethingelse,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"));
    }

    @Test
    void anEmptyPasswordWorksAsAnEmptyHmacKey() {
        Scram s = new Scram("", "", "abc");
        s.clientFinal("r=abcdef,s=c2FsdA==,i=4096");     // must not throw
    }
}
