# Security policy

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use GitHub's private vulnerability reporting on this repository
(Security tab → "Report a vulnerability"), which reaches the maintainer only. Include the module, the version, how to reproduce,
and what an attacker gains. You will get an answer within 7 days; a fix for a confirmed problem is published as a new version on
Maven Central and the advisory is made public once it is out.

## Supported versions

The latest release on Maven Central (`io.github.goku-kakarot97:hydrangea-*`). Earlier versions are not patched.

## What is already tested

`mvn test` includes fuzzing of every protocol decoder (HTTP/1.1, HPACK, HTTP/2 and HTTP/3 frames, QUIC datagrams, Redis, Kafka, MQTT,
AMQP, PostgreSQL and MySQL packets), targeted tests for HTTP request smuggling and header injection, WebSocket framing, TLS downgrade and
byte smuggling before the handshake, and hostile-server tests for the database clients (clear-text password requests, public-key
substitution, `LOAD DATA LOCAL`, forged SCRAM signatures). `SECURITY-TESTING.md` lists what was tested and what was not.

Known limits worth knowing before you deploy: handlers run on the event loop, so a slow handler stalls that loop's connections;
the QUIC/HTTP/3 implementation is experimental and has no 0-RTT or migration; the database clients refuse to send a password in
the clear or to trust a server-supplied public key without TLS unless you opt in (`withAllowClearTextPassword`, `withAllowPublicKeyRetrieval`).
