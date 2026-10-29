#!/bin/sh
# Test-only certificates for the AMQP TLS tests: a CA, a server certificate for localhost / 127.0.0.1 signed by it, and a second,
# unrelated CA (the client must refuse a server it does not vouch for). Not secrets: never use these keys for anything real.
set -e
cd "$(dirname "$0")"
T=$(mktemp -d)
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$T/ca.key" -out ca.pem -days 3650 -subj "/CN=Hydrangea Test CA" 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$T/other.key" -out other-ca.pem -days 3650 -subj "/CN=Some Other CA" 2>/dev/null
openssl genrsa -out server.key 2048 2>/dev/null
openssl req -new -key server.key -out "$T/server.csr" -subj "/CN=localhost"
cat > "$T/ext" <<X
subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
X
openssl x509 -req -in "$T/server.csr" -CA ca.pem -CAkey "$T/ca.key" -CAcreateserial -CAserial "$T/ca.srl" -out server.pem -days 3650 -extfile "$T/ext" 2>/dev/null
# a certificate from the same trusted CA that names a different host: the client must still refuse it (PKCS#8 key, for the JDK)
openssl genrsa -out "$T/wrong.key" 2048 2>/dev/null
openssl req -new -key "$T/wrong.key" -out "$T/wrong.csr" -subj "/CN=wrong.example"
printf 'subjectAltName=DNS:wrong.example\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n' > "$T/ext2"
openssl x509 -req -in "$T/wrong.csr" -CA ca.pem -CAkey "$T/ca.key" -CAserial "$T/ca.srl" -out wrongname.pem -days 3650 -extfile "$T/ext2" 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in "$T/wrong.key" -out wrongname.key
chmod 644 server.key
rm -rf "$T"
