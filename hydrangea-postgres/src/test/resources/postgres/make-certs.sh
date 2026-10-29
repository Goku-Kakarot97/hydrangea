#!/bin/sh
# Test certificates for the TLS containers of start-postgres.sh: ca.pem (the CA the tests trust), server.pem/server.key (valid for localhost and
# 127.0.0.1) and wrongname.pem/wrongname.key (valid only for other.example, to see the client refuse a certificate of another host).
# Valid for 10 years; run again to renew.
set -e
cd "$(dirname "$0")"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 3650 -subj "/CN=Hydrangea test CA" 2>/dev/null
mk() { # name san
  openssl req -newkey rsa:2048 -nodes -keyout "$1.key" -out "$1.csr" -subj "/CN=$1" 2>/dev/null
  printf 'subjectAltName=%s\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n' "$2" > "$1.ext"
  openssl x509 -req -in "$1.csr" -CA ca.pem -CAkey ca.key -CAcreateserial -out "$1.pem" -days 3650 -extfile "$1.ext" 2>/dev/null
  rm -f "$1.csr" "$1.ext"
}
mk server "DNS:localhost,IP:127.0.0.1"
mk wrongname "DNS:other.example"
rm -f ca.key ca.srl
