# Cleartext vs TLS on one Hydrangea pod limited to 0.5 CPU (Kubernetes)

Same pod and process (`h2.SoakServer`, 1 I/O thread, requests = limits = 500m), two listeners: cleartext 8080 and TLS 8443 (ALPN `h2`, TLS 1.3 negotiated by the JDK,
throw-away self-signed EC P-256 certificate, client verifies it and the host name). Load: `h2.CapLoad`, 64 in flight on 2 connections, separate Job pod (2 CPU),
5 s warm-up + 15 s measured, 3 rounds with the order cleartext/TLS alternating (log: `k8s-results/tls-0.5cpu.log`). 0 errors in all 24 runs.
Connections are long-lived, so this measures steady-state record encryption, not handshakes.

| scenario | cleartext req/s (r1/r2/r3) | TLS req/s (r1/r2/r3) | TLS / cleartext | p50 clear / TLS (us) |
|---|---:|---:|---:|---:|
| gRPC unary 16 B | 51 254* / 68 712 / 68 812 | 14 203 / 13 806 / 13 787 | ~0.20 | 460-490 / 2 510-2 670 |
| gRPC unary 4 KiB | 23 431 / 23 972 / 24 389 | 9 857 / 10 080 / 10 021 | ~0.42 | 1 640-1 680 / 4 010-4 050 |
| HTTP/2 GET 13 B | 124 309 / 124 527 / 116 655 | 28 201 / 28 173 / 28 116 | ~0.23 | 260-280 / 1 190-1 230 |
| HTTP/2 POST 1 KiB echo | 89 671 / 88 866 / 85 672 | 17 763 / 18 417 / 18 797 | ~0.21 | 390-420 / 1 980-2 000 |

(*) first run of the job, the server's JIT was still warming up.

Reading it honestly:
- With 0.5 CPU, TLS cuts small-request throughput to roughly a fifth (a 4 KiB call to ~0.4). Per request that is about 25-35 us of extra CPU,
  most of it the JDK's `SSLEngine` (wrap/unwrap, AEAD) on the server's single thread.
- That is a larger cost than I would expect from AES-GCM alone, and I have not profiled it: it may be JDK `SSLEngine` overhead per record, or something in
  Hydrangea's TLS stage (record sizing, copies, the number of wraps per response). Treat 14k req/s as what the code does today, not as a hard limit of TLS on this CPU.
- A CPU limit that small is where TLS hurts most; terminating TLS at an ingress/mesh and speaking cleartext h2c to the pod would give the cleartext column.
- Not measured: handshake rate (new connections per second), session resumption, RSA certificates, mutual TLS.

## After profiling and fixing (same pod, same load, same method; log: `k8s-results/tls-0.5cpu-after.log`)

JFR on the TLS server showed (1) `TlsStage` allocating and zeroing a 16 KiB `ByteBuffer` for every `wrap` and `unwrap` (~16% of the samples) and one TLS record per
written buffer (a gRPC response is headers + data + trailers = 3 records), and (2) the HPACK Huffman decoder reading one bit at a time through a synchronized
`ByteArrayOutputStream` (~10-17%; this one also costs cleartext HTTP/2). Fixes: per-loop scratch buffers with a right-sized copy of the result, one `wrap` call
for everything queued (up to 16 buffers), and a nibble-table Huffman decoder with direct array output. 3 new tests (`HuffmanTest`); mutants on the padding rule are caught.

| scenario | cleartext before -> after | TLS before -> after | TLS / cleartext before -> after |
|---|---:|---:|---:|
| gRPC unary 16 B | ~68.7k -> ~79-80k | ~13.9k -> ~50.7k | 0.20 -> 0.64 |
| gRPC unary 4 KiB | ~24k -> ~23.5k (same) | ~10k -> ~18.2k | 0.42 -> 0.78 |
| HTTP/2 GET 13 B | ~120k -> ~135-146k | ~28.2k -> ~101-105k | 0.23 -> 0.75 |
| HTTP/2 POST 1 KiB echo | ~88k -> ~95-97k | ~18k -> ~54-55k | 0.21 -> 0.57 |

Notes: the very first run of the job (cleartext gRPC 16 B, round 1) logged one `Connection refused` because the job started before the server was ready; it is a start-up
artefact and its 47 775 req/s is left out of the figures above. TLS now costs roughly 25-40% on small requests instead of 75-80%. The rest is the JDK's
`SSLEngine` (AES-GCM wrap/unwrap), which Hydrangea cannot remove. Still not measured: handshakes per second, session resumption, RSA certificates, mutual TLS.
