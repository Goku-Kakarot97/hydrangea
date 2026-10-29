# TLS: Hydrangea vs Undertow on Kubernetes at 0.5 CPU

Same setup as `RESULTS-K8S-HTTP-UNDERTOW.md` (1 I/O thread, heap 256 MB, 500m CPU, one server at a time, rotation per round, 3 rounds, 0 errors in all 54 runs; logs in
`k8s-results/tls-undertow/`), but over https on port 8443 with a throw-away self-signed EC P-256 certificate, ALPN `h2` / `http/1.1`. The load is `h2.CapLoad` with the Hydrangea
client for every server (so the client's TLS cost is identical; it runs in a 2 CPU pod). Undertow: `h2,tuned` with the handler on the I/O thread ("tuned") and with `dispatch()` to the worker pool.
Requests per second, average of 3 rounds:

| scenario (https) | Hydrangea | Undertow tuned | Undertow tuned + dispatch |
|---|---:|---:|---:|
| HTTP/1.1 GET, 64 connections | 27 325 | 28 779 | 19 853 |
| HTTP/1.1 GET, 256 connections | 32 048 | 31 213 | 22 029 |
| HTTP/1.1 POST 1 KiB echo, 64 connections | 32 495 | 28 872 | 18 011 |
| h2 GET, 64 streams on 2 connections | 104 671 | 36 906 | 13 726 |
| h2 POST 1 KiB echo, 64 streams | 56 726 | 11 644 | 9 058 |
| HTTP/1.1, a new connection (TLS handshake) per request, 16 in parallel | 389 | 315 | 287 |

Reading it honestly:
- HTTP/1.1 over TLS: a tie for GET (Undertow 5% ahead at 64 connections, Hydrangea 3% ahead at 256), Hydrangea 13% ahead on the 1 KiB POST. With `dispatch()` Undertow does about 70% of that, as without TLS.
- h2 over TLS: Hydrangea 2.8x (GET) and 4.9x (echo) ahead of the tuned Undertow, again with the caveat of `RESULTS-K8S-HTTP-UNDERTOW.md` (Undertow's default 64 KiB window; its h2 CPU cost per request).
- Handshakes: all three are limited to roughly 300-390 new connections per second on 0.5 CPU (about 2.6-3.2 ms of CPU per full handshake), almost entirely the JDK's key agreement and signature
  which both servers share; Hydrangea is 23% ahead of the tuned Undertow, within what the TLS stack and framing around it can explain, not a different algorithm. TLS session resumption is probably in play
  (the JDK client caches sessions), so this is a mix of resumed and full handshakes, not a pure full-handshake rate.
- Compared with cleartext (`RESULTS-K8S-HTTP-UNDERTOW.md`) TLS costs Hydrangea about 40% of its HTTP/1.1 GET throughput at 64 connections (51k -> 27k) and 28% of its h2 GET throughput (145k -> 105k); the HTTP/1.1 cost is larger because
  each request is its own TLS record in each direction, while h2 coalesces several into one.
- Not covered: RSA certificates, TLS 1.2, client certificates, session tickets under load, other CPU limits.
