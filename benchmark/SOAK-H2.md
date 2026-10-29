# gRPC + HTTP/2 soak (Kubernetes)

Server samples every 30 s: 130, from t=30 s to t=3900 s.

## Client side (verified workloads)

- ok=1310931 errors=0 | grpc unary echo 16 B / 4 KiB / 64 KiB (8 loops)
- ok=620007 errors=0 | grpc server stream 100 x 1 KiB (4 loops)
- ok=30149 errors=0 | grpc bidi ping-pong 100 messages (4 loops)
- ok=341171 errors=0 | grpc client stream 50 messages (2 loops)
- ok=2495990 errors=0 | http/2 GET /plain, POST /echo 1-100 KiB, GET /download (6 loops)
- ok=366773 errors=0 | channel churn: new channel, 5 calls, close (2 loops)
- ok=1894950 errors=0 | cancel storm: cancel at a random moment, expiring deadlines, refused requests (8 loops) | {unary cancelled=378177, deadline exceeded=378269, http cancelled=379472, refused, then ok=378907, stream cancelled=380125}
- ok=187335 errors=0 | rude raw clients: garbage, truncated frames, resets (2 loops)

Total errors: **0**; worker crashes: 0.

## Server health

| | idle before | first sixth under load | last sixth under load | after the load (last sample) |
|---|---:|---:|---:|---:|
| live heap after GC (MB) | 0 | 23.6 | 26.4 | 27 |
| threads | 9 | 13.6 | 10.4 | 10 |
| file descriptors | 12 | 17.9 | 17.6 | 12 |
| active connections | 0 | 5.7 | 5.5 | 0 |
| max event-loop block (ms) | 0 | max 36 | max 25 | 0 |

Connections opened 554891, closed 554891 (difference 0, those still open); rejected 0; HTTP responses 7746437; calls the server saw cancelled 393339.

Live heap under load: min 0, max 42, change between the first and last sixth +2.8 MB.

## Verdict

- [x] 0 errors in every verified workload
- [x] connections back to 0 after the load
- [x] descriptors back within 3 of the idle baseline
- [x] threads back within 3 of the idle baseline
- [x] live heap did not grow (last sixth - first sixth < 25 MB)
- [x] opened - closed <= 3 after the load

Server container memory (MiB, every 2 minutes): min 321, max 581; restarts: 0.
