# Chi phí của việc đo (metrics bật so với tắt)

"Bật" nghĩa là đã gắn một sink (`MetricsSink.NOOP`), nên ghi độ trễ chạy thật; bộ đếm kết quả luôn chạy. Chạy xen kẽ (tắt, bật, tắt, bật...), mỗi lần một JVM mới, nghỉ 2–6 giây giữa các lần. Máy macOS, Docker cho Redis/Kafka: dao động giữa các lần chạy ±15–40%, nên chỉ thấy được khác biệt lớn hơn mức đó. **Không thấy khác biệt nào ở cả bốn nơi đã đo.**

| nơi đo | cách đo | tắt | bật |
|---|---|---|---|
| Redis (`get`, 32 thread) | `RedisBench ... metrics=on`, 8 cặp | 57,6 k ops/s | 57,5 k ops/s |
| Redis (pipeline 100) | cùng trên | 160,5 k | 156,5 k |
| HTTP server (1.1, 32 kết nối keep-alive, 2 loop) | `HttpMetricsAb server`, 8 cặp | 146 k req/s, 27,8 µs CPU/req, p99 1020 µs | 157 k, 27,3 µs, 904 µs |
| HttpClient (32 request đồng thời) | `HttpMetricsAb client`, 8 cặp | 159 k req/s, 20,3 µs CPU/req, p99 686 µs | 150 k, 20,8 µs, 755 µs |
| Kafka producer (idempotent, 3 partition) | `KafkaBench ... metrics=on`, 8 cặp | trung vị 0,94 M rec/s (0,43–1,5 M), 0,057 s CPU/100k | 1,11 M (0,43–1,6 M), 0,053 s CPU/100k |

Vi mô (một thread, `Latency.begin` + `end` + một bộ đếm): 7 ns khi tắt, 39 ns khi bật. Với 8 thread cùng ghi trên macOS: 11 ns khi tắt, 684 ns khi bật, vì `System.nanoTime()` tốn 345 ns mỗi lần khi 8 thread cùng gọi (12 ns với một thread). Redis đọc đồng hồ một lần cho cả batch để tránh chỗ này; HttpClient thì mỗi request một lần trên thread gọi, nhưng benchmark 32 request đồng thời không thấy ảnh hưởng.

Chưa đo: Linux (nơi `nanoTime` thường mở rộng tốt hơn), tải cao hơn nhiều so với 150 k req/s, MQTT, AMQP, gRPC interceptor, TLS.
