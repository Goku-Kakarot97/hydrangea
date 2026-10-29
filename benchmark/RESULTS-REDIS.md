# Hydrangea Redis client so với Jedis 5.2.0

Cùng một Redis 7.4 trong Docker (`hydrangea-redis-test`), client chạy trên **host macOS** qua port mapping của Docker (khoảng 260 µs mỗi vòng request/reply, nên mọi số "một request một lúc" bị chặn bởi độ trễ này, không phải bởi client). Khác với bench MQTT: không dùng container chung network namespace. 3 vòng, thứ tự xoay, lấy **trung vị**. Mã: `benchmark/src/redis/RedisBench.java`, `redis-bench.sh`, `aggregate-redis.py`; số thô: `benchmark/redis-results/`.

Ba cấu hình: `hydrangea` = MỘT kết nối dùng chung cho mọi luồng (cách dùng chuẩn); `hydrangea-conns` = mỗi luồng một kết nối riêng; `jedis` = `JedisPool`, mỗi luồng một kết nối. Gọi chặn của Hydrangea là `client.get(k).join()`.

| kịch bản | hydrangea | hydrangea-conns | jedis |
|---|---|---|---|
| GET/SET 1 luồng (ops/s) | 3 132 | 3 153 | **3 293** |
| 32 luồng (ops/s) | **65 961** | 30 237 | 30 964 |
| 32 luồng p99 (µs) | **1 310** | 2 621 | 2 490 |
| pipeline 1000 lệnh/lô (ops/s) | **623 963** | **630 753** | 493 533 |
| pipeline 100 lệnh/lô* (ops/s) | 176 350 | 176 601 | 174 647 |
| MULTI/INCR/EXPIRE/GET/EXEC, 8 luồng (tx/s) | **21 531** | 10 490 | 5 877 |
| pub/sub (msg/s nhận) | 139 209 | 141 582 | 140 111 |
| mở kết nối (conn/s) | 666 | 690 | **1 095** |
| giá trị 1 MB ghi / đọc (MB/s) | 114 / 148 | 111 / 153 | 113 / 159 |
| giá trị 8 MB ghi / đọc (MB/s) | 125 / 168 | 125 / 166 | 124 / 167 |
| lrange 10 000 phần tử, 1 luồng (ops/s) | 435 | 440 | **460** |
| hgetall 1 000 trường (ops/s) | 1 324 | 1 392 | **1 483** |

\* Dòng này gộp nhầm GET và SET (khoá tổng hợp không có `op`), nên chỉ mang tính tham khảo; hai bên ngang nhau.

## Đọc kết quả
- **Thắng rõ khi nhiều luồng dùng chung**: 32 luồng trên một kết nối cho 2,1 lần Jedis (và độ trễ p99 gần một nửa), vì lệnh của mọi luồng được gom ghi cùng lúc (pipeline tự động). Cùng 32 kết nối riêng thì **ngang Jedis** (30,2k so với 31,0k): lợi thế đến từ việc chia sẻ kết nối, không phải từ parser. Transaction 3,7 lần vì năm lệnh được ghi trong một lần.
- **Pipeline lớn** (1000 lệnh) hơn Jedis 26%.
- **Thua nhẹ**: một luồng một request một lúc chậm hơn Jedis 5% (và tốn CPU hơn: 4,5 so với 3,1 s/100k lệnh) vì mỗi lời gọi chặn qua hai lần chuyển luồng (luồng gọi → event loop → luồng gọi); trả lời lớn nhiều phần tử (lrange, hgetall) chậm hơn 5–10% và tốn CPU hơn 15–30%; **mở kết nối chậm hơn 1,6 lần** (handshake HELLO chạy trên event loop, thêm một vòng); 1 KB tốn CPU/GB nhiều hơn (nhưng throughput ngang).
- **Ngang nhau**: giá trị lớn (1–8 MB, bị băng thông Docker chặn), pub/sub, mget.
- Số luồng tối đa của Hydrangea cao hơn (10–11 so với 6–8) do event loop.

## Virtual thread dùng chung một client / một pool
`RedisBench get threads=N virtual=true`, 80 % GET + 20 % SET giá trị 128 byte, mỗi lệnh một lời gọi chặn. Hydrangea: MỘT kết nối dùng chung, `.join()` trong virtual thread. Jedis: `JedisPool` cỡ 32 hoặc 128, mỗi lệnh mượn một kết nối (`borrow=true pool=N`). Ba vòng, khoảng giá trị (min–max) vì chưa lấy trung vị cho kịch bản này.

| virtual thread | Hydrangea (1 kết nối) | Jedis pool 32 | Jedis pool 128 |
|---|---|---|---|
| 1 000 luồng, ops/s | 403k–468k | 22k–28k | 40k–44k |
| 1 000 luồng, p99 | 6,5–9 ms | 490–670 ms | 260–270 ms |
| 10 000 luồng, ops/s | 340k–523k | 26k–27k | 41k–44k |
| 10 000 luồng, p99 | 37–92 ms | 5,1–5,4 s | 2,7–3,5 s |

Jedis chặn trên I/O và trên pool, nên khi số virtual thread lớn hơn nhiều so với số kết nối thì phần lớn phải xếp hàng chờ mượn kết nối; Hydrangea gom lệnh của mọi luồng vào một kết nối nên không có hàng chờ pool. Cỡ pool (32, 128) do người đo chọn: pool lớn hơn giúp Jedis nhưng tốn thêm kết nối và client cho Redis. Vẫn đo qua Docker trên macOS và chỉ với GET/SET; lệnh chậm hoặc lệnh chặn (BLPOP, WATCH) cần kết nối riêng (`duplicate()`) và chưa đo.

## Hạn chế của phép đo
Port mapping Docker chặn mọi kịch bản một-request-một-lúc ở khoảng 3 200 ops/s nên chênh lệch nhỏ (5%) nằm gần mức nhiễu; chưa đo trên Linux loopback. Redis một luồng là nút cổ chai ở pipeline lớn. Chưa đo Cluster, Sentinel, TLS, `JedisPooled`/Lettuce.
