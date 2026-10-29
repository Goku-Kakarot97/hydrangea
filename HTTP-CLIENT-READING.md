# Hướng dẫn đọc HttpClient

Dành cho người mới với code Hydrangea. Đọc `READING-GUIDE.md` phần 0–2 trước (NIO, `CompletableFuture`, quy tắc "một kết nối thuộc một event loop"), rồi quay lại đây.

Mọi đường dẫn nằm trong `hydrangea-http/src/main/java/io/hydrangea/http/`. Số dòng chỉ là điểm neo ở thời điểm viết; nếu file đã đổi, tìm theo tên hàm.

## 1. Bức tranh tổng thể

Một request đi qua bốn tầng, mỗi tầng ứng với một class:

```
người gọi ── client.send(req) ──► HttpClient         nhận request, trả CompletableFuture ngay
                                      │ dispatch()
                                      ▼
                                  HostPool           pool kết nối của một host + hàng đợi chờ
                                      │ PooledConn.run()
                                      ▼
                                  HttpClientStage    ghi request, parse response (chạy trên event loop)
                                      │
                           Pipeline → (TlsStage) → socket        (hydrangea-core)
```

- `HttpClient`, `HostPool`, `PooledConn`, `Exchange` đều là class trong cùng file `HttpClient.java` (977 dòng).
- `HttpClientStage` (HTTP/1.1) và `Http2ClientStage` (HTTP/2) là hai "giao thức" gắn vào một kết nối. HTTP/2 dài (~1500 dòng): **bỏ qua ở lần đọc đầu**.
- Tên gọi:
  - **Exchange**: một request đang bay (request + future kết quả + trạng thái retry/redirect).
  - **PooledConn**: một kết nối TCP (có thể có TLS) cùng stage đang chạy trên nó.
  - **HostPool**: các kết nối tới cùng `scheme + host + port`.

## 2. Cần nắm trước khi đọc

1. **`send()` không bao giờ chặn.** Nó trả `CompletableFuture<ClientResponse>`. Việc thật chạy trên event loop; future hoàn thành trên thread của loop, nên `thenApply(...)` không được chặn.
2. **HTTP/1.1: một request tại một thời điểm trên mỗi kết nối** (không pipelining). Request vượt số kết nối tối đa phải xếp hàng trong `HostPool.waiters`. Mặc định `maxConnectionsPerHost = 32` (`HttpClientConfig.defaults()`).
3. **Timeout tính cả thời gian xếp hàng.** Request timeout mặc định 30 giây bắt đầu đếm từ `send()`.
4. **Mọi code của một kết nối chạy trên một thread** (loop của kết nối). `HttpClientStage` giữ trạng thái bằng field thường, không có `synchronized`. Ngược lại `HostPool` bị gọi từ nhiều thread nên dùng `synchronized (this)`.

## 3. Thứ tự đọc

### Bước 1. Dữ liệu vào/ra (~20 phút)
- `ClientRequest.java` (171 dòng): `get`, `post`, `header`, `body`, `timeout`.
- `ClientResponse.java` (43 dòng): record `status`, `reason`, `headers`, `body`, `uri`.
- `HttpClientConfig.java` (124 dòng): tham số. Chú ý `maxConnectionsPerHost`, `maxIdlePerHost`, các timeout, `maxRedirects`, `withHttp2`.

### Bước 2. Cửa vào: `HttpClient.java` dòng ~105–180
- `get()`, `post()` rồi `send()` (dòng 114): tạo `Exchange` (dòng 450), gọi `start()`, trả `ex.result`. Đó là future người gọi nhận.
- `start()` (dòng 148): đặt timer timeout, gắn `whenComplete` (đếm metrics, hủy timer, đóng kết nối nếu người gọi `cancel`), rồi gọi `dispatch(ex)`.
- Đọc các field của `Exchange` (dòng 450–470): `reused`, `forceNew`, `retried`, `redirects` cho biết nó đang ở trạng thái nào.

### Bước 3. Chọn kết nối: `dispatch()` (dòng 200) và `HostPool` (dòng 704)
- `dispatch()` tra `HostPool` của host (map `pools`, có cache `lastKey`), rồi gọi `acquire(ex)`.
- **`HostPool.acquire` (dòng 721)** là hàm quan trọng nhất của file. Ba kết quả:
  1. có kết nối nhàn rỗi → dùng lại (`reuse`);
  2. chưa đủ `maxConnectionsPerHost` → tăng `total`, mở kết nối mới (`openConnection`);
  3. hết chỗ → `waiters.add(ex)`.
- **`HostPool.release` (dòng 765)**: khi kết nối rảnh, lấy request đầu hàng đợi (bỏ qua request đã timeout/cancel), hoặc cất vào `idle`, hoặc đóng nếu `idle` đã đầy.
- **Ở lần đọc đầu, bỏ qua** các nhánh HTTP/2: `reuseH2`, `tryReserveStream`, `protocolUnknown`, `h2Released`, `protocolKnown`.
- `openConnection()` (dòng 896): tra DNS trên một executor riêng, dựng pipeline (proxy tunnel? → TLS? → stage của giao thức), gọi `Client.connect(...)`, rồi `pc.run(ex, false)`.

### Bước 4. Một kết nối thật sự: `PooledConn` (dòng 537) và `HttpClientStage.java`
- `PooledConn.run()` (dòng 604): chuyển việc sang event loop bằng `eventLoop().execute(...)` rồi gọi `stage.submit(ex)`. Đây là chỗ duy nhất request đổi thread.
- `HttpClientStage.submit()` (dòng 124): `serialize()` request thành byte (`HttpClient.serialize`, dòng 309), đặt `state = HEAD`, `flush()` ghi ra socket.
- `HttpClientStage.onRead()` (dòng 86): nhận byte, nối vào `Accumulator`, gọi `parse()`.
- `parse()` là máy trạng thái (enum `State`): `HEAD → BODY` (có `Content-Length`) hoặc `CHUNK_SIZE → CHUNK_DATA → CHUNK_CRLF → TRAILERS` (chunked) hoặc `UNTIL_CLOSE` (đọc đến khi đóng). Đọc theo thứ tự: `parseHead` (dòng 388), `readFixed` (dòng 569), rồi các hàm chunk.
- `finish()` (dòng 738): dựng `ClientResponse`, tính `reusable` (response đủ khung, keep-alive, không dư byte), rồi báo về `PooledConn.finished`.
- `PooledConn.finished()` (dòng 673): nếu tái dùng được thì `pool.release(this)` (có thể giao ngay cho request đang chờ), nếu không thì đóng; sau đó gọi `onResponse()`.

### Bước 5. Tình huống đặc biệt (`HttpClient.java`)
- `onResponse()` (dòng 237): redirect (301/302/303/307/308), quy tắc đổi POST→GET, bỏ `Authorization`/`Cookie` khi đổi origin, không theo redirect từ https xuống http.
- `onFailure()` (dòng 217): thử lại **một lần** khi kết nối cũ đã chết trước byte phản hồi đầu tiên và request là idempotent (GET/HEAD/PUT/DELETE/OPTIONS). POST/PATCH không bao giờ thử lại.
- `Exchange.fail()` / `abortConnection()` (dòng 450 trở đi): hủy và dọn kết nối khi timeout hoặc người gọi `cancel`.

### Bước 6. Làm sau cùng
`Http2ClientStage.java` (multiplexing, HPACK, flow control), `ClientProtocolStage.java`, `ProxyTunnelStage.java`, cookie (`CookieJar`, `InMemoryCookieJar`), giải nén (`ContentDecoder`), streaming (`stream()`, `BodyListener`, `StreamHandle`).

## 4. Theo dấu một request GET

1. `client.get(url)` → `send()` tạo `Exchange` trong `result` (future chưa xong).
2. `start()` đặt timeout, gọi `dispatch()` → `HostPool.acquire()`.
3. Chưa có kết nối: `openConnection()` → DNS → `Client.connect` → `pc.run(ex, false)`.
   Đã có kết nối nhàn rỗi: `pc.run(ex, true)` ngay.
4. `run()` đẩy việc sang loop → `HttpClientStage.submit()` → `serialize()` → `ctx.write(...)`.
5. Server trả lời → `onRead()` → `parse()` → `parseHead()` → `readFixed()` → `finish()`.
6. `finished()` trả kết nối về pool (hoặc giao cho người đang chờ) → `onResponse()` → `ex.result.complete(resp)`.
7. `whenComplete` trong `start()` hủy timer, ghi metrics; continuation của người gọi chạy (vẫn trên thread loop).

## 5. Học bằng test và debugger

Test trong `hydrangea-http/src/test/java/io/hydrangea/HttpClientTest.java`, mỗi test là một tình huống có thật:

| Muốn hiểu | Test |
|---|---|
| Luồng cơ bản | `getPostHeadersAndStatus` |
| Tái dùng kết nối | `keepAliveReusesOneConnection` |
| Giới hạn kết nối + hàng đợi | `concurrencyIsCappedPerHostAndQueuedRequestsComplete` |
| Body chunked, close-delimited | `chunkedResponseWithExtensionsAndTrailers`, `closeDelimitedBodyEndsAtEofAndConnectionIsNotReused` |
| Response sai định dạng | `malformedResponsesFailTheRequestWithIoException` |
| Timeout / cancel | `requestTimeoutFailsAndClosesTheConnection`, `cancellingTheFutureClosesTheConnection`, `queuedRequestTimesOutWithoutNeedingAConnection` |
| Retry | `idempotentRequestIsRetriedOnceOnAFreshConnection`, `nonIdempotentRequestIsNotRetried` |

Test khác: `HttpClientStreamTest` (streaming), `HttpUploadClientTest` (upload), `ClientCookieTest`, `ClientDecompressionTest`, `Http2Client*Test` (HTTP/2).

Cách thực hành: chạy `getPostHeadersAndStatus` trong IDE, đặt breakpoint ở `HttpClient.send`, `HostPool.acquire`, `HttpClientStage.submit`, `HttpClientStage.parseHead`, `HttpClientStage.finish`, rồi step qua một lượt. Dùng "Find usages" trên `acquire` và `finished` khi muốn xem ai gọi.

## 6. Hiệu năng và so sánh

Kết quả đo HttpClient so với blocking I/O và virtual thread: `benchmark/RESULTS-CONCURRENCY.md` (HTTP/1.1 và h2c). Chương trình đo: `benchmark/src/bench/ConcBench.java`, `ConcBench2.java`.
