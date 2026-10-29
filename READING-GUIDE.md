# Hướng dẫn đọc code Hydrangea

Dành cho dev Java mức middle. Đừng đọc theo thứ tự thư mục: đọc từ ngoài vào trong, theo đường đi của một request.

## Bản đồ mã nguồn (đọc cái này trước)
Mã chia thành module Maven; tên gói không đổi. Gói `io.hydrangea.X` nằm ở `hydrangea-<module>/src/main/java/io/hydrangea/X/`:

| Gói | Module |
|---|---|
| `io.hydrangea` (EventLoop, Connection, Server, Client...), `codec`, `pipeline`, `tls` | `hydrangea-core` |
| `http`, `http2` (HPACK/QPACK), `websocket`, `sse` | `hydrangea-http` |
| `grpc` / `amqp` / `mqtt` / `redis` / `kafka` / `quic` | `hydrangea-grpc` / `-amqp` / `-mqtt` / `-redis` / `-kafka` / `-quic` |
| `examples` | `hydrangea-examples` |

Nên đường dẫn kiểu `http/HttpServerStage.java` trong hướng dẫn này nghĩa là `hydrangea-http/src/main/java/io/hydrangea/http/HttpServerStage.java`. Test nằm ở `src/test/java` của cùng module; test chạy xuyên module (gRPC, hardening, fuzz) ở `hydrangea-examples`.
Đồ thị phụ thuộc: core ← quic ← http ← grpc, amqp chỉ phụ thuộc core; mqtt phụ thuộc core và, chỉ cho `MqttWebSocket`, http. Muốn hiểu một giao thức mới, đọc `hydrangea-core` rồi đúng module đó, không cần đọc module khác.

## 0. Cần biết trước
- `java.nio`: `Selector`, `SelectionKey`, `ByteBuffer` (position/limit/flip), channel non-blocking.
- `CompletableFuture`; TCP là luồng byte (không có ranh giới message).
- Chưa quen `Selector`: đọc `NIO-INTRO.md` trước, rồi mới sang `EventLoop`.

## 1. Chạy trước, đọc sau (khoảng 1 giờ)
Chạy rồi gọi thử, chỉ để thấy API bên ngoài trông thế nào:

```
java -cp target/classes io.hydrangea.examples.HttpExample
java -cp target/classes io.hydrangea.examples.GrpcHelloExample 50052
java -cp target/classes io.hydrangea.examples.GrpcHelloClientExample
```

Ví dụ để học: `HttpExample`, `GrpcHelloExample` + `GrpcHelloClientExample`. `GrpcEchoExample` là server dùng cho test, đừng bắt đầu từ đó.

## 2. Quy tắc quan trọng nhất
> Mỗi kết nối thuộc về đúng một `EventLoop` (một thread). Code của kết nối chỉ chạy trên thread đó.

Hệ quả:
- Handler và stage giữ trạng thái bằng field thường, không cần `synchronized`.
- Không được **chặn** loop (không `Future.get()`, không `sleep`, không I/O blocking trong handler).
- Từ thread khác, muốn tác động vào kết nối thì `eventLoop().execute(...)`. Chỉ các hàm ghi rõ "thread-safe" trong javadoc mới gọi trực tiếp được (`write`, `close`, `setAutoRead`…).

Đọc các file sau theo thứ tự (đều ngắn, javadoc giải thích ý đồ):

| Thứ tự | File | Đọc gì |
|---|---|---|
| 1 | `EventLoopGroup.java` (51 dòng) | Nhóm loop, chọn loop kế tiếp theo vòng tròn (`next()` dùng `floorMod` để chỉ số không âm khi bộ đếm tràn) |
| 2 | `EventLoop.java` (~400 dòng) | Một thread + `Selector` + hàng đợi task + heap timer. Đọc `run()`, `execute`, `schedule`. Lướt qua phần chống "selector spin" |
| 3 | `Connection.java` (60 dòng) | Ống byte hai chiều: `write`, `close`, `setAutoRead`, `isWritable` (backpressure) |
| 4 | `Handler.java` | Sự kiện của một kết nối: `onActive`, `onRead`, `onWritabilityChanged`, `onDrain`, `onClose` |
| 5 | `Server.java` | `bind`: accept kết nối, gán cho loop, tạo một `Handler` mới cho mỗi kết nối |

Lưu ý ở `Handler.onRead`: `ByteBuffer` chỉ hợp lệ trong lúc gọi (là view của buffer dùng chung); muốn giữ thì phải copy.

### Đọc `EventLoop` từng bước

File khá dài vì nó lẫn hai thứ: vòng lặp chính (ngắn) và nhiều tầng phòng thủ cho các trường hợp hiếm. Đọc vòng lặp chính trước, phần phòng thủ để sau.

**Vòng lặp chính** (`run()`), bỏ hết chú thích và xử lý lỗi:

```java
while (true) {
    runTasks();                    // 1. chạy các việc do thread khác giao
    long next = runTimers();       // 2. chạy timer đã đến hạn, trả về giờ của timer kế tiếp
    wakeupPending.set(false);
    select(next);                  // 3. ngủ: chờ sự kiện mạng, tối đa tới giờ timer kế tiếp
    dispatch();                    // 4. xử lý các socket sẵn sàng
}
```

| Bước | Làm gì |
|---|---|
| `runTasks()` | Lấy task trong hàng đợi `tasks` ra chạy cho tới khi hết (tối đa 1024 mỗi lượt). Mỗi task chạy trong `safeRun`, nên một task lỗi không làm sập loop. Cuối lượt gọi `flushDeferred()` để gửi dữ liệu đã gom |
| `runTimers()` | Timer nằm trong `PriorityQueue` sắp theo giờ đến hạn. Cái nào đến hạn thì chạy; gặp cái chưa đến thì trả về giờ của nó. Timer đã huỷ chỉ bị bỏ qua khi gặp |
| `select(next)` | Có task đang chờ hoặc đang tắt: `selectNow()` (không ngủ). Không có timer: `select()` ngủ vô hạn. Có timer: `select(thời gian còn lại)`. Đây là cách một thread vừa chờ socket vừa chờ timer |
| `dispatch()` | Với mỗi key sẵn sàng, gọi `SelectionHandle.ready(readyOps)`; với kết nối TCP đó là `SocketConnection.ready`, nơi gọi `flush()` hoặc `read()`. Có đo thời gian xử lý để biết handler nào chặn loop quá lâu |

**Gom ghi để đỡ syscall** (`runTasks`, `flushDeferred`, `batchingTasks`, `flushAtEndOfBatch`):
- Mỗi lần `channel.write` là một syscall. Khi ghi trong một task hay trong `onRead`, `SocketConnection.enqueue` chỉ cất dữ liệu vào hàng đợi `outbound` và đánh dấu kết nối đó cần gửi.
- `deferredFlushes` là danh sách các kết nối có dữ liệu chờ. Cuối lượt task (hoặc cuối `read()`), mỗi kết nối được gửi đúng một lần.
- Không có đợi theo thời gian: loop không đi ngủ khi còn dữ liệu bị giữ, và chốt chặn 1024 task ngăn việc giữ quá lâu.
- `flushDeferred` duyệt bằng chỉ số chứ không dùng for-each, vì lúc gửi, một callback có thể ghi vào kết nối khác và thêm phần tử vào chính danh sách đang duyệt.

**Phần phòng thủ, lần đầu lướt qua:**

| Đoạn | Giải quyết gì |
|---|---|
| `wakeupPending` và chú thích dài trước `select` | Tránh mất tín hiệu `wakeup()` khi `execute` chạy đồng thời với `select`. Chỉ cần nhớ: cờ này liên quan đến `execute` |
| `spinCount`, `rebuildSelector()` | Một số JDK/kernel khiến `select()` trả về ngay liên tục (CPU 100%). Thấy 512 lần liên tiếp thì chuyển mọi channel sang Selector mới |
| `closeAndRelease`, `closeNow` | Đóng socket đúng: đóng channel chưa đủ, phải `selectNow()` để kernel thả cổng |
| `shutdownStep`, `closeAll`, `finally` trong `run()` | Tắt loop gọn gàng |
| `metrics.*`, `maxBlockNanos` | Đo đạc, không ảnh hưởng luồng chính |
| `purgeAt` trong `addTimer` | Dọn timer đã huỷ để hàng đợi không phình khi có nhiều kết nối ngắn |

Thứ tự đọc gợi ý: `run()` → một trong bốn hàm ở bảng trên (bỏ qua nhánh `catch` và `metrics`) → các chú thích dài, vốn thường nói *vì sao* có đoạn đó. Khi muốn thấy luồng thật, đặt breakpoint ở `dispatch()` rồi `curl` một request. Bài `NIO-INTRO.md` giải thích nền tảng (`Selector`, `OP_WRITE`, `wakeup()`).

### Đọc `SocketConnection` từng bước

`SocketConnection` (~540 dòng) là chỗ `EventLoop` và `Handler` gặp nhau: `EventLoop.dispatch()` gọi `ready(readyOps)`, còn `ready` gọi `handler.onRead`. Nó làm ba việc: **nhận byte vào, gửi byte ra, đóng kết nối**. Phần lớn độ dài là xử lý ca biên của việc gửi và đóng.

Đọc theo **đường đi của dữ liệu**, từ lúc kết nối sinh ra đến lúc chết. Mỗi bước có một câu hỏi để mang theo khi đọc:

| Bước | Đọc gì | Câu hỏi cần trả lời được |
|---|---|---|
| 1. Bản đồ | Javadoc đầu lớp; hai khối trạng thái *loop-thread state* và *cross-thread state*; constructor | Trường nào được đụng từ thread nào? (`outbound`, `closed`, `inRead`… chỉ thread của loop; `pendingBytes` là `AtomicLong`, `writable` và `closing` là `volatile` vì thread khác cũng đọc) |
| 2. Sinh ra | `start()` | Kết nối bắt đầu sống thế nào? (bật TCP_NODELAY và keepalive, đăng ký `Selector` với `OP_READ` nếu `autoRead`, đặt timer idle, gọi `handler.onActive`) |
| 3. Cửa vào mọi sự kiện | `ready()` (chỉ ~8 dòng) | `EventLoop.dispatch()` đi vào đây rồi rẽ đâu? (`OP_WRITE` thì `flush`, `OP_READ` thì `read`; `OP_WRITE` được xử lý **trước**, để giải phóng bộ đệm gửi trước khi nhận thêm) |
| 4. Byte vào | `read()` → `readLoop()` | Dữ liệu lên tầng trên bằng đường nào? (đọc vào buffer **dùng chung** của loop, `flip()`, `handler.onRead`, rồi gửi phần handler vừa ghi. Tối đa 16 lần mỗi sự kiện để không chiếm loop. `n < 0` là bên kia đóng; `n < capacity` nghĩa là kernel đã cạn nên khỏi đọc thêm) |
| 5. Byte ra | `write(data, done)` → `enqueue` → `flush` → `setWriteInterest` | Dữ liệu ra ngoài bằng đường nào, và socket đầy thì sao? (`write` nhận từ thread nào cũng được; `enqueue` cất vào `outbound`, theo dõi `pendingBytes` và `highWaterMark`, rồi gửi ngay hoặc hoãn; `flush` gửi tối đa 16 buffer một lần, hết thì tắt `OP_WRITE`, đầy thì bật `OP_WRITE`) |
| 6. Chết | `doClose()` | Tài nguyên được giải phóng ra sao? (idempotent: gửi nốt dữ liệu hoãn, đặt `closed`, huỷ timer và key, đóng channel, gọi `done` của buffer chưa gửi, cuối cùng `handler.onClose` đúng một lần) |
| 7. Các đường dẫn tới đóng | `close()`, `closeAfterFlush()`, `terminate()` | Các cách yêu cầu đóng dẫn tới đâu? (đều đi về `doClose`, hoặc về `finishClosing` rồi `doClose`) |

Bước 5 là phần dày nhất, nên đọc kỹ nhất; `NIO-INTRO.md` mục 7 là bản đơn giản của đúng đoạn này. Muốn thấy bước 4 và 5 gặp nhau: đặt breakpoint ở `readLoop` (chỗ gọi `handler.onRead`), rồi bước vào `enqueue`. Response được ghi lúc `inRead` đang bật nên bị hoãn tới `flushDeferredWrites()` ngay sau `onRead`.

**Để sau (khi cần):**

| Đoạn | Vì sao để sau |
|---|---|
| `finishClosing`, `lingering`, `lingerFor` | Đóng nhẹ nhàng: gửi hết rồi `shutdownOutput`, đọc bỏ dữ liệu của bên kia tới khi họ đóng. Cần hiểu RST và FIN: đóng socket còn dữ liệu chưa đọc làm kernel gửi RST, có thể phá mất thông điệp cuối (frame close, trang lỗi) |
| `MAX_HEAP_WRITE` và vòng `limits` trong `flush` | Tối ưu hiệu năng: JDK copy cả buffer heap sang buffer tạm trước mỗi lần ghi, nên buffer 1 MiB mà socket chỉ nhận 256 KiB thì bị copy lặp lại. Không đổi hành vi |
| `scheduleIdleCheck` | Cần hiểu timer của `EventLoop` trước. Mỗi kết nối một timer, nổ rồi mới kiểm tra `lastActivity` |
| `inRead`, `flushDeferred`, `flushDeferredWrites` | Gom ghi để giảm syscall; hiểu luồng chính trước đã (xem mục `EventLoop`) |
| `setAutoRead`, `drain`, `rebind` | Điều khiển đặc biệt: tạm dừng đọc (backpressure), tắt server, đổi selector |

## 3. Pipeline và stage
- `pipeline/Stage.java`, `Context.java`, `Pipeline.java`.
- Pipeline là một `Handler` chạy sự kiện qua một chuỗi stage. Sự kiện vào đi từ stage 0 (gần socket) tới stage cuối; dữ liệu ra đi ngược lại. Stage nào không quan tâm thì mặc định chuyển tiếp (`ctx.fireRead`).
- `codec/` và `LineStage` là ví dụ nhỏ nhất: tách luồng byte thành các message.
- `tls/TlsStage.java` (348 dòng): TLS chỉ là một stage ở đầu chuỗi. Đây cũng là chỗ ALPN được thương lượng. Hướng dẫn đọc chi tiết ở mục con ngay dưới.

Cách ghép thật nằm trong `http/HttpServer.java` (39 dòng):
```
không TLS : Pipeline.of(HttpServerStage)
TLS       : Pipeline.of(TlsStage, HttpServerStage)
TLS + ALPN: Pipeline.of(TlsStage(alpn=h2,http/1.1), HttpServerStage)   // ALPN quyết định HTTP/1.1 hay HTTP/2
```

### Đọc `TlsStage` từng bước

`TlsStage` (~350 dòng) là bộ dịch nằm ở đầu pipeline, sát socket: phía dưới là byte mã hóa, phía trên là byte thường. Nó không tự làm mật mã mà **điều khiển** `javax.net.ssl.SSLEngine` của JDK và chuyển dữ liệu qua lại.

```
mạng ⇄ SocketConnection ⇄ TlsStage ⇄ HttpServerStage
         (byte mã hóa)               (byte thường)
```

**Khái niệm nền về `SSLEngine`:** engine chỉ xử lý buffer, không đụng socket. `unwrap(in, out)` biến byte mã hóa thành byte thường; `wrap(srcs, out)` làm ngược lại. Trong bắt tay nó báo `HandshakeStatus` cho biết phải làm gì tiếp: `NEED_WRAP` (lấy byte để gửi), `NEED_UNWRAP` (xử lý byte vừa nhận), `NEED_TASK` (chạy tác vụ nặng như kiểm tra chứng chỉ), `FINISHED`/`NOT_HANDSHAKING` (xong).

Đọc theo thứ tự này, mỗi bước có một câu hỏi:

| Bước | Đọc gì | Câu hỏi cần trả lời được |
|---|---|---|
| 1. Cách tạo | Javadoc đầu lớp; `server(...)`, `client(...)`, `applicationProtocol()` | Tạo stage cho server/client thế nào, và ALPN được đặt ở đâu? (`SSLParameters.setApplicationProtocols`; sau bắt tay `applicationProtocol()` trả giao thức đã chọn, `HttpServer.bind` truyền hàm này cho `HttpServerStage`) |
| 2. Ba cửa vào | `onActive`, `onRead`, `onWrite` | Dữ liệu vào stage từ đâu? (`onRead` gom byte mã hóa vào `netIn`; `onWrite` cất byte thường vào `pendingPlain`; cả hai rồi gọi `pump()`. `onActive` đặt timer bắt tay 10 giây rồi `beginHandshake`) |
| 3. Bộ điều khiển | `pump()` rồi `drive()` | Ai quyết định làm gì tiếp? (`drive` hỏi engine trạng thái bắt tay và làm đúng việc được yêu cầu, lặp tới khi không còn tiến triển; khi đã bắt tay xong thì `flushPlain` rồi `unwrap`) |
| 4. Chống gọi chồng | `pumping`, `repump` trong `pump()` | Vì sao có hai cờ này? (trong `drive`, tầng trên có thể `write` ngay lúc `fireRead`/`fireActive`, mà `write` lại gọi `pump`. Gọi chồng thì engine hỏng trạng thái, nên gọi lồng chỉ đặt `repump` và vòng ngoài chạy thêm một lượt) |
| 5. Gọi engine thật | `wrap`, `unwrap` | Byte đi qua engine thế nào? (xử lý `BUFFER_OVERFLOW` bằng cách nhân đôi buffer; kết quả `CLOSED` thì chuẩn bị đóng) |
| 6. Mở cửa cho tầng trên | `noteHandshake` | Khi nào tầng trên được báo "kết nối sẵn sàng"? (`onActive` chỉ chuyển tiếp **sau khi** bắt tay `FINISHED` lần đầu; nhờ vậy tầng trên không cần biết TLS tồn tại, và dữ liệu nó ghi trước đó chỉ xếp hàng chờ) |
| 7. Đóng | `closeNotify`, `onCloseRequest`, phần cuối `drive` | Đóng thế nào cho đúng TLS? (gửi hết dữ liệu đang chờ rồi mới `closeOutbound()` để gửi `close_notify`; xong cả hai chiều mới đóng socket. Bên kia gửi `close_notify` trước thì ta đáp lại sau khi gửi hết thứ còn nợ) |

**Lần đầu lướt qua:**

| Đoạn | Giải quyết gì |
|---|---|
| `SCRATCH`, `scratch()`, `copyOut()` | Engine cần buffer đầu ra cỡ 16 KiB; cấp phát và xóa số cái này cho mỗi `wrap`/`unwrap` tốn hơn cả mã hóa một tin nhỏ. Nên mỗi thread của loop dùng một cặp buffer đệm dùng chung, kết quả copy ra buffer đúng cỡ thật. An toàn vì stage chỉ chạy trên thread của loop |
| Gom nhiều buffer trong `flushPlain` (mảng `gather`) | Response gồm vài mảnh nhỏ (header, body, trailer) thành **một** TLS record thay vì nhiều |
| `handshakeTimeout` (mặc định 10 giây) | Client mở kết nối rồi im lặng sẽ giữ socket và bộ nhớ mãi; hết giờ mà chưa bắt tay xong thì đóng |
| `copyOut` trước `fireRead` trong `unwrap` | Tầng trên có thể `write` ngay, mà `write` dùng chung buffer đệm đó |

**Cảnh báo trong javadoc:** các tác vụ bắt tay (`NEED_TASK`) chạy ngay trên thread của loop. Với chuỗi chứng chỉ bình thường thì ngắn, nhưng một `TrustManager` tự viết có gọi mạng (OCSP, tra cứu từ xa) sẽ chặn cả loop đó.

## 4. Một giao thức hoàn chỉnh: HTTP/1.1
Học một giao thức cho kỹ, chưa cần đọc hết các giao thức khác.
1. `http/HttpHandler.java`, `HttpRequest.java`, `HttpResponse.java`: tầng người dùng thấy.
2. `http/HttpRouter.java` (~390 dòng): định tuyến theo đường dẫn và method.
3. `http/HttpServerStage.java` (~1160 dòng): bộ phân tích HTTP/1.1. Lần đầu đọc cấu trúc (field, các trạng thái, hàm chính); lướt qua các nhánh xử lý lỗi, chúng làm file dài nhưng không phải luồng chính.
4. `http/HttpClient.java`: phía client. Hướng dẫn từng bước riêng: `HTTP-CLIENT-READING.md`.

### Đọc `HttpServerStage` từng bước

`HttpServerStage` là stage **cuối** của pipeline: nhận byte thường, tách thành request, gọi `handler.handle(request)`, rồi dựng response gửi đi. File dài (~1160 dòng) vì gom nhiều tính năng phụ; luồng chính chỉ khoảng 300 dòng.

```
byte ──► onRead ──► parse ──► dispatch ──► handler.handle ──► finish ──► sendResponse ──► byte
```

**Khái niệm nền:** TCP là dòng byte, không có ranh giới request. Một request có thể đến làm nhiều mảnh, hoặc nhiều request dính trong một mảnh. Nên stage giữ byte chưa xử lý trong `acc` (`Accumulator`) và nhớ mình đang đọc tới đâu bằng `state` (`HEAD`, `BODY`, `CHUNK_SIZE`...). Các hàm `parseXxx`/`readXxx` trả `true` khi ăn được byte và có thể chạy tiếp, `false` khi **thiếu byte** (chờ lần `onRead` sau). Lỗi không trả `false` mà ném `HttpException(status)`.

| Bước | Đọc gì | Câu hỏi cần trả lời được |
|---|---|---|
| 1. Gom byte | `onRead`, `Accumulator.append` | Mảnh byte mới đi đâu? (`append` chép vào buffer riêng của `acc`, vùng chưa xử lý là `position..limit`; ăn byte bằng cách tăng `position`) |
| 2. Vòng parse | `parse`, `parseLoop` | Khi nào dừng? (`while (!busy && !closing)`, gọi hàm theo `state`, dừng khi hàm trả `false`) |
| 3. Đọc phần đầu | `parseHead`, `parseRequestLine`, `parseHeaderLine` | Biết đã đủ head bằng cách nào? (tìm `\r\n\r\n`; chưa thấy thì chờ, nhưng `\n` trần hoặc quá `maxHeaderBytes` thì lỗi ngay. Phần lớn dòng code là kiểm tra chặt chống request smuggling) |
| 4. Quyết định body | `decideBody` | Request có body không, đọc kiểu nào? (`Transfer-Encoding: chunked` → `CHUNK_SIZE`; `Content-Length: N` → `BODY` với `remaining = N`; không có gì → `dispatch` ngay. Có cả hai header, hoặc `Content-Length` mâu thuẫn → 400. Quá `maxBodyBytes` → 413 từ lúc thấy header) |
| 5. Đọc body | `readBody`, `readChunkSize`, `readChunkData`, `bodyComplete` | Body được gom thế nào? (lấy `min(remaining, byte đang có)` vào `body`; về 0 thì `bodyComplete` → `dispatch`) |
| 6. Giao cho handler | `dispatch` | Vì sao có `busy` và `setAutoRead(false)`? (`busy` giữ `parseLoop` không parse request kế tiếp; `setAutoRead(false)` ngừng đọc socket để `acc` không phình. Handler trả `CompletionStage`, xong thì `eventLoop().execute(...)` quay về loop thread gọi `finish`) |
| 7. Trả lời | `finish`, `sendResponse`, `serialize` | Sau khi gửi thì làm gì? (lỗi → thay bằng 500; `busy = false`; không keep-alive thì `closeAfterFlush`; còn keep-alive thì bật lại autoRead rồi **`parse` ngay**, vì request kế tiếp có thể đã nằm sẵn trong `acc`) |

**Pipelining** (client gửi request 2, 3 liền sau request 1 mà không chờ): HTTP/1.1 bắt buộc trả response đúng thứ tự. Stage làm đơn giản bằng cách chỉ xử lý **một request tại một thời điểm**: `dispatch` đặt `busy = true`, `finish` gỡ rồi mới parse request kế tiếp. Nhờ đó không cần hàng đợi response. Đánh đổi là các handler của một connection không chạy song song.

**Lần đầu bỏ qua (nhánh cứ coi là `false`):**

| Phần | Làm gì |
|---|---|
| `updateTimers`, `onTimeout`, `Timing` | Hạn keep-alive/header/body; mỗi lúc chỉ một timer, byte đến không làm nó dài thêm |
| `startStreamingIfWanted`, `StreamingBody`, `earlyResponse`, `draining` | Đường riêng cho upload lớn: body đi vào listener thay vì gom vào bộ nhớ. Đây là nửa lý do file dài |
| `sniffHttp2`, `switchToHttp2`, `h2cUpgradeSettings`, `upgradeToHttp2` | Nhận ra HTTP/2 (preface hoặc `Upgrade: h2c`) rồi giao connection cho `Http2ServerStage` qua `upgraded` |
| `takeover` trong `finish` | WebSocket/SSE: response 101 xong thì đổi sang stage khác; byte client gửi dính liền request upgrade nằm trong `acc` và được chuyển sang stage mới |
| `onDrain` | Tắt server êm: connection rảnh thì đóng, đang chạy thì xong rồi trả `Connection: close` |
| `recordResponse` | Access log và metrics |

**Hai chỗ dễ nhầm:**
- `send` dùng `ctx.write` chứ không ghi thẳng vào connection, để `TlsStage` phía trước mã hóa (đúng quy tắc của pipeline ở mục 3).
- Server **không nén và không giải nén** body HTTP; gzip chỉ có ở HTTP client (`ContentDecoder`, tùy chọn), gRPC và WebSocket `permessage-deflate`. `Transfer-Encoding` khác `chunked` bị từ chối 501.

## 5. Các giao thức khác (khi cần)
| Muốn hiểu | Bắt đầu từ | Ghi chú |
|---|---|---|
| HTTP/2 | `http/Http2ServerStage.java`, `http2/Hpack*.java` | Mỗi stream của h2 cũng là một `Connection`, nên SSE và WebSocket chạy nguyên trên cả h2 |
| gRPC | `grpc/GrpcServer.java`, `GrpcChannel.java`, `MethodDescriptor.java` | gRPC chạy trên h2; xem `GrpcHelloExample` trước |
| WebSocket, SSE | `websocket/`, `sse/` | Stage nhỏ, dễ đọc |
| Client AMQP (RabbitMQ) | `amqp/Wire.java`, `AmqpConnection.java`, `AmqpChannel.java`; xem `AmqpExample`, `AmqpProducerExample` | Một giao thức nhị phân đầy đủ chạy trên `Client.connect` + một `Stage`; đọc theo mục "Đọc client AMQP từng bước" bên dưới |
| QUIC / HTTP/3 | `QUIC-STATUS.md`, rồi `quic/QuicServer.java`, `quic/QuicConnection.java`, `http/Http3Server.java` | Khó nhất; đọc song song với RFC 9000/9001/9002/9114. Để cuối, hoặc bỏ qua nếu không cần |

### Đọc client AMQP từng bước

Đây là ví dụ nhỏ nhất về **một giao thức client hoàn chỉnh** trên Hydrangea: không có server, chỉ `Client.connect(...)` cộng một `Stage` (`AmqpStage`, nằm trong `AmqpConnection`) nằm sau `TlsStage` nếu bật TLS.
Nếu bạn vừa đọc `HttpServerStage`, cấu trúc quen: gom byte trong `Accumulator`, cắt frame, dispatch. Khác ở chỗ ở đây có **trạng thái kết nối** và **nhiều channel ghép trên một TCP**.

```
byte ──► AmqpStage.onRead ──► parseFrames ──► dispatch ─┬─► connectionMethod (kênh 0: bắt tay, heartbeat, đóng)
                                                        └─► AmqpChannel.method / header / body (kênh ≥ 1)
lời gọi của người dùng ──► AmqpChannel.call ──► sendMethodFrame ──► ctx.write
```

| Bước | Đọc gì | Câu hỏi cần trả lời được |
|---|---|---|
| 1. Kiểu dữ liệu | `Wire.Writer`, `Wire.Reader` | Bit được đóng gói thế nào? (nhiều cờ bool chung một byte, cờ đầu ở bit 0). Vì sao `Reader` không bao giờ ném `IndexOutOfBounds`? (mọi lần đọc kiểm tra còn đủ byte, lỗi là `AmqpException` để kết nối đóng sạch) |
| 2. Frame | `AmqpConnection.parseFrames` | Khi nào chờ thêm byte? (thiếu 7 byte header, hoặc chưa đủ `size + 1`). Hai kiểm tra chặn kẻ xấu? (`size > maxInbound` và byte kết thúc phải là `0xCE`) |
| 3. Bắt tay | `onStart`, `onTune`, `onOpenOk` | `Start` → `StartOk` → `Tune` → `TuneOk` + `Open` → `OpenOk`. Giá trị thương lượng chọn thế nào? (`negotiate`: một bên là 0 thì lấy bên kia, còn lại lấy nhỏ hơn) |
| 4. Heartbeat | `scheduleHeartbeat` | Hai việc trong một timer: gửi heartbeat khi im lặng nửa chu kỳ, và coi broker chết nếu im lặng quá **hai** chu kỳ |
| 5. Gọi và chờ trả lời | `AmqpChannel.call`, `reply`, hàng `pending` | Broker trả lời đúng thứ tự gửi, nên mỗi channel chỉ cần một hàng đợi `Rpc`. Điều gì xảy ra khi trả lời không khớp? (`UNEXPECTED_FRAME`, đóng cả kết nối) |
| 6. Message nhiều frame | `Content`, `header`, `body`, `complete` | Một message gồm method + header + n frame body. Vì sao `body` cấp phát tăng dần chứ không theo kích thước khai báo? (kích thước khai báo không phải bộ nhớ thật; tránh bị lừa cấp phát 1 TB) |
| 7. Publish | `basicPublish`, `writeProperties` | Body lớn cắt theo `frameMax - 8`. Confirm mode đánh số thế nào? (`nextPublishSeq` tăng cho **mọi** publish sau `confirmSelect`, vì broker đếm như vậy) |
| 8. Back-pressure | `whenWritable`, `AmqpStage.onWritabilityChanged` | `publish` không chặn, nên làm sao producer biết dừng? Future hoàn thành khi `SocketConnection` báo ghi được lại; và **thất bại** khi kết nối đóng (`onClose`), để không ai chờ mãi |
| 9. Đóng và lỗi | `fatal`, `onClose`, `AmqpChannel.terminate` | Lỗi mức channel (404, 406) chỉ đóng channel đó; lỗi mức kết nối đóng tất cả. Mọi future đang chờ và mọi consumer đều được báo |

**Kết nối tự phục hồi** (`RecoveringConnection`, `RecoveringChannel`) là lớp bọc bên ngoài, không đụng vào lõi trên; đọc sau cùng. Ý chính: ghi lại những gì đã khai báo, khi mất kết nối thì một virtual thread
lặp "chờ, nối lại, khai báo lại, mở lại channel, đăng ký lại consumer". Hai chỗ tinh tế đáng đọc: **delivery tag** (`tagOffset`: tag mà ứng dụng thấy luôn tăng qua các lần nối lại, nên ack tag cũ bị bỏ qua thay vì làm broker đóng channel),
và **queue do broker đặt tên** (`aliases`: tên cũ vẫn dùng được). Test là tài liệu tốt nhất ở đây: `RecoveryTest` dùng `ChaosProxy` (cắt kết nối, từ chối kết nối mới, "nuốt im lặng") đặt giữa client và RabbitMQ thật.

**Chạy test cần RabbitMQ:** `sh hydrangea-amqp/src/test/resources/amqp/start-broker.sh` dựng broker trong Docker (cổng thường 25672, TLS 25671). Không có broker thì các test đó tự bỏ qua. `HostileBrokerTest` và `WireTest` không cần broker.

### Đọc client MQTT từng bước

Client chạy được qua nhiều **transport** (`MqttTransport`): `TcpTransport` (mặc định, một `Stage` nhỏ) hoặc `MqttWebSocket` (module riêng). Transport chỉ chuyển byte và báo ba sự kiện (`connected`, `data`, `closed`); toàn bộ giao thức nằm trong `MqttClient`. Hỗ trợ cả MQTT 3.1.1 và 5.0 bằng cùng một lớp: mọi chỗ khác nhau đều rẽ nhánh theo `v5`.

MQTT đơn giản hơn AMQP về khung (một byte kiểu + độ dài biến thiên), nhưng **trạng thái phiên** phức tạp hơn: tin QoS 1/2 phải sống sót qua lần nối lại. Vì vậy cả việc nối lại nằm trong một lớp `MqttClient`
(không có lớp bọc như `RecoveringConnection`), và mọi trạng thái nằm trên **một event loop duy nhất** suốt đời client, kể cả khi kết nối đổi (`Client.connect(EventLoop, ...)`), nên không cần khoá.

| Bước | Đọc gì | Câu hỏi cần trả lời được |
|---|---|---|
| 1. Mã hoá | `Packets` (`Writer`, `publish`, `Reader`) | Độ dài còn lại mã hoá thế nào? (7 bit mỗi byte, tối đa 4 byte, 268 435 455). Vì sao `publish` dựng mảng byte một lần, không qua `Writer`? (payload nhiều MiB chỉ sao chép một lần) |
| 2. Cắt gói | `MqttClient.parse` | Khi nào chờ thêm? (độ dài chưa đủ, hoặc gói chưa đủ). Kiểm tra chặn kẻ xấu? (độ dài > `maxPacketBytes` bị từ chối **chỉ từ header**, trước khi gom byte; 5 byte độ dài là lỗi) |
| 3. Kiểm tra gói | `dispatch`, `checkFlags`, `Reader` | Gói nào broker không được gửi? (CONNECT, SUBSCRIBE, PINGREQ, DISCONNECT...). Cờ cố định sai, byte thừa, chuỗi UTF-8 hỏng, QoS 3, packet id 0: đều đóng kết nối |
| 4. Bắt tay | `onActive`, `onConnack` | Mã từ chối nào thì thử lại? (chỉ 3 "server unavailable"; sai mật khẩu hay không được phép thì không). `sessionPresent` quyết định gì? (gửi lại tin dở dang, hay báo hỏng và đăng ký lại topic) |
| 5. QoS 1 và 2 | `publish`, `pump`, `onPuback`, `onPubrec`, `onPubcomp` | Vì sao có hai hàng `queue` và `inflight`? (`maxInflight` giới hạn số tin đang bay; phần còn lại chờ, theo đúng thứ tự). QoS 2 gửi: PUBLISH → PUBREC → PUBREL → PUBCOMP, tin ở trạng thái `released` thì sau khi nối lại gửi PUBREL, **không** gửi lại PUBLISH |
| 6. Nhận QoS 2 | `onPublish`, `incomingQos2`, `onPubrel` | Làm sao giao đúng một lần? (nhớ packet id đã giao cho tới khi nhận PUBREL; PUBLISH lặp chỉ được trả lời PUBREC) |
| 7. Đăng ký | `subscribe`, `onSuback`, `deliver` | Vì sao handler được đăng ký **trước** khi có SUBACK? (tin retained có thể đến trước). Vì sao `subArray` là bản chụp? (handler được phép đăng ký thêm khi đang giao tin) |
| 8. Keep-alive | `keepAliveTick` | Gửi PINGREQ sau nửa chu kỳ im lặng của **phía gửi**; coi broker chết khi **phía nhận** im lặng quá 1,5 chu kỳ |
| 9. MQTT 5 | `Props5`, `onConnack` (nhánh `v5`), `readAck`, `deliver`, `onServerDisconnect` | Khối thuộc tính đọc thế nào và vì sao mọi vi phạm (id lạ, lặp, ngoài khoảng) đóng kết nối? Reason code ≥ 0x80 làm future thất bại ra sao (`reason`, `retriableReason`)? Vì sao định tuyến theo subscription identifier thay vì khớp filter? (filter chồng lấn không giao trùng). Vì sao 0x8E không nối lại? |
| 9b. MQTT 5 mở rộng | `MqttAuthenticator`, `onAuth`, `buildPublish`, `request`, `onResponse` | Vì sao một `Exchange` mới cho mỗi kết nối và mỗi lần xác thực lại? (nó giữ trạng thái, ví dụ nonce của SCRAM). Vì sao alias chỉ được ghi vào bảng **sau khi** gói đã dựng xong và qua kiểm tra kích thước? (broker chỉ biết alias khi nhận đúng gói đó). Vì sao tin phải gửi lại thì khai báo lại alias? (bảng của broker bắt đầu lại mỗi kết nối). Vì sao tin hết hạn trong hàng đợi thì bỏ, còn tin gửi lại thì vẫn gửi với 1 giây? (số hiệu gói không thể bỏ; 0 nghĩa là "không hết hạn" với một số broker). Một `request` có thể sống sót qua mất kết nối ra sao? (nó chỉ phụ thuộc vào đồng hồ hết hạn, không phụ thuộc kết nối) |
| 10. Mất kết nối | `connectionEnded`, `scheduleReconnect`, `giveUp` | Lần kết nối đầu thất bại thì trả lỗi cho người gọi, không thử lại. Sau khi đã từng online: thử lại với `Backoff`, trừ khi lỗi không thể khắc phục |

**Chạy test cần Mosquitto:** `sh hydrangea-mqtt/src/test/resources/mqtt/start-broker.sh` (cổng 21883 thường, 21884 có mật khẩu và ACL, 28883 TLS, 29001 WebSocket); không có broker thì các test đó tự bỏ qua.
`MqttProtocolTest`, `Mqtt5ProtocolTest` và `Mqtt5ExtrasTest` (AUTH, alias, hết hạn, request/response; có cả `ScramSha256` với ví dụ của RFC 7677) dùng `FakeBroker` (một peer giả mà test điều khiển từng byte) nên không cần broker và là chỗ tốt nhất để thấy chính xác từng gói được gửi và nhận.

## 6. Dùng test làm tài liệu
Mỗi lớp lạ: mở test tương ứng trước (`src/test/java/` của cùng module, hoặc `hydrangea-examples` cho test xuyên module). Test cho thấy cách dùng và các ca biên nhanh hơn đọc thân hàm.

Gợi ý: `EventLoopTest` (event loop), `HttpTest`, `RouterTest` (HTTP), `HttpRequestTargetTest` (ca biên nhỏ, dễ đọc), `Grpc*Test` (gRPC), `amqp/WireTest` (kiểu dữ liệu AMQP, không cần broker), `amqp/HostileBrokerTest` (broker giả độc hại viết bằng vài dòng `Socket`: mẫu hay để học cách kiểm thử giao thức).

## 7. Mẹo
- Đặt breakpoint trong `EventLoop` rồi `curl` một request. Đi theo stack trace từ lúc byte vào tới lúc handler của bạn chạy: cách nhanh nhất để hiểu toàn bộ luồng.
- Đọc cấu trúc trước (interface, field), thân hàm sau.
- Thư viện không có phụ thuộc ngoài, nên mọi thứ (HPACK, QPACK, TLS 1.3 cho QUIC, protobuf) đều viết tay. Gặp mã lạ, đoán trước là nó làm theo một RFC, và đọc RFC bên cạnh.

## 8. Tài liệu khác
- `README.md`: tính năng và cách chạy.
- `QUIC-STATUS.md`: QUIC / HTTP/3 đã có gì, kiểm chứng thế nào.
- `benchmark/RESULTS-*.md`: số đo, kèm giới hạn của từng phép đo.

### Đọc client Redis từng bước

Giao thức RESP rất nhỏ; phần đáng đọc là **một kết nối cho nhiều luồng**. Đọc theo thứ tự: `Resp.encode` (lệnh = mảng bulk string, tính đúng kích thước trước) → `RespParser` (parser có trạng thái và ngăn xếp khung nên không phân tích lại từ đầu mỗi lần đọc; bulk ≥ 64 KiB được điền thẳng vào mảng đích; giới hạn bulk/phần tử/độ sâu chặn dữ liệu xấu) → `RedisClient.send/submit/drain` (hộp thư `ConcurrentLinkedQueue` + cờ `drainScheduled`: luồng nào đẩy lệnh đầu tiên lập lịch một lần drain trên event loop, nên lệnh của mọi luồng được ghi cùng lúc; hàng `pending` khớp trả lời theo thứ tự) → `handshake` (HELLO 3, lùi về RESP2 + AUTH/CLIENT SETNAME/SELECT) → `onData/dispatch/onPush` (tin push `>` của pub/sub xen giữa trả lời thường) → kết nối lại và đăng ký lại. Vì sao cần `duplicate()`? (WATCH, SELECT, lệnh chặn là trạng thái của kết nối, mà kết nối được chia sẻ.)
Test: `RespParserTest`, `RespEncoderTest`, `RedisWireTest` (peer giả `FakeRedis`, không cần Redis) và `RedisClientTest` (cần `sh hydrangea-redis/src/test/resources/redis/start-redis.sh`: cổng 26379 thường, 26380 TLS, 26381 có mật khẩu + ACL; không có thì tự bỏ qua).

### Đọc client Kafka từng bước

Kafka khác Redis/MQTT ở chỗ client nói chuyện với **nhiều broker** và phải tự biết partition nào nằm ở broker nào. Thứ tự đọc: `Wire` (số nguyên big-endian, chuỗi, varint; `Reader` kiểm tra mọi độ dài nên dữ liệu bị cắt hoặc nói dối thành lỗi giao thức chứ không phải lỗi của JDK) → `Records` (định dạng record batch v2: CRC32C, nén gzip, giới hạn giải nén chống zip bomb, batch bị cắt dở ở cuối phản hồi thì bỏ qua) → `Broker` (một kết nối: mỗi request có correlation id, phản hồi theo thứ tự nên khớp bằng hàng đợi, giới hạn kích thước khung trước khi gom byte, request quá hạn thì **bỏ kết nối** vì phản hồi đến trễ sẽ bị nhầm với request kế) → `Cluster` (metadata, kết nối theo broker; vì sao các request cùng chờ một kết nối đang mở phải xếp **FIFO** chứ không đăng ký chung vào một `CompletableFuture`: nó chạy các hàm chờ theo thứ tự ngược, làm batch bị gửi đảo) → `KafkaProducer` → `KafkaConsumer`.

Producer: `send` từ mọi luồng chỉ đẩy vào hộp thư; vòng lặp sự kiện gom thành batch theo partition (`Part`), gửi một request chứa một batch của nhiều partition. Vì sao khi không idempotent mặc định một partition chỉ có một batch bay? (retry có thể vượt mặt batch khác). Idempotent: `ensureProducerId`, số thứ tự gán lúc gửi lần đầu và giữ nguyên khi retry (cả các byte), `recovering` (một batch lỗi thì đợi các batch đang bay trả lời rồi gửi lại **tất cả theo thứ tự**), và vì sao đến khi có một batch được xác nhận thì chỉ cho một batch bay (broker nhận batch đầu tiên của một producer cho partition dù sequence là bao nhiêu, nên batch 2 vào trước batch 1 sẽ khiến batch 1 bị từ chối mãi).
Consumer: `fetch` dài hạn trên kết nối của consumer, `JoinGroup` trên **kết nối riêng** tới coordinator (broker không xử lý request khác trên một kết nối khi một request đang chờ), thuật toán range, vì sao commit trước khi trả partition, `epoch` của từng partition để bỏ phản hồi fetch cũ sau `seek`.
Test: `WireRecordsTest`, `BrokerWireTest` (broker giả), `ProducerWireTest` (broker giả nói Metadata/InitProducerId/Produce, kiểm byte thật của idempotence) và `KafkaClientTest` (cần `sh hydrangea-kafka/src/test/resources/kafka/start-kafka.sh`: cổng 29092 thường, 29094 SASL/PLAIN; không có thì tự bỏ qua).
