# Nhận xét code XNIO

Phạm vi đọc: `WorkerThread`, `NioSocketConduit`, `NioTcpServer` (một phần), `ByteBufferPool`, cấu trúc `api/` và `nio-impl/` (375 file Java, ~45k dòng).

## Điểm tốt
- **Kiến trúc đúng hướng**: mỗi `WorkerThread` sở hữu 1 `Selector`, I/O của một kênh luôn chạy trên 1 thread → ít cần lock cho state của kênh.
- **Tách API / implementation** (`api` vs `nio-impl`), có thể thay backend.
- **Mô hình Conduit** (chồng các lớp: SSL, framing, deflate, fixed-length) khá linh hoạt, tái sử dụng được.
- Có xử lý các bug thực tế của selector (`ops == 0`, `CancelledKeyException` trên Mac), shutdown có trình tự.
- Test khá nhiều (mock channel, race-condition tests).

## Vấn đề cần cải thiện
1. **Độ phức tạp API quá lớn**: Channel *và* Conduit *và* StreamConnection *và* listener `Setter`, mỗi thứ có nhiều lớp Abstract/Translating/Delegating. Rất khó học; người dùng phải hiểu 3 tầng trừu tượng chỉ để echo dữ liệu.
2. **Callback-style với `ChannelListener` + `Setter`**: trạng thái rải rác ở nhiều listener (read/write/close), dễ sai thứ tự; không có điểm tập trung cho error handling.
3. **Lỗi bị nuốt**: `NioSocketConduit.handleReady` có `catch (Throwable ignored) {}` quanh `readReady()/writeReady()`; `WorkerThread` chỉ `tracef` khi handler ném lỗi → bug của người dùng biến mất, kết nối treo. Nên đóng kết nối + báo cause.
4. **Lock thô ở hot path**: `synchronized (workLock)` cho mỗi `execute()` và mỗi vòng lặp (`ArrayDeque` + `TreeSet`); hàng đợi MPSC lock-free (`ConcurrentLinkedQueue`) sẽ tốt hơn. Thêm `synchronized (selector)`, `synchronized (keySet)` mỗi vòng lặp.
5. **Hai cờ hệ thống "old-locking" / "thread-safe-selection-keys"** và code legacy (`AccessController`, `SecurityManager`—đã deprecated for removal từ Java 17) làm rối code; target Java 11.
6. **Không có backpressure rõ ràng**: write trả về số byte, người dùng tự quản lý `suspendWrites`; không có high/low watermark → dễ OOM với peer chậm.
7. **Timeout/idle** rải rác (`writeTimeout` kiểm tra thủ công trong từng conduit, `lastWrite` field không volatile, `checkWriteTimeout` chỉ chạy khi có I/O → không phát hiện kết nối treo thật sự).
8. **Class khổng lồ**: `Buffers` 2.4k dòng, `JsseStreamConduit` 1.6k, `XnioWorker` 1.6k, `WorkerThread` 900 dòng làm quá nhiều việc (connect, accept, pipe, timer, selector loop).
9. **Hạ tầng cũ**: ThreadLocal cache trong `ByteBufferPool`, finalizer/`AutomaticReference`, JBoss Logging, OSGi activator, `Version` class; không dùng record/sealed/virtual-thread của Java 17–21.
10. **Thiếu observability**: không có metrics (số kết nối, byte queue, loop lag).

## Kết luận
Nền tảng **tốt về mặt kỹ thuật** (reactor + thread-confinement), nhưng API **quá nặng**, xử lý lỗi **kém an toàn**, và code mang nhiều **di sản**. Hydrangea giữ lại ý tưởng cốt lõi và sửa các điểm trên.

## Hydrangea đã giải quyết thế nào
| Vấn đề XNIO | Hydrangea |
|---|---|
| API 3 tầng | 1 tầng: `Handler` + `Connection` (~1000 dòng cả core) |
| Callback rải rác | 1 `Handler` / kết nối, state là field thường |
| Lỗi bị nuốt | Exception trong callback → đóng kết nối, `onClose(conn, cause)` |
| Lock thô | Queue MPSC lock-free + wakeup bằng CAS, timer heap chỉ chạm từ loop |
| Backpressure | `isWritable()` + high/low watermark + `setAutoRead` |
| Idle timeout | Timer trên loop, `IdleTimeoutException` |
| Buffer per-connection | 1 read buffer direct dùng chung mỗi loop (zero-copy tới handler) |
| Framing | `FramingHandler` có giới hạn kích thước frame (chống OOM) |
| Java cũ | Java 21: record, pattern matching, `System.Logger`, không SecurityManager |
| Không có TLS đơn giản | `TlsStage` (SSLEngine, handshake bất đồng bộ, verify hostname) |
| Không có UDP | `UdpSocket` với giới hạn hàng đợi |
| Spin-bug selector | Phát hiện 512 lần select sớm liên tiếp → chuyển kênh sang Selector mới |
| Không có metrics | `Metrics` (LongAdder) + `maxBlockNanos` |
| Cấp phát buffer | `BufferPool` + `write(buf, onDone)` |
| Chuỗi xử lý | `Pipeline`/`Stage` thay cho chồng Conduit |
| Không có HTTP | `io.hydrangea.http` (server HTTP/1.1, parser chặt, giới hạn kích thước) |
| Chỉ TCP | Unix domain socket qua cùng `Server`/`Client` |
| UDP chỉ unicast | Multicast: `joinGroup`, `setOption`, `Config` |

## Sau khi đo và soak (cập nhật)
- **Hiệu năng** (so với grpc-java/Netty cùng tải, xem `benchmark/` và mục "Hiệu năng" của README): server gRPC ≥ grpc-java ở payload nhỏ/vừa, thấp hơn ~6–8% ở 64 KiB, streaming nhanh gấp 1,6–2 lần; HTTP/2 ≥ Netty; client unary nhỏ nhanh hơn grpc-java ~20%, unary 64 KiB còn thấp hơn ~12%.
  Nhờ gộp ghi theo lô (flush cuối callback đọc và cuối mỗi lô task), gather-write cho DATA, cache HPACK.
- **Soak 60 phút trên k8s** (`benchmark/SOAK-H2.md`): ~7,25 triệu thao tác, 0 lỗi, heap/thread/fd không tăng, kết nối mở = đóng. Công suất 1 pod 0,5 CPU: `benchmark/RESULTS-K8S-CAP.md`.
- **Lỗi thật tìm được**: race trong `Exchange.abortConnection` (huỷ call trước khi stream bắt đầu); đã sửa.
- **Hai lỗ hổng soak lộ ra, đã xử lý**: (1) server gRPC bị rapid-reset defence ngắt khi client huỷ nhiều → `Http2Config.grpcDefaults()`; (2) client gRPC không gửi lại call bị GOAWAY/REFUSED_STREAM → thử lại trong suốt tối đa 2 lần.
- **Chưa làm**: đo TLS, đối chứng grpc-java trên k8s, virtual thread, `waitForReady`.
