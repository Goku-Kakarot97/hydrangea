# Hydrangea Kafka client so với kafka-clients 3.9.0

Cùng một broker Apache Kafka 3.9.0 (KRaft, một node, Docker, `hydrangea-kafka-test`); client chạy trên **host macOS** qua port mapping của Docker (không dùng container chung network namespace như bench MQTT). Mỗi lần chạy là một JVM; 3 vòng xen kẽ thứ tự, lấy **trung vị**. Mã: `benchmark/src/kafka/KafkaBench.java`, `kafka-bench.sh`, `aggregate-kafka.py`; số thô và tổng hợp: `benchmark/kafka-results/` (`summary.md`). Bản Hydrangea đo là bản **sau khi tối ưu và sửa bộ chọn partition** (mục 3 và mục điều tra).

Ba cấu hình producer, giống nhau cho hai thư viện (record 100 B, 3 partition nếu không ghi khác, linger 5 ms, batch 64 KiB):
- **IDEM** (mặc định của cả hai client): idempotent, `acks=-1`, 5 batch bay mỗi partition.
- **FAST**: không idempotent, `acks=1`, 5 bay (retry có thể đảo thứ tự hoặc trùng; mặc định của client Java trước 3.0).
- **SAFE**: không idempotent, `acks=1`, 1 bay (retry không thể vượt batch khác).

## Kết quả (Hydrangea | kafka-clients; số lớn hơn tốt hơn trừ độ trễ và CPU)

| kịch bản | Hydrangea | kafka-clients |
|---|---|---|
| produce IDEM, 3 partition (records/s) | **1 928 k** | 1 611 k |
| produce IDEM, 1 partition (xem A/B có kiểm soát bên dưới: ngược lại) | 1 294 k | 1 491 k |
| produce IDEM, 1 KB | **257 k** | 227 k |
| produce FAST, 3 partition | **2 067 k** | 1 537 k |
| produce SAFE, 3 partition | 995 k | **1 193 k** |
| p50 / p99 produce IDEM 3 partition (ms) | **21 / 96,5** | 26 / **84** |
| p50 / p99 produce IDEM 1 partition (ms) | 27 / **243** | 28 / **84** |
| CPU client cho 100 k record, produce IDEM 3 partition (giây) | **0,043** | 0,108 |
| 1 000 virtual thread dùng chung, mỗi cái chờ ack, IDEM (records/s) | **723 k** | 359 k |
| 1 000 virtual thread, FAST | **670 k** | 337 k |
| virtual thread p99, IDEM (ms) | **4,5** | 10 |
| consume 3 triệu × 100 B (records/s) | **5,6 M** | 3,9 M |
| consume 500 k × 1 KB | **730 k** | 693 k |
| consume trong consumer group | **6,1 M** | 3,0 M |
| CPU client cho 100 k record, consume | **0,022** | 0,032 |
| độ trễ gửi → nhận, p50 / p99 (ms) | **0,3 / 1,2** | 0,5 / 1,7 |
| tạo producer + gửi bản ghi đầu, p50 (ms) | **10** | 13,6 |

## Đọc kết quả
- **Bộ này đo bản đã sửa bộ chọn partition** (mục điều tra bên dưới). Hydrangea thắng ở: produce IDEM và FAST 3 partition (+20–35%), 1 KB, nhiều virtual thread dùng chung một producer (**2 lần**, p99 thấp hơn 2 lần, CPU 2,7 lần ít hơn), độ trễ end-to-end, và consume (5–100%, CPU ít hơn ~30%; số consume của kafka-clients dao động mạnh giữa các bộ: 5,2 M ở bộ trước, 3,9 M ở bộ này).
- **Hydrangea còn thua** trong bộ này: 1 partition (IDEM) 13% với p99 gấp ~3 (243 so với 84 ms), và SAFE (1 batch bay mỗi partition, không idempotent) 17%. **Thua ở 1 partition không tái hiện** khi đo có kiểm soát (mục dưới: Hydrangea 1,70 M so với 1,41 M, p99 ngang nhau), nên coi là nhiễu của bộ 3 vòng; SAFE chưa được đo lại có kiểm soát.
- CPU client mỗi record ở produce bằng khoảng 40–45% của kafka-clients.
- **Phương sai**: thông lượng cùng cấu hình dao động ±40% giữa các lần chạy (Docker trên macOS), nên chênh dưới ~30% không đáng tin; các bộ đo khác nhau trước đây cho hướng chênh lệch khác nhau ở produce (xem mục dưới).

## Hạn chế của phép đo
Docker trên macOS (độ trễ ~260 µs, băng thông bị giới hạn), một broker một node không replica (acks=-1 không chờ đồng bộ thật), dữ liệu ngẫu nhiên nên chưa đo nén, khoá `null`. Chưa đo Kafka 4.x, TLS, SASL, transaction (client này không có), cooperative rebalance.

## Lỗi tìm ra trong lúc viết test và tối ưu
1. Request cùng chờ một kết nối đang mở bị gửi **đảo thứ tự** (`CompletableFuture` chạy hàm chờ của một future theo LIFO).
2. Idempotent: broker nhận batch đầu tiên của một producer cho partition dù sequence khác 0, nên batch 1 lỗi tạm thời còn batch 2 vào trước thì batch 1 bị từ chối vĩnh viễn (`OUT_OF_ORDER_SEQUENCE_NUMBER`). Sửa: chưa có batch nào được xác nhận thì chỉ một batch bay mỗi partition.
3. Retry của producer tính độ trễ hai lần (có jitter) nên có thể không bao giờ được đánh thức lại: treo.
4. Hàng đợi MPSC tự viết giữ bộ nhớ (nút đã lấy ra vẫn trỏ nút sau): thời gian dừng GC gấp 7–9 lần. Sửa: ngắt liên kết khi lấy ra.

## 3. Tối ưu đã làm (profile JFR)
Produce: kích thước record tính một lần (`varlongSize` từng chiếm 21% mẫu), mã hóa thẳng vào mảng đúng cỡ, `Writer.varlong` không còn đặt trước 10 byte (nó làm cấp phát lại mảng batch rồi `toArray`), hộp thư MPSC thay `ConcurrentLinkedQueue` (27% mẫu tranh chấp), bộ đệm request cấp đúng cỡ. Consume: tra trạng thái partition một lần cho mỗi đoạn record liên tiếp (`TopicPartition.equals` từng 23% mẫu), đường nhanh cho varint một byte, giải mã thẳng vào hàng đợi. Kết quả đo: CPU mỗi record giảm 8–15%, GC không còn bất thường; thông lượng cải thiện nhưng nằm trong biên nhiễu.

## Điều tra: vì sao p99 produce cao hơn?
Gắn bộ đếm theo giai đoạn vào producer (bản sao, không nằm trong mã chính): độ trễ request tới broker p50 3–4 ms, p99 ≈ 20 ms; độ trễ của event loop (timer 2 ms) p99 ≈ 2,5 ms; GC không có lần dừng nào trên 20 ms; `complete()` p99 0,1 ms. Đuôi nằm ở **thời gian một batch chờ trước khi được ghi** (p50 ≈ 20–26 ms, p99 ≥ 150 ms), tức là chờ một ô "bay" trống của partition.
Nguyên nhân: bản ghi không có khóa chọn partition **ngẫu nhiên** mỗi khi một batch mới bắt đầu; nếu partition đó đang đầy 5 batch bay thì batch đứng chờ trong khi partition khác rảnh. Client Java từ 3.3 dùng partitioning thích ứng (KIP-794) vì lý do này. Sửa: batch mới của bản ghi không khóa vào partition có ít batch đang chờ hoặc đang bay nhất (hòa thì luân phiên).
A/B 6 vòng xen kẽ, IDEM, 3 partition, 100 B (`kafka-results/ab-partitioner.txt`; trung vị):

| | Hydrangea trước | Hydrangea sau | kafka-clients |
|---|---|---|---|
| records/s | 1 325 k | **1 968 k** | 1 697 k |
| p50 (ms) | 26,3 | **21,5** | 22,6 |
| p99 (ms) | 197 | **75,5** | 81,8 |

Bảng đầu tài liệu này là bộ đầy đủ chạy lại sau khi sửa.

## Điều tra: 1 partition
Với 1 partition request tới broker rất nhanh (p50 1,3 ms) và cửa sổ 5 batch bay luôn đầy (4,98 trung bình); thông lượng = 5 ÷ độ trễ trung bình (~2,9 ms, bị đuôi của broker kéo lên). Tìm được hai hao hụt trong mã và sửa: batch chỉ đầy ~60 KB thay vì 64 KB (kiểm tra giới hạn cộng cả 12 byte kế toán bộ nhớ mỗi record), và sau mỗi phản hồi batch kế tiếp đợi một vòng lặp sự kiện (~0,2 ms). A/B 6 vòng xen kẽ, IDEM, 100 B (`kafka-results/ab-1partition.txt`; trung vị records/s, p99 ms):

| | Hydrangea trước | Hydrangea sau | kafka-clients |
|---|---|---|---|
| 1 partition | 1 807 k, p99 79 | 1 695 k, p99 92 | 1 407 k, p99 97 |
| 3 partition | 2 185 k, p99 73 | **2 234 k**, p99 69 | 1 768 k, p99 69 |

Hai sửa trên **không tạo cải thiện đo được** (sau ≈ trước trong biên nhiễu; 1 partition thấp hơn 6% nhưng các lần chạy chồng lấp nhau), nhưng giữ lại vì đúng về ngữ nghĩa (`batchBytes` là kích thước thật trên dây). Điều quan trọng hơn: ở đo có kiểm soát Hydrangea **không thua** kafka-clients ở 1 partition (nhanh hơn ~20–29%) và p99 ngang nhau; khoảng cách trong bộ 3 vòng là nhiễu của thứ tự chạy/trạng thái broker.
