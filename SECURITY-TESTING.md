# Kiểm thử bảo mật

Phạm vi: chính code của Hydrangea (không có dependency ngoài nên không có CVE từ thư viện bên thứ ba). Tất cả chạy được bằng `mvn test`;
mặc định là bản "smoke" ngắn, chiến dịch dài dùng `-DargLine="-Dfuzz.iterations=N -Dfuzz.protocol.iterations=N"`.

## Đã làm

| Lớp kiểm thử | Mục tiêu | Số input đã chạy trong chiến dịch dài | Kết quả |
|---|---|---:|---|
| `DecoderFuzzTest` | khung gRPC (cắt mảnh ngẫu nhiên, gzip), JSON, service config, xDS bootstrap, 4 loại tài nguyên xDS (protobuf), gRPC-Web text | 400 000 mỗi mục tiêu | 0 phát hiện |
| `HpackFuzzTest`, `HuffmanTest` | HPACK (decoder mới và decoder dùng chung qua nhiều input), Huffman | 1 000 000 mỗi mục tiêu | 0 phát hiện |
| `ProtocolFuzzTest` | server thật qua socket: HTTP/1.1, h2c (frame thô, HPACK thật), gRPC, gRPC-Web, WebSocket (có deflate), SSE, h2c Upgrade | 4 x 12 000 kết nối | 0 phát hiện |
| `ClientFuzzTest` | client trước server độc hại: HTTP/1.1, h2, gRPC, WebSocket, SSE | 5 x 5 000 phản hồi | 0 phát hiện (1 báo động giả, xem dưới) |
| `TlsFuzzTest` | listener TLS: ClientHello thật bị biến đổi, HTTP trần vào cổng TLS, alert, record khổng lồ, SSLv2 hello | 15 000 kết nối | 0 phát hiện |
| `SecurityAttackTest` | tấn công có hình dạng cụ thể, kiểm bằng bất biến | 8 test | 0 phát hiện |

Bất biến ("oracle") của fuzz giao thức: server không ghi log WARNING/ERROR về lỗi nội bộ (stage báo bug của chính nó bằng RuntimeException), event loop
không bị chặn quá 250 ms, không rò kết nối sau khi client đóng, và sau mỗi chiến dịch các request tốt (HTTP/1.1, h2, gRPC) vẫn chạy. Phía client: mọi call
phải kết thúc (thành công hoặc lỗi sạch), không log lỗi nội bộ, không rò kết nối. Oracle được kiểm chứng bằng mutant cố ý (thêm NPE vào Huffman: fuzz bắt được ở
input thứ 0 và 61; bỏ kiểm tra CL+TE: `SecurityAttackTest` bắt được) và một test tự kiểm của bộ fuzz.

`SecurityAttackTest` gồm: 17 biến thể request smuggling HTTP/1.1 (CL+TE, hai Content-Length, TE bị làm rối, độ dài âm/hex/tràn, chunk lỗi) với bất biến
"request giấu phía sau không bao giờ tới handler" (có đối chứng cho thấy request pipeline hợp lệ thì tới); 10 kiểu message sai chuẩn (LF trần, CR trần,
NUL, obs-fold, thiếu/trùng Host...) phải bị từ chối; body khổng lồ bị từ chối mà không đọc; header/dòng request/số header/chunk vượt giới hạn; ký tự điều khiển
không bao giờ tới handler (HTTP/1.1 và h2, kể cả `\r\n` trong `:path` và giá trị header); 10 kiểu header h2 vi phạm (chữ hoa, `transfer-encoding`, `connection`,
`te: gzip`, pseudo-header sai chỗ/trùng/thiếu, content-length lệch body); WebSocket khác origin bị 403; 9 kiểu frame WebSocket hỏng (không mask, RSV, opcode
dành riêng, control frame dài/phân mảnh, UTF-8 sai, vượt giới hạn, continuation lạc, mã đóng sai) không bao giờ được echo tiếp.

## Rà soát code (không dùng công cụ ngoài)
- Không có giải tuần tự hóa Java, không `Runtime.exec`, không `setAccessible`, không TrustManager/HostnameVerifier tự viết (client xác minh hostname theo mặc định).
- Số ngẫu nhiên: nonce WebSocket và masking key dùng `SecureRandom`; `ThreadLocalRandom` chỉ dùng cho jitter/chọn trọng số, không phục vụ bảo mật.
- SHA-1 chỉ có trong handshake WebSocket (RFC 6455 bắt buộc).

## Báo động giả đã loại
Một lần fuzz client báo "call never finished" khi server trả `301 Location: http://127999999999999999999999`: JDK đọc host số đó thành IPv4 79.255.255.255,
không ai trả lời nên chờ hết `connectTimeout` 5 giây (không phải treo; DNS chạy trên executor riêng, không trên event loop). Giới hạn của test đã nâng lên 8 giây.
Hệ quả cần biết: một server độc hại có thể làm client đi theo redirect tới địa chỉ bất kỳ (SSRF phía client là việc của ứng dụng: hãy lọc URL đích nếu
URL đến từ nguồn không tin cậy).

## Chưa làm / giới hạn
- Fuzz là ngẫu nhiên có hướng dẫn theo corpus, không có phản hồi độ phủ code (không phải AFL/libFuzzer); 0 phát hiện không phải là chứng minh không có lỗi.
- Chưa fuzz: frame HTTP/2 trong luồng TLS (ALPN h2) sau handshake thành công, upload streaming, parser Unix socket, `ReflectionService` với descriptor lạ, kết hợp nhiều kết nối đồng thời.
- Chưa có: review mật mã học của người ngoài, kiểm thử xuyên thấu độc lập, thử nghiệm DoS ở quy mô lớn (số kết nối, băng thông), đo thời gian handshake dưới tấn công.
- TLS cấu hình mặc định là của JDK (TLS 1.2/1.3, suite mặc định); framework không ép chính sách riêng.
