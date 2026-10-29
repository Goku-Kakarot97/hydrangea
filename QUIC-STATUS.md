# QUIC / HTTP/3: tình trạng

## Giai đoạn 1 (xong): server QUIC có TLS 1.3 riêng
| Phần | Trạng thái |
|---|---|
| Gói tin Initial / Handshake / 1-RTT, bảo vệ header, AEAD, key update | có, khớp vector RFC 9001 và chạy với aioquic |
| TLS 1.3 server: x25519, AES-128-GCM, ECDSA P-256 / RSA-PSS, ALPN, SNI | có; 1 cipher suite, không HelloRetryRequest, không PSK/ticket, không 0-RTT |
| ACK, phát hiện mất gói và PTO (RFC 9002), NewReno | có; ACK ngay mỗi datagram (chưa delay), không pacing, không phát hiện tắc nghẽn kéo dài |
| Flow control kết nối và stream, giới hạn số stream, cấp lại cửa sổ | có |
| Anti-amplification (3x), padding Initial 1200 | có |
| Retry + address-validation token (`QuicConfig.withRetry(true)`, tắt mặc định; token không trạng thái, HMAC, 30 s, gắn với IP) | có; kiểm với aioquic và fuzz token |
| Migration, stateless reset, PMTUD, 0-RTT, ECN | chưa |
| Client QUIC | chưa |

## Đã kiểm chứng thế nào
- `QuicCryptoTest`, `QuicWireTest`: vector RFC 9001 / 9000 (khóa Initial, mặt nạ header, varint, giải mã số gói tin), `Ranges` so với mô hình tập hợp.
- `QuicInteropTest` (aioquic, 8 kịch bản x chứng chỉ EC và RSA): echo nhỏ và 2 MB, 120 stream tuần tự (tái chế giới hạn), mất gói 10% hai chiều, 3 lần key update, đóng từ server với mã ứng dụng, ALPN sai, Version Negotiation.
- Thủ công: 20 MB trên một stream (~3,6 s qua loopback với client Python), mất gói bắt đầu từ gói đầu tiên: 10% và 20% luôn qua; 30% hai chiều qua 38/40 lần, hai lần kẹt vì client lùi thời gian thử lại quá lâu (máy chủ hết hạn nhàn rỗi 30 s).
- `QuicFuzzTest`: 18 000 datagram độc hại (nội dung ClientHello, frame trong Initial hợp lệ, cả datagram bị biến đổi): không lỗi nội bộ, không chặn loop, không rò kết nối, bắt tay tốt vẫn chạy sau đó.
- `QuicFrame1RttFuzzTest`: aioquic bắt tay thật, rồi gửi gói 1-RTT mã hoá bằng khoá của chính kết nối với frame ngẫu nhiên/biến đổi (mọi loại frame, varint cực đoan). Đợt dài: 3 seed x 60 kết nối x 120 gói, không lỗi nội bộ, không chặn loop, không rò kết nối, client sạch vẫn echo được sau đó. Đã xác nhận qua trace là server giải mã được và nhiều kết nối bị đóng vì vi phạm giao thức.
- Lỗi thật tìm được nhờ test mất gói: stream bị giải phóng khi FIN đã được ACK trong lúc dữ liệu trước đó còn thất lạc, nên bản gửi lại không bao giờ đi. Đã sửa.

## Cần biết
- `-Dquic.trace=true` in từng gói và sự kiện phục hồi ra stderr (công cụ phát triển).
- Hiệu năng chưa tối ưu: `Cipher` đã được dùng lại theo khoá; còn ACK mỗi datagram, một syscall mỗi datagram.
- Bắt tay tốn một phép x25519 và một chữ ký; trên mạng không tin cậy cần giới hạn tốc độ (bật `withRetry(true)` để không tốn x25519/chữ ký cho địa chỉ giả mạo; đổi lại thêm một RTT).

## Giai đoạn 2: HTTP/3 (đã làm phần server)
`io.hydrangea.http.Http3Server` (+ `io.hydrangea.http2.Qpack`): khung HTTP/3, QPACK chỉ với bảng tĩnh (đã quảng bá capacity 0; tham chiếu bảng động bị từ chối), stream điều khiển và hai stream QPACK, SETTINGS, kiểm tra đầu vào (tên header viết hoa, pseudo-header sai chỗ, header riêng của kết nối, content-length sai, giá trị có CR/LF/NUL), cookie được nối lại, HEAD, 413/431/500/501.
- `Http3InteropTest` (client HTTP/3 của aioquic, 12 kịch bản): GET, echo 4 B và 900 KB, 3 MB xuống, 60 request song song, 250 tuần tự (stream mới liên tục được cấp), 404, header lặp/cookie/rất dài, 431, 413, handler ném lỗi (500), handler bất đồng bộ.
- `QpackTest`: bảng tĩnh (nội dung sinh từ `pylsqpack`), round trip, Huffman, từ chối tham chiếu động, đầu vào hỏng, 20 000 chuỗi ngẫu nhiên.
- `Http3FuzzTest`: aioquic bắt tay thật rồi ghi frame HTTP/3, khối QPACK, loại stream một chiều ngẫu nhiên hoặc bị biến đổi, có và không có FIN. Các kết nối bị đóng với nhiều mã lỗi HTTP/3 khác nhau (0x105, 0x106, 0x10a, 0x201, 0x103, 0x104, 0x109, 0x200); không lỗi nội bộ, không chặn loop, không rò kết nối, client bình thường vẫn được phục vụ.
- Giới hạn của chính client kiểm thử: aioquic từ chối phản hồi HEAD có `content-length` mà không có body, nên HEAD chưa có kịch bản interop (server làm đúng RFC 9110 §9.3.2); encoder của nó giới hạn khoảng 4 KB header, nên các test header lớn dùng `maxHeaderBytes` nhỏ (3000).
- Chưa có: streaming request/response, upgrade, server push, trailer ở response, RESET_STREAM/STOP_SENDING (lỗi mức stream trả 400), client HTTP/3, `Alt-Svc`, delayed ACK.
- Hiệu năng (giai đoạn 3): sửa lỗi mỗi request tốn 2 datagram (phản hồi và ACK tách rời): còn 1,1 datagram/request, CPU server trên laptop 82 -> 50 µs/request. Trên k8s 0,5 CPU: ~24 nghìn request/s, xem `benchmark/RESULTS-K8S-H3.md`. Phần còn lại chủ yếu là syscall UDP (Java không có sendmmsg/GSO).
