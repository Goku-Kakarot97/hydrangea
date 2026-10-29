# Giới thiệu Java NIO (đủ để đọc Hydrangea)

Bài này giải thích ba thứ Hydrangea dựa vào: `ByteBuffer`, channel non-blocking và `Selector`. Đọc xong bạn có thể mở `EventLoop.java` mà không bị lạ.

## 1. Vấn đề: một thread cho mỗi kết nối

Cách cổ điển (`java.io`):

```java
Socket s = server.accept();                 // chặn tới khi có kết nối
new Thread(() -> {
    int n = s.getInputStream().read(buf);   // chặn tới khi có dữ liệu
    ...
}).start();
```

`read()` chặn thread. Muốn phục vụ 10 000 kết nối thì cần 10 000 thread. Mỗi thread tốn bộ nhớ stack và chi phí chuyển ngữ cảnh, trong khi phần lớn thời gian chúng chỉ ngồi chờ. NIO giải quyết bằng cách để **một thread trông nhiều kết nối**.

(Java 21 có virtual thread cũng giảm chi phí này theo hướng khác. Hydrangea chọn mô hình event loop của NIO.)

## 2. Ba thành phần

| Thành phần | Vai trò |
|---|---|
| `ByteBuffer` | Vùng nhớ chứa byte, có con trỏ đọc/ghi |
| `SocketChannel` / `ServerSocketChannel` | Socket có thể đặt chế độ không chặn |
| `Selector` | Hỏi một lần: "trong các channel đã đăng ký, cái nào sẵn sàng?" |

## 3. `ByteBuffer`

Buffer có ba số: `position` (vị trí hiện tại), `limit` (ranh giới), `capacity` (sức chứa). Đây là chỗ người mới hay vấp nhất.

```
vừa tạo allocate(8):   [ _ _ _ _ _ _ _ _ ]   position=0 limit=8 capacity=8
ghi 3 byte "abc":      [ a b c _ _ _ _ _ ]   position=3 limit=8
flip():                [ a b c _ _ _ _ _ ]   position=0 limit=3     <- chuyển sang chế độ đọc
đọc hết 3 byte:        [ a b c _ _ _ _ _ ]   position=3 limit=3     hasRemaining() = false
clear():               [ a b c _ _ _ _ _ ]   position=0 limit=8     <- sẵn sàng ghi lại (dữ liệu cũ bị coi như bỏ)
```

- Ghi vào buffer (kể cả `channel.read(buf)` đọc từ mạng vào buffer) làm `position` tăng.
- **`flip()`**: chuyển từ "vừa ghi xong" sang "chuẩn bị đọc ra" (`limit = position; position = 0`).
- **`clear()`**: bắt đầu ghi lại từ đầu.
- **`compact()`**: giữ phần chưa đọc, dồn về đầu buffer, rồi tiếp tục ghi. Dùng khi còn nửa message chưa xử lý.
- `remaining()` = `limit - position` = còn bao nhiêu byte để đọc (hoặc còn chỗ để ghi).

Hai loại buffer:
- `allocate(n)`: heap, có `array()`.
- `allocateDirect(n)`: nằm ngoài heap, hệ điều hành đọc/ghi trực tiếp, đỡ một lần copy khi I/O. Đổi lại cấp phát đắt hơn nên thường dùng lại. `EventLoop` có một direct buffer 64 KB dùng chung cho mọi kết nối của nó.

## 4. Chế độ không chặn

```java
channel.configureBlocking(false);
int n = channel.read(buf);
```

Với channel non-blocking, `read` **không bao giờ chờ**:
- `n > 0`: đọc được n byte.
- `n == 0`: lúc này chưa có gì để đọc.
- `n == -1`: bên kia đã đóng.

`write(buf)` cũng không chờ: nó chỉ ghi được **một phần** nếu bộ đệm gửi của hệ điều hành đầy, và trả về số byte đã ghi. Phần còn lại bạn phải gửi sau. Đây là lý do mọi thư viện mạng đều phải có hàng đợi ghi và cơ chế backpressure (trong Hydrangea: `Connection.isWritable()`, `setAutoRead`).

Nhưng nếu `read` trả `0` mà ta cứ gọi lặp lại thì đốt CPU. Đó là việc của `Selector`.

## 5. `Selector`

```java
Selector selector = Selector.open();
channel.register(selector, SelectionKey.OP_READ);   // "báo tôi khi channel này đọc được"
selector.select();                                  // ngủ tới khi có channel sẵn sàng
for (SelectionKey key : selector.selectedKeys()) { ... }
```

Các loại sự kiện (`interest ops`):

| Op | Nghĩa |
|---|---|
| `OP_ACCEPT` | server socket có kết nối mới |
| `OP_CONNECT` | kết nối đi (client) đã hoàn tất |
| `OP_READ` | có dữ liệu để đọc |
| `OP_WRITE` | bộ đệm gửi còn chỗ (ghi được) |

`select()` chặn thread, nhưng là *một* thread chờ *tất cả* channel, và hệ điều hành đánh thức nó khi có việc (epoll trên Linux, kqueue trên macOS).

`SelectionKey` nối một channel với selector. Dùng `key.attach(obj)` để gắn đối tượng của bạn (Hydrangea gắn một `SelectionHandle`). `OP_WRITE` thường chỉ nên bật khi bạn còn dữ liệu chưa ghi hết, vì bộ đệm gửi gần như lúc nào cũng còn chỗ nên nó báo liên tục.

## 6. Ví dụ hoàn chỉnh: server echo một thread

```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;

public class MiniEcho {
    public static void main(String[] args) throws IOException {
        Selector selector = Selector.open();

        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(9000));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        ByteBuffer buf = ByteBuffer.allocate(4096);
        while (true) {
            selector.select();                                   // chờ có việc
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();                                     // phải tự xóa, nếu không lần sau lại thấy nó

                if (key.isAcceptable()) {
                    SocketChannel client = server.accept();
                    client.configureBlocking(false);
                    client.register(selector, SelectionKey.OP_READ);
                } else if (key.isReadable()) {
                    SocketChannel ch = (SocketChannel) key.channel();
                    buf.clear();
                    int n = ch.read(buf);
                    if (n < 0) {                                 // bên kia đóng
                        ch.close();
                        continue;
                    }
                    buf.flip();                                  // chuyển sang chế độ đọc
                    while (buf.hasRemaining()) {
                        ch.write(buf);
                    }
                }
            }
        }
    }
}
```

Ví dụ này đã chạy được trên máy: gửi `xin chao` tới cổng 9000 và nhận lại đúng chuỗi đó. Một thread phục vụ mọi kết nối.

**Ví dụ này cố ý đơn giản và có hai điểm yếu** (đúng những điểm mà Hydrangea phải xử lý):
1. `while (buf.hasRemaining()) ch.write(buf)` quay vòng chờ khi bộ đệm gửi đầy, tức là *chặn cả loop*. Bản đúng nằm ở mục 7: hàng đợi ghi cộng `OP_WRITE`.
2. Đọc `n` byte không có nghĩa là đủ một message. TCP là luồng byte: một message có thể đến thành nhiều mảnh, hoặc nhiều message dính vào nhau. Cần stage/codec để tách (`codec/`, `LineStage`, hay bộ phân tích HTTP).

## 7. Sửa điểm yếu 1: hàng đợi ghi và `OP_WRITE`

Ý tưởng: không bao giờ chờ trong `write`. Ghi được bao nhiêu thì ghi, phần còn lại cất vào hàng đợi của riêng kết nối, rồi **bật `OP_WRITE`**. Khi hệ điều hành báo bộ đệm gửi có chỗ trống, selector đánh thức ta, ta ghi tiếp. Hết dữ liệu thì **tắt `OP_WRITE`**.

Tại sao phải tắt: bộ đệm gửi hầu như lúc nào cũng còn chỗ, nên nếu để `OP_WRITE` luôn bật thì `select()` trả về liên tục và loop đốt 100% CPU mà không có việc gì.

```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

public class QueueEcho {
    public static void main(String[] args) throws IOException {
        Selector selector = Selector.open();
        ServerSocketChannel server = ServerSocketChannel.open();
        server.bind(new InetSocketAddress(9001));
        server.configureBlocking(false);
        server.register(selector, SelectionKey.OP_ACCEPT);

        ByteBuffer buf = ByteBuffer.allocate(4096);
        while (true) {
            selector.select();
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                if (!key.isValid()) continue;

                if (key.isAcceptable()) {
                    SocketChannel client = server.accept();
                    client.configureBlocking(false);
                    // attachment: hàng đợi byte chờ gửi của riêng kết nối này
                    client.register(selector, SelectionKey.OP_READ, new ArrayDeque<ByteBuffer>());
                    continue;
                }

                SocketChannel ch = (SocketChannel) key.channel();
                @SuppressWarnings("unchecked")
                Queue<ByteBuffer> pending = (Queue<ByteBuffer>) key.attachment();

                if (key.isReadable()) {
                    buf.clear();
                    int n = ch.read(buf);
                    if (n < 0) {
                        ch.close();
                        continue;
                    }
                    buf.flip();
                    ByteBuffer copy = ByteBuffer.allocate(buf.remaining()).put(buf).flip();
                    pending.add(copy);
                    // có dữ liệu cần gửi: thử gửi ngay, phần còn lại để OP_WRITE lo
                    flush(ch, pending);
                }
                if (key.isValid() && key.isWritable()) {
                    flush(ch, pending);
                }
                if (key.isValid()) {
                    // bật OP_WRITE chỉ khi còn dữ liệu chưa gửi hết; hết thì tắt
                    int ops = pending.isEmpty() ? SelectionKey.OP_READ : SelectionKey.OP_READ | SelectionKey.OP_WRITE;
                    if (key.interestOps() != ops) key.interestOps(ops);
                }
            }
        }
    }

    static void flush(SocketChannel ch, Queue<ByteBuffer> pending) throws IOException {
        while (!pending.isEmpty()) {
            ByteBuffer head = pending.peek();
            ch.write(head);
            if (head.hasRemaining()) return;   // bộ đệm gửi đầy: dừng, đợi OP_WRITE
            pending.poll();
        }
    }
}
```

Các điểm đáng chú ý:
- `client.register(selector, OP_READ, new ArrayDeque<>())`: tham số thứ ba là *attachment*, ở đây là hàng đợi riêng của kết nối. Lấy lại bằng `key.attachment()`.
- `flush()` dừng ngay khi `head.hasRemaining()` còn đúng sau `write`: nghĩa là bộ đệm gửi đầy, và ta không quay vòng chờ.
- Dòng `key.interestOps(...)` ở cuối là chỗ bật/tắt `OP_WRITE` theo việc hàng đợi còn hay hết.

Đã chạy thử: gửi 8 MB tới server này trong khi client chủ động đọc chậm 1 giây (để bộ đệm gửi của server đầy), nhận lại đủ 8 388 608 byte, server không bị kẹt. Với bản `MiniEcho` ở mục 6, tình huống này làm cả loop quay vòng chờ.

Trong Hydrangea, chỗ tương ứng là `SocketConnection`: `setWriteInterest(boolean)` bật/tắt `OP_WRITE`, và `flush()` ghi từ hàng đợi `outbound`. Nó dừng khi `channel.write` trả về 0 ("socket buffer full") rồi bật `OP_WRITE`; hàng đợi rỗng thì tắt `OP_WRITE`. Ngoài ra còn có `pendingBytes` và high/low watermark để báo cho tầng trên tạm dừng sản xuất dữ liệu (`onWritabilityChanged`).

## 8. `selector.wakeup()` dùng để làm gì

`select()` làm thread **ngủ** cho tới khi có channel sẵn sàng. Vấn đề: nếu thread khác muốn giao việc cho loop (ví dụ gọi `connection.write(...)` từ thread bên ngoài, hay đặt một timer) trong lúc loop đang ngủ trong `select()`, thì loop không biết gì. Việc đó phải chờ tới khi có sự kiện mạng tình cờ đánh thức, có thể là rất lâu.

`selector.wakeup()` làm `select()` trả về ngay. Cách dùng là mẫu "hàng đợi task + wakeup":

```java
// thread bất kỳ
tasks.add(() -> ...);        // 1. bỏ việc vào hàng đợi an toàn thread (ConcurrentLinkedQueue)
selector.wakeup();           // 2. đánh thức loop nếu nó đang ngủ

// thread của loop
while (true) {
    selector.select();       // trả về vì có sự kiện mạng, hoặc vì wakeup()
    ... xử lý các key sẵn sàng ...
    Runnable t;
    while ((t = tasks.poll()) != null) t.run();   // 3. chạy các việc được giao
}
```

Nhờ vậy mọi thao tác trên channel vẫn xảy ra trên đúng thread của loop (không cần lock), còn thread khác chỉ giao việc.

Trong Hydrangea (`EventLoop.execute`):

```java
tasks.add(task);
if (!inEventLoop() && wakeupPending.compareAndSet(false, true)) {
    selector.wakeup();
}
```
- `!inEventLoop()`: nếu đang ở chính thread của loop thì không cần đánh thức, vì loop đang chạy rồi và sẽ lấy task ở vòng này.
- `wakeupPending` (cờ nguyên tử): nhiều thread giao việc cùng lúc thì chỉ gọi `wakeup()` một lần. `wakeup()` là một lời gọi hệ thống, khá đắt, nên không gọi thừa.
- `shutdown()` thì gọi `wakeup()` vô điều kiện, để việc tắt loop không phụ thuộc vào cờ.

Một chi tiết hay bị hiểu nhầm: nếu `wakeup()` được gọi lúc loop **chưa** đang ở `select()`, lần `select()` kế tiếp sẽ trả về ngay. Tức là lời gọi không bị mất.

## 9. Đối chiếu với Hydrangea

| Khái niệm NIO | Trong Hydrangea |
|---|---|
| Vòng `while (true) { select(); ... }` | `EventLoop.run()` (kèm hàng đợi task và heap timer) |
| `register(selector, ops, attachment)` | `EventLoop.register(channel, ops, handle)`; attachment là `SelectionHandle` |
| Việc từ thread khác + `wakeup()` | `EventLoop.execute` (hàng đợi `tasks` + cờ `wakeupPending`), mục 8 |
| Một thread, nhiều kết nối | Mỗi `EventLoop` một thread; `EventLoopGroup` có nhiều loop, gán kết nối theo vòng tròn |
| Buffer dùng chung để đọc | `EventLoop.readBuffer()`: direct 64 KB, nên `Handler.onRead` chỉ hợp lệ trong lúc gọi |
| Ghi một phần, bật/tắt `OP_WRITE` | `SocketConnection` (hàng đợi ghi, `OP_WRITE` chỉ bật khi còn dữ liệu), báo qua `Handler.onWritabilityChanged` |
| Tách message từ luồng byte | `pipeline/Stage`, `codec/`, `HttpServerStage` |
| Gọi từ thread khác | `EventLoop.execute(Runnable)` đẩy task rồi `selector.wakeup()` đánh thức vòng `select()` |

## 10. Đọc tiếp
Quay lại `READING-GUIDE.md` và đi theo mục 2: `EventLoopGroup` → `EventLoop` → `Connection` → `Handler` → `Server`.
