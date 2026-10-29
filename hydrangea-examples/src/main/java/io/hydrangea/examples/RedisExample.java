package io.hydrangea.examples;

import io.hydrangea.EventLoopGroup;
import io.hydrangea.redis.RedisClient;
import io.hydrangea.redis.RedisConfig;
import io.hydrangea.redis.RedisException;
import io.hydrangea.redis.Script;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Redis: one connection shared by every thread, commands pipelined automatically, a transaction, a script, pub/sub and an error.
 * <pre>
 * docker run -d --name redis -p 6379:6379 redis:7
 * java -cp target/classes io.hydrangea.examples.RedisExample [host] [port]
 * </pre>
 */
public final class RedisExample {

    public static void main(String[] args) throws Exception {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 6379;
        try (EventLoopGroup group = new EventLoopGroup(1)) {
            RedisClient redis = RedisClient.connect(group, RedisConfig.of(host, port)).get(10, TimeUnit.SECONDS);
            System.out.println("connected, RESP" + redis.protocol() + ", Redis " + redis.serverInfo().get("version"));

            redis.set("example:greeting", "héllo").get();
            System.out.println("get: " + redis.get("example:greeting").get());

            // commands issued together travel together: 10 000 increments without waiting for each reply
            List<CompletableFuture<Long>> pending = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                pending.add(redis.incr("example:counter"));
            }
            System.out.println("last incr: " + pending.get(pending.size() - 1).get());

            System.out.println("transaction: " + redis.transaction(new Object[] {"SET", "example:a", "1"}, new Object[] {"INCR", "example:a"}).get());
            System.out.println("script: " + redis.eval(new Script("return redis.call('get', KEYS[1])"), List.of("example:a"), List.of()).get());

            RedisClient subscriber = redis.duplicate().get(10, TimeUnit.SECONDS);
            CompletableFuture<String> heard = new CompletableFuture<>();
            subscriber.subscribe("example:news", (channel, pattern, payload) -> heard.complete(new String(payload))).get();
            redis.publish("example:news", "hello subscribers").get();
            System.out.println("pub/sub: " + heard.get(5, TimeUnit.SECONDS));

            try {
                redis.lpush("example:greeting", "x").get();
            } catch (Exception e) {
                if (e.getCause() instanceof RedisException re) {
                    System.out.println("server error " + re.code() + " (fromServer=" + re.fromServer() + ")");
                }
            }
            redis.del("example:greeting", "example:counter", "example:a").get();
            subscriber.close();
            redis.close();
        }
    }
}
