package io.hydrangea.kafka;

import io.hydrangea.EventLoop;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** What the client knows of the cluster (brokers, topics, partition leaders) and the connections to the brokers. Everything here runs on the loop. */
final class Cluster {

    record Node(int id, String host, int port) {
    }

    record Part(int partition, int leader, int error) {
    }

    record Topic(String name, int error, List<Part> partitions) {
    }

    final KafkaConfig config;
    final EventLoop loop;
    private final Map<Integer, Node> nodes = new HashMap<>();
    private final Map<String, Topic> topics = new HashMap<>();
    private final Map<Integer, Broker> conns = new HashMap<>();
    /** Requests waiting for a connection that is being made, in the order they were made: CompletableFuture runs the dependents of ONE future last-in-first-out, which would send them in reverse. */
    private final Map<Integer, List<CompletableFuture<Broker>>> connecting = new HashMap<>();
    private CompletableFuture<Void> refreshing;
    private final List<String> refreshTopics = new ArrayList<>();
    private boolean closed;

    Cluster(KafkaConfig config, EventLoop loop) {
        this.config = config;
        this.loop = loop;
    }

    Topic topic(String name) {
        return topics.get(name);
    }

    Node node(int id) {
        return nodes.get(id);
    }

    boolean isClosed() {
        return closed;
    }

    /** A connection to the node (existing, being made, or new). */
    CompletableFuture<Broker> broker(int nodeId) {
        Broker b = conns.get(nodeId);
        if (b != null && b.isOnline()) {
            return CompletableFuture.completedFuture(b);
        }
        List<CompletableFuture<Broker>> waiting = connecting.get(nodeId);
        if (waiting != null) {
            CompletableFuture<Broker> mine = new CompletableFuture<>();
            waiting.add(mine);
            return mine;
        }
        Node n = nodes.get(nodeId);
        if (n == null) {
            return CompletableFuture.failedFuture(new KafkaException("unknown broker " + nodeId, true));
        }
        if (closed) {
            return CompletableFuture.failedFuture(new KafkaException("the client is closed", false));
        }
        Broker nb = new Broker(n.id, n.host, n.port, config, loop);
        CompletableFuture<Broker> f = new CompletableFuture<>();
        List<CompletableFuture<Broker>> queue = new ArrayList<>();
        queue.add(f);
        connecting.put(nodeId, queue);
        nb.onLost(lost -> {
            if (conns.get(nodeId) == lost) {
                conns.remove(nodeId);
            }
        });
        nb.connect().whenComplete((ok, err) -> {
            connecting.remove(nodeId);
            if (err == null) {
                conns.put(nodeId, nb);
            }
            Throwable cause = err == null ? null : err instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null ? ce.getCause() : err;
            for (CompletableFuture<Broker> w : queue) {   // in the order the requests were made
                if (cause != null) {
                    w.completeExceptionally(cause);
                } else {
                    w.complete(nb);
                }
            }
        });
        return f;
    }

    /** A connection of its own to the node, not shared with other requests: for a request the broker answers late (a JoinGroup). The caller closes it. */
    CompletableFuture<Broker> dedicated(Node n) {
        Broker nb = new Broker(n.id, n.host, n.port, config, loop);
        return nb.connect();
    }

    CompletableFuture<Wire.Reader> request(int nodeId, int api, int version, Consumer<Wire.Writer> body) {
        return request(nodeId, api, version, 256, body);
    }

    CompletableFuture<Wire.Reader> request(int nodeId, int api, int version, int sizeHint, Consumer<Wire.Writer> body) {
        return broker(nodeId).thenCompose(b -> b.send(api, version, sizeHint, body));
    }

    /** Asks any broker: the ones known first, then the bootstrap addresses, until one answers. */
    CompletableFuture<Wire.Reader> requestAny(int api, int version, Consumer<Wire.Writer> body) {
        List<Integer> ids = new ArrayList<>(nodes.keySet());
        // the connected ones first
        ids.sort((a, b) -> Boolean.compare(conns.containsKey(b), conns.containsKey(a)));
        CompletableFuture<Wire.Reader> result = new CompletableFuture<>();
        tryNode(ids, 0, api, version, body, result, null);
        return result;
    }

    private void tryNode(List<Integer> ids, int i, int api, int version, Consumer<Wire.Writer> body, CompletableFuture<Wire.Reader> result, Throwable last) {
        if (i < ids.size()) {
            request(ids.get(i), api, version, body).whenComplete((r, e) -> {
                if (e == null) {
                    result.complete(r);
                } else {
                    tryNode(ids, i + 1, api, version, body, result, unwrap(e));
                }
            });
            return;
        }
        tryBootstrap(0, api, version, body, result, last);
    }

    private void tryBootstrap(int i, int api, int version, Consumer<Wire.Writer> body, CompletableFuture<Wire.Reader> result, Throwable last) {
        if (closed) {
            result.completeExceptionally(new KafkaException("the client is closed", false));
            return;
        }
        if (i >= config.bootstrap().size()) {
            result.completeExceptionally(last != null ? last : new KafkaException("no broker reachable", true));
            return;
        }
        String addr = config.bootstrap().get(i);
        int colon = addr.lastIndexOf(':');
        if (colon < 0) {
            tryBootstrap(i + 1, api, version, body, result, new KafkaException("bad bootstrap address " + addr + " (host:port)", false));
            return;
        }
        Broker b = new Broker(-1 - i, addr.substring(0, colon), Integer.parseInt(addr.substring(colon + 1)), config, loop);
        b.connect().thenCompose(ok -> b.send(api, version, body)).whenComplete((r, e) -> {
            if (e == null) {
                // the bootstrap connection is done with once the answer is read
                result.complete(r);
                b.close();
            } else {
                b.close();
                tryBootstrap(i + 1, api, version, body, result, unwrap(e));
            }
        });
    }

    static Throwable unwrap(Throwable e) {
        return e instanceof java.util.concurrent.CompletionException && e.getCause() != null ? e.getCause() : e;
    }

    /** Reloads the metadata of the topics (all known ones when none are named). Concurrent calls share one request; topics asked meanwhile are added to the next. */
    CompletableFuture<Void> refresh(Collection<String> wanted) {
        for (String t : wanted) {
            if (!refreshTopics.contains(t)) {
                refreshTopics.add(t);
            }
        }
        if (refreshing != null) {
            CompletableFuture<Void> current = refreshing;
            // a refresh in progress may not include the new topics: run another when it is done
            return current.handle((v, e) -> null).thenCompose(x -> doRefresh());
        }
        return doRefresh();
    }

    private CompletableFuture<Void> doRefresh() {
        if (refreshing != null) {
            return refreshing;
        }
        List<String> ask = new ArrayList<>(refreshTopics);
        refreshTopics.clear();
        CompletableFuture<Void> f = new CompletableFuture<>();
        refreshing = f;
        requestAny(Broker.Api.METADATA, 4, w -> {
            if (ask.isEmpty()) {
                w.i32(-1);
            } else {
                w.i32(ask.size());
                ask.forEach(w::str);
            }
            w.i8(0);   // do not create the topic
        }).whenComplete((r, e) -> {
            refreshing = null;
            if (e != null) {
                f.completeExceptionally(unwrap(e));
                return;
            }
            try {
                parseMetadata(r);
                f.complete(null);
            } catch (RuntimeException ex) {
                f.completeExceptionally(ex);
            }
        });
        return f;
    }

    private void parseMetadata(Wire.Reader r) {
        r.i32();   // throttle
        int nb = r.arrayLength();
        Map<Integer, Node> fresh = new HashMap<>();
        for (int i = 0; i < nb; i++) {
            int id = r.i32();
            String host = r.str();
            int port = r.i32();
            r.str();   // rack
            fresh.put(id, new Node(id, host, port));
        }
        r.str();   // cluster id
        r.i32();   // controller
        int nt = r.arrayLength();
        Map<String, Topic> got = new HashMap<>();
        for (int i = 0; i < nt; i++) {
            int err = r.i16();
            String name = r.str();
            r.i8();   // internal
            int np = r.arrayLength();
            List<Part> parts = new ArrayList<>(np);
            for (int p = 0; p < np; p++) {
                int perr = r.i16();
                int id = r.i32();
                int leader = r.i32();
                int nr = r.arrayLength();
                for (int k = 0; k < nr; k++) r.i32();
                int ni = r.arrayLength();
                for (int k = 0; k < ni; k++) r.i32();
                parts.add(new Part(id, leader, perr));
            }
            parts.sort((a, b) -> Integer.compare(a.partition, b.partition));
            got.put(name, new Topic(name, err, parts));
        }
        nodes.clear();
        nodes.putAll(fresh);
        topics.putAll(got);
    }

    /** After a failure that says the connection is gone, drop it so that the next request makes a new one. */
    void forgetConnectionIfLost(int nodeId, Throwable cause) {
        Broker b = conns.get(nodeId);
        if (b != null && !b.isOnline()) {
            conns.remove(nodeId);
        }
    }

    void addNode(Node n) {
        nodes.putIfAbsent(n.id, n);
    }

    void forgetConnection(int nodeId) {
        Broker b = conns.remove(nodeId);
        if (b != null) {
            b.close();
        }
    }

    void close() {
        closed = true;
        for (Broker b : new ArrayList<>(conns.values())) {
            b.close();
        }
        conns.clear();
    }
}
