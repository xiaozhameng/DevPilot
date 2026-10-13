package com.devpilot.gateway.service;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 网关侧的事件总线：**面向客户端**的那一条流。
 *
 * <h2>为什么不让网页直接连内核的 SSE</h2>
 *
 * 因为那会把内核暴露给浏览器：内核的地址、鉴权 token、以及"内核必须可被直连"
 * 这个网络前提，全都要成立。而内核自己声明了"我只接受内网调用"。
 *
 * 更实际的问题是**两套事件源**：任务的状态变化不只来自内核。
 * 「网关发现租约过期、判定失联」这个动作发生在网关内部，
 * 内核根本不知道，因此内核的事件流里没有这条。如果网页只听内核，
 * 它就永远看不到"超时重派"这个关键环节 —— 而那恰恰是这个项目要展示的东西。
 *
 * 所以网关必须有自己的事件流：把内核事件**翻译并叠加**网关自身的判断后，
 * 再推给客户端。
 *
 * <h2>实现：每条任务一个 replay 槽位</h2>
 *
 * {@link Sinks.Many} 用 {@code replay().all()} 模式，它同时满足两个要求：
 * <ul>
 *   <li><b>回放</b>：后连上来的客户端（比如用户中途刷新页面）能拿到这之前的全部事件，
 *       不会看到"从一半开始"的进度。</li>
 *   <li><b>实时</b>：已经在流的客户端继续收到后续事件。</li>
 * </ul>
 *
 * <p>终态时 {@code tryEmitComplete()}：让客户端的连接**主动结束**。
 * 不结束的话，浏览器会一直挂着一个永不关闭的连接，服务端资源也收不回来 ——
 * 这是 SSE 最常见的坑。
 *
 * <h2>诚实的边界：这是内存实现，重启即丢</h2>
 *
 * 事件的权威历史在内核的 SQLite 里（{@code events} 表）。网关这份是**转发缓冲**。
 * 网关重启后，新连上来的客户端会拿到空历史 —— 此时正确的做法是回源内核
 * （{@code GET /v1/tasks/{id}/events} 会先回放持久化事件再追实时），
 * 但为了不把网关变成长连接代理工厂，这里先不自动回源，
 * 而是在 {@link #stream} 里给出空历史的明确信号。
 * 这个取舍写在注释里，是因为它是**已知缺口**，不是没想到。
 */
@Component
public class GatewayEventBus {

    private static final Logger log = LoggerFactory.getLogger(GatewayEventBus.class);

    /** 客户端看到的事件。字段刻意与内核事件对齐，前端一个渲染函数吃两种源。 */
    public record GatewayEvent(long seq, String type, Object data, long ts) {
        public String toSse() {
            // data 用 Jackson 序列化由 controller 完成；这里只负责 event: 行。
            return "event: " + type + "\n";
        }
    }

    private static final String TASK_DONE = "task.done";
    private static final String TASK_FAILED = "task.failed";
    private static final String TASK_STATUS = "task.status";

    private final Map<String, Sinks.Many<GatewayEvent>> sinks = new ConcurrentHashMap<>();
    private final Map<String, Long> seqs = new ConcurrentHashMap<>();
    private final Map<String, Long> lastActivity = new ConcurrentHashMap<>();

    /**
     * 显式历史。
     *
     * <p>{@code replay().all()} 本身已经能回放，为什么还要单独存一份？
     * 因为**回放只能通过订阅拿到**。有两个地方需要一个"不订阅也能读"的视图：
     * <ul>
     *   <li>测试与排查：看一眼某任务到目前为止发过哪些事件，不该为此建立一条连接；</li>
     *   <li>页面初次加载：先一次性拉历史渲染完，再开 SSE 追增量 ——
     *       两条路分开，比"开一条流边收边渲染"更可控。</li>
     * </ul>
     *
     * <p>两份数据的**写入顺序是先历史后 sink**，所以不存在"sink 里有、历史里没有"的状态；
     * 反过来（历史有、sink 里没有）是允许的，因为那只是说明当时还没有订阅者。
     */
    private final Map<String, List<GatewayEvent>> history = new ConcurrentHashMap<>();

    /** 每条任务保留的事件上限。超出就丢最旧的 —— 这是转发缓冲，不是审计存储。 */
    private static final int MAX_HISTORY = 2000;

    /**
     * 发一条事件（客户端可见）。
     *
     * <p>线程安全：会被内核事件流的订阅线程、Reaper 的调度线程、
     * HTTP 请求线程同时调用，所以状态都放在 ConcurrentHashMap + 原子自增里。
     */
    public void publish(String taskId, String type, Object data) {
        long seq = seqs.merge(taskId, 1L, Long::sum);
        long ts = System.currentTimeMillis();
        lastActivity.put(taskId, ts);
        GatewayEvent ev = new GatewayEvent(seq, type, data, ts);

        // 注意：这里刻意不把列表命名为 log —— 那会遮住上面的 Logger 变量，
        // 编译期表现是"找不到符号 log.debug"，排查时容易看错方向。
        List<GatewayEvent> hist = history.computeIfAbsent(taskId,
                k -> new CopyOnWriteArrayList<>());
        hist.add(ev);
        // 只留最近 MAX_HISTORY 条。不设上限的话，一个长跑任务的事件会一直堆在内存里，
        // 而网关是**长期常驻**的进程，这种"只增不减"是内存泄漏的标准形态。
        if (hist.size() > MAX_HISTORY) {
            hist.subList(0, hist.size() - MAX_HISTORY).clear();
        }

        Sinks.Many<GatewayEvent> sink = sinks.get(taskId);
        if (sink == null) {
            // 还没人订阅也要留痕：说明"事件产生得比订阅早"是常态，
            // 所以 sink 必须在第一次 publish 时就建好，否则先发生的事件全丢。
            sink = newSink();
            Sinks.Many<GatewayEvent> prev = sinks.putIfAbsent(taskId, sink);
            if (prev != null) {
                sink = prev;
            }
        }
        Sinks.EmitResult r = sink.tryEmitNext(ev);
        if (r.isFailure()) {
            log.debug("事件投递未成功（任务 {}，类型 {}）：{}", taskId, type, r);
        }

        if (TASK_DONE.equals(type) || TASK_FAILED.equals(type)) {
            // 终态：关闭槽位，让客户端连接结束。
            sink.tryEmitComplete();
            sinks.remove(taskId, sink);
        }
    }

    /**
     * 订阅某任务的事件流。
     *
     * <p>{@code since} 用于断线重连：客户端带上最后一次收到的 seq，
     * 这里只把它之后的事件给它。没有这个参数，重连时要么重放全部（界面重复渲染），
     * 要么从当前开始（中间那段永远看不到）。
     */
    public Flux<GatewayEvent> stream(String taskId, long since) {
        // 终态任务：直接回放历史，然后结束。
        // 不能落到下面的 computeIfAbsent —— 终态时槽位已经被移除，那样会**新建一个空槽**，
        // 迟到的订阅者（用户在任务跑完之后才打开详情页，这是最常见的情况）
        // 就会盯着一条永远没有事件的流，界面显示"等待事件…"，
        // 而历史其实一直躺在 history 里。实测踩过这个坑。
        if (isTerminal(taskId)) {
            return Flux.fromIterable(history(taskId, since));
        }

        Sinks.Many<GatewayEvent> sink = sinks.computeIfAbsent(taskId, k -> newSink());
        // 新建的槽里没有历史（历史在 history 列表里），先把历史补齐再接直播。
        // 用 distinct(seq) 去重：已存在的 replay 槽会把同一段事件再放一遍。
        Flux<GatewayEvent> past = Flux.fromIterable(history(taskId, since));
        return past.concatWith(sink.asFlux())
                .filter(e -> e.seq() > since)
                .distinct(GatewayEvent::seq)
                .timeout(Duration.ofMinutes(30))
                .onErrorResume(e -> {
                    // 30 分钟没有任何事件（也没有心跳）说明这条流已经名存实亡，
                    // 主动结束比挂着好 —— 客户端会重连并带 since 续传。
                    log.debug("事件流空闲超时，主动关闭（任务 {}）", taskId);
                    return Flux.empty();
                });
    }

    private Sinks.Many<GatewayEvent> newSink() {
        // replay().all() + 多播：任意多个订阅者各自拿到完整历史，互不影响。
        return Sinks.many().replay().all();
    }

    /**
     * 不订阅、直接读历史（供页面初次加载与测试使用）。
     *
     * <p>{@code since} 语义与 {@link #stream} 一致：只返回 seq 更大的那些。
     */
    public List<GatewayEvent> history(String taskId, long since) {
        List<GatewayEvent> log = history.get(taskId);
        if (log == null) {
            return List.of();
        }
        return log.stream().filter(e -> e.seq() > since).toList();
    }

    /** 该任务是否已经到终态（终态后无需再等）。 */
    public boolean isTerminal(String taskId) {
        return !sinks.containsKey(taskId) && seqs.containsKey(taskId);
    }

    public long lastSeq(String taskId) {
        return seqs.getOrDefault(taskId, 0L);
    }

    /** 供测试与调试：丢掉一个任务的事件缓冲。 */
    public void forget(String taskId) {
        Sinks.Many<GatewayEvent> s = sinks.remove(taskId);
        if (s != null) {
            s.tryEmitComplete();
        }
        seqs.remove(taskId);
        history.remove(taskId);
        lastActivity.remove(taskId);
    }

    public int trackedTasks() {
        return seqs.size();
    }

    @PreDestroy
    void shutdown() {
        sinks.values().forEach(Sinks.Many::tryEmitComplete);
        sinks.clear();
        history.clear();
    }
}
