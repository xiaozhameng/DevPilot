package com.devpilot.gateway.controller;

import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.service.GatewayEventBus;
import com.devpilot.gateway.service.KernelClient;
import com.devpilot.gateway.service.TaskDispatcher;
import com.devpilot.gateway.service.TaskStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
/**
 * 面向客户端的任务接口。
 *
 * <h2>这一层做什么、不做什么</h2>
 *
 * <p>做三件事：参数校验、把请求交给 service、把结果整理成"能直接渲染"的形状。
 * 不做的：任何业务判断。状态怎么变、什么时候重派、审批怎么流转 ——
 * 全在 {@link TaskDispatcher} 与 {@link com.devpilot.gateway.service.Reaper} 里。
 *
 * <p>为什么会强调这条界线：controller 是最容易被写胖的地方，
 * 因为"顺手加个 if"太方便了。一旦 controller 里有了状态判断，
 * 同一套规则就会同时存在于 controller 和 service 两处，
 * 而它们迟早会不一致 —— 这种不一致不会报错，只会让行为变得难以预测。
 */
@RestController
@RequestMapping("/api")
public class TaskController {

    private final TaskStore store;
    private final TaskDispatcher dispatcher;
    private final GatewayEventBus bus;
    private final KernelClient kernel;
    private final ObjectMapper json = new ObjectMapper();

    public TaskController(TaskStore store, TaskDispatcher dispatcher,
                          GatewayEventBus bus, KernelClient kernel) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.bus = bus;
        this.kernel = kernel;
    }

    // -------------------- 请求体 --------------------

    public record CreateRequest(String requirement, Map<String, Object> options) {}

    public record ApproveRequest(boolean approved, String comment) {}

    public record ResumeRequest(boolean autoApprove) {}

    // ============================================================
    // 提交 / 列表 / 详情
    // ============================================================

    /**
     * 提交一个交付任务。
     *
     * <p>幂等键走 HTTP 头 {@code Idempotency-Key} 而不是请求体。
     * 原因：它是**传输层语义**（"这次请求和上一次是同一个"），
     * 不是业务参数。放在头里，客户端库可以统一注入（比如一个拦截器
     * 给所有 POST 自动加），业务代码不用每处都想着它。
     *
     * <p>返回值里带 {@code deduplicated}：调用方据此知道"这是复用"还是"新建"。
     * 不带的话，客户端重试后看到两个一样的任务 id 也不知道是自己重试命中，
     * 还是系统真的建了两个。
     */
    @PostMapping("/tasks")
    public Mono<Map<String, Object>> create(
            @RequestBody CreateRequest req,
            @RequestHeader(value = "Idempotency-Key", required = false) String idemKey) {

        if (req.requirement() == null || req.requirement().isBlank()) {
            throw new IllegalArgumentException("requirement 不能为空");
        }

        return Mono.fromSupplier(() -> {
            TaskStore.SubmitOutcome outcome = store.submit(
                    req.requirement(), idemKey, req.options());
            TaskRecord rec = outcome.record();

            if (!outcome.deduplicated()) {
                // 只在**新建**时派发。命中幂等键说明内核那边已经在跑了，
                // 再派一次会白白多创建一条执行链（虽然内核幂等会兜住，
                // 但没必要制造无意义的请求）。
                dispatcher.dispatch(rec.getTaskId());
            }

            Map<String, Object> out = rec.toView();
            out.put("deduplicated", outcome.deduplicated());
            return out;
        });
    }

    @GetMapping("/tasks")
    public Map<String, Object> list(@RequestParam(defaultValue = "50") int limit) {
        List<Map<String, Object>> items = store.list(Math.min(limit, 200)).stream()
                .map(TaskRecord::toView)
                .toList();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("items", items);
        out.put("countByState", store.countByState());
        return out;
    }

    /**
     * 任务详情 = 网关记录 + 内核侧详情。
     *
     * <p>为什么要合？因为两边各有对方没有的东西：
     * <ul>
     *   <li>网关知道 attempts、租约、是否失联过 —— 这是控制面信息。</li>
     *   <li>内核知道走到哪个节点、检查点是什么、沙箱里有哪些文件 —— 这是执行面信息。</li>
     * </ul>
     * 客户端要的是"一条任务的完整画像"，不该让它自己拼两个接口 ——
     * 那样每个客户端都要重新理解一次两边的字段。
     *
     * <p>内核不可用时**不报错**：网关记录照常返回，只是没有执行面细节。
     * 这是刻意的降级 —— 内核挂了不代表网关也查不了任务，
     * 而"能查到任务但看不到沙箱文件"远好于"整个详情页 500"。
     */
    @GetMapping("/tasks/{taskId}")
    public Mono<Map<String, Object>> detail(@PathVariable String taskId) {
        TaskRecord rec = store.require(taskId);
        Map<String, Object> view = rec.toView();
        view.put("ageSeconds", Duration.between(rec.getCreatedAt(),
                java.time.Instant.now()).toSeconds());

        if (rec.getKernelTaskId() == null) {
            view.put("kernel", null);
            return Mono.just(view);
        }
        return kernel.getTask(rec.getKernelTaskId())
                .map(k -> {
                    view.put("kernel", k);
                    return view;
                })
                .onErrorResume(e -> {
                    Map<String, Object> degraded = new LinkedHashMap<>(view);
                    degraded.put("kernel", null);
                    degraded.put("kernelError", e.getClass().getSimpleName());
                    return Mono.just(degraded);
                });
    }

    // ============================================================
    // 事件流（SSE）
    // ============================================================

    /**
     * 订阅任务事件。
     *
     * <p>{@code since} 是断线重连的关键：客户端记住收到最后一个 seq，
     * 重连时带上，就不会重复渲染或漏掉中间事件。
     *
     * <h2>为什么返回 {@code Flux<ServerSentEvent<String>>} 而不是拼好的字符串</h2>
     *
     * 因为 {@code produces = text/event-stream} 的端点，如果返回的是
     * {@code Flux<String>}，Spring 会把每个字符串当成**data 段**再包一层，
     * 于是我们手工拼的 {@code "event: x\ndata: {...}\n\n"} 变成了
     * {@code "data:event: x\ndata: {...}\n\n"} —— 前端收到的是被转义的乱码。
     *
     * <p>交给 {@link ServerSentEvent} 来表达"事件名 + 数据"这两件事，
     * 格式化由框架负责，就不会出现"我自己拼了一遍、框架又拼了一遍"。
     * 这个 bug 在服务端日志里完全看不出来（状态码 200，流也正常推送），
     * 只有真正用浏览器连一次才会暴露 —— 所以它值得在这里被记下来。
     *
     * <p>三个头都不能省：
     * <ul>
     *   <li>{@code text/event-stream} —— 让浏览器认出这是 SSE，走 EventSource 的解析。</li>
     *   <li>{@code X-Accel-Buffering: no} —— 关掉中间层（nginx 等）的响应缓冲。
     *       不关的话事件会被攒成一坨，表现是"进度一直不动，最后一次性全出来"。</li>
     *   <li>{@code Cache-Control: no-cache} —— SSE 是实时流，缓存没有任何意义。</li>
     * </ul>
     */
    @GetMapping(value = "/tasks/{taskId}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> events(@PathVariable String taskId,
                                                @RequestParam(defaultValue = "0") long since) {
        store.require(taskId);   // 任务不存在时直接 404，而不是给一条永远没事件的空流

        // cache() 很关键：下面的心跳需要独立订阅一次这个流来判断"任务结束了没有"，
        // 没有它就会变成两个上游订阅，事件被消费两遍。
        Flux<ServerSentEvent<String>> events = bus.stream(taskId, since)
                .map(ev -> ServerSentEvent.<String>builder()
                        .id(String.valueOf(ev.seq()))
                        .event(ev.type())
                        .data(toJson(Map.of(
                                "seq", ev.seq(),
                                "type", ev.type(),
                                "ts", ev.ts(),
                                "data", ev.data())))
                        .build())
                .cache();

        // 心跳。
        //
        // 一个任务可能卡在人工审批点上几十分钟 —— 这期间**一条数据事件都没有**。
        // 而中间的代理、负载均衡、甚至浏览器自己，都会把"长时间没有字节流动"的
        // 连接当成死连接掐掉。表现是：SSE 静默断开，界面上的进度停住不动，
        // 用户以为任务挂了。
        //
        // 所以周期性地发一个空事件，告诉链路上所有人"这条连接还活着"。
        // 心跳必须**随任务一起结束**（takeUntilOther），否则任务完成后
        // 连接会被无限期地挂着 —— 而浏览器对 SSE 有连接数上限，
        // 泄漏几条之后整个页面就再也连不上任何流了。
        Flux<ServerSentEvent<String>> heartbeat = Flux.interval(HEARTBEAT)
                .map(i -> ServerSentEvent.<String>builder()
                        .event("gateway.heartbeat")
                        .data(toJson(Map.of("ts", System.currentTimeMillis())))
                        .build())
                .takeUntilOther(events.ignoreElements());

        return Flux.merge(events, heartbeat);
    }

    /** 心跳间隔。太小会白费带宽，太大则撑不住一些默认 60 秒超时的中间件。 */
    private static final Duration HEARTBEAT = Duration.ofSeconds(20);

    private String toJson(Object o) {
        try {
            return json.writeValueAsString(o);
        } catch (Exception e) {
            return "{}";
        }
    }

    // ============================================================
    // 人工审批 / 取消 / 恢复
    // ============================================================

    /**
     * 人工审批。
     *
     * <p>网关**只转发**，不改自己的状态 —— 状态由内核的
     * {@code approval.resolved} 事件驱动。原因见 TaskDispatcher.approve：
     * 审批的真相在那个阻塞等待的工作线程上，谁掌握真相谁写状态。
     */
    @PostMapping("/tasks/{taskId}/approve")
    public Map<String, Object> approve(@PathVariable String taskId,
                                       @RequestBody ApproveRequest req) {
        dispatcher.approve(taskId, req.approved(),
                req.comment() == null ? "" : req.comment());
        return Map.of("ok", true, "taskId", taskId, "approved", req.approved(),
                "note", "已转发给内核，状态将在内核确认后更新");
    }

    @PostMapping("/tasks/{taskId}/cancel")
    public Map<String, Object> cancel(@PathVariable String taskId) {
        dispatcher.cancel(taskId);
        return Map.of("ok", true, "taskId", taskId, "state", "cancelled");
    }

    @PostMapping("/tasks/{taskId}/resume")
    public Mono<Map<String, Object>> resume(@PathVariable String taskId,
                                            @RequestBody(required = false) ResumeRequest req) {
        boolean auto = req != null && req.autoApprove();
        return dispatcher.resume(taskId, auto)
                .map(rec -> {
                    Map<String, Object> out = new LinkedHashMap<>(rec.toView());
                    out.put("resumedFrom", taskId);
                    return out;
                });
    }

    // ============================================================
    // 产物
    // ============================================================

    @GetMapping("/tasks/{taskId}/artifacts")
    public Mono<ResponseEntity<JsonNode>> artifacts(@PathVariable String taskId) {
        TaskRecord rec = store.require(taskId);
        if (rec.getKernelTaskId() == null) {
            // 还没派发到内核：产物必然是空的。返回一个结构一致的响应
            // （空数组），而不是 404 —— 客户端就不用为"还没开始"单独写一条分支。
            var empty = json.createObjectNode();
            empty.set("items", json.createArrayNode());
            return Mono.just(ResponseEntity.ok(empty));
        }
        return kernel.artifacts(rec.getKernelTaskId())
                .map(ResponseEntity::ok);
    }

    @GetMapping(value = "/tasks/{taskId}/artifacts/raw", produces = "text/plain;charset=UTF-8")
    public Mono<ResponseEntity<String>> artifactRaw(@PathVariable String taskId,
                                                    @RequestParam String path) {
        TaskRecord rec = store.require(taskId);
        if (rec.getKernelTaskId() == null) {
            return Mono.just(ResponseEntity.notFound().build());
        }
        // 路径越界检查在**内核侧**（Sandbox.read 做 resolve + 前缀比对）。
        // 网关不重复实现一遍：两份实现迟早会不一致，
        // 而不一致的那一份就是漏洞。
        return kernel.artifactRaw(rec.getKernelTaskId(), path)
                .map(ResponseEntity::ok);
    }
}
