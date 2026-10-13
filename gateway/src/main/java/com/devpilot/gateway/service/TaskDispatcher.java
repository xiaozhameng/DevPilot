package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务派发器：把网关受理的任务交给内核，并把内核的进展**翻译**回网关的状态机。
 *
 * <h2>这个类是整个网关最值得讲的地方</h2>
 *
 * 它同时处理三个问题，而这三个问题恰好是"分布式任务调度"的全部难点：
 *
 * <ol>
 *   <li><b>派发</b>：调内核创建任务，拿到内核 id，写租约。失败要区分
 *       "网络没到"（可重试）和"内核拒绝"（不可重试）。</li>
 *   <li><b>跟踪</b>：内核是通过 SSE 推事件的，网关要一直听着，
 *       把 {@code node.started}、{@code approval.required} 这类**执行面事件**
 *       翻译成 {@code RUNNING}、{@code WAITING_APPROVAL} 这类**控制面状态**。</li>
 *   <li><b>失联兜底</b>：SSE 断了怎么办？—— 这就是租约存在的原因。
 *       订阅线程死了不要紧，租约到期会有 {@link Reaper} 来收尸。</li>
 * </ol>
 *
 * <h2>为什么"跟踪"要有独立线程/订阅，而不是轮询</h2>
 *
 * 轮询的代价不是性能，是**语义**：轮询只能看到"现在的状态"，
 * 看不到"经过了哪些状态"。而这里要展示的恰恰是流转过程
 * （尤其是回环：覆盖率不达标打回编码）。事件流天然带顺序和时间，
 * 轮询把这段信息压平成了快照。
 *
 * <p>所以：**实时性靠订阅，正确性靠租约。** 两者职责不同，不能互相替代 ——
 * 只靠订阅，订阅线程崩了就永远没人知道；只靠租约，进度就是分钟级粒度的。
 */
@Component
public class TaskDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TaskDispatcher.class);

    /**
     * 内核事件类型 → 网关状态。
     *
     * <p>这张映射表是**契约的一部分**：内核的事件名改了，这里必须跟着改，
     * 否则状态机会停留在中间态（表现为"任务一直在跑，其实是审批在等人"）。
     * 之所以不用 if-else 串：映射表可以一眼看全，也能被测试逐条覆盖。
     */
    private static final Map<String, TaskState> EVENT_TO_STATE = Map.of(
            "node.started", TaskState.RUNNING,
            "approval.required", TaskState.WAITING_APPROVAL,
            "approval.resolved", TaskState.RUNNING,
            "task.done", TaskState.SUCCEEDED,
            "task.failed", TaskState.FAILED
    );

    private final TaskStore store;
    private final KernelClient kernel;
    private final GatewayEventBus bus;
    private final GatewayProperties props;
    private final AuditLog audit;

    /**
     * 每条任务一条订阅。
     *
     * <p>保留引用才能在取消时主动断开 —— 否则流会一直挂到内核那边超时。
     *
     * <p>用 {@link Track} 包一层而不是直接存 {@code Disposable}，是为了解决
     * "订阅对象在回调里才拿得到"这个先有鸡还是先有蛋的问题：
     * 完成回调里要清理自己，但回调是在 {@code subscribe()} **返回之前**
     * 就可能被调用的。用一个可写的持有者把这条引用先建好，回调就能安全地
     * 按"值相等"来清理，而不会误删后来替换上去的新订阅。
     */
    private static final class Track {
        volatile Disposable sub;
    }

    private final Map<String, Track> subscriptions = new ConcurrentHashMap<>();

    public TaskDispatcher(TaskStore store, KernelClient kernel, GatewayEventBus bus,
                          GatewayProperties props, AuditLog audit) {
        this.store = store;
        this.kernel = kernel;
        this.bus = bus;
        this.props = props;
        this.audit = audit;
    }

    // ============================================================
    // 派发
    // ============================================================

    /**
     * 派发一个任务到内核。
     *
     * <p>幂等键用的是**网关自己的任务 id**。这个选择是有意的：
     * 网关和内核各自都做幂等，但如果两边用不同的键，
     * "网关重试一次派发"这件事在内核眼里就是两个不同的请求 ——
     * 内核会老老实实创建两个任务，一起跑，门禁各跑各的，钱翻倍。
     *
     * <p>用同一个键之后，链路变成：网关重试 → 内核幂等命中 → 返回原任务。
     * 整条链上"同一个逻辑任务"只有一个标识。
     */
    public void dispatch(String taskId) {
        TaskRecord rec = store.require(taskId);
        String idemKey = rec.getTaskId();

        kernel.submit(rec.getRequirement(), rec.getOptions(), idemKey)
                .subscribe(
                        task -> onDispatched(taskId, task),
                        err -> onDispatchError(taskId, err));
    }

    private void onDispatched(String taskId, KernelClient.KernelTask task) {
        try {
            if (task.deduplicated()) {
                // 内核说"这个任务我之前就受理过"。说明我们上次派发其实成功了，
                // 只是没收到回执。此时**不能**再加一次 attempts ——
                // 否则重派上限会被虚耗，任务还没真正重跑就被判"次数用尽"。
                log.info("内核幂等命中：网关任务 {} 复用内核任务 {}", taskId, task.taskId());
                store.renewLease(taskId, props.getLease().durationMillis());
                watch(taskId, task.taskId());
                return;
            }
            store.markDispatched(taskId, task.taskId(), props.getLease().durationMillis());
            audit.record("dispatch", taskId, Map.of(
                    "kernelTaskId", task.taskId(),
                    "attempt", store.require(taskId).getAttempts()));
            bus.publish(taskId, "gateway.dispatched", Map.of(
                    "kernelTaskId", task.taskId(),
                    "attempt", store.require(taskId).getAttempts()));
            watch(taskId, task.taskId());
        } catch (Exception e) {
            // 走到这里通常是状态机拒绝了（比如用户刚刚取消了任务）。
            // 这不是系统故障，是正常的并发：记一笔日志即可，不要报错。
            log.warn("派发回执处理失败（任务 {}）：{}", taskId, e.toString());
        }
    }

    private void onDispatchError(String taskId, Throwable err) {
        String reason = err.getClass().getSimpleName() + ": " + err.getMessage();
        log.warn("派发失败（任务 {}）：{}", taskId, reason);
        store.markDispatchFailed(taskId, reason);
        audit.record("dispatch_failed", taskId, Map.of("error", reason));
        bus.publish(taskId, "gateway.dispatch_failed", Map.of("error", reason));
        // 派发失败也发终态事件，让客户端的 SSE 能结束 ——
        // 否则用户看到一个永远"转圈"的任务，不知道它其实已经死了。
        bus.publish(taskId, "task.failed", Map.of("error", reason));
    }

    // ============================================================
    // 跟踪：订阅内核事件流，翻译成网关状态
    // ============================================================

    /**
     * 开始跟踪一个内核任务的事件流。
     *
     * <h2>为什么不写成 {@code subscriptions.computeIfAbsent(id, k -> ...subscribe()...)}</h2>
     *
     * 因为那是一个**必然踩雷**的写法，而且踩得很隐蔽。
     * {@code computeIfAbsent} 会持有 ConcurrentHashMap 的分桶锁执行映射函数；
     * 而映射函数里 {@code subscribe()} 一旦**同步完成**（比如内核任务已到终态，
     * 回放完就结束），完成回调立刻在同一个线程上执行 ——
     * 它去 {@code subscriptions.remove(id)}，于是对同一个桶发生了嵌套修改，
     * ConcurrentHashMap 直接抛 {@code IllegalStateException: Recursive update}。
     *
     * <p>更麻烦的是这个异常会被 Reactor 的 {@code onErrorDropped} 吞掉：
     * 测试全绿、日志里只有一行 ERROR，而实际后果是**订阅表里留下了一条死引用**，
     * `dispose` 之后再也不会被清理。任务一多就是内存泄漏，且没有任何任务失败。
     *
     * <p>修法是两件事分开：先把槽位建好（映射函数里只做 {@code new Track()}，
     * 无回调、无嵌套），再在锁外做订阅。清理时用 {@code remove(key, value)}
     * 按值匹配，避免删掉后来替换上去的新订阅。
     */
    private void watch(String taskId, String kernelTaskId) {
        Track track = subscriptions.computeIfAbsent(taskId, k -> new Track());
        Disposable existing = track.sub;
        if (existing != null && !existing.isDisposed()) {
            return;                    // 已经在跟踪了，不重复订阅
        }

        log.debug("开始跟踪任务 {} → 内核 {}", taskId, kernelTaskId);
        Disposable d = kernel.events(kernelTaskId, 0)
                .subscribe(
                        ev -> onKernelEvent(taskId, ev),
                        err -> {
                            log.warn("跟踪任务 {} 的事件流异常终止：{}", taskId, err.toString());
                            clear(taskId, track);
                        },
                        () -> {
                            log.debug("任务 {} 的内核事件流正常结束", taskId);
                            clear(taskId, track);
                        });
        track.sub = d;
        if (d.isDisposed()) {
            // 同步完成：subscribe() 返回时流已经结束了，槽位要立刻清掉。
            clear(taskId, track);
        }
    }

    /** 按值清理：只有当槽位里还是这条订阅时才删，避免误删新订阅。 */
    private void clear(String taskId, Track track) {
        subscriptions.remove(taskId, track);
    }

    /**
     * 同步一次内核的真实状态（对账）。
     *
     * <p>用途：重派之前先问一句"它是不是其实已经跑完了"。
     * 网络丢包、事件流断开都会让网关错过终态回执，而任务其实成功了 ——
     * 这时候重派就是**白跑一遍**（而且用户会看到一次莫名其妙的重复执行）。
     * 先对账再重派，能把这部分浪费消掉。
     *
     * @return 是否已经对账出一个终态（true 表示不用重派了）
     */
    public boolean reconcile(String taskId) {
        TaskRecord rec = store.find(taskId);
        if (rec == null || rec.getKernelTaskId() == null) {
            return false;
        }
        try {
            JsonNode node = kernel.getTask(rec.getKernelTaskId()).block();
            if (node == null) {
                return false;
            }
            String status = node.path("status").asText("");
            TaskState target = switch (status) {
                case "succeeded" -> TaskState.SUCCEEDED;
                case "failed" -> TaskState.FAILED;
                case "cancelled" -> TaskState.CANCELLED;
                default -> null;
            };
            if (target == null) {
                return false;
            }
            store.markResult(taskId, target, verdictOf(node),
                    "由重派对账确认（内核侧状态：" + status + "）");
            bus.publish(taskId, "gateway.reconciled", Map.of("kernelStatus", status));
            return true;
        } catch (Exception e) {
            log.warn("对账失败（任务 {}）：{}", taskId, e.toString());
            return false;
        }
    }

    /**
     * 重派。
     *
     * <p>策略是**两段**：
     * <ol>
     *   <li>优先走内核的 {@code /resume} —— 它从检查点重建一个执行，
     *       是"内核重启过"之后唯一能真正把活接着干完的方式。
     *       注意 resume 会**新建**一个内核任务，所以要用 {@link #adopt} 认领它，
     *       而不是再 submit 一次。</li>
     *   <li>resume 失败（没有检查点、或内核根本不可达）时退回**重新提交**，
     *       带同一个幂等键。</li>
     * </ol>
     *
     * <h2>诚实说明这条路走不通的地方</h2>
     *
     * 如果内核"还记得"这个任务（幂等命中）但它已经不再执行（进程重启、
     * 没有人去跑它），那重新提交会是**空操作**，任务就会在"超时→重派→空操作"
     * 之间循环，attempts 也涨不上去（因为我们不把"内核说复用"算作一次执行，
     * 这是对的）。
     *
     * <p>要彻底解决，需要**内核在启动时扫描未完成任务并续跑** —— 那是执行面
     * 的职责，不是网关能单方面补齐的。这里不假装解决它，而是明确记下来：
     * 一个诚实的已知缺口，比一个看起来完整、实际在特定时刻会静默丢任务的实现更安全。
     *
     * @param wasWaitingApproval 失联时是否正挂在审批点上。必须由调用方在
     *                           状态被改成 TIMEOUT **之前**判断并传进来 ——
     *                           改了之后就看不出来它原本在等审批了，
     *                           恢复时会让内核又停下来等人（而可能根本没人会来点）。
     */
    public void redispatch(String taskId, boolean wasWaitingApproval) {
        TaskRecord rec = store.find(taskId);
        if (rec == null) {
            return;
        }
        String kernelTaskId = rec.getKernelTaskId();

        if (kernelTaskId == null) {
            dispatch(taskId);
            return;
        }

        // 先对账：也许它其实已经跑完了，只是回执丢了。
        if (reconcile(taskId)) {
            log.info("任务 {} 对账后已是终态，无需重派", taskId);
            dispose(taskId);
            return;
        }

        kernel.resume(kernelTaskId, wasWaitingApproval).subscribe(
                resp -> {
                    String newKernelId = resp.path("task_id").asText(kernelTaskId);
                    log.info("任务 {} 已从检查点重建为内核任务 {}", taskId, newKernelId);
                    adopt(taskId, newKernelId);
                },
                err -> {
                    log.warn("断点恢复失败（任务 {}），退回重新提交：{}", taskId, err.toString());
                    dispatch(taskId);
                });
    }

    /**
     * 收到一条内核事件：先翻译状态，再转发给客户端。
     *
     * <p>顺序很重要 —— **先改状态再发事件**。反过来的话，
     * 客户端收到"任务已成功"时去查任务详情，可能查到的还是 RUNNING，
     * 于是界面显示自相矛盾。这类"事件与状态不一致"的 bug 非常难查，
     * 因为它取决于两个操作的相对时序。
     */
    private void onKernelEvent(String taskId, KernelClient.KernelEvent ev) {
        TaskRecord rec = store.find(taskId);
        if (rec == null) {
            return;
        }

        TaskState target = EVENT_TO_STATE.get(ev.type());
        if (target != null) {
            applyState(taskId, rec, target, ev);
        } else if (!rec.getState().isTerminal()) {
            // 非状态类事件（node.finished / sandbox.run / log ...）当作**心跳**：
            // 续租，表示"执行者还活着"。没有这一条，一个跑 30 分钟的任务
            // 会在第 10 分钟被 Reaper 判成失联并重派 —— 而它其实好好的。
            //
            // 注意续租长度必须**跟着当前状态走**：等审批时用的是审批租约
            // （30 分钟），如果这里一律用 10 分钟，那么内核在等人期间随便吐一条
            // log 事件，就会把租约缩短到 10 分钟，20 分钟后把还在等人的任务重派掉。
            // 这类 bug 只在"内核恰好发了非状态事件"时出现，非常难复现。
            store.renewLease(taskId, leaseFor(rec.getState()));
        }

        bus.publish(taskId, ev.type(), ev.data());
        if (TASK_TERMINAL.contains(ev.type())) {
            dispose(taskId);
        }
    }

    private static final java.util.Set<String> TASK_TERMINAL =
            java.util.Set.of("task.done", "task.failed");

    private void applyState(String taskId, TaskRecord rec, TaskState target,
                            KernelClient.KernelEvent ev) {
        if (target == TaskState.WAITING_APPROVAL) {
            // 等审批时租约要**换成审批等待的时长**，而不是清空。
            // 清空看起来更自然（等人期间执行者本来就没进展），但它有个洞：
            // findExpiredLeases 只扫 RUNNING/DISPATCHED，一旦清空，
            // 这个任务就彻底脱离重派候选 —— 内核若在等人期间挂了，
            // 任务永远停在"等待审批"，用户点了按钮却报错。
            // 用"审批超时 + 余量"的租约，内核活着时它自己会先失败；
            // 内核死了则由 Reaper 接住重派。
            if (store.tryTransition(taskId, TaskState.WAITING_APPROVAL)) {
                store.renewLease(taskId, props.getApproval().leaseMillis());
            }
            return;
        }

        if (target == TaskState.SUCCEEDED || target == TaskState.FAILED) {
            // 终态走 markResult：它一次完成"校验转移 + 写状态 + 写结果 + 撤租约"。
            // 注意**不能**先 tryTransition 再 markResult —— 第二步会看到状态
            // 已经是终态，于是判定为"过期上报"并丢弃，结果和错误就写不进去了。
            TaskRecord after = store.markResult(taskId, target, verdictOf(ev.data()), errorOf(ev));
            if (after != null && after.getState() == target) {
                audit.record("task_terminal", taskId, Map.of("state", target.name()));
            }
            return;
        }

        if (store.tryTransition(taskId, target)) {
            // 从审批里回来，重新给租约。
            if (target == TaskState.RUNNING) {
                store.renewLease(taskId, props.getLease().durationMillis());
            }
        }
    }

    /**
     * 从内核的终态事件里抽出**判决摘要**。
     *
     * <p>刻意只留几个字段，不镜像整个 result。理由：
     * 控制面的账本记的是"这个任务最终判成了什么"，不是执行面的产物本身。
     * 把产物整份复制过来会有两个后果 —— 状态重复（两边不一致时没人知道该信谁），
     * 以及内存随任务数无上限增长。需要完整产物时直接问内核
     * （{@code /api/tasks/{id}/artifacts}），那是执行面的真相。
     *
     * <p>但摘要**必须**留：否则"成功了"和"成功但门禁降级接受"在网关侧
     * 长得一模一样 —— 列表页会把一份测试都没跑通的交付显示成正常成功。
     * 这正是"谁掌握真相谁写状态"之外的第二个原则：**判决要落到账本上**。
     */
    private Map<String, Object> verdictOf(JsonNode data) {
        if (data == null) {
            return null;
        }
        JsonNode res = data.path("result");
        if (res.isMissingNode() || res.isNull()) {
            return null;
        }
        Map<String, Object> verdict = new LinkedHashMap<>();
        for (String key : VERDICT_KEYS) {
            JsonNode n = res.get(key);
            if (n == null || n.isNull() || n.isMissingNode()) {
                continue;
            }
            verdict.put(key, n.isBoolean() ? n.booleanValue()
                    : n.isNumber() ? n.numberValue()
                    : n.asText());
        }
        return verdict.isEmpty() ? null : verdict;
    }

    /** 判决摘要的白名单：只留"决定这份交付能不能用"的字段。 */
    private static final java.util.List<String> VERDICT_KEYS = java.util.List.of(
            "coverage", "coverage_target", "test_passed",
            "gate_passed", "gate_degraded", "gate_reason",
            "codegen_rounds", "clarify_rounds", "degraded");

    /** 当前状态该用多长的租约。等审批与执行中用的是两个量级，不能混。 */
    private long leaseFor(TaskState state) {
        return state == TaskState.WAITING_APPROVAL
                ? props.getApproval().leaseMillis()
                : props.getLease().durationMillis();
    }

    private String errorOf(KernelClient.KernelEvent ev) {
        JsonNode data = ev.data();
        return data == null ? null : data.path("error").asText(null);
    }

    /** 主动断开订阅。取消任务、或任务到终态时调用。 */
    public void dispose(String taskId) {
        Track track = subscriptions.remove(taskId);
        if (track == null) {
            return;
        }
        Disposable d = track.sub;
        if (d != null && !d.isDisposed()) {
            d.dispose();
        }
    }

    /**
     * 认领一个"已经在别处创建好"的内核任务：写内核 id、计一次执行、续租、开始跟踪。
     *
     * <p>给 Reaper 的断点恢复路径用 —— 那条路上内核任务是由 {@code /resume}
     * **新建**的，不是我们提交的，所以不能走 {@link #dispatch}（那会再建一个）。
     */
    public void adopt(String taskId, String kernelTaskId) {
        store.markDispatched(taskId, kernelTaskId, leaseFor(store.require(taskId).getState()));
        bus.publish(taskId, "gateway.dispatched", Map.of(
                "kernelTaskId", kernelTaskId,
                "attempt", store.require(taskId).getAttempts()));
        watch(taskId, kernelTaskId);
    }

    // ============================================================
    // 人工审批 / 取消 / 恢复 —— 都要转发给内核，因为"等人"这件事发生在内核侧
    // ============================================================

    /**
     * 放行或拒绝。
     *
     * <p>注意状态**不由网关改**：网关只是把决定转给内核，
     * 内核的 {@code approval.resolved} 事件回来时，状态才会变。
     * 为什么不让网关先改？因为审批的真相在内核那个阻塞的工作线程上 ——
     * 如果网关先记"已放行"而内核那边其实超时失效了，网关就记了一个假状态。
     * **谁掌握真相，谁写状态。**
     */
    public void approve(String taskId, boolean approved, String comment) {
        TaskRecord rec = store.require(taskId);
        String kernelTaskId = requireKernelId(rec, "审批");
        kernel.approve(kernelTaskId, approved, comment)
                .subscribe(
                        n -> {
                            audit.record("approve", taskId, Map.of("approved", approved,
                                    "comment", comment == null ? "" : comment));
                        },
                        err -> {
                            // 409（不在等审批）是预期内的：用户可能点晚了。
                            // 记警告即可，不需要把它上升成系统错误。
                            log.warn("转发审批失败（任务 {}）：{}", taskId, err.toString());
                            bus.publish(taskId, "gateway.approve_failed",
                                    Map.of("error", err.toString()));
                        });
    }

    public void cancel(String taskId) {
        TaskRecord rec = store.require(taskId);
        String kernelTaskId = rec.getKernelTaskId();

        // 先改网关状态：这是"用户意图"，应当立刻生效，不该等内核。
        // 内核那边即使慢半拍回了个成功，tryTransition 也会把它丢掉（终态不可逆）。
        store.transition(taskId, TaskState.CANCELLED);
        bus.publish(taskId, "task.status", Map.of("status", "cancelled"));
        dispose(taskId);

        if (kernelTaskId != null) {
            kernel.cancel(kernelTaskId).subscribe(
                    n -> audit.record("cancel", taskId, Map.of()),
                    err -> log.warn("转发取消失败（任务 {}）：{}", taskId, err.toString()));
        }
        audit.record("cancel", taskId, Map.of("kernelTaskId", String.valueOf(kernelTaskId)));
    }

    /**
     * 断点恢复：内核会**新建**一个内核任务（终态不可逆），
     * 网关对应地也新建一条记录，并把两条记录关联起来。
     *
     * <p>为什么网关不直接改原记录？因为那会破坏"历史只追加"。
     * 恢复后的执行是一次新的执行，它有自己的 attempts、自己的租约、
     * 自己的事件流。混进原记录里，事后就没法还原"第一次是怎么失败的"。
     */
    public Mono<TaskRecord> resume(String taskId, boolean autoApprove) {
        TaskRecord old = store.require(taskId);
        String kernelTaskId = requireKernelId(old, "恢复");

        // 这里返回 Mono 而不是 block()：controller 跑在 WebFlux 的事件循环上，
        // 在事件循环线程里 block() 会直接抛异常（Reactor 刻意防这个）。
        // 就算侥幸不抛，block 期间也会把该线程负责的其它连接一起卡住。
        return kernel.resume(kernelTaskId, autoApprove).map(resp -> {
            if (resp.path("task_id").isMissingNode()) {
                throw new IllegalStateException("内核未返回新的任务 id");
            }
            String newKernelId = resp.path("task_id").asText();

            TaskStore.SubmitOutcome outcome = store.submit(
                    old.getRequirement(), null, old.getOptions());
            String newGatewayId = outcome.record().getTaskId();
            store.markDispatched(newGatewayId, newKernelId, props.getLease().durationMillis());

            bus.publish(newGatewayId, "gateway.resumed", Map.of(
                    "resumedFrom", taskId,
                    "fromNode", resp.path("from_node").asText("")));
            audit.record("resume", taskId, Map.of("newTaskId", newGatewayId,
                    "kernelTaskId", newKernelId));
            watch(newGatewayId, newKernelId);
            return store.require(newGatewayId);
        });
    }

    private String requireKernelId(TaskRecord rec, String action) {
        if (rec.getKernelTaskId() == null) {
            throw new IllegalStateException(
                    "任务 " + rec.getTaskId() + " 还没派发到内核，无法" + action);
        }
        return rec.getKernelTaskId();
    }
}
