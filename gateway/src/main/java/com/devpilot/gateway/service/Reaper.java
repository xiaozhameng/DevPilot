package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 超时重派器（Reaper）：定时扫描租约过期的任务，决定"重派"还是"判死"。
 *
 * <h2>为什么需要它 —— 这是"不漏"的唯一保证</h2>
 *
 * 派发出去的任务有四种方式结束：成功、失败、被取消、**执行者消失**。
 * 前三种都会产生一条事件，网关能听到。第四种**什么都不会发生**：
 * 内核进程被 kill、机器断电、网络分区 —— 事件流只是静静地断了，
 * 网关的订阅线程也不会报错（沉默的 TCP 连接要很久才超时）。
 *
 * <p>如果不做任何事，这个任务就永远停在 RUNNING。用户界面上它"一直在跑"，
 * 直到有人手工清理。这就是"漏"。
 *
 * <p>检测"沉默的失败者"只有两种通用手段：**心跳**或**租约**。
 * 这里选租约，因为租约不需要执行者配合（心跳需要内核定期上报，
 * 而内核崩了就不会上报了 —— 但崩了恰恰是我们要检测的情况，
 * 心跳在这里是循环论证）。
 *
 * <h2>为什么"重派"而不是"直接判失败"</h2>
 *
 * 因为失联的原因里，**离线的成本远高于重复的成本**：
 * <ul>
 *   <li>误判（任务其实还在跑）重派一次：浪费一次模型调用，约几毛钱。</li>
 *   <li>漏判（任务其实已经死了）不重派：这个交付永远完不成，
 *       用户要手工重提，还可能以为系统坏了。</li>
 * </ul>
 * 所以默认倾向重派，用 {@code maxAttempts} 兜住"无限重派"的风险。
 *
 * <p>依赖同一个幂等键（网关任务 id），即便"原执行者其实还活着"，
 * 重派在内核侧也会命中幂等 —— 不会真的跑出两份交付。
 * 这是"重复的成本"被进一步压低的关键。
 */
@Component
public class Reaper {

    private static final Logger log = LoggerFactory.getLogger(Reaper.class);

    private final TaskStore store;
    private final TaskDispatcher dispatcher;
    private final GatewayEventBus bus;
    private final GatewayProperties props;
    private final AuditLog audit;

    public Reaper(TaskStore store, TaskDispatcher dispatcher, GatewayEventBus bus,
                  GatewayProperties props, AuditLog audit) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.bus = bus;
        this.props = props;
        this.audit = audit;
    }

    /**
     * 定时扫描。
     *
     * <p>{@code fixedDelay} 而不是 {@code fixedRate}：前者是"上一轮跑完之后再等 N 秒"，
     * 后者是"每 N 秒必须开一轮（不管上一轮完没完）"。
     * 扫描本身要发起网络重派，可能比间隔还慢；用 fixedRate 会让任务堆积，
     * 同一个任务被两轮同时重派。fixedDelay 天然串行。
     *
     * <p>间隔比租约时长小一个数量级（30s vs 600s），保证过期任务能在
     * 一个租约周期内被处理掉，而不会拖到下一个周期。
     */
    @Scheduled(fixedDelayString = "${gateway.lease.reap-interval-seconds:30}000")
    public void reap() {
        List<TaskRecord> expired = store.findExpiredLeases();
        if (expired.isEmpty()) {
            return;
        }
        log.info("发现 {} 个租约过期的任务", expired.size());
        for (TaskRecord rec : expired) {
            try {
                handle(rec);
            } catch (Exception e) {
                // 一个任务处理失败不能中断整轮扫描 —— 否则一个坏任务
                // 会让后面所有过期任务都得不到处理。
                log.warn("重派处理失败（任务 {}）：{}", rec.getTaskId(), e.toString());
            }
        }
    }

    private void handle(TaskRecord rec) {
        String taskId = rec.getTaskId();
        int max = props.getLease().getMaxAttempts();

        if (rec.getAttempts() >= max) {
            // 重派次数用尽：接受失败。这是**有意的放弃**，不是遗漏 ——
            // 无限重派比失败更糟，因为它会持续烧钱且永远不收敛。
            store.markResult(taskId, TaskState.FAILED, null,
                    "执行副本多次失联（已重派 " + rec.getAttempts() + " 次），放弃");
            audit.record("reap_giveup", taskId, Map.of("attempts", rec.getAttempts()));
            bus.publish(taskId, "gateway.timeout", Map.of(
                    "attempts", rec.getAttempts(), "giveUp", true));
            bus.publish(taskId, "task.failed", Map.of(
                    "error", "执行副本多次失联，已放弃"));
            dispatcher.dispose(taskId);
            return;
        }

        log.warn("任务 {} 租约过期（当前 {}，已派发 {} 次），准备重派",
                taskId, rec.getState(), rec.getAttempts());
        audit.record("reap_redispatch", taskId, Map.of(
                "attempts", rec.getAttempts(),
                "fromState", rec.getState().name()));
        bus.publish(taskId, "gateway.timeout", Map.of(
                "attempts", rec.getAttempts(), "giveUp", false));

        // 必须在改状态**之前**记住"它原本在等审批"：
        // 改成 TIMEOUT 之后就看不出来了，恢复时内核会又停下来等人 ——
        // 而这一次很可能根本没有人会来点。
        boolean wasWaitingApproval = rec.getState() == TaskState.WAITING_APPROVAL;

        // 先断开可能还挂着的旧订阅，避免旧流的迟到事件和新流的事件混在一起
        // （旧流如果其实还活着，它报的进度是过期的，会把状态往回带）。
        dispatcher.dispose(taskId);

        // 状态机要求走 TIMEOUT 这一跳，而不是在 RUNNING 上直接重派：
        // 这样一来，"这个任务曾经失联过"在状态历史里留下了痕迹。
        // 直接重派的话，事后只看状态序列是看不出发生过超时的。
        store.transition(taskId, TaskState.TIMEOUT);
        dispatcher.redispatch(taskId, wasWaitingApproval);
    }

    /** 供测试直接驱动，不必等定时器。 */
    public void reapNow() {
        reap();
    }
}
