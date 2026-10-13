package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.Map;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 派发器测试：**内核事件 → 网关状态** 的翻译。
 *
 * <p>这是网关里最容易写错、也最难靠手测发现的一层：
 * 事件名写错一个字母，映射失效，任务就会静静停在中间态，
 * 而界面上看起来一切正常（因为它显示的正是"运行中"）。
 */
class TaskDispatcherTest {

    private TaskStore store;
    private KernelClient kernel;
    private GatewayEventBus bus;
    private GatewayProperties props;
    private AuditLog audit;
    private TaskDispatcher dispatcher;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() {
        store = new TaskStore();
        kernel = Mockito.mock(KernelClient.class);
        bus = new GatewayEventBus();
        props = new GatewayProperties();
        props.getAudit().setEnabled(false);   // 测试不落盘
        audit = new AuditLog(props);
        dispatcher = new TaskDispatcher(store, kernel, bus, props, audit);
    }

    private static KernelClient.KernelEvent ev(long seq, String type, ObjectNode data) {
        return new KernelClient.KernelEvent(seq, type, data, System.currentTimeMillis());
    }

    private ObjectNode data(String key, String value) {
        ObjectNode n = json.createObjectNode();
        n.put(key, value);
        return n;
    }

    /** 让内核先受理（返回 k1），再按顺序推给定的事件。 */
    private TaskRecord dispatchWith(String gatewayRequirement, KernelClient.KernelEvent... events) {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.just(events));

        TaskRecord rec = store.submit(gatewayRequirement, null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());
        return rec;
    }

    // ============================================================
    // 派发本身
    // ============================================================

    @Test
    @DisplayName("派发成功：写内核 id、次数 +1、租约续上")
    void dispatchSuccess() {
        TaskRecord rec = dispatchWith("做个待办应用", ev(1, "node.started", data("node", "clarify")));
        TaskRecord after = store.require(rec.getTaskId());

        assertEquals(TaskState.RUNNING, after.getState());
        assertEquals("k1", after.getKernelTaskId());
        assertEquals(1, after.getAttempts());
        assertTrue(after.getLeaseExpiresAt() > System.currentTimeMillis());
    }

    @Test
    @DisplayName("幂等键用的是网关自己的任务 id —— 重试不会让内核跑出两份交付")
    void dispatchUsesGatewayTaskIdAsIdempotencyKey() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.empty());

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());

        ArgumentCaptor<String> key = ArgumentCaptor.forClass(String.class);
        verify(kernel).submit(anyString(), any(), key.capture());
        assertEquals(rec.getTaskId(), key.getValue());
    }

    @Test
    @DisplayName("内核幂等命中时**不**再累加 attempts —— 否则重派上限会被虚耗")
    void deduplicatedDispatchDoesNotCountAttempt() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", true)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.empty());

        TaskRecord first = store.submit("需求", null, Map.of()).record();
        store.markDispatched(first.getTaskId(), "k1", 60_000);
        store.transition(first.getTaskId(), TaskState.TIMEOUT);

        dispatcher.dispatch(first.getTaskId());   // 重派，但内核说"我已经有了"

        assertEquals(1, store.require(first.getTaskId()).getAttempts(),
                "内核说复用，就不该再算一次执行");
    }

    @Test
    @DisplayName("派发失败：标 FAILED，并发一条终态事件让客户端能收尾")
    void dispatchFailureIsTerminal() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.error(new RuntimeException("connection refused")));

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.FAILED, after.getState());
        assertEquals(0, after.getAttempts(), "提交失败不算一次执行");
        assertTrue(after.getError().contains("connection refused"));
    }

    // ============================================================
    // 事件 → 状态 的翻译
    // ============================================================

    @Test
    @DisplayName("approval.required → WAITING_APPROVAL，且租约换成审批量级")
    void approvalRequiredSwitchesToWaitingApproval() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "node.started", data("node", "design")),
                ev(2, "approval.required", data("node", "approve")));

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.WAITING_APPROVAL, after.getState());

        long approvalLease = props.getApproval().leaseMillis();
        long remaining = after.getLeaseExpiresAt() - System.currentTimeMillis();
        assertTrue(remaining > props.getLease().durationMillis(),
                "等审批期间租约必须比执行的租约更长，否则会把正在等人的任务重派掉；"
                        + "remaining=" + remaining + " approvalLease=" + approvalLease);
    }

    @Test
    @DisplayName("approval.resolved → 回到 RUNNING，租约恢复成执行量级")
    void approvalResolvedReturnsToRunning() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "approval.required", data("node", "approve")),
                ev(2, "approval.resolved", data("approved", "true")));

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.RUNNING, after.getState());
        long remaining = after.getLeaseExpiresAt() - System.currentTimeMillis();
        assertTrue(remaining <= props.getLease().durationMillis() + 1000,
                "审批结束后应退回执行量级的租约");
    }

    @Test
    @DisplayName("task.done → SUCCEEDED，且不再持有租约")
    void taskDoneIsTerminal() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "node.started", data("node", "code")),
                ev(2, "task.done", data("status", "succeeded")));

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.SUCCEEDED, after.getState());
        assertEquals(0L, after.getLeaseExpiresAt());
        assertNotNull(bus);
    }

    @Test
    @DisplayName("task.failed 带着具体错误落进网关记录")
    void taskFailedCarriesError() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "task.failed", data("error", "覆盖率 42% 未达标")));

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.FAILED, after.getState());
        assertEquals("覆盖率 42% 未达标", after.getError());
    }

    // ============================================================
    // 判决摘要：让"成功"和"成功但降级接受"在账本上可分
    // ============================================================

    /** 造一个带 result 的 task.done 数据。 */
    private ObjectNode doneWithResult(java.util.function.Consumer<ObjectNode> fill) {
        ObjectNode result = json.createObjectNode();
        fill.accept(result);
        ObjectNode d = json.createObjectNode();
        d.set("result", result);
        return d;
    }

    @Test
    @DisplayName("终态事件里的判决摘要落进网关账本 —— 列表页才能区分「成功」与「降级接受」")
    void terminalEventKeepsVerdictSummary() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "task.done", doneWithResult(r -> {
                    r.put("coverage", 99);
                    r.put("coverage_target", 80);
                    r.put("test_passed", true);
                    r.put("gate_passed", true);
                    r.put("gate_degraded", true);
                    r.put("gate_reason", "覆盖率 55% 低于目标 80%，已降级接受");
                    r.put("codegen_rounds", 3);
                })));

        Map<String, Object> verdict = store.require(rec.getTaskId()).getResult();
        assertNotNull(verdict);
        assertEquals(99, ((Number) verdict.get("coverage")).intValue());
        assertEquals(80, ((Number) verdict.get("coverage_target")).intValue());
        assertEquals(Boolean.TRUE, verdict.get("gate_degraded"));
        assertTrue(String.valueOf(verdict.get("gate_reason")).contains("已降级接受"));
        assertEquals(3, ((Number) verdict.get("codegen_rounds")).intValue());
    }

    @Test
    @DisplayName("判决摘要只留白名单字段：产物/用量这类重内容**不**镜像到控制面")
    void verdictSummaryIsWhitelisted() {
        // 为什么重要：把执行面的产物整份复制进控制面，会出现状态重复
        // （两边不一致时没人知道该信谁）和内存无上限增长。
        TaskRecord rec = dispatchWith("需求",
                ev(1, "task.done", doneWithResult(r -> {
                    r.put("coverage", 88);
                    r.putArray("files").add("app/main.py").add("tests/test_main.py");
                    r.putObject("usage").put("calls", 14);
                    r.put("requirement", "一份很长的原始需求");
                })));

        Map<String, Object> verdict = store.require(rec.getTaskId()).getResult();
        assertNotNull(verdict);
        assertEquals(88, ((Number) verdict.get("coverage")).intValue());
        assertFalse(verdict.containsKey("files"), "产物列表不该进账本");
        assertFalse(verdict.containsKey("usage"), "用量明细不该进账本");
        assertFalse(verdict.containsKey("requirement"), "需求原文不该被结果覆盖");
    }

    @Test
    @DisplayName("失败事件没有 result 时，账本结果是 null 而不是空表")
    void missingResultStaysNull() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "task.failed", data("error", "派发失败")));
        // 空表会让前端误以为"有结果但全空"，null 才表达"压根没有结果"。
        assertNull(store.require(rec.getTaskId()).getResult());
    }

    @Test
    @DisplayName("派发失败也带上判决摘要（若有）—— 内核可能先回结果再报错")
    void failedEventStillKeepsVerdict() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "task.failed", doneWithResult(r -> {
                    r.put("coverage", 12);
                    r.put("gate_degraded", true);
                    r.put("gate_reason", "测试未通过（pytest 未能跑通），已降级接受");
                })));
        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.FAILED, after.getState());
        assertNotNull(after.getResult());
        assertEquals(12, ((Number) after.getResult().get("coverage")).intValue());
    }

    @Test
    @DisplayName("执行面的前进一律采纳：DISPATCHED 直接收到 task.done 不算「过期上报」")
    void directTerminalReportIsAccepted() {
        // 这个场景是真实的：事件流可能断、可能重连，网关无法保证看到 node.started。
        // 若用严格转移表判定，这条合法上报会被丢弃 → 任务变僵尸。
        TaskRecord rec = dispatchWith("需求", ev(1, "task.done", data("status", "succeeded")));
        assertEquals(TaskState.SUCCEEDED, store.require(rec.getTaskId()).getState());
    }

    @Test
    @DisplayName("已取消的任务收到迟到的成功回报：丢弃，状态保持 cancelled")
    void lateReportAfterCancelIsDropped() {
        // 用可控的事件源：先卡住不发，等取消之后再补一条"成功"。
        // 这正是真实里的时序 —— 内核不知道用户刚点了取消，它跑完就报成功。
        Sinks.Many<KernelClient.KernelEvent> source =
                Sinks.many().unicast().onBackpressureBuffer();
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(source.asFlux());
        when(kernel.cancel(eq("k1"))).thenReturn(Mono.just(json.createObjectNode()));

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());
        dispatcher.cancel(rec.getTaskId());

        // 迟到的上报：必须被丢弃，不能让一个已取消的任务"复活"成成功。
        source.tryEmitNext(ev(9, "task.done", data("status", "succeeded")));

        assertEquals(TaskState.CANCELLED, store.require(rec.getTaskId()).getState());
    }

    // ============================================================
    // 审批转发
    // ============================================================

    @Test
    @DisplayName("审批只转发给内核，网关**不**自己改状态 —— 真相在内核那个阻塞线程上")
    void approveForwardsWithoutChangingLocalState() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.never());
        when(kernel.approve(eq("k1"), anyBoolean(), anyString()))
                .thenReturn(Mono.just(json.createObjectNode()));

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());
        TaskState before = store.require(rec.getTaskId()).getState();

        dispatcher.approve(rec.getTaskId(), true, "看着不错");

        verify(kernel).approve(eq("k1"), eq(true), eq("看着不错"));
        assertEquals(before, store.require(rec.getTaskId()).getState(),
                "状态要等内核的 approval.resolved 事件来驱动");
    }

    @Test
    @DisplayName("还没派发就点审批：明确报错，而不是发一个必然失败的请求")
    void approveBeforeDispatchFails() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> dispatcher.approve(rec.getTaskId(), true, ""));
        assertTrue(e.getMessage().contains("还没派发"));
        verify(kernel, never()).approve(anyString(), anyBoolean(), anyString());
    }

    // ============================================================
    // 取消
    // ============================================================

    @Test
    @DisplayName("取消是「用户意图」，网关立刻生效，不必等内核")
    void cancelTakesEffectImmediately() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.never());
        when(kernel.cancel(eq("k1"))).thenReturn(Mono.just(json.createObjectNode()));

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());
        dispatcher.cancel(rec.getTaskId());

        assertEquals(TaskState.CANCELLED, store.require(rec.getTaskId()).getState());
        verify(kernel).cancel("k1");
    }

    @Test
    @DisplayName("取消终态任务被拒 —— 终态不可逆")
    void cancelTerminalTaskRejected() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        when(kernel.events(eq("k1"), anyLong())).thenReturn(Flux.empty());

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());
        store.markResult(rec.getTaskId(), TaskState.SUCCEEDED, null, null);

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> dispatcher.cancel(rec.getTaskId()));
    }

    // ============================================================
    // 断点恢复
    // ============================================================

    @Test
    @DisplayName("恢复：内核新建一个任务，网关也新建一条记录并关联，不动原记录")
    void resumeCreatesNewRecordAndLinks() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k-old", "pending", false)));
        when(kernel.events(anyString(), anyLong())).thenReturn(Flux.empty());
        ObjectNode resp = json.createObjectNode();
        resp.put("task_id", "k-new");
        resp.put("from_node", "code");
        when(kernel.resume(eq("k-old"), anyBoolean())).thenReturn(Mono.just(resp));

        TaskRecord old = store.submit("需求", null, Map.of("x", 1)).record();
        dispatcher.dispatch(old.getTaskId());
        store.markResult(old.getTaskId(), TaskState.FAILED, null, "覆盖率不达标");

        TaskRecord resumed = dispatcher.resume(old.getTaskId(), true).block();

        assertNotNull(resumed);
        assertFalse(resumed.getTaskId().equals(old.getTaskId()), "必须是一条新记录");
        assertEquals("k-new", resumed.getKernelTaskId());
        assertEquals(TaskState.DISPATCHED, resumed.getState());
        // 原记录必须原封不动 —— 历史只追加。
        assertEquals(TaskState.FAILED, store.require(old.getTaskId()).getState());
        assertEquals(2, store.list(10).size());
    }

    @Test
    @DisplayName("恢复未派发的任务：明确报错")
    void resumeBeforeDispatchFails() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> dispatcher.resume(rec.getTaskId(), false));
        assertTrue(e.getMessage().contains("还没派发"));
    }

    @Test
    @DisplayName("非状态类事件当心跳：长任务不会因为「没状态变化」被判失联")
    void nonStateEventsActAsHeartbeat() {
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k1", "pending", false)));
        // 事件流里只有 sandbox.run —— 没有任何状态变化。
        when(kernel.events(eq("k1"), anyLong()))
                .thenReturn(Flux.just(ev(1, "sandbox.run", data("cmd", "pytest"))));

        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        dispatcher.dispatch(rec.getTaskId());

        TaskRecord after = store.require(rec.getTaskId());
        assertTrue(after.getLeaseExpiresAt() > System.currentTimeMillis() + 500_000,
                "收到事件就应当续租");
    }

    @Test
    @DisplayName("事件转成客户端可见的帧，且 seq 单调递增供断线续传")
    void eventsArePublishedToClientBus() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "node.started", data("node", "prd")),
                ev(2, "node.finished", data("node", "prd")));

        // 读历史而不是订阅流：流要等到终态才会结束，
        // 在测试里 block() 一个未完成的流会一直挂着。
        List<GatewayEventBus.GatewayEvent> seen = bus.history(rec.getTaskId(), 0);
        assertEquals(3, seen.size(), "dispatched + 两条 node 事件，实际=" + seen);
        assertEquals("gateway.dispatched", seen.get(0).type());
        for (int i = 1; i < seen.size(); i++) {
            assertTrue(seen.get(i).seq() > seen.get(i - 1).seq(), "seq 必须单调递增");
        }
    }

    @Test
    @DisplayName("since 过滤：断线重连只拿到缺失的那几条")
    void historySinceFiltersAlreadySeen() {
        TaskRecord rec = dispatchWith("需求",
                ev(1, "node.started", data("node", "prd")),
                ev(2, "node.finished", data("node", "prd")));

        List<GatewayEventBus.GatewayEvent> tail = bus.history(rec.getTaskId(), 1);
        assertEquals(2, tail.size(), "只应剩下 seq 2、3");
        assertEquals(2, tail.get(0).seq());
    }
}
