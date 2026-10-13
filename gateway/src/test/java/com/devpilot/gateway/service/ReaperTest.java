package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
 * 超时重派测试。
 *
 * <p>这是整个网关里**唯一能证明「不漏」的部分**。
 * 其它所有机制（幂等、状态机、审批）都建立在"任务最终会有人处理"这个前提上，
 * 而保证这个前提的正是 Reaper。所以它的行为必须被逐条钉死。
 */
class ReaperTest {

    private TaskStore store;
    private KernelClient kernel;
    private GatewayEventBus bus;
    private GatewayProperties props;
    private TaskDispatcher dispatcher;
    private Reaper reaper;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() {
        store = new TaskStore();
        kernel = Mockito.mock(KernelClient.class);
        bus = new GatewayEventBus();
        props = new GatewayProperties();
        props.getAudit().setEnabled(false);
        AuditLog audit = new AuditLog(props);
        dispatcher = new TaskDispatcher(store, kernel, bus, props, audit);
        reaper = new Reaper(store, dispatcher, bus, props, audit);

        when(kernel.events(anyString(), anyLong())).thenReturn(Flux.empty());
    }

    /** 造一个"已派发、租约已过期"的任务。 */
    private TaskRecord expiredTask() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        store.markDispatched(rec.getTaskId(), "k-" + rec.getTaskId(), -1);
        return store.require(rec.getTaskId());
    }

    /** 内核侧任务"还在跑"（对账时会看到非终态，因此不能凭对账结案）。 */
    private void kernelStillRunning() {
        ObjectNode node = json.createObjectNode();
        node.put("task_id", "k-x");
        node.put("status", "running");
        when(kernel.getTask(anyString())).thenReturn(Mono.just(node));
    }

    /** 断点恢复成功，返回一个新的内核任务 id。 */
    private void resumeYields(String newKernelId) {
        when(kernel.resume(anyString(), anyBoolean())).thenReturn(Mono.just(resumeResp(newKernelId)));
    }

    private ObjectNode resumeResp(String kernelId) {
        ObjectNode n = json.createObjectNode();
        n.put("task_id", kernelId);
        n.put("from_node", "code");
        return n;
    }

    // ============================================================
    // 重派的两段策略
    // ============================================================

    @Test
    @DisplayName("租约过期未达上限：走 TIMEOUT → 断点恢复 → 认领新内核任务")
    void expiredLeaseIsRedispatchedViaResume() {
        TaskRecord rec = expiredTask();
        kernelStillRunning();
        resumeYields("k-reborn");

        reaper.reapNow();

        TaskRecord after = store.require(rec.getTaskId());
        assertEquals(TaskState.DISPATCHED, after.getState(), "重派后应回到已派发");
        assertEquals("k-reborn", after.getKernelTaskId(), "必须指向重建出来的新执行");
        assertEquals(2, after.getAttempts(), "重派要计入一次执行");
        assertTrue(after.getLeaseExpiresAt() > System.currentTimeMillis(), "必须重新给租约");
    }

    @Test
    @DisplayName("重派优先走断点恢复 —— 这是内核重启后唯一能真正续跑的方式")
    void resumeIsPreferredOverResubmit() {
        TaskRecord rec = expiredTask();
        kernelStillRunning();
        resumeYields("k-reborn");

        reaper.reapNow();

        verify(kernel).resume(eq("k-" + rec.getTaskId()), anyBoolean());
        // 走恢复路径时**不该**再提交一个新任务 —— 那会多跑一份。
        verify(kernel, never()).submit(anyString(), any(), anyString());
    }

    @Test
    @DisplayName("没有检查点/内核不可达时退回重新提交，且带同一个幂等键")
    void resumeFailureFallsBackToResubmit() {
        TaskRecord rec = expiredTask();
        kernelStillRunning();
        when(kernel.resume(anyString(), anyBoolean()))
                .thenReturn(Mono.error(new RuntimeException("409 没有可用的检查点")));
        when(kernel.submit(anyString(), any(), anyString()))
                .thenReturn(Mono.just(new KernelClient.KernelTask("k-fresh", "pending", false)));

        reaper.reapNow();

        verify(kernel).submit(eq("需求"), any(), eq(rec.getTaskId()));
        assertEquals(TaskState.DISPATCHED, store.require(rec.getTaskId()).getState());
        assertEquals("k-fresh", store.require(rec.getTaskId()).getKernelTaskId());
    }

    @Test
    @DisplayName("重派前先对账：任务其实已经成功，就不再白跑一遍")
    void reconcileBeforeRedispatchAvoidsDuplicateWork() {
        TaskRecord rec = expiredTask();
        ObjectNode node = json.createObjectNode();
        node.put("status", "succeeded");
        when(kernel.getTask(anyString())).thenReturn(Mono.just(node));

        reaper.reapNow();

        assertEquals(TaskState.SUCCEEDED, store.require(rec.getTaskId()).getState());
        verify(kernel, never()).resume(anyString(), anyBoolean());
        verify(kernel, never()).submit(anyString(), any(), anyString());
    }

    // ============================================================
    // 收敛与边界
    // ============================================================

    @Test
    @DisplayName("重派次数用尽：判 FAILED 并发出终态事件，不再无限重试")
    void attemptsExhaustedFailsTask() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        String id = rec.getTaskId();

        int max = props.getLease().getMaxAttempts();
        store.markDispatched(id, "k1", -1);
        for (int i = 1; i < max; i++) {
            store.transition(id, TaskState.TIMEOUT);
            store.markDispatched(id, "k" + (i + 1), -1);
        }
        assertEquals(max, store.require(id).getAttempts());

        reaper.reapNow();

        TaskRecord after = store.require(id);
        assertEquals(TaskState.FAILED, after.getState(), "次数用尽必须收敛到失败");
        assertTrue(after.getError().contains("失联"), "错误信息要说明原因，实际=" + after.getError());
        assertEquals(0L, after.getLeaseExpiresAt(), "终态不再持有租约");

        List<GatewayEventBus.GatewayEvent> events = bus.history(id, 0);
        assertTrue(events.stream().anyMatch(e -> "task.failed".equals(e.type())),
                "必须给客户端一个终态事件，否则界面会一直转圈");
        verify(kernel, never()).resume(anyString(), anyBoolean());
        verify(kernel, never()).submit(anyString(), any(), anyString());
    }

    @Test
    @DisplayName("重派过程会在事件流里留下痕迹 —— 「曾经失联」必须可见")
    void timeoutIsVisibleInEventStream() {
        TaskRecord rec = expiredTask();
        kernelStillRunning();
        resumeYields("k-reborn");

        reaper.reapNow();

        List<GatewayEventBus.GatewayEvent> events = bus.history(rec.getTaskId(), 0);
        assertTrue(events.stream().anyMatch(e -> "gateway.timeout".equals(e.type())),
                "没有这条，外部完全看不出发生过失联");
        assertTrue(events.stream().anyMatch(e -> "gateway.dispatched".equals(e.type())),
                "重派也要发事件");
    }

    @Test
    @DisplayName("租约没过期的任务不受影响 —— 正常的慢任务不能被误杀")
    void healthyTaskIsUntouched() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        store.markDispatched(rec.getTaskId(), "k1", 600_000);

        reaper.reapNow();

        assertEquals(TaskState.DISPATCHED, store.require(rec.getTaskId()).getState());
        assertEquals(1, store.require(rec.getTaskId()).getAttempts());
        verify(kernel, never()).resume(anyString(), anyBoolean());
        verify(kernel, never()).submit(anyString(), any(), anyString());
    }

    @Test
    @DisplayName("终态任务永不重派 —— 否则一个已完成的交付会被跑第二遍")
    void terminalTaskIsNeverReaped() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        store.markDispatched(rec.getTaskId(), "k1", -1);
        store.markResult(rec.getTaskId(), TaskState.SUCCEEDED, null, null);

        reaper.reapNow();

        assertEquals(TaskState.SUCCEEDED, store.require(rec.getTaskId()).getState());
        verify(kernel, never()).resume(anyString(), anyBoolean());
        verify(kernel, never()).submit(anyString(), any(), anyString());
    }

    @Test
    @DisplayName("一批里有一个任务处理失败，不能连累其它任务")
    void oneBadTaskDoesNotBlockTheRest() {
        TaskRecord bad = expiredTask();
        TaskRecord good = expiredTask();

        when(kernel.getTask(anyString())).thenReturn(Mono.error(new RuntimeException("内核不可达")));
        when(kernel.resume(anyString(), anyBoolean()))
                .thenAnswer(inv -> {
                    if (inv.getArgument(0).equals(bad.getKernelTaskId())) {
                        return Mono.error(new RuntimeException("boom"));
                    }
                    return Mono.just(resumeResp("k-good-new"));
                });
        when(kernel.submit(anyString(), any(), anyString()))
                .thenAnswer(inv -> {
                    // 坏任务连重新提交也失败 —— 模拟这一条彻底走不通。
                    if (inv.getArgument(2).equals(bad.getTaskId())) {
                        return Mono.error(new RuntimeException("submit also failed"));
                    }
                    return Mono.just(new KernelClient.KernelTask("k-fresh", "pending", false));
                });

        reaper.reapNow();

        // 好任务被成功重派；坏任务的处理失败没有中断整轮扫描。
        assertEquals(TaskState.DISPATCHED, store.require(good.getTaskId()).getState());
        assertEquals("k-good-new", store.require(good.getTaskId()).getKernelTaskId());
    }

    @Test
    @DisplayName("没有过期任务时什么都不做 —— 空转不应该产生任何调用")
    void nothingToDoIsCheap() {
        store.submit("需求", null, Map.of());
        reaper.reapNow();
        verify(kernel, never()).submit(anyString(), any(), anyString());
        verify(kernel, never()).resume(anyString(), anyBoolean());
    }

    // ============================================================
    // 等审批的任务也必须能被接住
    // ============================================================

    @Test
    @DisplayName("内核在等人期间挂掉：等审批的任务也必须能被打回重派")
    void waitingApprovalCanBeReapedAfterApprovalTimeout() {
        TaskRecord rec = store.submit("需求", null, Map.of()).record();
        String id = rec.getTaskId();
        store.markDispatched(id, "k1", 600_000);
        store.transition(id, TaskState.RUNNING);
        store.transition(id, TaskState.WAITING_APPROVAL);

        long approvalLease = props.getApproval().leaseMillis();
        assertTrue(approvalLease > props.getLease().durationMillis(),
                "审批租约必须比执行租约长，否则内核还在等人就被重派了");

        // 用极短的租约模拟"已经等过头了"（比 sleep 一分钟快得多，语义相同）。
        store.renewLease(id, 30);
        sleep(60);
        assertFalse(store.findExpiredLeases().isEmpty(),
                "等审批的超时任务必须出现在重派候选里");

        kernelStillRunning();
        resumeYields("k-reborn");
        reaper.reapNow();

        assertEquals(TaskState.DISPATCHED, store.require(id).getState(),
                "内核失联时，等审批的任务必须能被重新派发，而不是永远卡住");
        // 恢复时要告诉内核"这是从审批点回来的"，让它别再停下来等人。
        verify(kernel).resume(eq("k1"), eq(true));
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @DisplayName("对账拿到非终态时不能结案 —— 否则会把一个还在跑的任务提前判死")
    void reconcileLeavesRunningTaskAlone() {
        TaskRecord rec = expiredTask();
        ObjectNode node = json.createObjectNode();
        node.put("status", "running");
        when(kernel.getTask(anyString())).thenReturn(Mono.just(node));
        resumeYields("k-reborn");

        reaper.reapNow();

        assertNotNull(store.require(rec.getTaskId()).getKernelTaskId());
        assertEquals(2, store.require(rec.getTaskId()).getAttempts(),
                "非终态不算结案，应当继续重派");
    }
}
