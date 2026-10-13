package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 任务表测试：幂等、状态流转、租约。
 *
 * <p>这三件事是网关的全部价值，所以每一项都要有能证明"它真的成立"的测试，
 * 而不是"调用了没抛异常"这种假测试。
 */
class TaskStoreTest {

    private TaskStore store;

    @BeforeEach
    void setUp() {
        store = new TaskStore();
    }

    // ============================================================
    // 幂等
    // ============================================================

    @Test
    @DisplayName("同一幂等键重复提交：返回原任务，不新建")
    void submitWithSameKeyReusesRecord() {
        TaskStore.SubmitOutcome first = store.submit("做个待办应用", "key-1", Map.of());
        TaskStore.SubmitOutcome second = store.submit("做个待办应用", "key-1", Map.of());

        assertFalse(first.deduplicated());
        assertTrue(second.deduplicated(), "第二次必须被识别为复用");
        assertSame(first.record(), second.record(), "必须是同一条记录，而不是内容相同的新记录");
        assertEquals(1, store.list(10).size(), "任务表里只应有 1 条");
    }

    @Test
    @DisplayName("并发提交同一幂等键：只允许一个成功，其余全部复用")
    void concurrentSubmitWithSameKeyCreatesExactlyOne() throws Exception {
        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);

        Set<String> createdIds = ConcurrentHashMap.newKeySet();
        AtomicInteger freshCount = new AtomicInteger();
        AtomicInteger dedupCount = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    TaskStore.SubmitOutcome o = store.submit("并发需求", "same-key", Map.of());
                    createdIds.add(o.record().getTaskId());
                    if (o.deduplicated()) {
                        dedupCount.incrementAndGet();
                    } else {
                        freshCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "并发提交未在 10 秒内完成");
        pool.shutdown();

        // 这是 putIfAbsent 相对 check-then-act 的核心差别：
        // 32 个线程同时打进来，只有一个能新建。
        // 换成"先 get 判空再 put"，这里会看到多个不同的 id —— 幂等直接破功。
        assertEquals(1, createdIds.size(), "只允许产生 1 个任务 id，实际=" + createdIds);
        assertEquals(1, freshCount.get(), "只允许 1 次「新建」");
        assertEquals(threads - 1, dedupCount.get());
        assertEquals(1, store.list(10).size());
    }

    @Test
    @DisplayName("不同幂等键各建一条；不传键则每次都新建")
    void distinctKeysAndNoKey() {
        store.submit("A", "k-a", Map.of());
        store.submit("B", "k-b", Map.of());
        store.submit("C", null, Map.of());
        store.submit("C", null, Map.of());
        assertEquals(4, store.list(10).size(), "不传键时不做去重，这是刻意的");
    }

    // ============================================================
    // 状态流转
    // ============================================================

    @Test
    @DisplayName("非法转移直接抛异常（内部动作走强校验）")
    void transitionRejectsIllegalMove() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        assertThrows(IllegalStateException.class,
                () -> store.transition(rec.getTaskId(), TaskState.SUCCEEDED));
    }

    @Test
    @DisplayName("过期上报被丢弃而不是抛异常（外部动作走弱校验）")
    void tryTransitionDropsStaleReport() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        String id = rec.getTaskId();
        store.markDispatched(id, "k1", 60_000);
        store.transition(id, TaskState.CANCELLED);

        // 内核随后才回报成功（它不知道任务被取消了）。
        // 这里必须返回 false，而不是让网关报 500。
        assertFalse(store.tryTransition(id, TaskState.RUNNING));
        assertEquals(TaskState.CANCELLED, store.require(id).getState(), "终态不可被改写");
    }

    @Test
    @DisplayName("markDispatched 三件事原子完成：内核 id + 次数 + 租约")
    void markDispatchedIsAtomic() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        String id = rec.getTaskId();
        long before = System.currentTimeMillis();

        store.markDispatched(id, "kernel-1", 5_000);

        TaskRecord after = store.require(id);
        assertEquals(TaskState.DISPATCHED, after.getState());
        assertEquals("kernel-1", after.getKernelTaskId());
        assertEquals(1, after.getAttempts());
        assertTrue(after.getLeaseExpiresAt() >= before + 5_000,
                "租约必须被续上，否则 Reaper 会立刻把它判成失联");

        // 第二次派发（重派场景）：次数累加，内核 id 更新。
        store.transition(id, TaskState.TIMEOUT);
        store.markDispatched(id, "kernel-2", 5_000);
        assertEquals(2, store.require(id).getAttempts());
        assertEquals("kernel-2", store.require(id).getKernelTaskId());
    }

    @Test
    @DisplayName("markResult 写终态并撤租约；迟到的第二次上报被丢弃")
    void markResultLocksTerminalState() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        String id = rec.getTaskId();
        store.markDispatched(id, "k1", 60_000);

        store.markResult(id, TaskState.SUCCEEDED, Map.of("files", 3), null);
        TaskRecord after = store.require(id);
        assertEquals(TaskState.SUCCEEDED, after.getState());
        assertEquals(0L, after.getLeaseExpiresAt(), "终态不该继续持有租约");
        assertNotNull(after.getResult());

        // 一个失败回报在网络里绕了一圈才到 —— 不能被它改回 FAILED。
        store.markResult(id, TaskState.FAILED, null, "late");
        assertEquals(TaskState.SUCCEEDED, store.require(id).getState());
    }

    @Test
    @DisplayName("不存在的任务：require 抛 NoSuchElement，find 返回 null")
    void missingTask() {
        assertThrows(NoSuchElementException.class, () -> store.require("nope"));
        assertEquals(null, store.find("nope"));
        assertFalse(store.tryTransition("nope", TaskState.RUNNING));
    }

    // ============================================================
    // 租约
    // ============================================================

    @Test
    @DisplayName("租约过期只对「还在等执行」的任务有意义")
    void expiredLeaseDetection() {
        TaskRecord a = store.submit("a", null, Map.of()).record();
        TaskRecord b = store.submit("b", null, Map.of()).record();
        TaskRecord c = store.submit("c", null, Map.of()).record();

        store.markDispatched(a.getTaskId(), "ka", -1);   // 一开始就是过期的
        store.markDispatched(b.getTaskId(), "kb", 60_000);
        store.markDispatched(c.getTaskId(), "kc", -1);
        store.markResult(c.getTaskId(), TaskState.SUCCEEDED, null, null);

        List<TaskRecord> expired = store.findExpiredLeases();
        List<String> ids = expired.stream().map(TaskRecord::getTaskId).toList();

        assertTrue(ids.contains(a.getTaskId()), "过期且在等执行的必须被找到");
        assertFalse(ids.contains(b.getTaskId()), "租约未过期的不该被找到");
        assertFalse(ids.contains(c.getTaskId()), "已到终态的不该被找到，否则会重派一个已完成的任务");
    }

    @Test
    @DisplayName("续租把到期时间往后推；撤租（0）让任务彻底离开重派候选")
    void renewAndClearLease() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        String id = rec.getTaskId();
        store.markDispatched(id, "k1", -1);
        assertFalse(store.findExpiredLeases().isEmpty());

        store.renewLease(id, 60_000);
        assertTrue(store.findExpiredLeases().isEmpty(), "续租后不该再被判超时");

        // 等待人工审批时撤租：等人期间执行者本来就没有进展，
        // 留着租约会被 Reaper 误杀。
        store.transition(id, TaskState.RUNNING);
        store.transition(id, TaskState.WAITING_APPROVAL);
        store.renewLease(id, 0);
        assertEquals(0L, store.require(id).getLeaseExpiresAt());
        assertTrue(store.findExpiredLeases().isEmpty());
    }

    @Test
    @DisplayName("审批等待中的任务单独可见 —— 运维要能回答「卡在等人」的有几个")
    void waitingApprovalListing() {
        TaskRecord rec = store.submit("x", null, Map.of()).record();
        String id = rec.getTaskId();
        store.markDispatched(id, "k1", 60_000);
        store.transition(id, TaskState.RUNNING);
        store.transition(id, TaskState.WAITING_APPROVAL);

        assertEquals(List.of(id), store.waitingApproval());
        assertEquals(Map.of("waiting_approval", 1L), store.countByState());
    }

    @Test
    @DisplayName("列表按创建时间倒序 —— 界面上最新提交的必须在最上面")
    void listIsNewestFirst() {
        TaskRecord first = store.submit("1", null, Map.of()).record();
        TaskRecord second = store.submit("2", null, Map.of()).record();
        List<TaskRecord> list = store.list(10);
        assertEquals(second.getTaskId(), list.get(0).getTaskId());
        assertEquals(first.getTaskId(), list.get(1).getTaskId());
    }

    @Test
    @DisplayName("对外视图不泄露内部字段（options 不出去）")
    void viewDoesNotLeakInternals() {
        TaskRecord rec = store.submit("x", "k", Map.of("secret", "s")).record();
        Map<String, Object> view = rec.toView();
        assertFalse(view.containsKey("options"), "options 可能带内部参数，不应直接外泄");
        assertTrue(view.containsKey("state"));
        assertTrue(view.containsKey("attempts"));
    }

    @Test
    @DisplayName("配置默认值符合文档里写的权衡")
    void defaultLeaseConfig() {
        GatewayProperties props = new GatewayProperties();
        assertEquals(600, props.getLease().getDurationSeconds());
        assertEquals(2, props.getLease().getMaxAttempts());
        assertEquals(30, props.getLease().getReapIntervalSeconds());
        assertTrue(props.getLease().getReapIntervalSeconds() * 10 <= props.getLease().getDurationSeconds(),
                "扫描间隔必须显著小于租约时长，否则过期任务会被拖到下一个周期");
    }
}
