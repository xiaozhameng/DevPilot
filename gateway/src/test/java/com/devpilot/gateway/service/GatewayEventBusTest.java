package com.devpilot.gateway.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 网关事件总线的回放语义。
 *
 * <p>这里最值得钉住的一条：**任务到终态之后才连上来的订阅者，必须能看到完整历史**。
 * 这是实际使用中最常见的路径 —— 用户不是盯着页面等任务跑完，
 * 而是任务结束后才点开详情页。如果这条断了，界面上就是一片
 * "等待事件…"，而事件明明都在内存里。
 *
 * <p>实测踩过：任务已完成，详情页的事件流面板却是空的。
 * 原因是终态时把 sink 槽位整个删了，迟到的订阅者会触发
 * {@code computeIfAbsent} 新建一个空槽 —— replay 的是**那个空槽**，
 * 不是内存里的 history。
 */
class GatewayEventBusTest {

    private final GatewayEventBus bus = new GatewayEventBus();

    private void publishAll(String taskId, String... types) {
        for (String type : types) {
            bus.publish(taskId, type, Map.of("seq-note", type));
        }
    }

    @Test
    @DisplayName("终态任务：迟到的订阅者能拿到完整历史，然后流正常结束")
    void lateSubscriberReplaysTerminalHistory() {
        publishAll("t1", "node.started", "node.finished", "task.done");

        StepVerifier.create(bus.stream("t1", 0))
                .expectNextCount(3)
                .verifyComplete();          // 而且必须会结束，不能挂着
    }

    @Test
    @DisplayName("终态任务 + since 断点续传：只给缺失的那段")
    void terminalRespectSince() {
        publishAll("t2", "node.started", "node.finished", "task.done");

        StepVerifier.create(bus.stream("t2", 1))
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("非终态任务：先补历史再接直播，且不重复")
    void liveStreamDeduplicatesHistoryOverlap() {
        publishAll("t3", "node.started", "node.finished");

        StepVerifier.create(bus.stream("t3", 0).take(2))
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    @DisplayName("没有任何事件的未知任务：给一条空流而不是报错")
    void unknownTaskYieldsEmptyStream() {
        StepVerifier.create(bus.stream("nope", 0))
                .verifyTimeout(java.time.Duration.ofMillis(300));
    }

    @Test
    @DisplayName("seq 单调递增，供客户端断线续传定位")
    void seqIsMonotonic() {
        publishAll("t4", "node.started", "node.finished", "task.done");
        List<GatewayEventBus.GatewayEvent> hist = bus.history("t4", 0);
        assertThat(hist).extracting(GatewayEventBus.GatewayEvent::seq)
                .containsExactly(1L, 2L, 3L);
    }
}
