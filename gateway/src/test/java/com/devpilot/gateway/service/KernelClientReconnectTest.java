package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 事件流的重连与续传。
 *
 * <p>这一节测的不是"正常情况下能不能收到事件"，而是**异常情况下会不会静默变哑**。
 * 后者才是这条链路上真正难查的问题：没有异常、没有错误码，
 * 只是事件不再到达，而界面上看起来一切正常（任务一直"运行中"）。
 *
 * <p>历史上这里真的踩过一次：给流加了 {@code .timeout(30s, Flux.never())}
 * 作为"别无限等"的兜底，结果 30 秒没有事件就切到 never()——
 * 既不报错也不结束，只是永远沉默。而流水线在两次模型调用之间
 * 完全可能静默超过 30 秒，于是这个 bug 在真实任务上必然发生，
 * 却完全无法从日志里看出来。
 */
class KernelClientReconnectTest {

    private GatewayProperties props;
    private KernelClient client;
    private final ObjectMapper json = new ObjectMapper();

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        // 退避压到 1 毫秒：重连逻辑本身要测，但不该让测试等 1 秒、2 秒、4 秒。
        props.getKernel().setReconnectBaseMillis(1);
        client = new KernelClient(props);
    }

    private KernelClient.KernelEvent ev(long seq, String type) {
        ObjectNode data = json.createObjectNode();
        return new KernelClient.KernelEvent(seq, type, data, System.currentTimeMillis());
    }

    @Test
    @DisplayName("流断了会自动重连，而不是静默变哑")
    void reconnectsAfterStreamError() {
        AtomicInteger attempts = new AtomicInteger();
        List<KernelClient.KernelEvent> got = client.withReconnect(ignored -> {
            int n = attempts.incrementAndGet();
            if (n < 3) {
                return Flux.error(new RuntimeException("connection reset by peer"));
            }
            return Flux.just(ev(9, "task.done"));
        }, 0).collectList().block();

        assertEquals(3, attempts.get(), "前两次都失败，应当重试到成功为止");
        assertEquals(1, got.size());
        assertEquals("task.done", got.get(0).type());
    }

    @Test
    @DisplayName("重连时从已收到的最大 seq 续传 —— 不重复、不漏")
    void reconnectResumesFromCursor() {
        List<Long> sinceValues = new ArrayList<>();
        AtomicInteger n = new AtomicInteger();

        List<KernelClient.KernelEvent> got = client.withReconnect(since -> {
            sinceValues.add(since);
            return switch (n.incrementAndGet()) {
                // 第一次：先给两条，再断开。
                case 1 -> Flux.concat(Flux.just(ev(1, "node.started"), ev(2, "log")),
                        Flux.error(new RuntimeException("broken")));
                // 第二次：连上又立刻断。
                case 2 -> Flux.error(new RuntimeException("broken again"));
                default -> Flux.just(ev(7, "task.done"));
            };
        }, 0).collectList().block();

        assertEquals(List.of(0L, 2L, 2L), sinceValues,
                "第一次从头订阅；之后必须从已收到的最大 seq（2）续传");
        assertEquals(3, got.size(), "两条旧事件 + 一条新事件，没有重复");
        assertEquals(1, got.get(0).seq());
        assertEquals(7, got.get(2).seq());
    }

    @Test
    @DisplayName("主动取消不触发重连 —— 否则取消任务后后台会一直重连")
    void cancellationDoesNotTriggerReconnect() {
        AtomicInteger attempts = new AtomicInteger();
        Flux<KernelClient.KernelEvent> stream = client.withReconnect(ignored -> {
            attempts.incrementAndGet();
            return Flux.error(new java.util.concurrent.CancellationException("disposed"));
        }, 0);

        // 取消异常不该被重试，因此会直接以错误终止。
        assertThrows(Exception.class, () -> stream.collectList().block());
        assertEquals(1, attempts.get(), "只订阅了一次，没有重试");
    }

    @Test
    @DisplayName("重连次数不受限，兜底交给租约与 Reaper（职责分离）")
    void retriesAreNotCappedHere() {
        AtomicInteger attempts = new AtomicInteger();
        List<KernelClient.KernelEvent> got = client.withReconnect(ignored -> {
            int n = attempts.incrementAndGet();
            if (n < 8) {
                return Flux.error(new RuntimeException("still down"));
            }
            return Flux.just(ev(1, "task.done"));
        }, 0).collectList().block();

        assertEquals(8, attempts.get(),
                "重连本身不设上限 —— 上限由租约 + Reaper 负责，两者不能互相替代");
        assertEquals(1, got.size());
    }

    @Test
    @DisplayName("正常结束（内核推完终态）不触发重连")
    void normalCompletionDoesNotReconnect() {
        AtomicInteger attempts = new AtomicInteger();
        client.withReconnect(ignored -> {
            attempts.incrementAndGet();
            return Flux.just(ev(1, "task.done"));
        }, 0).collectList().block();

        assertEquals(1, attempts.get(), "正常完成不是故障");
    }

    @Test
    @DisplayName("永远连不上时会持续重试，但退避封顶在 30 秒（不会忙等）")
    void backoffIsCapped() {
        // 只验证"前几次仍然会重试"，不真的等 30 秒。
        AtomicInteger attempts = new AtomicInteger();
        Flux<KernelClient.KernelEvent> stream = client.withReconnect(ignored -> {
            attempts.incrementAndGet();
            return Flux.error(new RuntimeException("down"));
        }, 0);

        assertThrows(Exception.class, () -> stream.timeout(java.time.Duration.ofSeconds(2))
                .collectList().block());
        assertTrue(attempts.get() >= 3,
                "两秒内应当已经重试了若干次（退避 1ms 起步），实际=" + attempts.get());
    }
}
