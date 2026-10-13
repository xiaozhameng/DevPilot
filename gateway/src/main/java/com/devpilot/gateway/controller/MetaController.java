package com.devpilot.gateway.controller;

import com.devpilot.gateway.config.GatewayProperties;
import com.devpilot.gateway.service.GatewayEventBus;
import com.devpilot.gateway.service.KernelClient;
import com.devpilot.gateway.service.TaskStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 运维/元信息接口。
 *
 * <h2>健康检查为什么要"聚合"内核的状态</h2>
 *
 * 网关自己健康，不等于系统可用。内核挂了的时候：
 * <ul>
 *   <li>网关能正常受理任务（写内存）</li>
 *   <li>但派发全部失败（连不上内核）</li>
 * </ul>
 * 如果健康检查只看自己，它会一直报 "UP"，而实际上没有任何任务能跑成。
 * 这种"报健康的坏系统"比"报坏的坏系统"危险得多。
 *
 * <p>所以这里的 {@code /api/health} 一定会去问内核一句，
 * 并把内核的状态单独列出来。内核不可达时网关仍然返回 200，
 * 但 {@code kernel.reachable=false} —— 让调用方自己判断"能不能干活"。
 */
@RestController
@RequestMapping("/api")
public class MetaController {

    private final TaskStore store;
    private final KernelClient kernel;
    private final GatewayEventBus bus;
    private final GatewayProperties props;

    public MetaController(TaskStore store, KernelClient kernel, GatewayEventBus bus,
                          GatewayProperties props) {
        this.store = store;
        this.kernel = kernel;
        this.bus = bus;
        this.props = props;
    }

    @GetMapping("/health")
    public Mono<Map<String, Object>> health() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("ok", true);
        out.put("service", "devpilot-gateway");
        out.put("at", Instant.now().toString());
        out.put("gateway", Map.of(
                "tasks", store.countByState(),
                "waitingApproval", store.waitingApproval(),
                "trackedEventStreams", bus.trackedTasks(),
                "lease", Map.of(
                        "durationSeconds", props.getLease().getDurationSeconds(),
                        "maxAttempts", props.getLease().getMaxAttempts(),
                        "reapIntervalSeconds", props.getLease().getReapIntervalSeconds())
        ));

        return kernel.health()
                .map(k -> {
                    out.put("kernel", k);
                    out.put("kernelReachable", true);
                    return out;
                })
                .onErrorResume(e -> {
                    out.put("kernelReachable", false);
                    out.put("kernelError", e.getClass().getSimpleName() + ": " + e.getMessage());
                    return Mono.just(out);
                });
    }

    /**
     * 流水线结构（直接透传内核）。
     *
     * <p>演示页用它画流程图。为什么不让演示页直接连内核？
     * 因为那要求浏览器能访问内网地址，且知道内核的 token ——
     * 那是把内网服务暴露给前端。网关做这一跳，前端就只需要认识一个地址。
     */
    @GetMapping("/pipeline")
    public Mono<Map<String, Object>> pipeline() {
        return kernel.pipeline().map(node -> {
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("graph", node);
            return out;
        });
    }
}
