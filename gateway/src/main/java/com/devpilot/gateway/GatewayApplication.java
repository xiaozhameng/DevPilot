package com.devpilot.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * DevPilot 网关。
 *
 * <h2>这个进程在系统里的位置</h2>
 *
 * <pre>
 *   浏览器 / 命令行
 *        │  /api/**        （对外接口，将来在这里做鉴权、限流、多租户）
 *        ▼
 *   ┌──────────────┐
 *   │ Java 网关     │  ← 控制面：任务表、状态机、幂等、租约、超时重派、审计
 *   └──────┬───────┘
 *          │  /v1/**  + X-Internal-Token  （内网接口）
 *          ▼
 *   ┌──────────────┐
 *   │ Python 内核   │  ← 执行面：编排引擎、Agent 节点、沙箱、检查点
 *   └──────────────┘
 * </pre>
 *
 * <h2>为什么这么切，而不是让内核一个人全干了</h2>
 *
 * 因为两者的**失败模式与生命周期完全不同**，混在一起会让"任务账本"和
 * "任务执行"共享同一个生死。内核是重活（跑模型、起子进程、等审批），
 * 它必然会重启、会 OOM、会被升级；网关是轻活（内存表 + 定时扫描），
 * 它可以一直活着。
 *
 * <p>把账本放在"更可能一直活着"的那一侧，超时重派才有意义 ——
 * 否则收尸的人自己也死了，谁来收尸。
 *
 * <h2>{@code @EnableScheduling}</h2>
 *
 * 是给 {@link com.devpilot.gateway.service.Reaper} 用的。
 * 定时重派是这个系统"不漏"的唯一保证，所以调度器必须是启动类级别开启的，
 * 不能依赖某个配置开关 —— 忘了开就是静默的漏任务。
 */
@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
public class GatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
