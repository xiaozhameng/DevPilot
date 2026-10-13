package com.devpilot.gateway.service;

import com.devpilot.gateway.config.GatewayProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 审计日志：把网关的关键动作落成 JSONL（一行一个 JSON）。
 *
 * <h2>为什么要有它 —— 任务表已经记了状态，还差什么</h2>
 *
 * 任务表记的是**当前状态**：这个任务现在是 RUNNING。
 * 审计日志记的是**发生的动作**：谁在什么时候点了审批、第几次重派是因为租约过期。
 *
 * <p>这两者的差别在排查问题时是决定性的。用户说"我明明点了通过"，任务表只会告诉你
 * 它现在是 WAITING_APPROVAL —— 你无法区分"请求没到"和"请求到了但被拒绝"。
 * 审计日志能直接回答。
 *
 * <p>格式选 JSONL 而不是普通日志行，是因为它**既是给人看的、又是给机器读的**：
 * 一个 {@code grep} 能定位，一个 {@code jq} 能统计，不需要写正则去解析。
 *
 * <h2>写盘不阻塞业务</h2>
 *
 * 审计是"旁路"：它再重要也不该拖慢主流程。所以用一个单线程队列异步写。
 * 单线程保证了行内顺序与发生顺序一致（多线程写会出现"先发生的后落盘"，
 * 审计链就乱了）。
 *
 * <p>进程退出时 {@link #flush()} 会等在途日志写完 —— 丢审计比慢一点更糟。
 */
@Component
public class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final GatewayProperties props;
    private final ObjectMapper json = new ObjectMapper();
    private final ExecutorService writer = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "audit-writer");
        t.setDaemon(true);
        return t;
    });

    public AuditLog(GatewayProperties props) {
        this.props = props;
    }

    /**
     * 记一条审计。
     *
     * <p>注意这个方法是**同步返回、异步落盘**。调用方不会因为磁盘慢而被拖住，
     * 但也不能假设"方法返回时已经写进文件了"。
     */
    public void record(String action, String taskId, Map<String, Object> detail) {
        if (!props.getAudit().isEnabled()) {
            return;
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("at", Instant.now().toString());
        row.put("action", action);
        row.put("taskId", taskId);
        row.put("detail", detail == null ? Map.of() : detail);

        String line;
        try {
            line = json.writeValueAsString(row);
        } catch (Exception e) {
            log.warn("审计序列化失败：{}", e.toString());
            return;
        }
        writer.submit(() -> append(line));
    }

    private void append(String line) {
        Path path = Path.of(props.getAudit().getPath());
        try {
            Path parent = path.getParent();
            if (parent != null && !Files.isDirectory(parent)) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            // 审计写不进去不是业务失败（业务已经完成了），所以只记警告。
            // 但如果反复出现，说明配置有问题（目录只读、磁盘满）。
            log.warn("审计写入失败：{}", e.toString());
        }
    }

    @PreDestroy
    void flush() {
        writer.shutdown();
        try {
            if (!writer.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("审计队列在 5 秒内未排空，可能有日志丢失");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
