package com.devpilot.gateway.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * 一条任务记录 —— 网关侧的任务表行。
 *
 * <p>可变对象，但所有字段都是 volatile，且**状态变更必须在 TaskStore 里
 * 持有该对象锁进行**。为什么不在这个类里加 synchronized 方法？
 * 因为状态转移需要"读当前状态 → 校验 → 写新状态 + 改其他字段"这三步原子完成，
 * 把粒度留在 TaskStore 更容易看清楚哪几步是必须一起完成的。
 * 分散到各个 setter 上会让人误以为"每个 setter 原子"就等于"整个转移原子"。
 *
 * <p>{@code kernelTaskId} 与业务 id 是两回事：
 * 网关的 {@code taskId} 在受理时就生成（幂等键要挂在它上面），
 * 内核的 id 要等派发成功才拿得到。两者都留着，
 * 是为了排障时能一眼看出"这个任务是派给哪个内核实例的哪个执行"。
 */
public class TaskRecord {

    private final String taskId;
    private final String requirement;
    private final String idempotencyKey;
    private final Map<String, Object> options;
    private final Instant createdAt;

    private volatile TaskState state = TaskState.PENDING;
    private volatile String kernelTaskId;
    /** 派发次数。超时重派时 +1，用它对上界做判断。 */
    private volatile int attempts = 0;
    /**
     * 租约到期时刻（epoch ms）。0 表示当前没有有效租约。
     *
     * <p>租约的作用：任务由内核执行，而内核可能挂着、崩了、或者网络断了。
     * 网关**不能**只等结果 —— 否则那个任务就永远停在 RUNNING。
     * 所以派发时给一个期限，到期还没收到结果就认为"执行副本失联"，重派。
     * 这是分布式任务里"检测失败"的通用做法（比心跳更省，因为不需要额外的消息）。
     */
    private volatile long leaseExpiresAt = 0L;
    private volatile String error;
    private volatile Map<String, Object> result;
    private volatile Instant updatedAt;

    public TaskRecord(String taskId, String requirement, String idempotencyKey,
                      Map<String, Object> options) {
        this.taskId = taskId;
        this.requirement = requirement;
        this.idempotencyKey = idempotencyKey;
        this.options = options == null ? new HashMap<>() : new HashMap<>(options);
        this.createdAt = Instant.now();
        this.updatedAt = this.createdAt;
    }

    public String getTaskId() { return taskId; }
    public String getRequirement() { return requirement; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public Map<String, Object> getOptions() { return options; }
    public Instant getCreatedAt() { return createdAt; }

    public TaskState getState() { return state; }
    public void setState(TaskState state) {
        this.state = state;
        this.updatedAt = Instant.now();
    }

    public String getKernelTaskId() { return kernelTaskId; }
    public void setKernelTaskId(String kernelTaskId) { this.kernelTaskId = kernelTaskId; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public long getLeaseExpiresAt() { return leaseExpiresAt; }
    public void setLeaseExpiresAt(long leaseExpiresAt) { this.leaseExpiresAt = leaseExpiresAt; }

    public String getError() { return error; }
    public void setError(String error) { this.error = error; }

    public Map<String, Object> getResult() { return result; }
    public void setResult(Map<String, Object> result) { this.result = result; }

    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }

    /** 租约是否已过期（只对"正在执行"的任务有意义）。 */
    public boolean isLeaseExpired(long nowMs) {
        return leaseExpiresAt > 0 && leaseExpiresAt < nowMs;
    }

    /** 对外视图。刻意不直接返回实体 —— 那样将来往实体里加内部字段（比如密钥）就漏出去了。 */
    public Map<String, Object> toView() {
        Map<String, Object> m = new HashMap<>();
        m.put("taskId", taskId);
        m.put("requirement", requirement);
        m.put("state", state.name().toLowerCase());
        m.put("kernelTaskId", kernelTaskId);
        m.put("attempts", attempts);
        m.put("createdAt", createdAt.toString());
        m.put("updatedAt", updatedAt.toString());
        m.put("leaseExpiresAt", leaseExpiresAt);
        m.put("error", error);
        m.put("result", result);
        return m;
    }
}
