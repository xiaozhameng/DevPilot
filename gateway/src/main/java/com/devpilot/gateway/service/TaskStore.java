package com.devpilot.gateway.service;

import com.devpilot.gateway.model.TaskRecord;
import com.devpilot.gateway.model.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务表。网关侧的"账本"，也是这一层存在的理由。
 *
 * <h2>为什么任务表在网关，而不在内核</h2>
 *
 * 内核负责"把活干完"，网关负责"这件事有没有人干、干到哪了、干了几次"。
 * 这两件事的失败模式完全不同：
 * <ul>
 *   <li>内核会崩、会重启、会因为模型超时而卡住 —— 它是**可能失联的执行者**。</li>
 *   <li>网关是**控制面**：它必须始终知道每个任务的状态，即使执行者失联了。</li>
 * </ul>
 * 如果把任务表放在内核里，内核一崩，任务状态就丢了，
 * 于是"哪些任务需要重派"这个问题没人能回答。
 *
 * <h2>不重不漏靠什么</h2>
 *
 * <b>不重</b>（幂等）：靠 {@code ConcurrentHashMap.putIfAbsent} 的原子性。
 * 注意这里没有写"先 get 判断存在、再 put"——那是典型的 check-then-act 竞态：
 * 两个请求同时 get 都拿到 null，然后都 put，于是同一个幂等键产生了两个任务。
 * putIfAbsent 把"检查+写入"合成一个原子操作，没有这个窗口。
 *
 * <b>不漏</b>（不丢任务）：靠租约 + 超时重派（见 {@link TaskDispatcher}）。
 * 派发出去的任务如果执行者失联，租约到期就会被重新派发。
 *
 * <h2>诚实说明：这套实现在多副本下会失效</h2>
 *
 * 内存 Map 只在**单实例**下成立。网关一旦部署两个副本，
 * A 副本的 putIfAbsent 对 B 副本不可见 —— 同一个幂等键会被两个副本各建一个任务，
 * 「不重」直接破功。
 *
 * 生产做法是把这张表换成带唯一索引的数据库（幂等键上建 UNIQUE），
 * 让"唯一性"由存储层保证，而不是由某台机器的内存保证。
 * 再进一步，重派要靠带租约的分布式锁（Redis SETNX + TTL，
 * 或数据库行锁 + 版本号）。
 *
 * 这里选择内存实现，是因为要讲清楚的是**机制**（原子占位、状态机、租约、
 * 超时重派），而换成 DB 只是把这三个动作换了个执行者，逻辑形状不变。
 * 反过来说，知道"它为什么在单机成立、在多副本失效、怎么改"，
 * 比用一个自己讲不清内部行为的分布式组件更有价值。
 */
@Component
public class TaskStore {

    private static final Logger log = LoggerFactory.getLogger(TaskStore.class);

    /** 任务 id → 记录。 */
    private final Map<String, TaskRecord> byId = new ConcurrentHashMap<>();

    /** 幂等键 → 记录。这张表的存在就是为了让 putIfAbsent 一次完成"查重"。 */
    private final Map<String, TaskRecord> byIdemKey = new ConcurrentHashMap<>();

    /** 提交结果：记录本身 + 是否命中了幂等键（复用而非新建）。 */
    public record SubmitOutcome(TaskRecord record, boolean deduplicated) {}

    /**
     * 受理一个任务。
     *
     * <p>幂等键命中时返回**原记录**，并标记 {@code deduplicated=true}。
     * 为什么要把这个标记传出去？因为调用方无法从别处区分
     * "我的重试命中了已有任务"和"系统真的建了两个任务"——
     * 而这两种情况下它该做的事完全不同。
     */
    public SubmitOutcome submit(String requirement, String idemKey, Map<String, Object> options) {
        if (idemKey != null && !idemKey.isBlank()) {
            TaskRecord fresh = newRecord(requirement, idemKey, options);
            TaskRecord existing = byIdemKey.putIfAbsent(idemKey, fresh);
            if (existing != null) {
                log.info("幂等键 {} 命中，复用任务 {}（丢弃刚生成的 {}）",
                        idemKey, existing.getTaskId(), fresh.getTaskId());
                return new SubmitOutcome(existing, true);
            }
            byId.put(fresh.getTaskId(), fresh);
            return new SubmitOutcome(fresh, false);
        }

        TaskRecord rec = newRecord(requirement, null, options);
        byId.put(rec.getTaskId(), rec);
        return new SubmitOutcome(rec, false);
    }

    private TaskRecord newRecord(String requirement, String idemKey, Map<String, Object> options) {
        // 12 位十六进制：够用且短，方便在日志和 URL 里肉眼对照。
        String id = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        return new TaskRecord(id, requirement, idemKey, options);
    }

    public TaskRecord find(String taskId) {
        return byId.get(taskId);
    }

    public TaskRecord require(String taskId) {
        TaskRecord rec = byId.get(taskId);
        if (rec == null) {
            throw new NoSuchElementException("任务不存在：" + taskId);
        }
        return rec;
    }

    public List<TaskRecord> list(int limit) {
        return byId.values().stream()
                .sorted(Comparator.comparing(TaskRecord::getCreatedAt).reversed())
                .limit(limit)
                .toList();
    }

    public Map<String, Long> countByState() {
        Map<String, Long> m = new java.util.TreeMap<>();
        for (TaskState s : TaskState.values()) {
            long n = byId.values().stream().filter(r -> r.getState() == s).count();
            if (n > 0) {
                m.put(s.name().toLowerCase(), n);
            }
        }
        return m;
    }

    // ============================================================
    // 状态流转 —— 唯一入口，因此校验只写一处
    // ============================================================

    /**
     * 强制流转（调用方确信这次转移合法）。
     *
     * <p>非法转移直接抛异常。这是内部调用路径用的（提交、取消这类
     * 由我们自己发起的动作）：如果它非法，说明代码有 bug，应该立刻暴露。
     */
    public TaskRecord transition(String taskId, TaskState to) {
        TaskRecord rec = require(taskId);
        synchronized (rec) {
            rec.getState().requireTransitionTo(to);
            rec.setState(to);
        }
        return rec;
    }

    /**
     * 尝试采纳一次执行面上报，不合法时**返回 false 而不是抛异常**。
     *
     * <p>这个方法是专门给"外部上报"用的，比如内核回调说"我成功了"。
     * 为什么这里不能抛？因为外部上报是**不可信输入**：
     * 用户可能刚点了取消，而内核随后才回报成功（它不知道被取消了）。
     * 这时候抛异常会让网关报 500，而正确行为是**丢弃这次过期上报**并记一条日志。
     *
     * <p>校验用的是 {@link TaskState#acceptsReportOf}（比转移表宽），
     * 因为网关只能旁观执行面，无法保证看到每一个中间态 ——
     * 详见那个方法的说明。
     *
     * <p>"内部动作用强校验、外部上报用弱校验"这个区分很重要，
     * 混用会导致要么该拦的没拦（都弱），要么正常的竞态被当成崩溃（都强）。
     */
    public boolean tryTransition(String taskId, TaskState to) {
        TaskRecord rec = byId.get(taskId);
        if (rec == null) {
            return false;
        }
        synchronized (rec) {
            if (!rec.getState().acceptsReportOf(to)) {
                log.warn("丢弃过期上报：任务 {} 当前 {}，收到 {}", taskId, rec.getState(), to);
                return false;
            }
            rec.setState(to);
            return true;
        }
    }

    /**
     * 标记已派发：写内核 id、派发次数 +1、**续租**。
     *
     * <p>三件事必须在同一个同步块里完成。分开做的话，
     * 会出现"次数已经加了但租约还没续"的中间态 ——
     * 如果 Reaper 恰好在那一刻扫描，会把这个任务判成超时并重派，
     * 于是同一个任务被派了两遍。
     */
    public TaskRecord markDispatched(String taskId, String kernelTaskId, long leaseMillis) {
        TaskRecord rec = require(taskId);
        synchronized (rec) {
            rec.getState().requireTransitionTo(TaskState.DISPATCHED);
            rec.setKernelTaskId(kernelTaskId);
            rec.setAttempts(rec.getAttempts() + 1);
            rec.setLeaseExpiresAt(System.currentTimeMillis() + leaseMillis);
            rec.setState(TaskState.DISPATCHED);
        }
        return rec;
    }

    /** 派发失败（网络错误、内核拒绝）。不计入 attempts —— 那是"执行"的次数，不是"提交"的次数。 */
    public TaskRecord markDispatchFailed(String taskId, String reason) {
        TaskRecord rec = require(taskId);
        synchronized (rec) {
            rec.setError(reason);
            rec.setLeaseExpiresAt(0L);
            if (rec.getState().canTransitionTo(TaskState.FAILED)) {
                rec.setState(TaskState.FAILED);
            }
        }
        return rec;
    }

    /**
     * 采纳一次执行面的终态上报：写状态、写结果/错误、撤租约。
     *
     * <p>用 {@link TaskState#acceptsReportOf} 判定（弱校验），原因同上。
     * 返回 null 表示任务不存在；返回的记录若状态**不等于** {@code to}，
     * 说明这次上报被丢弃了（任务已是终态）—— 调用方据此判断是否真的生效。
     */
    public TaskRecord markResult(String taskId, TaskState to, Map<String, Object> result,
                                 String error) {
        TaskRecord rec = byId.get(taskId);
        if (rec == null) {
            return null;
        }
        synchronized (rec) {
            if (!rec.getState().acceptsReportOf(to)) {
                log.warn("结果上报被丢弃：任务 {} 当前 {}，收到 {}", taskId, rec.getState(), to);
                return rec;
            }
            rec.setState(to);
            rec.setResult(result);
            rec.setError(error);
            rec.setLeaseExpiresAt(0L);       // 终态不再持有租约
        }
        return rec;
    }

    // ============================================================
    // 租约
    // ============================================================

    /**
     * 找出所有"租约已过期且还没到终态"的任务 —— 它们是超时重派的候选。
     *
     * <p>三个状态都要包含，各自对应一类真实的失联：
     * <ul>
     *   <li>{@code DISPATCHED}：刚派出去内核就没影了（最常见的冷启动失败）。</li>
     *   <li>{@code RUNNING}：跑到一半内核挂了。</li>
     *   <li>{@code WAITING_APPROVAL}：**挂在审批点上内核挂了**。
     *       这一类最隐蔽 —— 界面上显示"等待审批"，用户点了按钮却报错，
     *       因为对面根本没人还在等。不把它纳入候选，这个任务就永远卡住。</li>
     * </ul>
     *
     * <p>{@code TIMEOUT} 也在候选里：那是"上一轮重派又失败了"的任务，
     * 需要等下一个周期再试（租约被推到了下一轮）。
     */
    public List<TaskRecord> findExpiredLeases() {
        long now = System.currentTimeMillis();
        return byId.values().stream()
                .filter(r -> r.getState() == TaskState.RUNNING
                        || r.getState() == TaskState.DISPATCHED
                        || r.getState() == TaskState.WAITING_APPROVAL
                        || r.getState() == TaskState.TIMEOUT)
                .filter(r -> r.isLeaseExpired(now))
                .sorted(Comparator.comparing(TaskRecord::getCreatedAt))
                .toList();
    }

    /**
     * 续租：确认执行者还活着时调用。
     *
     * <p>{@code leaseMillis <= 0} 表示**主动撤租**（把 leaseExpiresAt 置 0 =
     * 当前没有有效租约）。这个语义是给"任务进入等待人工审批"用的：
     * 等人期间执行者是**故意**不产生进展的，如果还留着租约，
     * Reaper 会在到期时把它判成失联并重派，把正在等人的任务杀掉。
     */
    public void renewLease(String taskId, long leaseMillis) {
        TaskRecord rec = byId.get(taskId);
        if (rec == null) {
            return;
        }
        synchronized (rec) {
            if (!rec.getState().isTerminal()) {
                rec.setLeaseExpiresAt(leaseMillis <= 0
                        ? 0L
                        : System.currentTimeMillis() + leaseMillis);
            }
        }
    }

    /** 无人认领的等待审批任务（用于健康检查里展示"卡在等人"的任务）。 */
    public List<String> waitingApproval() {
        return byId.values().stream()
                .filter(r -> r.getState() == TaskState.WAITING_APPROVAL)
                .map(TaskRecord::getTaskId)
                .toList();
    }

    /** 供测试与调试：清空全部数据。 */
    public void clear() {
        byId.clear();
        byIdemKey.clear();
    }
}
