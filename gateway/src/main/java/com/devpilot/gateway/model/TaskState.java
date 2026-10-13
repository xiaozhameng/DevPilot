package com.devpilot.gateway.model;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * 任务状态机。
 *
 * <p>为什么不用几个布尔字段（isRunning / isDone / isFailed）表示状态？
 * 三个绕不过去的问题：
 *
 * <ol>
 *   <li><b>组合爆炸且大多非法。</b>三个布尔量有 8 种组合，其中 5 种是矛盾的
 *       （isRunning=true 且 isDone=true 是什么意思？）。代码里没有任何地方
 *       阻止你写出这种状态，所以它一定会在并发下被写出来。</li>
 *   <li><b>没有"非法转移"这个概念。</b>一个已经 SUCCEEDED 的任务能不能被
 *       重派改回 RUNNING？布尔量版本连"转移"这个对象都没有，答不上来。</li>
 *   <li><b>查不了。</b>运维问"现在有多少个卡在等审批"，布尔量版本要拼一串
 *       与条件，而每个条件都可能漏掉一种组合；枚举版本就是一次分组计数。</li>
 * </ol>
 *
 * <p>显式状态机把这三件事一次性解决：状态是有限枚举，转移是显式的表，
 * 任何不在表里的转移被直接拒绝并抛异常 —— 错误在发生的那一行暴露，
 * 而不是在三天后的一堆脏数据里。
 *
 * <p>这张表和 Python 内核侧的 {@code state.py} 是**刻意对齐**的。
 * 两侧都维护同一套状态语义，是因为网关是"控制面"、内核是"执行面"，
 * 它们必须对"这个任务现在算成功还是失败"有完全一致的理解。
 * 如果两边的状态集合不一样，就一定会在某个边界上出现
 * "网关认为成功了、内核认为还在跑"的幽灵任务。
 */
public enum TaskState {

    /** 已受理，还没派给内核。 */
    PENDING,

    /** 已派发，等内核受理。 */
    DISPATCHED,

    /** 内核正在执行。 */
    RUNNING,

    /** 挂在人工审批点，等外部放行。 */
    WAITING_APPROVAL,

    /** 识别到执行副本失联（租约过期），准备重派。 */
    TIMEOUT,

    /** 终态。 */
    SUCCEEDED,

    /** 终态。 */
    FAILED,

    /** 终态。 */
    CANCELLED;

    /** 三个终态：进去就出不来。 */
    public static final Set<TaskState> TERMINAL =
            EnumSet.of(SUCCEEDED, FAILED, CANCELLED);

    /**
     * 合法转移表 —— 这是**网关自己发起动作**时的全部定义，别处不许绕过它改状态。
     *
     * <p>几个刻意的设计：
     * <ul>
     *   <li>{@code WAITING_APPROVAL → SUCCEEDED} <b>不允许</b>。
     *       审批还没给结果就报成功，等于把人当摆设。要成功必须
     *       先回 RUNNING 再跑完。</li>
     *   <li>终态没有出边。想重跑就新建任务 —— 这保证任务历史是**只追加**的，
     *       事后审计永远能还原真相。所以 {@code TIMEOUT} 不是终态：
     *       它表示"这次执行副本失联了"，还要重派，重派次数用尽才进 FAILED。</li>
     *   <li>{@code PENDING → RUNNING} 不允许，必须经过 DISPATCHED。
     *       跳过去意味着"没派发就执行"，那是幽灵任务。
     *       但 {@code PENDING → FAILED} 允许：受理后派不出去也是一个真实结局，
     *       不允许的话任务会卡在 PENDING（而它不在重派候选里）变成僵尸。</li>
     *   <li>{@code WAITING_APPROVAL → TIMEOUT} 允许，而且是必需的：
     *       审批等待本身也有上限（内核侧默认 30 分钟）。如果内核在等人期间
     *       挂了，这个任务就再也没人推进 —— 必须能被超时机制接住、重新派发。</li>
     * </ul>
     *
     * <p><b>注意：这张表只约束"网关自己发起的动作"。</b>
     * 执行面（内核）回报的状态走 {@link #acceptsReportOf} 那套更宽的规则，
     * 原因见那个方法的说明 —— 两套规则并存不是不一致，而是因为
     * 网关对这两个来源的**可见性**不一样。
     */
    private static final Map<TaskState, Set<TaskState>> TRANSITIONS = Map.of(
            // PENDING → FAILED 是必需的：受理之后"派不出去"（内核不可达、
            // 被拒绝）是一个真实的结局。少了这条边，任务会永远停在 PENDING ——
            // 而 PENDING 又不在重派候选里，于是它既不会失败也不会被重派，
            // 成了一个谁都看不见的僵尸。
            PENDING, EnumSet.of(DISPATCHED, FAILED, CANCELLED),
            DISPATCHED, EnumSet.of(RUNNING, TIMEOUT, CANCELLED),
            RUNNING, EnumSet.of(WAITING_APPROVAL, SUCCEEDED, FAILED, TIMEOUT, CANCELLED),
            WAITING_APPROVAL, EnumSet.of(RUNNING, FAILED, TIMEOUT, CANCELLED),
            // 超时不是终点：要么重派（回 DISPATCHED），要么重派次数用尽（FAILED）。
            TIMEOUT, EnumSet.of(DISPATCHED, FAILED, CANCELLED),
            SUCCEEDED, EnumSet.noneOf(TaskState.class),
            FAILED, EnumSet.noneOf(TaskState.class),
            CANCELLED, EnumSet.noneOf(TaskState.class)
    );

    public boolean canTransitionTo(TaskState next) {
        return TRANSITIONS.getOrDefault(this, EnumSet.noneOf(TaskState.class)).contains(next);
    }

    public Set<TaskState> allowedNext() {
        return TRANSITIONS.getOrDefault(this, EnumSet.noneOf(TaskState.class));
    }

    public boolean isTerminal() {
        return TERMINAL.contains(this);
    }

    /**
     * 执行面回报的状态是否该被采纳 —— 比 {@link #canTransitionTo} **宽**，这是故意的。
     *
     * <h2>为什么两套规则，而不是一套</h2>
     *
     * 因为网关对两种来源的**可见性**完全不同：
     * <ul>
     *   <li><b>网关自己的动作</b>（取消、派发、重派）：它知道任务的当前状态，
     *       也知道自己要做什么，所以能、也应该用严格校验 ——
     *       非法就是代码 bug，必须立刻炸出来。</li>
     *   <li><b>执行面的回报</b>：网关只能**旁观**。内核是通过异步事件流说话的，
     *       这条流可能断、可能重连、可能在内核崩溃时丢掉最后几条。
     *       网关无法保证观察到每一个中间态 —— 于是"从 DISPATCHED 直接到 SUCCEEDED"
     *       这种序列是**完全可能真实发生的**。</li>
     * </ul>
     *
     * <p>若对回报也用严格表，后果是：一条合法的终态上报被当成"过期上报"丢弃，
     * 任务就永远停在中间态，直到租约过期才被重派 —— 而它其实早就跑完了。
     * 这就是**僵尸任务**。用一个更宽但语义正确的规则去接住它，
     * 比维护一张漂亮的、和现实对不上的转移表更有价值。
     *
     * <p>放宽的边界仍然明确，只有两条：
     * <ol>
     *   <li>终态不可改写（成功之后收到失败必须丢弃）；</li>
     *   <li>{@code PENDING} 不接受任何执行结果（从没派发过，哪来的结果）。</li>
     * </ol>
     * 其余一律视为"执行面的前进"，采纳。
     */
    public boolean acceptsReportOf(TaskState reported) {
        return !isTerminal() && this != PENDING;
    }

    /** 非法转移直接抛异常。错误信息必须带上"允许去哪些"，否则排查时要翻代码。 */
    public void requireTransitionTo(TaskState next) {
        if (!canTransitionTo(next)) {
            throw new IllegalStateException(
                    "非法状态转移：" + this + " → " + next
                            + "。允许的下一步是 " + (allowedNext().isEmpty() ? "无（终态）" : allowedNext()));
        }
    }
}
