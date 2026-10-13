package com.devpilot.gateway.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 状态机测试。
 *
 * <p>为什么状态机值得单独测：它是**唯一真相**。所有上层逻辑（审批能不能点、
 * 结果上报要不要收、能不能重派）都以它为准。它错了，错的是整个系统，
 * 而且是静默地错 —— 不会报异常，只会让任务走进一个没人预期过的状态。
 */
class TaskStateTest {

    @Test
    @DisplayName("终态没有出边 —— 历史只追加，重跑必须新建任务")
    void terminalStatesHaveNoExit() {
        for (TaskState t : TaskState.TERMINAL) {
            assertTrue(t.allowedNext().isEmpty(), t + " 不该有出边");
            for (TaskState any : TaskState.values()) {
                assertFalse(t.canTransitionTo(any), t + " → " + any + " 必须被拒绝");
            }
        }
    }

    @Test
    @DisplayName("审批未给结果不能直接成功 —— 否则等于把人当摆设")
    void cannotSucceedStraightFromWaitingApproval() {
        assertFalse(TaskState.WAITING_APPROVAL.canTransitionTo(TaskState.SUCCEEDED));
        assertTrue(TaskState.WAITING_APPROVAL.canTransitionTo(TaskState.RUNNING));
        assertTrue(TaskState.WAITING_APPROVAL.canTransitionTo(TaskState.FAILED));
        assertTrue(TaskState.WAITING_APPROVAL.canTransitionTo(TaskState.CANCELLED));
    }

    @Test
    @DisplayName("必须先派发才能运行 —— 否则是幽灵任务")
    void cannotRunWithoutDispatch() {
        assertFalse(TaskState.PENDING.canTransitionTo(TaskState.RUNNING));
        assertTrue(TaskState.PENDING.canTransitionTo(TaskState.DISPATCHED));
        assertTrue(TaskState.PENDING.canTransitionTo(TaskState.CANCELLED));
    }

    @Test
    @DisplayName("受理后派不出去可以失败 —— 否则任务会卡在 PENDING 变成僵尸")
    void dispatchFailureIsAPossibleOutcome() {
        assertTrue(TaskState.PENDING.canTransitionTo(TaskState.FAILED),
                "PENDING 不在重派候选里，若不允许它失败，这个任务就既不会失败也不会被重派");
        assertTrue(TaskState.TIMEOUT.canTransitionTo(TaskState.FAILED));
    }

    @Test
    @DisplayName("控制面校验（canTransitionTo）比执行面上报（acceptsReportOf）更严")
    void controlPlaneIsStricterThanReportPlane() {
        // 网关自己发起动作时，必须严格遵守转移表。
        assertFalse(TaskState.WAITING_APPROVAL.canTransitionTo(TaskState.SUCCEEDED));
        assertFalse(TaskState.PENDING.canTransitionTo(TaskState.RUNNING));

        // 但它对执行面上报必须放宽：事件流可能断，网关无法保证看到每一个中间态。
        // 若这里也严格，一条"从 DISPATCHED 直接到 SUCCEEDED"的合法回报会被丢弃，
        // 任务就变成僵尸。
        assertTrue(TaskState.DISPATCHED.acceptsReportOf(TaskState.SUCCEEDED));
        assertTrue(TaskState.WAITING_APPROVAL.acceptsReportOf(TaskState.SUCCEEDED));
        assertTrue(TaskState.DISPATCHED.acceptsReportOf(TaskState.FAILED));

        // 放宽的边界只有两条：终态不可改写、没派发过不接受执行结果。
        for (TaskState terminal : TaskState.TERMINAL) {
            for (TaskState any : TaskState.values()) {
                assertFalse(terminal.acceptsReportOf(any),
                        terminal + " 是终态，任何上报都该被丢弃");
            }
        }
        for (TaskState any : TaskState.values()) {
            assertFalse(TaskState.PENDING.acceptsReportOf(any),
                    "从没派发过，哪来的执行结果");
        }
    }

    @Test
    @DisplayName("TIMEOUT 不是终态 —— 它表示「这次失联了」，还要重派")
    void timeoutIsNotTerminal() {
        assertFalse(TaskState.TIMEOUT.isTerminal());
        assertTrue(TaskState.TIMEOUT.canTransitionTo(TaskState.DISPATCHED),
                "超时后必须能重派");
        assertTrue(TaskState.TIMEOUT.canTransitionTo(TaskState.FAILED),
                "重派次数用尽后必须能判失败");
    }

    @Test
    @DisplayName("非法转移抛异常，且错误信息里带上「允许去哪些」")
    void illegalTransitionThrowsWithHelpfulMessage() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> TaskState.SUCCEEDED.requireTransitionTo(TaskState.RUNNING));
        assertTrue(e.getMessage().contains("SUCCEEDED"));
        assertTrue(e.getMessage().contains("RUNNING"));
        assertTrue(e.getMessage().contains("终态"), "必须说清为什么不行：实际=" + e.getMessage());
    }

    @Test
    @DisplayName("转移表只允许到达真实存在的状态，且每一条都可达")
    void transitionTableIsSane() {
        for (TaskState from : TaskState.values()) {
            Set<TaskState> next = from.allowedNext();
            if (from.isTerminal()) {
                assertTrue(next.isEmpty());
                continue;
            }
            assertFalse(next.isEmpty(), from + " 是非终态就应当有出边，否则任务会被卡死");
            assertFalse(next.contains(from), from + " 不该自环");
            for (TaskState to : next) {
                assertTrue(TaskState.TERMINAL.contains(to) || !to.isTerminal());
            }
        }
    }

    @Test
    @DisplayName("RUNNING 的出口覆盖全部可能的结局")
    void runningCoversAllOutcomes() {
        assertEquals(
                EnumSet.of(TaskState.WAITING_APPROVAL, TaskState.SUCCEEDED,
                        TaskState.FAILED, TaskState.TIMEOUT, TaskState.CANCELLED),
                TaskState.RUNNING.allowedNext());
    }
}
