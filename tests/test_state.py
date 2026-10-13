"""状态机与存储的测试。

重点全在"非法的东西必须被拒绝"上，而不是"正常的东西能跑"。
因为状态机的价值恰恰在于挡住错误的状态组合 —— 一个只会放行的状态机
和几个布尔字段没有区别。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from devpilot.state import (
    InvalidTransition,
    Store,
    TaskStatus,
    can_transition,
    ensure_transition,
)


@pytest.fixture()
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / "test.db")


# ============================================================
# 状态转移表
# ============================================================

def test_happy_path_transitions_are_allowed():
    assert can_transition(TaskStatus.PENDING, TaskStatus.RUNNING)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.WAITING_APPROVAL)
    assert can_transition(TaskStatus.WAITING_APPROVAL, TaskStatus.RUNNING)
    assert can_transition(TaskStatus.RUNNING, TaskStatus.SUCCEEDED)


def test_terminal_states_have_no_way_out():
    """终态不可逆 —— 想重跑就新建任务，保证历史是只追加的。"""
    for terminal in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED):
        for target in TaskStatus:
            assert not can_transition(terminal, target), f"{terminal} → {target} 不该被允许"


def test_cannot_skip_execution_and_claim_success():
    """PENDING 直接 SUCCEEDED：没跑就说成功。"""
    assert not can_transition(TaskStatus.PENDING, TaskStatus.SUCCEEDED)
    with pytest.raises(InvalidTransition, match="非法状态转移"):
        ensure_transition(TaskStatus.PENDING, TaskStatus.SUCCEEDED)


def test_approval_cannot_jump_straight_to_success():
    """等待审批时直接判成功，等于把人当摆设。必须先回 RUNNING 跑完。"""
    assert not can_transition(TaskStatus.WAITING_APPROVAL, TaskStatus.SUCCEEDED)
    with pytest.raises(InvalidTransition):
        ensure_transition(TaskStatus.WAITING_APPROVAL, TaskStatus.SUCCEEDED)


def test_invalid_transition_message_lists_allowed_targets():
    try:
        ensure_transition(TaskStatus.SUCCEEDED, TaskStatus.RUNNING)
    except InvalidTransition as exc:
        assert "终态" in str(exc)
    else:
        pytest.fail("应当抛异常")


# ============================================================
# 存储
# ============================================================

def test_create_and_get_task(store: Store):
    rec = store.create_task("t1", "做一个短链服务")
    assert rec.status is TaskStatus.PENDING
    assert rec.requirement == "做一个短链服务"
    assert store.get_task("t1").task_id == "t1"
    assert store.get_task("nope") is None


def test_update_status_persists_state_and_result(store: Store):
    store.create_task("t1", "需求")
    store.update_status("t1", TaskStatus.RUNNING, state={"phase": "coding"})
    rec = store.update_status("t1", TaskStatus.SUCCEEDED,
                              state={"phase": "done"}, result={"files": 3})
    assert rec.status is TaskStatus.SUCCEEDED
    assert rec.result == {"files": 3}


def test_update_status_rejects_invalid_transition(store: Store):
    """校验在存储层，不在调用方 —— 因为调用方有很多个。"""
    store.create_task("t1", "需求")
    with pytest.raises(InvalidTransition):
        store.update_status("t1", TaskStatus.SUCCEEDED)


def test_update_unknown_task_raises_keyerror(store: Store):
    with pytest.raises(KeyError):
        store.update_status("ghost", TaskStatus.RUNNING)


def test_idempotency_key_lookup(store: Store):
    """幂等键：同一个键重复提交必须命中同一条任务，而不是新建一个。"""
    store.create_task("t1", "需求", idempotency_key="key-abc")
    found = store.find_by_idempotency("key-abc")
    assert found is not None and found.task_id == "t1"
    assert store.find_by_idempotency("key-other") is None


def test_idempotency_key_is_unique_at_db_level(store: Store):
    """唯一约束必须落在库里，不能只靠应用层先查后写 —— 并发下会双重插入。"""
    import sqlite3
    store.create_task("t1", "需求", idempotency_key="dup")
    with pytest.raises(sqlite3.IntegrityError):
        store.create_task("t2", "另一份需求", idempotency_key="dup")


def test_list_tasks_newest_first(store: Store):
    for i in range(3):
        store.create_task(f"t{i}", f"需求{i}")
    ids = [t.task_id for t in store.list_tasks()]
    assert ids[0] == "t2"


# ============================================================
# 检查点
# ============================================================

def test_checkpoint_roundtrip_keeps_visits(store: Store):
    """visits 必须存下来：不存的话恢复后刹车计数器从 0 开始。"""
    store.create_task("t1", "需求")
    store.save_checkpoint("t1", "code", {"code": 2, "gate": 1}, steps=5,
                          state={"round": 2})
    cp = store.latest_checkpoint("t1")
    assert cp["node"] == "code"
    assert cp["visits"] == {"code": 2, "gate": 1}
    assert cp["steps"] == 5
    assert cp["state"] == {"round": 2}


def test_latest_checkpoint_returns_last_one(store: Store):
    store.create_task("t1", "需求")
    store.save_checkpoint("t1", "prd", {}, 1, {})
    store.save_checkpoint("t1", "design", {}, 2, {})
    store.save_checkpoint("t1", "code", {}, 3, {})
    assert store.latest_checkpoint("t1")["node"] == "code"
    assert [c["node"] for c in store.list_checkpoints("t1")] == ["prd", "design", "code"]


def test_checkpoint_seq_is_monotonic(store: Store):
    store.create_task("t1", "需求")
    seqs = [store.save_checkpoint("t1", f"n{i}", {}, i, {}) for i in range(3)]
    assert seqs == [1, 2, 3]


def test_missing_checkpoint_returns_none(store: Store):
    assert store.latest_checkpoint("ghost") is None


# ============================================================
# 事件持久化
# ============================================================

def test_events_append_and_load_since(store: Store):
    store.create_task("t1", "需求")
    for i in range(1, 6):
        store.append_event("t1", i, "log", {"i": i}, 1000.0 + i)
    assert len(store.load_events("t1")) == 5
    tail = store.load_events("t1", since=3)
    assert [e["seq"] for e in tail] == [4, 5]
