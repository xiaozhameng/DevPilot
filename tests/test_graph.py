"""编排引擎的测试。

这个文件里最重要的三条，是针对**静态固定边**和**运行条件边**的分界：
  - 固定边成环 → 必须在校验期就被拒绝（而不是跑起来才发现死循环）
  - 条件边成环 → 必须被允许（否则"覆盖率打回"根本写不出来）
  - 条件边绕不死 → 引擎的 max_visits 必须兜住（不能指望 router 自觉）
"""

from __future__ import annotations

import pytest

from devpilot import events
from devpilot.graph import (
    Graph,
    GraphError,
    GraphValidationError,
    NodeBudgetExceeded,
    NodeResult,
    run_graph,
)


class FakeCtx:
    """最小 ctx：只实现执行器用到的那几个方法。

    这也是把依赖收成一个 ctx 的直接好处 —— 测编排逻辑不需要真的
    SQLite、沙箱、事件循环，一个 20 行的假对象就够了。
    """

    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.state: dict = {}
        self.checkpoints: list[tuple[str, dict, int]] = []

    def emit(self, type_: str, **data) -> None:
        self.events.append((type_, data))

    def merge(self, patch: dict) -> None:
        self.state.update(patch)

    def checkpoint(self, node: str, visits: dict, steps: int, **kw) -> None:
        self.checkpoints.append((node, dict(visits), steps))

    def get(self, key: str, default=None):
        return self.state.get(key, default)

    def bump(self, key: str, step: int = 1) -> int:
        val = int(self.state.get(key, 0)) + step
        self.state[key] = val
        return val


def _ok(summary: str = "", **patch):
    def fn(ctx):
        return NodeResult(summary=summary or ctx.__class__.__name__, patch=patch)
    return fn


def _linear() -> Graph:
    g = Graph("a")
    g.add_node("a", _ok("a"))
    g.add_node("b", _ok("b"))
    g.add_node("c", _ok("c"))
    g.add_edge("a", "b").add_edge("b", "c")
    return g


# ============================================================
# 结构：拓扑排序与环
# ============================================================

def test_topological_order_is_stable_and_correct():
    assert _linear().topological_order() == ["a", "b", "c"]


def test_fixed_edge_cycle_is_rejected_at_validate():
    g = Graph("a")
    for n in ("a", "b", "c"):
        g.add_node(n, _ok(n))
    g.add_edge("a", "b").add_edge("b", "c").add_edge("c", "a")
    with pytest.raises(GraphValidationError, match="有环"):
        g.validate()


def test_conditional_edge_cycle_is_legal():
    """澄清追问 / 覆盖率打回都是条件边成环 —— 必须允许。"""
    g = Graph("ask")
    g.add_node("ask", _ok("ask"), max_visits=3)
    g.add_node("done", _ok("done"))
    g.add_conditional_edge("ask", lambda ctx: "ask" if ctx.get("again") else "done")
    g.declare_targets("ask", ["ask", "done"])
    g.validate()          # 不抛异常
    assert set(g.outgoing("ask")) == {"ask", "done"}


def test_node_with_two_outgoing_edges_is_rejected():
    g = Graph("a")
    g.add_node("a", _ok("a"))
    g.add_node("b", _ok("b"))
    g.add_edge("a", "b")
    with pytest.raises(GraphValidationError, match="已经有出边"):
        g.add_conditional_edge("a", lambda ctx: None)


def test_missing_entry_is_rejected():
    g = Graph("nope")
    g.add_node("a", _ok("a"))
    with pytest.raises(GraphValidationError, match="入口"):
        g.validate()


def test_unreachable_node_is_rejected():
    """定义了却从入口到不了 —— 是装配写错了，不该等到跑起来才发现。"""
    g = Graph("a")
    g.add_node("a", _ok("a"))
    g.add_node("orphan", _ok("orphan"))
    with pytest.raises(GraphValidationError, match="不可达"):
        g.validate()


def test_router_target_must_exist():
    g = Graph("a")
    g.add_node("a", _ok("a"))
    g.add_conditional_edge("a", lambda ctx: None)
    g.declare_targets("a", ["ghost"])
    with pytest.raises(GraphValidationError, match="不存在的出口"):
        g.validate()


def test_router_returning_unknown_node_raises_at_runtime():
    g = Graph("a")
    g.add_node("a", _ok("a"))
    g.add_conditional_edge("a", lambda ctx: "ghost")
    g.declare_targets("a", [])
    ctx = FakeCtx()
    with pytest.raises(GraphError, match="不存在的节点"):
        run_graph(g, ctx)


# ============================================================
# 执行
# ============================================================

def test_run_graph_visits_each_node_once_and_checkpoints():
    ctx = FakeCtx()
    visits = run_graph(_linear(), ctx)
    assert visits == {"a": 1, "b": 1, "c": 1}

    started = [d["node"] for t, d in ctx.events if t == events.NODE_STARTED]
    finished = [d["node"] for t, d in ctx.events if t == events.NODE_FINISHED]
    assert started == ["a", "b", "c"]
    assert finished == ["a", "b", "c"]

    # 检查点必须逐节点落地，且带着各节点已执行次数（断点恢复要用）
    assert [c[0] for c in ctx.checkpoints] == ["a", "b", "c"]
    assert ctx.checkpoints[-1][1] == {"a": 1, "b": 1, "c": 1}


def test_patch_is_merged_into_state():
    g = Graph("a")
    g.add_node("a", lambda ctx: NodeResult(patch={"answer": 42}))
    ctx = FakeCtx()
    run_graph(g, ctx)
    assert ctx.state["answer"] == 42


def test_conditional_routing_follows_router():
    g = Graph("check")
    g.add_node("check", _ok("check"))
    g.add_node("yes", _ok("yes"))
    g.add_node("no", _ok("no"))
    g.add_conditional_edge("check", lambda ctx: "yes" if ctx.get("pass") else "no")
    g.declare_targets("check", ["yes", "no"])

    ctx = FakeCtx()
    ctx.state["pass"] = True
    run_graph(g, ctx)
    assert [d["node"] for t, d in ctx.events if t == events.NODE_STARTED] == ["check", "yes"]

    ctx2 = FakeCtx()
    run_graph(g, ctx2)
    assert [d["node"] for t, d in ctx2.events if t == events.NODE_STARTED] == ["check", "no"]


def test_loop_terminates_when_router_converges():
    """条件边回环在 router 收敛时正常结束。"""
    g = Graph("work")
    g.add_node("work", lambda ctx: NodeResult(patch={"round": ctx.bump("round")}),
               max_visits=5)
    g.add_node("done", _ok("done"))
    g.add_conditional_edge(
        "work", lambda ctx: "work" if ctx.get("round", 0) < 3 else "done"
    )
    g.declare_targets("work", ["work", "done"])

    ctx = FakeCtx()
    visits = run_graph(g, ctx)
    assert visits["work"] == 3
    assert visits["done"] == 1


def test_brake_stops_runaway_loop():
    """router 永远不收敛时，必须是**引擎**兜住，而不是让它跑到天亮。"""
    g = Graph("loop")
    g.add_node("loop", _ok("loop"), max_visits=3)
    g.add_conditional_edge("loop", lambda ctx: "loop")   # 故意不收敛
    g.declare_targets("loop", ["loop"])

    with pytest.raises(NodeBudgetExceeded, match="超过上限 3"):
        run_graph(g, FakeCtx())


def test_node_exception_is_reported_then_reraised():
    def boom(ctx):
        raise ValueError("故意炸")

    g = Graph("a")
    g.add_node("a", boom)
    ctx = FakeCtx()
    with pytest.raises(ValueError):
        run_graph(g, ctx)
    failed = [d for t, d in ctx.events if t == events.NODE_FAILED]
    assert len(failed) == 1
    assert "ValueError" in failed[0]["error"]


def test_failed_node_result_stops_pipeline():
    g = Graph("a")
    g.add_node("a", lambda ctx: NodeResult(ok=False, error="业务上失败"))
    g.add_node("b", _ok("b"))
    g.add_edge("a", "b")
    with pytest.raises(GraphError, match="业务上失败"):
        run_graph(g, FakeCtx())


def test_start_from_middle_is_resume_entry():
    """断点恢复：从中间节点续跑，前面的节点不再执行。"""
    ctx = FakeCtx()
    visits = run_graph(_linear(), ctx, start="b")
    assert visits == {"b": 1, "c": 1}


# ============================================================
# 结构分析
# ============================================================

def test_downstream_of_uses_declared_conditional_targets():
    """影响范围依赖 declare_targets：不声明就会**漏算**，漏算比多算危险。"""
    g = Graph("a")
    for n in ("a", "b", "c"):
        g.add_node(n, _ok(n))
    g.add_conditional_edge("a", lambda ctx: "b")
    g.declare_targets("a", ["b", "c"])
    g.add_edge("b", "c")

    assert g.downstream_of("a") == {"b", "c"}
    assert g.downstream_of("b") == {"c"}
    assert g.downstream_of("c") == set()
    assert g.downstream_of("a", include_self=True) == {"a", "b", "c"}


def test_describe_marks_conditional_edges():

    g = Graph("ask")
    g.add_node("ask", _ok("ask"), max_visits=3)
    g.add_node("done", _ok("done"))
    g.add_conditional_edge("ask", lambda ctx: "done")
    g.declare_targets("ask", ["done"])
    text = g.describe()
    assert "条件" in text and "done" in text
