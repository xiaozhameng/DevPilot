"""依赖图静态分析的测试（实践 Q14 那道手撕题）。

这张图贯穿整个文件：

        a(10)
       /     \\
    b(100)   c(20)
       \\     /
        d(5)

关键路径是 a→b→d = 115；c 分支只要 30，早跑完也在等 ——
这正是"为什么要看最长路"最好的例子。
"""

from __future__ import annotations

import pytest

from devpilot.plan import (
    DepsError,
    critical_path,
    downstream,
    execution_plan,
    find_cycle,
    schedule,
    topo_order,
)


@pytest.fixture()
def diamond() -> dict[str, list[str]]:
    return {"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]}


@pytest.fixture()
def weights() -> dict[str, float]:
    return {"a": 10, "b": 100, "c": 20, "d": 5}


# ============================================================
# 拓扑排序
# ============================================================

def test_topo_order_respects_dependencies(diamond):
    order = topo_order(diamond)
    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")


def test_topo_order_is_deterministic(diamond):
    """同样输入必须每次同样输出，否则"合法执行顺序"没法被测试固定。"""
    assert topo_order(diamond) == topo_order(dict(reversed(list(diamond.items()))))


def test_cycle_is_rejected():
    with pytest.raises(DepsError, match="有环"):
        topo_order({"a": ["c"], "b": ["a"], "c": ["b"]})


def test_self_dependency_is_rejected():
    with pytest.raises(DepsError, match="依赖了自己"):
        topo_order({"a": ["a"]})


def test_dependency_on_unknown_node_is_rejected():
    with pytest.raises(DepsError, match="不存在的节点"):
        topo_order({"a": ["ghost"]})


def test_find_cycle_reports_involved_nodes():
    assert find_cycle({"a": [], "b": ["a"], "c": ["b"]}) is None
    cyc = find_cycle({"a": ["c"], "b": ["a"], "c": ["b"]})
    assert cyc == ["a", "b", "c"]


# ============================================================
# 关键路径 / 最小完成时间
# ============================================================

def test_critical_path_takes_the_longer_branch(diamond, weights):
    path, makespan = critical_path(diamond, weights)
    assert path == ["a", "b", "d"]
    assert makespan == 115
    # c 不在关键路径上：优化 b 有用，优化 c 在当前结构下毫无收益
    assert "c" not in path


def test_critical_path_single_node():
    path, makespan = critical_path({"only": []}, {"only": 42})
    assert path == ["only"] and makespan == 42


def test_critical_path_empty_graph():
    assert critical_path({}, {}) == ([], 0.0)


def test_critical_path_parallel_branches_are_summed_at_join():
    """两个分支都必须在 d 之前完成 → d 的最早完成是两者中较晚的那个加上自己。"""
    deps = {"s": [], "x": ["s"], "y": ["s"], "e": ["x", "y"]}
    w = {"s": 1, "x": 100, "y": 200, "e": 3}
    path, makespan = critical_path(deps, w)
    assert makespan == 1 + 200 + 3
    assert path == ["s", "y", "e"]


# ============================================================
# 故障重调度
# ============================================================

def test_downstream_covers_self_and_descendants(diamond):
    assert downstream(diamond, "a") == {"a", "b", "c", "d"}
    assert downstream(diamond, "b") == {"b", "d"}
    assert downstream(diamond, "c") == {"c", "d"}
    assert downstream(diamond, "d") == {"d"}


def test_downstream_never_includes_ancestors(diamond):
    """前置不用重跑 —— 它们的结果还在。全量重跑在多 Agent 里等于白烧 token。"""
    assert "a" not in downstream(diamond, "b")


def test_downstream_rejects_unknown_node(diamond):
    with pytest.raises(DepsError):
        downstream(diamond, "ghost")


# ============================================================
# 有限并行度调度
# ============================================================

def test_makespan_with_single_worker_equals_total_work(diamond, weights):
    res = schedule(diamond, weights, parallelism=1)
    assert res["makespan"] == 135
    assert res["order"] == ["a", "b", "c", "d"]


def test_makespan_with_enough_workers_equals_critical_path(diamond, weights):
    """并发足够时，理论下限就是关键路径长度，不可能更短。"""
    res = schedule(diamond, weights, parallelism=8)
    _, inf = critical_path(diamond, weights)
    assert res["makespan"] == inf == 115


def test_makespan_with_two_workers(diamond, weights):
    res = schedule(diamond, weights, parallelism=2)
    assert res["makespan"] == 115          # b 与 c 并行，b 决定收尾
    assert res["start_at"]["c"] > res["start_at"]["b"] - 1


def test_makespan_never_below_critical_path(diamond, weights):
    """工程值不可能优于理论下限 —— 这条断言能挡住调度算法的实现错误。"""
    _, inf = critical_path(diamond, weights)
    for p in (1, 2, 3, 4):
        assert schedule(diamond, weights, parallelism=p)["makespan"] >= inf


def test_schedule_rejects_zero_parallelism(diamond, weights):
    with pytest.raises(DepsError):
        schedule(diamond, weights, parallelism=0)


# ============================================================
# 合并成可执行计划
# ============================================================

def test_execution_plan_combines_everything(diamond, weights):
    plan = execution_plan(diamond, weights, parallelism=2, failed=["b"])
    assert plan["order"][0] == "a"
    assert plan["critical_path"] == ["a", "b", "d"]
    assert plan["makespan_inf"] == 115
    assert plan["makespan_real"] == 115
    assert plan["bottleneck"] == "b"          # 关键路径上最重的节点
    assert plan["retry"] == ["b", "d"]        # 只重跑 b 及其下游，c 不用


def test_execution_plan_without_failures_has_empty_retry(diamond, weights):
    assert execution_plan(diamond, weights)["retry"] == []
