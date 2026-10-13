"""流水线装配与端到端测试。

端到端那几条**故意在没有模型的情况下跑**（api_key 为空 → 全链路走降级分支）。
这不是偷懒，而是在验证一个很具体的承诺：

> 编排、状态机、沙箱、检查点这些确定性的东西，不该因为模型不可用而停摆。

如果这条测试挂了，说明流水线的骨架和模型耦合在了一起 ——
而那正是这套设计最想避免的事。
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import pytest

from devpilot import events
from devpilot.config import Settings
from devpilot.pipeline import (
    build_pipeline,
    restore_from_checkpoint,
    resume_start,
    run_pipeline,
)
from devpilot.runtime import ApprovalGate, ApprovalTimeout, RunContext
from devpilot.sandbox import Sandbox
from devpilot.state import Store, TaskStatus


def make_settings(tmp_path: Path, **over) -> Settings:
    base = dict(
        api_key="",                    # 故意留空 → 全链路降级
        api_base="", model="",
        max_clarify=1, max_codegen=1, coverage_target=0, approval_timeout=5,
        workspace_dir=tmp_path / "ws", sandbox_timeout=180,
        sandbox_output_limit=8000,
        serve_host="127.0.0.1", serve_port=0,
        internal_token="test-token", gateway_base="",
        root=tmp_path, db_path=tmp_path / "test.db",
    )
    base.update(over)
    return Settings(**base)


# ============================================================
# 结构
# ============================================================

def test_pipeline_validates(tmp_path):
    """图定义本身合法：入口可达、固定边无环、条件边出口存在。"""
    build_pipeline(make_settings(tmp_path)).validate()


def test_pipeline_has_two_conditional_loops(tmp_path):
    g = build_pipeline(make_settings(tmp_path))
    assert g._routers.keys() == {"clarify", "gate"}
    assert set(g.outgoing("clarify")) == {"clarify", "prd"}
    assert set(g.outgoing("gate")) == {"code"}


def test_pipeline_main_spine_order(tmp_path):
    g = build_pipeline(make_settings(tmp_path))
    order = g.topological_order()
    for a, b in [("prd", "design"), ("design", "approve"),
                 ("approve", "code"), ("code", "gate")]:
        assert order.index(a) < order.index(b)


def test_brakes_follow_settings(tmp_path):
    """刹车必须来自配置，不能写死在代码里 —— 否则调不动。"""
    g = build_pipeline(make_settings(tmp_path, max_clarify=4, max_codegen=3))
    assert g.nodes["clarify"].max_visits == 5      # 1 + 4
    assert g.nodes["code"].max_visits == 4         # 1 + 3
    assert g.nodes["prd"].max_visits == 1          # 线性节点不做重试


def test_every_node_on_a_loop_has_a_raised_budget(tmp_path):
    """**回环上的每个节点**都要放宽上限，不能只放宽"看起来最贵的那个"。

    这条测试是被一次真实失败逼出来的：编码回环是 code → gate → code，
    直觉上只给 code 放宽就够（毕竟它最贵），gate 留着默认值 1。
    结果第一轮覆盖率不达标、打回编码后，gate 第 2 次被调度直接撞上限，
    整个任务以 NodeBudgetExceeded 失败 —— 而错误信息指向
    "router 没能收敛"，把排查方向带偏（router 一点问题都没有）。

    所以这里遍历两个回环的**全部**成员，逐个断言它们的预算确实被抬高了。
    """
    st = make_settings(tmp_path, max_clarify=4, max_codegen=3)
    g = build_pipeline(st)

    # 回环 1：clarify → clarify；回环 2：gate → code → gate
    loop_members = {"clarify", "code", "gate"}
    assert set(g._routers) == {"clarify", "gate"}, "回环成员变了，这条测试的前提要重新确认"

    for name in loop_members:
        assert g.nodes[name].max_visits > 1, (
            f"节点 {name} 在回环上，上限却还是默认值 {g.nodes[name].max_visits} —— "
            f"绕第二圈就会以 NodeBudgetExceeded 失败"
        )

    # 而线性节点不该被放宽：宽了就等于"允许它被莫名重跑"。
    for name in ("prd", "design", "approve"):
        assert g.nodes[name].max_visits == 1


def test_downstream_of_gate_loops_back_to_code(tmp_path):
    """门禁挂了 → 影响范围必须包含 code，否则勾不出重跑清单。"""
    g = build_pipeline(make_settings(tmp_path))
    assert "code" in g.downstream_of("gate")
    assert g.downstream_of("code") == {"gate"}


# ============================================================
# 断点恢复
# ============================================================

def test_resume_start_is_entry_without_checkpoint(tmp_path):
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t1", "需求")
    ctx = RunContext(task_id="t1", requirement="需求", store=store,
                     sandbox=Sandbox("t1", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate())
    g = build_pipeline(st)
    assert resume_start(g, ctx) == "clarify"


def test_resume_start_is_next_node_not_the_same_one(tmp_path):
    """恢复点是"上次完成节点的下一个"，不是它自己。

    直接从上次的节点重跑 = 重复执行一个可能很贵的节点。
    这个断言就是在钉住这一点。
    """
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t1", "需求")
    store.save_checkpoint("t1", "design", {"clarify": 1, "prd": 1, "design": 1}, steps=3,
                          state={})
    ctx = RunContext(task_id="t1", requirement="需求", store=store,
                     sandbox=Sandbox("t1", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate())
    g = build_pipeline(st)
    assert resume_start(g, ctx) == "approve"


def test_resume_start_respects_router_state(tmp_path):
    """条件边的恢复点由 state 重新算出来，不是查死表。"""
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t1", "需求")
    store.save_checkpoint("t1", "gate", {"gate": 1}, steps=6,
                          state={"gate_passed": False})
    ctx = RunContext(task_id="t1", requirement="需求", store=store,
                     sandbox=Sandbox("t1", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate(), state={"gate_passed": False})
    g = build_pipeline(st)
    assert resume_start(g, ctx) == "code"       # 未达标 → 回 code


# ============================================================
# 审批门
# ============================================================

def test_approval_gate_blocks_until_resolved():
    gate = ApprovalGate(default_timeout=10)
    gate.request("t", "approve", "批准？")
    threading.Timer(0.2, lambda: gate.resolve("t", True, "同意")).start()

    t0 = time.perf_counter()
    decision = gate.wait("t", timeout=10)
    waited = time.perf_counter() - t0

    assert decision["approved"] is True
    assert waited >= 0.15, "没有真的阻塞等待，说明 wait 是空转的"
    assert not gate.is_waiting("t")


def test_approval_gate_rejects_on_timeout():
    gate = ApprovalGate(default_timeout=1)
    gate.request("t", "approve", "批准？")
    with pytest.raises(ApprovalTimeout):
        gate.wait("t", timeout=1)


def test_resolving_unknown_task_returns_false():
    assert ApprovalGate().resolve("ghost", True) is False


# ============================================================
# 端到端（无模型，全链路降级）
# ============================================================

REQUIREMENT = "做一个把整数列表去重并按升序排列的小工具"

# 断点恢复测试要用它：恢复出来的 design 必须真的驱动下游生成文件。
DESIGN = {
    "language": "Python",
    "files": [
        {"path": "app/tool.py", "purpose": "去重排序的核心实现"},
        {"path": "tests/test_tool.py", "purpose": "覆盖正常路径与空输入"},
    ],
    "interfaces": [],
    "test_plan": ["正常路径", "空输入"],
    "risks": [],
}


@pytest.fixture(scope="module")
def e2e(tmp_path_factory):
    """跑**一次**完整流水线，所有断言共享这一次运行。

    为什么用 module 作用域：一次端到端要串 6 个节点，其中编码节点还会
    真的起一个 pytest 子进程，总共十几秒。5 条断言各跑一遍就是一分多钟 ——
    而这些断言全是只读的，共享同一次运行不损失任何覆盖度。

    另外 SUCCEEDED 是终态（state.py 的转移表里没有出边），
    所以同一个任务本来也没法跑第二次 —— 想再跑必须新建任务。
    这个测试结构是被状态机的设计"逼"出来的，也算顺带验证了终态不可逆。
    """
    tmp = tmp_path_factory.mktemp("e2e")
    st = make_settings(tmp, coverage_target=0)
    store = Store(st.db_path)
    task_id = "e2e-1"
    store.create_task(task_id, REQUIREMENT)
    ctx = RunContext(task_id=task_id, requirement=REQUIREMENT, store=store,
                     sandbox=Sandbox(task_id, root=st.workspace_dir, timeout=180),
                     settings=st, gate=ApprovalGate(default_timeout=5),
                     state={"auto_approve": True})
    out = run_pipeline(ctx)
    return st, store, ctx, out


def test_end_to_end_completes_without_model(e2e):
    st, store, ctx, out = e2e
    assert out["ok"], out.get("error")
    rec = store.get_task(ctx.task_id)
    assert rec.status is TaskStatus.SUCCEEDED
    assert rec.result["test_passed"] is True
    assert rec.result["degraded"] is True        # 确实走的是降级路径


def test_end_to_end_produces_files_in_sandbox(e2e):
    st, store, ctx, out = e2e
    files = [f["path"] for f in ctx.sandbox.list_files()]
    assert "docs/PRD.md" in files
    assert "docs/DESIGN.md" in files
    assert "conftest.py" in files
    assert any(f.endswith(".py") for f in files)


def test_end_to_end_emits_terminal_event(e2e):
    st, store, ctx, out = e2e
    types = [e["type"] for e in store.load_events(ctx.task_id)]
    assert events.NODE_STARTED in types
    assert events.APPROVAL_REQUIRED in types
    assert events.APPROVAL_RESOLVED in types
    assert events.SANDBOX_RUN in types
    assert events.TASK_DONE in types
    assert types[-1] == events.TASK_DONE


def test_end_to_end_writes_checkpoints_for_every_node(e2e):
    st, store, ctx, out = e2e
    nodes = [c["node"] for c in store.list_checkpoints(ctx.task_id)]
    assert nodes == ["clarify", "prd", "design", "approve", "code", "gate"]


def test_end_to_end_result_carries_plan_and_usage(e2e):
    st, store, ctx, out = e2e
    res = out["result"]
    assert res["critical_path"][0] == "clarify"
    assert res["makespan_estimated_ms"] > 0
    assert "total" in res["usage"]
    assert res["visits"]["gate"] == 1
    assert res["clarify_rounds"] == 1
    assert res["codegen_rounds"] == 1


def test_resume_skips_completed_nodes(tmp_path):
    """真·断点恢复：检查点停在 design 之后，从 approve 续跑。

    两件事都要验：
      1. clarify/prd/design **不该再执行**（这是省钱的关键）
      2. 它们的产物**必须仍在共享状态里**

    第 2 条才是这条测试的重点。只断言"节点没重跑"是不够的 ——
    如果状态丢了，流水线照样能跑完，只是编码节点拿不到文件列表、
    悄悄产出一个空交付物。**不报错，但结果是错的。**
    """
    st = make_settings(tmp_path, coverage_target=0)
    store = Store(st.db_path)
    task_id = "resume-1"
    store.create_task(task_id, REQUIREMENT)
    store.save_checkpoint(
        task_id, "design",
        {"clarify": 1, "prd": 1, "design": 1}, steps=3,
        state={"spec": REQUIREMENT, "design": DESIGN, "prd": {"title": "去重排序工具"}},
    )
    ctx = RunContext(task_id=task_id, requirement=REQUIREMENT, store=store,
                     sandbox=Sandbox(task_id, root=st.workspace_dir, timeout=180),
                     settings=st, gate=ApprovalGate(default_timeout=5),
                     state={"auto_approve": True})
    out = run_pipeline(ctx, resume=True)
    assert out["ok"], out.get("error")

    visits = out["result"]["visits"]
    assert "clarify" not in visits
    assert "prd" not in visits
    assert "design" not in visits
    assert visits["approve"] == 1
    assert visits["code"] == 1

    # 恢复出来的 design 真的起了作用：源文件按它生成了
    files = [f["path"] for f in ctx.sandbox.list_files()]
    assert "app/tool.py" in files
    assert "tests/test_tool.py" in files


def test_restore_from_checkpoint_revives_shared_state(tmp_path):
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("r2", REQUIREMENT)
    store.save_checkpoint("r2", "design", {"design": 1}, steps=3,
                          state={"design": DESIGN, "spec": "原始规格"})
    ctx = RunContext(task_id="r2", requirement=REQUIREMENT, store=store,
                     sandbox=Sandbox("r2", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate(),
                     state={"auto_approve": True})     # 运行时选项

    assert restore_from_checkpoint(ctx) is True
    assert ctx.get("design") == DESIGN                 # 从检查点恢复
    assert ctx.get("spec") == "原始规格"
    assert ctx.get("auto_approve") is True             # 调用方选项优先保留


def test_restore_from_checkpoint_is_noop_without_checkpoint(tmp_path):
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("r3", REQUIREMENT)
    ctx = RunContext(task_id="r3", requirement=REQUIREMENT, store=store,
                     sandbox=Sandbox("r3", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate(), state={"keep": 1})
    assert restore_from_checkpoint(ctx) is False
    assert ctx.get("keep") == 1


def test_failure_marks_task_failed(tmp_path):
    """节点抛异常时，任务必须落到 FAILED（而不是永远停在 RUNNING）。"""
    st = make_settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t-bad", "需求")
    ctx = RunContext(task_id="t-bad", requirement="需求", store=store,
                     sandbox=Sandbox("t-bad", root=st.workspace_dir),
                     settings=st, gate=ApprovalGate(default_timeout=5),
                     state={"auto_approve": True})

    import devpilot.pipeline as P
    original = P.prd

    def boom(c):
        raise RuntimeError("故意炸在 prd")

    P.prd = boom
    try:
        out = P.run_pipeline(ctx)
        assert out["ok"] is False
        assert "故意炸在 prd" in out["error"]
        assert store.get_task("t-bad").status is TaskStatus.FAILED
        types = [e["type"] for e in store.load_events("t-bad")]
        assert events.TASK_FAILED in types
    finally:
        P.prd = original
