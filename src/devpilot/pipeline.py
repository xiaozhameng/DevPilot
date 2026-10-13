"""流水线装配：把 6 个 Agent 节点接成一张图。

```
                 ┌──── 信息不足（≤3 轮）────┐
                 ↓                          │
  [clarify] ──条件──> [prd] → [design] → [approve] → [code] → [gate]
                                                              │  ▲
                                                              └──┘
                                                          覆盖率不达标（≤2 次）
```

两条条件边、两个环。两个环都必须显式给出刹车，且刹车分两层：

| 层 | 在哪 | 管什么 |
|---|---|---|
| 策略层 | 节点内部（clarify / gate） | 达到轮次上限就**主动放行**，而不是死等 |
| 保护层 | 图定义（max_visits） | 就算策略层写错了，也不许超过 N 次 |

**刹车要配在回环上的每一个节点，而不是只配在最贵的那个。**
编码回环是 code → gate → code，两个节点每绕一圈都会被重新调度一次，
所以两个都必须放宽。只放宽 code 的话，第一轮打回后 gate 第 2 次被调度
就撞上默认上限 1，整个任务以 NodeBudgetExceeded 失败 ——
而报错信息会说"router 没能收敛"，把注意力引向完全正常的 router。
（这是真实踩过的坑，现在由 test_every_node_on_a_loop_has_a_raised_budget 钉住。）

为什么两层都要？因为它们的失效模式不同：
策略层可能被改错（判断条件写反），保护层可能被配错（max_visits 忘了调大）。
任何单层失效时，另一层还能兜住。而一旦两层同时失效，结果就是**静默烧钱** ——
没有报错、没有告警、任务永远"正在运行"。

装配时最容易犯的错是 `declare_targets` 忘了写：它不影响执行（router 照样会跑），
但会让 `downstream_of` 算出的影响范围偏小，也就是**该重跑的下游被漏掉**。
所以 validate() 里会检查声明的出口是否存在 —— 但"漏声明"本身没法自动发现，
只能靠这里的注释提醒。
"""

from __future__ import annotations

from . import events
from .agents.approve import approve
from .agents.clarify import clarify, clarify_router
from .agents.code import code
from .agents.design import design
from .agents.gate import gate, gate_router
from .agents.prd import prd
from .config import SETTINGS, Settings
from .graph import Graph, run_graph
from .plan import critical_path, schedule
from .state import TaskStatus


def build_pipeline(settings: Settings | None = None) -> Graph:
    st = settings or SETTINGS

    g = Graph("clarify")

    # avg_ms 是**估算值**，只用于关键路径与调度分析（见 plan.py）。
    # 它的意义不是"预测耗时"，而是回答"优化哪个节点收益最大"。
    g.add_node("clarify", clarify,
               description="需求澄清：把模糊需求变成可开工的规格",
               avg_ms=3500, max_visits=1 + st.max_clarify)
    g.add_node("prd", prd,
               description="产出 PRD：结构化输出 + Schema 校验",
               avg_ms=6000)
    g.add_node("design", design,
               description="技术方案：文件树 + 接口契约（收敛上下文）",
               avg_ms=6000)
    g.add_node("approve", approve,
               description="人工审批：挂起点，等外部回调放行",
               avg_ms=0)
    g.add_node("code", code,
               description="编码 + 单测：沙箱内写文件并真的跑 pytest",
               avg_ms=45000, max_visits=1 + st.max_codegen)
    # 注意：门禁节点**也在回环里**，所以它同样需要放宽上限。
    #
    # 这是装配图上最容易漏的一处：直觉上"要花钱的是编码节点，给它放宽就够了"，
    # 但 gate 会被重新调度一次（code → gate 每绕一圈都要再判一次）。
    # 上限留在默认值 1 的话，第一轮打回之后 gate 第 2 次被调度就撞上限，
    # 整个任务以 NodeBudgetExceeded 失败 —— 而失败信息会指向
    # "router 没能收敛"，把注意力引到错误的方向（router 完全正常）。
    #
    # 教训：刹车要配在**回环上的每一个节点**，不能只配在你以为最贵的那个。
    g.add_node("gate", gate,
               description="覆盖率门禁：不达标则打回重写",
               avg_ms=500, max_visits=1 + st.max_codegen)

    # ---- 条件边 1：澄清回环 ----
    g.add_conditional_edge("clarify", clarify_router)
    g.declare_targets("clarify", ["clarify", "prd"])

    # ---- 固定边：主干 ----
    g.add_edge("prd", "design")
    g.add_edge("design", "approve")
    g.add_edge("approve", "code")
    g.add_edge("code", "gate")

    # ---- 条件边 2：覆盖率打回 ----
    g.add_conditional_edge("gate", gate_router)
    g.declare_targets("gate", ["code"])

    return g


def restore_from_checkpoint(ctx) -> bool:
    """把共享状态从最新检查点恢复回来。返回是否真的做了恢复。

    这一步**必须发生在算恢复点之前**，因为 router 要读 state 才能决定往哪走。
    也**必须由 run_pipeline 自己保证**，不能靠调用方记得做 ——
    因为"忘了恢复状态就续跑"不会报错，它会安静地跑出错误结果：
    比如从 approve 续跑时 design 节点不执行，于是 code 节点拿不到文件列表，
    生成一个空测试文件，pytest 报 "no tests ran"，门禁判定不通过，
    然后回环到刹车 —— 整个过程没有一个异常，只有一堆看似合理的日志。
    这类"不报错但结果错"的问题是最贵的，所以把约束写进代码，而不是写进注释。

    合并顺序：checkpoint 里的状态是"当时的真实状态"，作为基底；
    调用方传进来的（比如 auto_approve 这类运行时开关）优先级更高。
    """
    cp = ctx.store.latest_checkpoint(ctx.task_id)
    if not cp:
        return False
    saved = cp.get("state") or {}
    if saved:
        ctx.state = {**saved, **ctx.state}
    return True


def resume_start(graph: Graph, ctx) -> str | None:
    """断点恢复的入口节点。

    逻辑很短，但有一处不能想当然：**恢复点不是"上次执行的节点"，
    而是"上次执行的节点的下一个"**。直接从上次的节点重跑会重复执行它 ——
    在那个节点是编码（很贵）的时候，这是实打实的重复花钱。

    条件边的下一个节点由 router 重新算（不是查表），所以恢复时
    ctx.state 必须已经是从检查点里恢复出来的那份（见 restore_from_checkpoint）。
    """
    cp = ctx.store.latest_checkpoint(ctx.task_id)
    if not cp or not cp.get("steps"):
        return graph.entry
    return graph.next_of(cp["node"], ctx)


def _collect(ctx, graph: Graph, visits: dict[str, int]) -> dict:
    """汇总任务产物。这里的每个数字都会出现在演示页与审计里。"""
    design_data = ctx.get("design") or {}
    files = design_data.get("files") or []
    weights = {name: float(node.avg_ms) for name, node in graph.nodes.items()}
    deps = {
        "clarify": [], "prd": ["clarify"], "design": ["prd"],
        "approve": ["design"], "code": ["approve"], "gate": ["code"],
    }
    path, inf = critical_path(deps, weights)
    real = schedule(deps, weights, parallelism=1)["makespan"]

    usage = ctx.usage()
    files_on_disk = ctx.sandbox.list_files()

    return {
        "requirement": ctx.requirement,
        "spec": ctx.get("spec"),
        "assumptions": ctx.get("assumptions") or [],
        "open_questions": ctx.get("open_questions") or [],
        "prd_title": (ctx.get("prd") or {}).get("title"),
        "files": files_on_disk,
        "source_count": len(ctx.get("sources") or []),
        "coverage": ctx.get("coverage"),
        "coverage_target": ctx.settings.coverage_target,
        "test_passed": ctx.get("test_passed"),
        "gate_passed": ctx.get("gate_passed"),
        "gate_degraded": ctx.get("gate_degraded", False),
        # 降级的**具体原因**。只给一个布尔标记是不够的：
        # "测试没跑过"和"覆盖率差一点"对使用者是两回事，后者可以放心用，
        # 前者交付物是坏的。把原因带出来，界面和人都不会被一个 True 误导。
        "gate_reason": ctx.get("gate_reason"),
        "clarify_rounds": ctx.get("clarify_round", 0),
        "codegen_rounds": ctx.get("codegen_round", 0),
        "visits": visits,
        "critical_path": path,
        "makespan_estimated_ms": int(inf),
        "usage": usage,
        "checkpoints": ctx.store.list_checkpoints(ctx.task_id),
        "degraded": bool(ctx.get("clarify_degraded") or ctx.get("prd_degraded")
                         or ctx.get("design_degraded") or ctx.get("code_degraded")),
    }


def run_pipeline(ctx, *, resume: bool = False) -> dict:
    """跑完一整条流水线，并把任务状态推到终态。

    这是内核里**唯一**负责把任务从 RUNNING 推到 SUCCEEDED/FAILED 的地方。
    收敛到一处的好处是：状态机的每一条边都在这一个函数里可见，
    不用去十个地方拼凑"这个任务是怎么变成 FAILED 的"。
    """
    graph = build_pipeline(ctx.settings)
    store = ctx.store

    start = None
    if resume:
        # 顺序不能反：先恢复状态，再算从哪继续。
        # 反了的话 router 会在空状态上做判断，算出一个错误的起点。
        restore_from_checkpoint(ctx)
        start = resume_start(graph, ctx)
        cp = store.latest_checkpoint(ctx.task_id)
        if cp and start:
            ctx.log(f"从检查点恢复：上次完成于 {cp['node']}（steps={cp['steps']}），"
                    f"从 {start} 继续；已恢复 {len(ctx.state)} 项共享状态")

    store.update_status(ctx.task_id, TaskStatus.RUNNING, state=ctx.state)
    ctx.emit(events.TASK_STATUS, status=TaskStatus.RUNNING.value, node=None,
             resumed=bool(resume))

    try:
        visits = run_graph(graph, ctx, start=start)
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        store.update_status(ctx.task_id, TaskStatus.FAILED, state=ctx.state, error=err)
        ctx.emit(events.TASK_FAILED, error=err, state=ctx.state)
        return {"ok": False, "error": err}

    result = _collect(ctx, graph, visits)
    store.update_status(ctx.task_id, TaskStatus.SUCCEEDED, state=ctx.state, result=result)
    ctx.emit(events.TASK_DONE, result=result)
    return {"ok": True, "result": result}
