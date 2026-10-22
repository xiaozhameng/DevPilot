"""DAG 编排引擎：手写，不依赖 LangGraph。

为什么手写？两个理由，一个是原则，一个是实用：
  1) **原则**：编排引擎不到 300 行，但 LangGraph 的每个概念（State/Reducer/
     Checkpointer）都要额外学一套心智模型。当业务只需要"节点 + 条件边"时，
     这层抽象是负债 —— 出错时你要先怀疑自己理解错了框架，再怀疑代码。
  2) **实用**：被问到"你的编排引擎怎么处理回环"，能指着自己的代码讲，
     比说"LangGraph 帮我处理了"强得多。而这个岗位考的就是编排本身。

## 核心设计：固定边 vs 条件边，规则完全不同

| | 固定边 (add_edge) | 条件边 (add_conditional_edge) |
|---|---|---|
| 谁决定下一个节点 | 定义时就定死 | 运行时由 router 函数算 |
| 能否成环 | **不能**，成环即定义错误 | **可以**，这正是澄清/打回的实现方式 |
| 校验方式 | 拓扑排序检测环 | 不静态检测，靠运行时刹车 |
| 表达 | "这一步之后总是那一步" | "这一步之后看情况" |

这个区分是整份代码里最重要的一个判断。
把两者混为一谈的后果很典型：要么禁止一切环（那么"覆盖率不达标就打回重写"就写不出来），
要么允许一切环（那么一个手误的 A→B→A 会变成静默的死循环，跑一整夜烧完配额）。

## 刹车为什么在引擎里，不在 router 里

因为**策略与保护必须分离**：
  - router 负责**策略**："覆盖率 62% < 80%，应该打回"
  - 引擎负责**保护**："无论 router 怎么说，这个节点最多执行 3 次"

如果保护也交给 router，那么当 router 有 bug（比如读了错的字段，导致永远
认为"还不达标"），整个流水线就会无限打回。而 router 是业务逻辑，
是这堆代码里最可能出错的地方 —— 让最可能出错的代码自己给自己上保险，逻辑上说不通。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

from . import events

# 单次任务的最大节点执行步数。这是最后一道兜底：
# 即使所有节点的 max_visits 都配得很宽松，总步数也不会失控。
MAX_STEPS = 200


class GraphError(RuntimeError):
    """编排相关的错误基类。"""


class GraphValidationError(GraphError):
    """图定义本身非法（入口不存在、固定边成环、一个节点两条出边…）。

    这类错误必须在**任务开始之前**暴露，而不是跑到一半才炸 ——
    因为跑到一半才炸意味着前面几步的钱已经花了。
    """


class NodeBudgetExceeded(GraphError):
    """节点执行次数超过上限 —— 刹车触发。

    出现这个异常说明 router 有 bug（它本该自己收敛）。所以错误信息里
    必须带上"实际执行了几次、上限是多少"，方便直接定位到那条路由规则。
    """


@dataclass
class NodeResult:
    """一个节点的执行结果。

    注意这里**没有 `goto` 字段** —— 下一个去哪由 router 决定，不由节点决定。
    为什么？因为"下一步去哪"和"这一步做成了什么"是两个不同的问题，
    混在一个返回值里会让节点既管业务又管流程，测试起来也要造两个东西。
    """
    ok: bool = True
    summary: str = ""
    error: str | None = None
    # 要合并进共享状态的增量。用 patch 而不是让节点直接改 ctx.state，
    # 是为了将来做并行分支时能安全合并（并行的节点各回各的 patch）。
    patch: dict = field(default_factory=dict)


@dataclass
class Node:
    name: str
    fn: Callable[[Any], NodeResult]
    description: str = ""
    # 关键路径估算用的权重（毫秒）。真实耗时方差很大（见 README 的说明），
    # 但用于"哪个节点是关键路径"的判断已经足够。
    avg_ms: int = 0
    # 引擎级刹车：允许被执行的次数上限。
    # 线性节点保持 1；回环节点必须显式调大（例如 1 + 打回次数上限）。
    max_visits: int = 1


class Graph:
    def __init__(self, entry: str) -> None:
        self.entry = entry
        self.nodes: dict[str, Node] = {}
        self._fixed: dict[str, str] = {}                       # src -> dst
        self._routers: dict[str, Callable[[Any], str | None]] = {}   # src -> router
        # 条件边的可能出口。router 是黑盒（要 ctx 才能算），静态算不出来，
        # 所以由使用者用 declare_targets() 显式声明。
        self._declared: dict[str, set[str]] = {}

    # ---------- 定义 ----------
    def add_node(self, name: str, fn: Callable[[Any], NodeResult],
                 description: str = "", avg_ms: int = 0, max_visits: int = 1) -> "Graph":
        if name in self.nodes:
            raise GraphValidationError(f"节点重名：{name}")
        self.nodes[name] = Node(name=name, fn=fn, description=description,
                                avg_ms=avg_ms, max_visits=max_visits)
        return self

    def add_edge(self, src: str, dst: str) -> "Graph":
        """固定边：src 执行完总是去 dst。"""
        self._check_free_slot(src)
        self._fixed[src] = dst
        return self

    def add_conditional_edge(self, src: str, router: Callable[[Any], str | None]) -> "Graph":
        """条件边：src 执行完后调 router(ctx)，返回节点名则去那里，返回 None 则结束。"""
        self._check_free_slot(src)
        self._routers[src] = router
        return self

    def _check_free_slot(self, src: str) -> None:
        if src in self._fixed or src in self._routers:
            raise GraphValidationError(
                f"节点 {src} 已经有出边了。一个节点只允许一条出边 —— "
                f"要表达分支请用条件边，把判断写进 router 里。"
            )

    # ---------- 结构查询 ----------
    def outgoing(self, name: str) -> list[str]:
        """所有可能的后继（固定边的终点 + 条件边的所有可能出口，取并集）。"""
        out: list[str] = []
        if name in self._fixed:
            out.append(self._fixed[name])
        if name in self._routers:
            out.extend(sorted(self._router_targets(name)))
        return out

    def _router_targets(self, name: str) -> set[str]:
        """条件边的可能出口。

        router 是黑盒函数（要 ctx 才能算），静态拿不到它返回什么。
        所以这里要求定义 graph 时用 `Graph.declare_targets()` 显式声明 ——
        否则拓扑排序和"故障影响范围"都算不出来。

        这是本引擎唯一要求使用者配合的地方，代价是一个显式的声明，
        换来的是：校验、画图、故障重调度三件事都能在**不执行**的前提下算出来。
        """
        return self._declared.get(name, set())

    def declare_targets(self, name: str, targets: list[str]) -> "Graph":
        """声明一个条件边的所有可能出口（用于校验与影响范围分析）。"""
        self._declared.setdefault(name, set()).update(targets)
        return self

    # ---------- 校验 ----------
    def validate(self) -> None:
        if self.entry not in self.nodes:
            raise GraphValidationError(f"入口节点不存在：{self.entry}")

        for src, dst in self._fixed.items():
            if src not in self.nodes:
                raise GraphValidationError(f"固定边的起点不存在：{src}")
            if dst not in self.nodes:
                raise GraphValidationError(f"固定边的终点不存在：{src} → {dst}")
        for src in self._routers:
            if src not in self.nodes:
                raise GraphValidationError(f"条件边的起点不存在：{src}")

        # 条件边声明的出口也必须存在
        for src, targets in self._declared.items():
            for t in targets:
                if t not in self.nodes:
                    raise GraphValidationError(f"条件边声明了不存在的出口：{src} → {t}")

        # 固定边成环 = 定义错误，必须在这里就拒绝
        self.topological_order()

        # 可达性：定义了一堆节点却从入口到不了，说明装配写错了
        reachable = self.reachable_from(self.entry)
        orphan = set(self.nodes) - reachable
        if orphan:
            raise GraphValidationError(f"从入口不可达的节点：{sorted(orphan)}")

    def topological_order(self) -> list[str]:
        """Kahn 算法拓扑排序；有环则抛异常。

        只看**固定边**——条件边的环是合法设计（澄清追问、覆盖率打回），
        它们由运行时刹车管，不在这里拦。
        """
        indeg = {n: 0 for n in self.nodes}
        for _, dst in self._fixed.items():
            indeg[dst] += 1
        queue = [n for n, d in indeg.items() if d == 0]
        queue.sort()                      # 排序保证输出稳定 —— 否则测试会随机失败
        order: list[str] = []
        while queue:
            cur = queue.pop(0)
            order.append(cur)
            dst = self._fixed.get(cur)
            if dst is not None:
                indeg[dst] -= 1
                if indeg[dst] == 0:
                    queue.append(dst)
        if len(order) != len(self.nodes):
            stuck = sorted(set(self.nodes) - set(order))
            raise GraphValidationError(
                f"固定边构成的图里有环（涉及节点 {stuck}）。"
                f"固定边表示「总是这样」，成环意味着流程永远出不来。"
                f"如果你要的是「看情况回头」，请改用 add_conditional_edge。"
            )
        return order

    def find_cycle(self) -> list[str] | None:
        """若固定边构成环则返回涉及环的节点，否则返回 None。

        （与 validate 的关系：validate 会抛异常，这个只回答"有没有"，
        方便在工具/接口里做一个"图健康检查"而不必捕获异常。）
        """
        try:
            self.topological_order()
            return None
        except GraphValidationError:
            indeg = {n: 0 for n in self.nodes}
            for _, dst in self._fixed.items():
                indeg[dst] += 1
            queue = [n for n, d in indeg.items() if d == 0]
            seen: list[str] = []
            while queue:
                cur = queue.pop()
                seen.append(cur)
                dst = self._fixed.get(cur)
                if dst is not None:
                    indeg[dst] -= 1
                    if indeg[dst] == 0:
                        queue.append(dst)
            return sorted(set(self.nodes) - set(seen))

    def reachable_from(self, start: str) -> set[str]:
        seen, stack = set(), [start]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(self.outgoing(cur))
        return seen

    def downstream_of(self, name: str, include_self: bool = False) -> set[str]:
        """故障影响范围：这个节点挂了，哪些下游节点需要重跑。

        对应"故障节点重调度：只重跑下游子树"这个常见问题。
        有了它才能做到"某个节点失败时不做全量重跑"——
        而全量重跑在多 Agent 里很贵（每个节点都在烧 token）。

        注意条件边的出口来自 declare_targets()：不声明的话，
        这里算出的影响范围会偏小 —— 偏小比偏大危险（会漏重跑）。
        """
        found = self.reachable_from(name)
        return found if include_self else (found - {name})

    # ---------- 路由 ----------
    def next_of(self, name: str, ctx: Any) -> str | None:
        if name in self._routers:
            nxt = self._routers[name](ctx)
            if nxt is not None and nxt not in self.nodes:
                raise GraphError(f"router（{name} 之后）返回了不存在的节点：{nxt}")
            return nxt
        return self._fixed.get(name)

    # ---------- 描述 ----------
    def describe(self) -> str:
        lines = [f"Graph(entry={self.entry})"]
        for name in self.topological_order():
            node = self.nodes[name]
            if name in self._routers:
                outs = ", ".join(sorted(self._router_targets(name))) or "?"
                lines.append(f"  {name:12s} -[条件]-> {outs}")
            else:
                dst = self._fixed.get(name, "(结束)")
                lines.append(f"  {name:12s} --------> {dst}")
        return "\n".join(lines)


# ============================================================
# 执行器
# ============================================================

def run_graph(graph: Graph, ctx: Any, *, start: str | None = None) -> dict[str, int]:
    """执行整张图，返回每个节点的执行次数。

    关于 `start`：传它就是**断点恢复**的入口。checkpoint 里存了"上次执行到哪个节点"，
    重启后从这个节点继续 —— 前面已经跑完的节点不会再花一次钱。
    这是"长任务挂掉后能从断点续跑"的实现方式。

    每一步之后都会 ctx.checkpoint(...)，所以任意时刻崩溃都能续。
    """
    graph.validate()
    visits: dict[str, int] = defaultdict(int)
    current: str | None = start or graph.entry
    steps = 0

    while current is not None:
        steps += 1
        if steps > MAX_STEPS:
            raise NodeBudgetExceeded(
                f"单次任务的节点执行步数超过 {MAX_STEPS}，疑似死循环。"
                f"已执行：{dict(visits)}"
            )

        node = graph.nodes[current]
        visits[current] += 1
        # 刹车检查放在**执行前**：宁可少跑一次，也不能先花了钱再报超限。
        if visits[current] > node.max_visits:
            raise NodeBudgetExceeded(
                f"节点 {current} 第 {visits[current]} 次被调度，超过上限 {node.max_visits}。"
                f"这通常意味着它的上游 router 没能收敛 —— 请检查该 router 的判断条件。"
            )

        ctx.emit(events.NODE_STARTED, node=current, visit=visits[current],
                 description=node.description)
        try:
            result = node.fn(ctx)
        except Exception as exc:  # noqa: BLE001
            ctx.emit(events.NODE_FAILED, node=current, visit=visits[current],
                     error=f"{type(exc).__name__}: {exc}")
            raise

        if result.patch:
            ctx.merge(result.patch)
        ctx.emit(events.NODE_FINISHED, node=current, visit=visits[current],
                 ok=result.ok, summary=result.summary, error=result.error)

        # 落检查点。放在 emit 之后、取下一个节点之前 ——
        # 这样万一 router 抛异常，我们至少知道"这个节点是成功完成的"。
        ctx.checkpoint(node=current, visits=dict(visits), steps=steps)

        if not result.ok:
            raise GraphError(f"节点 {current} 执行失败：{result.error}")

        current = graph.next_of(current, ctx)

    return dict(visits)
