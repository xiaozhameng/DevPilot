"""任务依赖图的静态分析：拓扑排序 · 关键路径 · 故障重调度 · 有限并行度调度。

（这个模块是一个经典 DAG 分析问题的可运行答案：
 "给定工具调用依赖图，部分节点需 LLM 产出参数、部分可并行，
  输出合法执行顺序并求最小完成时间，考虑重试与故障节点重调度"）

## 为什么和 graph.py 分开

graph.py 的 Graph 是**流水线引擎**：一个节点一条出边，条件边可以在运行时回头。
它的形状是"一条路，可能绕回来"。用它去表达"DAG 分叉"是别扭的 ——
分叉意味着一个节点之后有多个后继要**都执行**，而条件边意味着**只选一个**。
两者语义相反，硬塞进一个抽象里会让两边都变脏。

所以拆开：
  - graph.py  ——  运行时编排（单路径 + 条件边 + 刹车 + 检查点）
  - plan.py   ——  静态依赖分析（DAG + 关键路径 + 调度），纯函数，无副作用

plan.py 的每个函数都可以被单独测、单独讲、单独拿去回答白板题。
这也是"依赖表"这种输入的天然边界：它不关心节点是 Agent 还是编译任务。

## 约定

`deps[node] = [node 依赖的节点]`（前置，不是后继）。
选前置是因为拓扑排序天然按入度推进，而入度就是 `len(deps[node])`。
"""

from __future__ import annotations

import heapq
from collections import defaultdict


class DepsError(ValueError):
    pass


def _check(deps: dict[str, list[str]]) -> None:
    for node, preds in deps.items():
        for p in preds:
            if p not in deps:
                raise DepsError(f"节点 {node} 依赖了不存在的节点 {p}")
        if node in preds:
            raise DepsError(f"节点 {node} 依赖了自己")


def successors(deps: dict[str, list[str]]) -> dict[str, list[str]]:
    succ: dict[str, list[str]] = defaultdict(list)
    for node, preds in deps.items():
        for p in preds:
            succ[p].append(node)
    return {k: sorted(v) for k, v in succ.items()}


def topo_order(deps: dict[str, list[str]]) -> list[str]:
    """Kahn 拓扑排序。有环则抛 DepsError（环必须被拒绝，不能让系统硬跑）。

    实现细节：ready 队列用**有序列表 + pop(0) 后重新排序**，
    而不是普通 list —— 同一张图每次跑出来的顺序必须一样，
    否则"输出合法执行顺序"这个要求就没法被测试固定下来。
    """
    _check(deps)
    indeg = {n: len(set(ps)) for n, ps in deps.items()}
    ready = sorted(n for n, d in indeg.items() if d == 0)
    order: list[str] = []
    while ready:
        cur = ready.pop(0)
        order.append(cur)
        for nxt in sorted(successors(deps).get(cur, [])):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready.append(nxt)
        ready.sort()
    if len(order) != len(deps):
        stuck = sorted(set(deps) - set(order))
        raise DepsError(f"依赖图里有环，涉及节点：{stuck}。环意味着无法确定执行顺序。")
    return order


def find_cycle(deps: dict[str, list[str]]) -> list[str] | None:
    try:
        topo_order(deps)
        return None
    except DepsError:
        indeg = {n: len(set(ps)) for n, ps in deps.items()}
        ready = sorted(n for n, d in indeg.items() if d == 0)
        seen: set[str] = set()
        succ = successors(deps)
        while ready:
            cur = ready.pop()
            seen.add(cur)
            for nxt in succ.get(cur, []):
                indeg[nxt] -= 1
                if indeg[nxt] == 0:
                    ready.append(nxt)
        return sorted(set(deps) - seen)


def critical_path(deps: dict[str, list[str]],
                  weights: dict[str, float]) -> tuple[list[str], float]:
    """关键路径（带权最长路）与**最小完成时间**。

    为什么是**最长**路而不是最短路？
    因为在依赖图里，一个节点的最早开始时间取决于它**最晚就绪的那个前置**。
    短的那条分支早就跑完了，但没人能用它的结果 —— 它在等。
    所以总完成时间由最长的那条链决定，那条链就是关键路径。

    要缩短总工期，只能动关键路径上的节点；优化非关键路径上的节点，
    在关键路径变化之前**一点用都没有**。这就是关键路径法的全部价值。
    """
    _check(deps)
    order = topo_order(deps)
    succ = successors(deps)

    # 最早完成时间 earliest finish
    ef: dict[str, float] = {}
    prev: dict[str, str | None] = {}
    for n in order:
        preds = sorted(set(deps.get(n, [])))
        if not preds:
            ef[n] = float(weights.get(n, 0))
            prev[n] = None
        else:
            best = max(preds, key=lambda p: ef[p])
            ef[n] = ef[best] + float(weights.get(n, 0))
            prev[n] = best
    if not ef:
        return [], 0.0

    end = max(sorted(ef), key=lambda n: ef[n])
    makespan = ef[end]

    path: list[str] = []
    cur: str | None = end
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    return path, makespan


def downstream(deps: dict[str, list[str]], failed: str) -> set[str]:
    """故障重调度范围：failed 挂了，哪些节点必须重跑。

    结论是**它自己和它的所有后代**。前置不用重跑 —— 它们的结果还在，
    重跑它们纯属浪费（在多 Agent 里就是白白多烧几轮 token）。
    这是"只重跑下游子树"那句话的具体算法。
    """
    _check(deps)
    if failed not in deps:
        raise DepsError(f"不存在的节点：{failed}")
    succ = successors(deps)
    seen: set[str] = set()
    stack = [failed]
    while stack:
        cur = stack.pop()
        if cur in seen:
            continue
        seen.add(cur)
        stack.extend(succ.get(cur, []))
    return seen


def schedule(deps: dict[str, list[str]], weights: dict[str, float],
             parallelism: int = 2) -> dict:
    """有限并行度下的调度（列表调度的贪心版）。

    为什么需要它：关键路径给出的是"无限并行度下的理论下限"。
    现实中并发是有限的（LLM 有速率限制、同一类工具共享配额），
    所以实际完成时间会大于关键路径长度。能把这两个数都说出来，
    比只背一个关键路径公式强得多 —— 它说明你知道理论值和工程值的差别。

    算法：时间推进式模拟。有资源就派"已经就绪"的任务，
    没有就跳到一个任务结束的时刻。这是一个 2-近似（Graham 界）的经典贪心。
    """
    _check(deps)
    if parallelism < 1:
        raise DepsError("并行度至少为 1")

    order = topo_order(deps)
    succ = successors(deps)
    indeg = {n: len(set(deps.get(n, []))) for n in order}
    ready = sorted(n for n, d in indeg.items() if d == 0)

    running: list[tuple[float, str]] = []       # (完成时刻, 节点)
    start_at: dict[str, float] = {}
    now = 0.0

    while ready or running:
        while ready and len(running) < parallelism:
            n = ready.pop(0)
            start_at[n] = now
            heapq.heappush(running, (now + float(weights.get(n, 0)), n))
        if not running:
            break
        now, done = heapq.heappop(running)
        for nxt in succ.get(done, []):
            indeg[nxt] -= 1
            if indeg[nxt] == 0:
                ready.append(nxt)
        ready.sort()

    makespan = max((start_at[n] + float(weights.get(n, 0)) for n in order), default=0.0)
    return {"start_at": start_at, "makespan": makespan,
            "parallelism": parallelism, "order": order}


def execution_plan(deps: dict[str, list[str]], weights: dict[str, float],
                   parallelism: int = 2, failed: list[str] | None = None) -> dict:
    """把上面几个函数合成一份"可执行计划"，方便一次性讲清楚。

    返回：
      order          合法执行顺序
      critical_path  关键路径
      makespan_inf   无限并行度下的最小完成时间（= 关键路径长度）
      makespan_real  给定并行度下的实际完成时间
      bottleneck     关键路径上最重的节点（优化它收益最大）
      retry          需要重跑的节点（如果给了 failed）
    """
    order = topo_order(deps)
    path, inf = critical_path(deps, weights)
    real = schedule(deps, weights, parallelism)["makespan"]
    bottleneck = max(path, key=lambda n: float(weights.get(n, 0))) if path else None
    retry: list[str] = []
    if failed:
        need: set[str] = set()
        for f in failed:
            need |= downstream(deps, f)
        retry = [n for n in order if n in need]
    return {
        "order": order,
        "critical_path": path,
        "makespan_inf": inf,
        "makespan_real": real,
        "bottleneck": bottleneck,
        "retry": retry,
    }
