"""⑥ 覆盖率门禁节点：判定要不要打回重写。

## 这个节点为什么值得单独存在

它只做一件事：读覆盖率，跟阈值比大小，决定"过"还是"打回"。
这么小的逻辑为什么不塞进编码节点里？

因为它是一个**质量门禁**，不是编码的一部分。混在一起会带来两个问题：
  - 编码节点的失败和门禁的不通过会变成同一种信号，没法区分
    "代码写错了（跑不过）"和"代码能跑但测得不够（覆盖率低）"——
    这两种情况的处理方式完全不同（前者重写，后者补测试）。
  - 门禁的阈值是最需要被独立调整和审计的东西。藏在编码节点里，
    没人说得清"到底是谁在决定收不收"。

## 判定要区分三种情况，而不是两种

  1. 跑过了 + 覆盖率达标        → 通过，交付
  2. 跑过了 + 覆盖率不达标      → 打回，带上"哪没覆盖"的反馈
  3. **测试没通过**             → 这不叫"覆盖率 0%"，叫"验证失败"
     （代码有 ImportError 时 pytest 直接收集失败，压根没有覆盖率数字）
     第 3 种必须按"打回"处理，且反馈要指向"先让它能跑"而不是"去加测试"。

把第 3 种误当成第 1 种（"没解析到覆盖率就当通过"）是这类系统里最危险的漏洞：
门禁会永远放行，整个覆盖率机制形同虚设，而且没有任何报错。

## 第 3 种内部还要再分一次（靠 pytest 的退出码）

只有 `test_passed` 一个布尔是不够的 —— 它把两类完全不同的问题压成了一件事：

  - `rc=2`：收集/导入阶段就失败 → 代码压根没跑起来 → "先修到能跑"
  - `rc=1`：测试跑起来了但有失败用例 → 该修实现或断言

实测踩过一次：覆盖率 93%（超过目标 80%）却报"pytest 未能跑通"。
覆盖率能解析出来，恰恰说明测试**跑起来了**，只是有断言失败 ——
方向说反了，编码节点就会去改最不该改的地方。

## 打回次数用尽之后怎么办

接受现状并**显式标记降级**（gate_degraded=True），而不是无限重试。
理由和澄清节点一样：宁可交付一份"有点弱但看得见"的产物，
也不要卡在循环里什么都不给。但那个降级标记必须传出去 ——
下游和人都要知道"这份产物的质量门禁没通过"。
而且**必须带上原因**：`gate_reason` 要说清是"覆盖率差一点"（交付物可用）
还是"测试根本没通过"（交付物是坏的）。这两者对使用者的意义天差地别。
"""

from __future__ import annotations

from ..graph import NodeResult
from .base import as_list, ok, text_of


def gate(ctx) -> NodeResult:
    target = ctx.settings.coverage_target
    coverage = ctx.get("coverage")
    passed_run = bool(ctx.get("test_passed"))
    attempt = int(ctx.get("codegen_round", 1))
    max_rounds = ctx.settings.max_codegen

    remaining = max_rounds - (attempt - 1)      # 还能打回几次

    # ---- 情况 3：测试没通过。这不是"覆盖率 0"，而是"验证失败"。----
    #
    # 但不能止步于"没通过"这个结论 —— pytest 的**退出码**把这一类又分成两种，
    # 而且它们的修法完全不同：
    #
    #   rc=2  收集/导入阶段就失败（ImportError、SyntaxError、fixture 找不到）
    #         → 代码压根没跑起来，先修到能跑
    #   rc=1  测试跑起来了，但有失败用例（断言不成立、被测行为不对）
    #         → 该修的是实现逻辑或断言，而不是"让它能跑"
    #
    # 只看 `test_passed` 这个布尔会把两者混为一谈。实测出现过一次：
    # 覆盖率 93%（远超目标 80%）却报"pytest 未能跑通" —— 覆盖率能解析出来，
    # 说明测试明明跑起来了，只是有断言失败。反馈方向说反了，
    # 编码节点就会去改最不该改的地方。
    if not passed_run:
        rc = ctx.get("test_returncode")
        if ctx.get("test_timeout"):
            kind, advice = "测试超时", "测试跑不完（可能死循环或等待输入），请先消除阻塞点"
        elif rc == 2:
            kind, advice = "测试无法收集/导入", "请先修到测试能跑起来（导入、语法、fixture），再考虑覆盖率"
        elif rc == 1:
            kind, advice = "存在失败的用例", "实现或断言的预期不一致，请对照失败用例修正；不要为此加 try/except 把失败吞掉"
        else:
            kind, advice = "测试未通过", "请先修到测试能跑起来，再考虑覆盖率"
        # 退出码未知时 kind 就是"测试未通过"本身，再加括号会变成
        # "测试未通过（测试未通过）" —— 读起来像 bug，所以按需拼。
        detail = f"（{kind}）" if kind != "测试未通过" else ""
        feedback = (
            f"上一次 pytest 未通过{detail}（退出码 {rc}）。{advice}。\n"
            f"pytest 输出（末尾）：\n{text_of(ctx.get('test_output'))[-1500:]}"
        )
        if remaining <= 0:
            return ok("gate", f"测试仍未通过{detail}，但打回次数已用尽"
                              f"（{max_rounds} 次），接受现状",
                      gate_passed=True, gate_degraded=True, gate_feedback=feedback,
                      gate_reason=f"测试未通过{detail}，已降级接受")
        return ok("gate", f"测试未通过{detail}，打回重写（剩余 {remaining} 次）",
                  gate_passed=False, gate_feedback=feedback,
                  gate_reason=f"测试未通过{detail}")

    # ---- 情况 2 的前置：跑过了但拿不到覆盖率数字 ----
    if coverage is None:
        # 拿不到数字有两种可能：没装覆盖率插件，或者 --cov 路径不对。
        # 这两种都不该被当成"合格"，但也不该无限打回（打回也修不出来）。
        if attempt >= max_rounds:
            return ok("gate", "未解析到覆盖率，打回次数已用尽，接受现状",
                      gate_passed=True, gate_degraded=True,
                      gate_feedback="未能解析覆盖率数据",
                      gate_reason="未能解析到覆盖率数据（测试可能没被正常收集），已降级接受")
        return ok("gate", "未解析到覆盖率，打回一次试试",
                  gate_passed=False,
                  gate_feedback="上一次没能解析到覆盖率数据。"
                                "请确保测试文件能被 pytest 正常收集并执行。")

    # ---- 情况 1 / 2：有数字，比阈值 ----
    if coverage >= target:
        return ok("gate", f"覆盖率 {coverage:.0f}% ≥ 目标 {target}%，门禁通过",
                  gate_passed=True, gate_degraded=False)

    feedback = (
        f"覆盖率只有 {coverage:.0f}%，低于目标 {target}%。"
        f"请为**未被覆盖的分支**补测试：边界值、空输入、异常路径。"
        f"不要改动已经正确的实现逻辑，只补测试。\n"
        f"pytest 输出（末尾）：\n{text_of(ctx.get('test_output'))[-1200:]}"
    )
    if remaining <= 0:
        return ok("gate", f"覆盖率 {coverage:.0f}% 未达标，但打回次数已用尽"
                          f"（{max_rounds} 次），接受现状",
                  gate_passed=True, gate_degraded=True, gate_feedback=feedback,
                  gate_reason=f"覆盖率 {coverage:.0f}% 低于目标 {target}%，已降级接受")

    return ok("gate", f"覆盖率 {coverage:.0f}% < {target}%，打回重写（剩余 {remaining} 次）",
              gate_passed=False, gate_feedback=feedback)


def gate_router(ctx) -> str | None:
    """条件边：不达标就回 code，达标就结束整条流水线。

    返回 None 表示"到此为止" —— 流水线正常收尾。
    """
    if ctx.get("gate_passed"):
        return None
    ctx.log("门禁未通过，回到编码节点重写", level="warn")
    return "code"
