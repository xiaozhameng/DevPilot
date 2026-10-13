"""③ 技术方案节点：产出文件树与接口契约。

## 这个节点存在的唯一理由：防止上下文污染

多 Agent 系统里最容易被忽视的成本不是 token，是**上下文污染**。
如果编码 Agent 直接读前面四步的全部对话（澄清讨论、PRD 草稿、备选方案、被否掉的思路），
它会：把已经否掉的设计又实现一遍、在无关的细节上纠结、以及最糟的 ——
把"讨论过但没定"的东西当成"已经定了"。

所以每个 Agent 之间传的**不是对话，是一份收敛后的契约**。
这个节点的职责就是做那次收敛：把前面所有讨论压成一张
"要建哪些文件、每个文件负责什么、对外暴露什么接口"的表。

这也解释了为什么它要单独占一个节点、单独花一次模型调用：
**收敛是有成本、也有价值的**，混在编码节点里做，模型就会跳过这一步直接开写。

## 文件数量必须被限制

提示词里明确要求 3~6 个文件。不限制的话，模型天然倾向于规划一个
"架构完整"的系统（十几二十个文件），而每个文件都要一次模型调用 ——
交付时间线性上涨，且大部分文件只是为了显得完整。
限制文件数，本质上是把"这次交付的目标是能跑起来"这个约束写进规划里。
"""

from __future__ import annotations

from ..graph import NodeResult
from ..llm import LLMError, OutputTruncated
from .base import as_list, call_json, ok, text_of

SYSTEM = """你是一个资深架构师，负责把一个 PRD 落成一份**最小可行**的技术方案。

硬性约束：
- 源文件数量控制在 3~6 个。这是刻意的限制：本次目标是"能跑起来、结构清晰"，
  不是"架构完备"。多余的抽象层只会拖慢交付、增加出错的表面积。
- 每个文件必须说明它**为什么存在**（不是"它包含什么"）。
- 接口契约要具体到函数签名级别，否则下游会自己发明。
- 测试计划要指向具体的分支/边界，不要写"补充单元测试"这种废话。"""

USER = """PRD：
{prd}

需求规格：
{spec}

请输出 JSON：
{{
  "language": "实现语言",
  "files": [{{"path": "相对路径", "purpose": "这个文件为什么存在（一句话）"}}],
  "interfaces": [{{"name": "函数/类名", "signature": "签名", "behavior": "行为约定"}}],
  "test_plan": ["要覆盖的分支或边界，逐条具体"],
  "risks": ["实现时最可能出错的地方"]
}}"""


def _fallback_design(ctx) -> dict:
    """降级方案：给出一个最小可跑的骨架（单个模块 + 一个测试）。"""
    return {
        "language": "Python",
        "files": [
            {"path": "app/main.py", "purpose": "核心逻辑入口，承载需求描述的主要能力"},
            {"path": "tests/test_main.py", "purpose": "覆盖核心逻辑的正常与边界分支"},
        ],
        "interfaces": [{"name": "main", "signature": "main() -> None",
                        "behavior": "无副作用；核心逻辑拆成可测试的纯函数"}],
        "test_plan": ["核心路径", "空输入", "非法输入"],
        "risks": ["模型不可用，方案为最小占位版本"],
        "_degraded": True,
    }


def design(ctx) -> NodeResult:
    prd_data = ctx.get("prd") or {}
    spec = text_of(ctx.get("spec")) or ctx.requirement

    try:
        data = call_json(
            ctx, node="design", system=SYSTEM,
            user=USER.format(prd=text_of(ctx.get("prd_markdown"))[:2500],
                             spec=spec[:1500]),
            required=["files", "interfaces", "test_plan"],
            # 这个数字不能小气。技术方案的产出是一份**文件树 + 每个文件的职责说明**，
            # 十几个文件轻松超过 2000 token。给 2000 的后果不是"失败"，
            # 而是输出被静默截断 → JSON 解析不了 → 三次重试都截断 → 降级成最小骨架，
            # 而整条流水线**依然报告成功**。交付物质量掉了却没有任何告警，
            # 这比直接失败危险得多。6000 是实测能覆盖 15 个左右文件的值。
            max_tokens=6000,
        )
        degraded = False
        degrade_reason = ""
    except OutputTruncated as exc:
        # 截断与"模型不可用"是两回事，日志必须分开 —— 否则排查时
        # 会去查 key、查配额，而真正的问题是我们没给够输出空间。
        ctx.log(f"技术方案输出被长度截断，降级为最小骨架：{exc}", level="warn")
        data = _fallback_design(ctx)
        degraded = True
        degrade_reason = "模型输出被截断"
    except LLMError as exc:
        ctx.log(f"技术方案降级（模型不可用）：{exc}", level="warn")
        data = _fallback_design(ctx)
        degraded = True
        degrade_reason = "模型不可用"

    files = [f for f in as_list(data.get("files")) if isinstance(f, dict) and f.get("path")]
    if not files:
        # 方案里一个文件都没有 → 下游无从下手。这不是"降级"，是输出不可用，
        # 必须失败，否则会产出一个"成功但什么都没有"的任务。
        return NodeResult(ok=False, summary="技术方案未产出任何文件",
                          error="design 节点返回的 files 为空")

    markdown = _render_design(data, files, spec)
    ctx.sandbox.write("docs/DESIGN.md", markdown)
    ctx.add_artifact("docs/DESIGN.md", "design", f"{len(files)} 个文件的技术方案")

    summary = f"方案已产出（{len(files)} 个源文件）"
    if degraded:
        # 把**原因**写进摘要，而不是笼统一句"模型不可用"。
        # 这两种降级的处置方式完全不同：截断要去调 max_tokens 或拆小任务，
        # 不可用要去查 key 与配额。摘要会出现在界面与审计里，
        # 它是排查时第一眼看到的东西，含糊就等于没写。
        summary += f"｜降级：{degrade_reason}，为最小骨架"

    return ok("design", summary,
              design={"language": text_of(data.get("language"), "Python"),
                      "files": files,
                      "interfaces": as_list(data.get("interfaces")),
                      "test_plan": as_list(data.get("test_plan")),
                      "risks": as_list(data.get("risks"))},
              design_markdown=markdown,
              design_degraded=degraded)


def _render_design(data: dict, files: list[dict], spec: str) -> str:
    lines = ["# 技术方案", "",
             f"- 实现语言：{text_of(data.get('language'), 'Python')}",
             f"- 源文件数：{len(files)}", ""]

    lines += ["## 文件树", ""]
    for f in files:
        lines.append(f"- `{text_of(f.get('path'))}` — {text_of(f.get('purpose'))}")
    lines.append("")

    ifaces = [i for i in as_list(data.get("interfaces")) if isinstance(i, dict)]
    if ifaces:
        lines += ["## 接口契约", ""]
        for i in ifaces:
            lines.append(f"### `{text_of(i.get('signature'))}`")
            lines.append("")
            lines.append(text_of(i.get("behavior")))
            lines.append("")

    plan = as_list(data.get("test_plan"))
    if plan:
        lines += ["## 测试计划", ""] + [f"- {text_of(p)}" for p in plan] + [""]

    risks = as_list(data.get("risks"))
    if risks:
        lines += ["## 实现风险", ""] + [f"- {text_of(r)}" for r in risks] + [""]

    lines += ["---", "", "> 需求规格：", "", spec, ""]
    return "\n".join(lines)
