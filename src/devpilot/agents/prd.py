"""② PRD 节点：把规格展开成一份产品需求文档，落到沙箱里。

为什么先要 JSON 再渲染 Markdown，而不是直接让模型写 Markdown？
因为 Markdown 是**给人看的**，而下游的编码节点需要的是**能被程序读的**结构。
如果只产出一份 Markdown，下游就得用正则去解析它 —— 那等于把结构化的问题
降级成了文本处理问题，模型稍一发挥（换个写法、多个空格）就解析不出来。

所以链路是：模型产 JSON（可校验）→ 代码渲染 Markdown（可读）→ 两者都留在任务里。
JSON 喂下游，Markdown 给人看。
"""

from __future__ import annotations

from ..graph import NodeResult
from ..llm import LLMError
from .base import as_list, call_json, ok, text_of

SYSTEM = """你是一个资深产品经理，正在为一场"从零到可运行"的自动交付写 PRD。

要求：
- 需求必须能被**有限的几个文件**实现。不要规划一个需要 50 个文件的大系统 ——
  这次交付的目标是"能跑起来、有测试、结构清晰"，不是"功能完备"。
- 每条用户故事都要有可检验的验收标准（能被一条测试断言的那种）。
- out_of_scope 必须写清楚。不写边界，下游就会自己加需求。"""

USER = """需求规格：
{spec}

已知假设：
{assumptions}

请输出 JSON：
{{
  "title": "一句话产品名",
  "background": "为什么做这个，2~3 句",
  "goals": ["可衡量的目标，2~4 条"],
  "stories": [{{"as": "角色", "want": "想要什么", "so_that": "为了什么"}}],
  "acceptance": ["验收标准，必须可被测试断言，3~6 条"],
  "in_scope": ["本次要做的"],
  "out_of_scope": ["本次明确不做的"]
}}"""


def _render(prd: dict, spec: str, assumptions: list[str]) -> str:
    lines = [f"# {text_of(prd.get('title'), '未命名需求')}", ""]

    bg = text_of(prd.get("background"))
    if bg:
        lines += ["## 背景", "", bg, ""]

    goals = as_list(prd.get("goals"))
    if goals:
        lines += ["## 目标", ""] + [f"- {text_of(g)}" for g in goals] + [""]

    stories = as_list(prd.get("stories"))
    if stories:
        lines += ["## 用户故事", ""]
        for i, s in enumerate(stories, 1):
            if isinstance(s, dict):
                lines.append(f"{i}. 作为 {text_of(s.get('as'), '用户')}，"
                             f"我想要 {text_of(s.get('want'))}，"
                             f"以便 {text_of(s.get('so_that'))}")
            else:
                lines.append(f"{i}. {text_of(s)}")
        lines.append("")

    acc = as_list(prd.get("acceptance"))
    if acc:
        lines += ["## 验收标准", ""] + [f"- [ ] {text_of(a)}" for a in acc] + [""]

    for key, title in (("in_scope", "本次范围"), ("out_of_scope", "明确不做")):
        items = as_list(prd.get(key))
        if items:
            lines += [f"## {title}", ""] + [f"- {text_of(x)}" for x in items] + [""]

    if assumptions:
        lines += ["## 前提假设", ""] + [f"- {a}" for a in assumptions] + [""]

    lines += ["---", "", "> 规格原文：", "", spec, ""]
    return "\n".join(lines)


def _fallback(ctx, spec: str, assumptions: list[str]) -> dict:
    """模型不可用时的降级 PRD：结构完整、内容保守、显式标注。"""
    first = spec.strip().splitlines()[0] if spec.strip() else "未命名需求"
    return {
        "title": first[:40],
        "background": "模型不可用，本 PRD 为结构化占位版本。",
        "goals": ["实现需求中描述的核心能力", "至少有一条自动化测试覆盖"],
        "stories": [{"as": "使用者", "want": first[:60], "so_that": "完成目标"}],
        "acceptance": ["模块可被导入", "核心函数有测试且通过"],
        "in_scope": ["核心逻辑与其单元测试"],
        "out_of_scope": ["性能优化", "并发与部署"],
        "_degraded": True,
    }


def prd(ctx) -> NodeResult:
    spec = text_of(ctx.get("spec")) or ctx.requirement
    assumptions = [str(a) for a in as_list(ctx.get("assumptions", []))]

    try:
        data = call_json(
            ctx, node="prd", system=SYSTEM,
            user=USER.format(spec=spec,
                             assumptions="\n".join(assumptions) or "（无）"),
            required=["title", "stories", "acceptance", "in_scope", "out_of_scope"],
        )
        degraded = False
    except LLMError as exc:
        ctx.log(f"PRD 降级（模型不可用）：{exc}", level="warn")
        data = _fallback(ctx, spec, assumptions)
        degraded = True

    markdown = _render(data, spec, assumptions)
    ctx.sandbox.write("docs/PRD.md", markdown)
    ctx.add_artifact("docs/PRD.md", "prd", text_of(data.get("title")))

    n_stories = len(as_list(data.get("stories")))
    n_acc = len(as_list(data.get("acceptance")))
    summary = f"PRD 已产出（{n_stories} 条故事 / {n_acc} 条验收）"
    if degraded:
        summary += "｜模型不可用，为占位版本"

    return ok("prd", summary, prd=data, prd_markdown=markdown, prd_degraded=degraded)
