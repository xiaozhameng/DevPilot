"""① 需求澄清节点：把一句模糊需求变成「可以开工」的规格。

## 判断标准是这个节点唯一重要的东西

"信息够不够"是个很容易做错的决定。做错的两个方向代价完全不同：

- **判"不够"判多了**：用户被反复追问，体验崩，而且很多问题用户自己也没想清楚 ——
  你问不出来，只是把不确定性推给了用户。
- **判"不够"判少了**：流水线带着错误的假设往下跑，产出 500 行没人要的代码。

所以这里的判据不是"需求完不完美"，而是**"一个工程师拿到它能不能不问问题就开工"**。
缺的必须是"不写代码就定不下来"的东西（技术栈、数据模型、核心接口、验收标准）；
风格偏好、扩展性设想这类，按行业惯例给默认值并**显式写进 assumptions** 就够了。

写成 assumptions 而不是丢掉，是这一步最关键的设计：
假设被记录下来，下游每一步都能看到"我们是在这个前提下工作的"，
出了问题时也能一眼看出是哪条假设错了。

## 刹车

`max_clarify`（默认 3 轮）是这里的环的刹车。而且它是双重的：
prompt 里告诉模型"够用就行"，引擎里再用 max_visits 兜底。
只靠 prompt 自律是不够的 —— 模型完全可能连续三轮都说"还缺东西"。
"""

from __future__ import annotations

from ..graph import NodeResult
from ..llm import LLMError
from .base import as_list, call_json, ok, text_of

SYSTEM = """你的任务是把一句模糊的需求，变成一份信息完整、可以开工的需求说明。

判断 sufficient 的唯一标准：
一个工程师拿到你的 spec，能不能**不问任何问题**就开始写代码？

能 → true。不能 → false，并列出到底缺什么。

注意：不要因为"需求不够完美"就判 false。只有缺的东西属于
"不写代码就定不下来"时才判 false —— 技术栈、数据模型、核心接口、验收标准。
风格偏好、扩展性设想、部署方式这类，直接按行业惯例取默认值，
写进 assumptions 即可。反复追问用户是更差的结果，不是更好的结果。"""

USER = """原始需求：
{requirement}

已确认的补充信息：
{answers}

请输出 JSON（不要任何解释）：
{{
  "sufficient": true 或 false,
  "questions": ["sufficient=false 时必填，列出真正阻塞开工的问题"],
  "spec": "把需求整理成明确、可执行的规格：做什么、边界在哪、不做什么",
  "assumptions": ["信息不足处采用的技术假设，每条一句话"]
}}"""

_FALLBACK_ASSUMPTION = "未做澄清（模型不可用），以下按原始需求字面执行"


def clarify(ctx) -> NodeResult:
    requirement = ctx.requirement
    answers = [str(a) for a in as_list(ctx.get("clarify_answers", []))]
    rounds = ctx.bump("clarify_round")
    max_rounds = ctx.settings.max_clarify

    ctx.log(f"需求澄清 · 第 {rounds} 轮（上限 {max_rounds}）")

    try:
        data = call_json(
            ctx, node="clarify", system=SYSTEM,
            user=USER.format(requirement=requirement,
                             answers="\n".join(answers) or "（无）"),
            required=["sufficient", "spec", "assumptions"],
        )
    except LLMError as exc:
        # 降级：不阻塞流水线。用原需求当规格，并显式标注"未经澄清"。
        # 宁可产出"结构完整但内容保守"的东西，也不要在这里断掉整条链 ——
        # 后面的步骤（沙箱、状态机、检查点）都是确定性的，不该被一个 API 故障拖死。
        ctx.log(f"澄清降级（模型不可用）：{exc}", level="warn")
        return ok("clarify", "模型不可用，直接使用原始需求",
                  spec=requirement,
                  assumptions=[_FALLBACK_ASSUMPTION],
                  open_questions=[],
                  clarify_sufficient=True,
                  clarify_degraded=True)

    sufficient = bool(data.get("sufficient"))
    questions = [str(q) for q in as_list(data.get("questions"))]
    spec = text_of(data.get("spec"), requirement)
    assumptions = [str(a) for a in as_list(data.get("assumptions"))]

    # 即使模型说"还不够"，到了上限也必须放行 —— 否则就是死等。
    # 这条判断的位置很关键：它必须在**引擎刹车之外**再有一道，
    # 否则触发器是引擎异常（整个任务失败），而不是"带着风险继续"。
    give_up = (not sufficient) and rounds >= max_rounds
    done = sufficient or give_up

    if done:
        reason = "信息已足够" if sufficient else f"已达轮次上限（{max_rounds}），带着假设继续"
        if questions:
            ctx.log(f"未能解决的问题将作为风险记录：{questions}", level="warn")
        return ok("clarify", f"{reason}｜{len(assumptions)} 条假设",
                  spec=spec, assumptions=assumptions, open_questions=questions,
                  clarify_sufficient=True)

    ctx.log(f"信息不足，需要追问 {len(questions)} 个问题：{questions}", level="warn")
    return ok("clarify", f"信息不足，第 {rounds} 轮追问", spec=spec,
              assumptions=assumptions, open_questions=questions,
              clarify_sufficient=False)


def clarify_router(ctx) -> str | None:
    """条件边：够了就去写 PRD，不够就回头再澄清一轮。

    回环的安全性由两处保证：
      - 这里读的 `clarify_sufficient` 在达到轮次上限时会被强制置 True
      - 引擎侧 max_visits 兜底（就算上面那个判断写错了，也不会跑飞）
    """
    return "prd" if ctx.get("clarify_sufficient") else "clarify"
