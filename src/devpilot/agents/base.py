"""Agent 节点的公共基座：结构化输出 + Schema 校验 + 不合格重试。

## 这就是"harness 第 4 层：验证"的具体实现

模型输出 JSON 这件事，实践中的失败率远高于直觉：字段名拼错、少一个必填字段、
把数组写成字符串、把 true 写成 "true"。不校验就往下传，错误会在下游
以完全无关的形式爆出来（"为什么文件树是空的"），排查成本极高。

所以在**每个**节点的出口做一次校验，不合格就把错误原样回灌给模型重试。
注意这里的关键不是"重试"，而是**把具体的错误告诉它**：
只说"请重新输出"没用的，模型会原样再错一遍；
说"缺少字段 test_plan、files 应为数组而不是字符串"，它才有可能改对。

## 为什么还要降级模板

模型不可用（没配 key、配额耗尽、上游故障）时，整条流水线的骨架
（编排、状态机、沙箱、断点恢复）不该跟着一起停摆。
骨架是确定性的，模型只是其中一环 —— 把这一环做成可替换的，
系统在模型挂掉时仍能跑完并产出"占位但结构完整"的交付物。

这道降级还有第二个用处：**演示时的确定性**。网络抖动不该让 demo 失败。
"""

from __future__ import annotations

from ..llm import LLMError, OutputTruncated
from ..graph import NodeResult


class SchemaError(ValueError):
    pass


SYSTEM_BASE = (
    "你是一个资深的全栈交付工程师，正在参与一条自动化交付流水线。"
    "你的输出会被程序直接解析并写入文件，因此必须严格遵守约定的格式。"
    "只输出要求的 JSON，不要输出任何解释、前言或 Markdown 围栏。"
)


def call_json(ctx, *, node: str, system: str, user: str, required: list[str],
              retries: int = 2, max_tokens: int = 2000) -> dict:
    """调模型并强制校验必填字段，不合格则带错误重试。

    <h2>为什么"被截断"必须立刻放弃重试</h2>

    格式类错误（缺字段、类型不对）值得重试，因为把**具体的错误**回灌给模型
    是有可能改对的。但输出被 max_tokens 截断是另一回事：重试多少次，
    长度上限都一样，只会得到同样截断的结果 —— 三次重试换来的只是三倍的钱，
    以及一条把矛头指向模型的日志「模型未返回合法 JSON」。

    <p>真实案例：design 节点默认 2000 tokens，而它的产出是一份十几个文件的
    技术方案，必然截断。于是它连续三次"不合规"、静默降级成最小骨架，
    整条流水线**依然报告成功**。交付物质量掉了，日志里没有任何一处说
    "这里出过问题"。所以这里对 {@link OutputTruncated} 直接放行不重试，
    让真正的错误信息冒到最上面。
    """
    llm = ctx.llm_for(node)
    if not llm.enabled:
        raise LLMError("未配置可用的模型")

    messages = [
        {"role": "system", "content": f"{SYSTEM_BASE}\n\n{system}"},
        {"role": "user", "content": user},
    ]
    last_err: Exception | None = None

    for attempt in range(retries + 1):
        try:
            data, _ = llm.complete_json(messages, max_tokens=max_tokens, temperature=0.2)
            if not isinstance(data, dict):
                raise SchemaError(f"顶层应为 JSON 对象，实际是 {type(data).__name__}")
            missing = [k for k in required if k not in data]
            if missing:
                raise SchemaError(f"缺少必填字段：{missing}")
            return data
        except OutputTruncated as exc:
            # 重试无用：同样的预算只会同样被截断。把原因如实报上去，
            # 由调用方决定是降级、还是把任务拆小。
            ctx.log(f"{node} 输出被长度截断，放弃重试：{exc}", level="warn")
            raise
        except (LLMError, SchemaError, ValueError) as exc:
            last_err = exc
            if attempt >= retries:
                break
            ctx.log(f"{node} 第 {attempt + 1} 次输出不合规（{exc}），带错误重试", level="warn")
            # 关键：把**具体的错误**回灌，而不是只说"重来"
            messages.append({
                "role": "user",
                "content": (f"你上一次的输出不合规：{exc}。"
                            f"请重新只输出一个 JSON 对象，且必须包含这些字段：{required}。"),
            })

    raise LLMError(f"{node} 节点连续 {retries + 1} 次未能产出合规输出：{last_err}")


def as_list(value) -> list:
    """把模型写的"类数组"归一成真正的数组。

    模型经常把单元素数组写成裸值（`"files": "a.py"` 而不是 `["a.py"]`）。
    这类错误不值得让整个节点重试 —— 语义毫无歧义，就地修正是更划算的选择。
    区分标准很清楚：**有歧义的（缺字段、类型完全不对）重启，无歧义的顺手修**。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [value]


def text_of(value, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (list, tuple)):
        return "\n".join(str(v) for v in value)
    return str(value)


def ok(node: str, summary: str, **patch) -> NodeResult:
    return NodeResult(ok=True, summary=summary, patch=patch)


def failed(node: str, error: str) -> NodeResult:
    return NodeResult(ok=False, summary=f"{node} 失败", error=error)
