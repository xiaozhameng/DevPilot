"""④ 人工审批节点：流水线在这里挂起，等人放行。

## 为什么这个节点必须真的"挂起"，而不是"发个通知继续跑"

很多系统把人工审批做成"发一条消息给人，然后继续执行"——那不叫审批，
那叫通知。真正的审批意味着**在得到答复之前，下游一步都不能走**。

所以这里的实现是阻塞等待（threading.Event），代价是占一个工作线程，
换来的是语义正确：任务真的停住了，状态真的是 WAITING_APPROVAL，
数据库里真的能看到"有一个任务在等人"。

## 超时为什么按"拒绝"处理，而不是"通过"

这是安全默认值的问题。如果超时算通过，那么当审批人休假、离职、消息丢了，
流水线会**自作主张地继续**——而这正是审批要防的事。
超时算拒绝，最坏结果是任务白做（可重跑），
反过来则是"未经批准的东西被交付了"，代价不对称。

## 状态流转

进入时 RUNNING → WAITING_APPROVAL，获批后 WAITING_APPROVAL → RUNNING。
这两条转移在 state.py 的转移表里都是显式允许的；
而 WAITING_APPROVAL → SUCCEEDED 被**刻意禁止**——审批还没给结果就报成功，
等于把人当摆设。
"""

from __future__ import annotations

from .. import events
from ..graph import NodeResult
from ..runtime import ApprovalTimeout
from ..state import TaskStatus
from .base import as_list, failed, ok, text_of


def approve(ctx) -> NodeResult:
    design = ctx.get("design") or {}
    files = as_list(design.get("files"))
    prd_title = text_of((ctx.get("prd") or {}).get("title"), "未命名需求")

    question = f"技术方案已完成（{len(files)} 个文件），批准进入编码阶段？"
    payload = {
        "title": prd_title,
        "files": [{"path": text_of(f.get("path")), "purpose": text_of(f.get("purpose"))}
                  for f in files if isinstance(f, dict)],
        "risks": [text_of(r) for r in as_list(design.get("risks"))],
    }

    # 先落状态，再发事件 —— 顺序不能反。
    # 反了的话，前端会先收到"需要审批"然后去查状态，此时状态还是 RUNNING，
    # 界面会显示一个自相矛盾的任务（要审批但不在等待中）。
    ctx.set_status(TaskStatus.WAITING_APPROVAL)
    ctx.gate.request(ctx.task_id, "approve", question, payload=payload)
    ctx.emit(events.APPROVAL_REQUIRED, node="approve", question=question, **payload)
    ctx.log(f"已挂起等待人工审批：{question}")

    if ctx.get("auto_approve"):
        # 演示/测试模式：不等外部回调，立刻放行。
        # 注意这里仍然走完整的 resolve → wait 路径，而不是直接 return ——
        # 直接 return 的话，这段审批代码在测试里就永远跑不到，
        # 于是"测试通过"和"审批逻辑正确"变成了两件事。
        ctx.log("auto_approve 已开启，自动放行")
        ctx.gate.resolve(ctx.task_id, True, "auto_approve（演示/测试模式）")

    try:
        decision = ctx.gate.wait(ctx.task_id, timeout=ctx.settings.approval_timeout)
    except ApprovalTimeout as exc:
        return failed("approve", str(exc))

    approved = bool(decision.get("approved"))
    comment = text_of(decision.get("comment"))
    ctx.emit(events.APPROVAL_RESOLVED, node="approve", approved=approved, comment=comment)

    if not approved:
        return failed("approve", f"人工审批未通过：{comment or '（未给出原因）'}")

    # 获批后必须回到 RUNNING —— 见模块 docstring 里对状态转移的说明。
    ctx.set_status(TaskStatus.RUNNING)
    how = "（已批准）"
    if comment:
        how += f" 审批意见：{comment}"
    return ok("approve", f"审批通过 {how}", approved=True, approval_comment=comment)
