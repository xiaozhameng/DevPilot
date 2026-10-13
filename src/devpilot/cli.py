"""命令行界面：内核的薄客户端之一。

这里刻意不重复实现任何业务逻辑 —— 它只做三件事：
创建任务、订阅事件、把事件渲染成人能看的东西。
（唯一的例外是 `plan` 子命令，那是纯计算，不需要内核。）

这也是"内核与 UI 严格分离"的一次实际检验：
命令行能做到的，网页和 Java 网关必然也能做到，因为它们用的是同一套接口。
如果某天发现"这个功能只有网页能做"，那说明 UI 里混进了业务逻辑。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import threading
import time
import uuid

from . import events
from .config import SETTINGS
from .events import BUS
from .pipeline import build_pipeline, run_pipeline
from .plan import execution_plan
from .runtime import ApprovalGate, RunContext
from .sandbox import Sandbox
from .state import Store, TaskStatus

# ANSI 颜色。用裸转义而不是 rich，是为了让 `devpilot ... | grep` 在管道里
# 也能正常工作 —— rich 会检测到非 tty 后关掉颜色，但会额外塞入控制字符。
_DIM = "\033[2m"
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_BLUE = "\033[34m"
_CYAN = "\033[36m"
_BOLD = "\033[1m"
_OFF = "\033[0m"

_ICONS = {
    events.TASK_CREATED: ("+", _CYAN),
    events.TASK_STATUS: ("~", _BLUE),
    events.NODE_STARTED: (">", _BLUE),
    events.NODE_FINISHED: ("v", _GREEN),
    events.NODE_FAILED: ("x", _RED),
    events.NODE_SKIPPED: ("-", _DIM),
    events.APPROVAL_REQUIRED: ("!", _YELLOW),
    events.APPROVAL_RESOLVED: ("v", _GREEN),
    events.ARTIFACT: ("*", _CYAN),
    events.SANDBOX_RUN: ("$", _BLUE),
    events.TASK_DONE: ("V", _GREEN),
    events.TASK_FAILED: ("X", _RED),
}


def _fmt_time(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts))


def _render(ev) -> None:
    icon, color = _ICONS.get(ev.type, ("·", _DIM))
    d = ev.data or {}
    node = d.get("node") or ""
    stamp = f"{_DIM}{_fmt_time(ev.ts)}{_OFF}"

    if ev.type == events.NODE_STARTED:
        print(f"{stamp} {color}{icon}{_OFF} {_BOLD}{node:<9}{_OFF} "
              f"{_DIM}{d.get('description', '')}{_OFF}")
    elif ev.type == events.NODE_FINISHED:
        mark = _GREEN + icon + _OFF if d.get("ok") else _RED + icon + _OFF
        print(f"{stamp} {mark} {_BOLD}{node:<9}{_OFF} {d.get('summary', '')}")
        if not d.get("ok"):
            print(f"{stamp}   {_RED}{d.get('error')}{_OFF}")
    elif ev.type == events.NODE_FAILED:
        print(f"{stamp} {_RED}{icon}{_OFF} {_BOLD}{node:<9}{_OFF} "
              f"{_RED}{d.get('error')}{_OFF}")
    elif ev.type == events.LOG:
        level = d.get("level", "info")
        if level == "debug":
            return                     # 检查点这类噪声默认不显示
        color2 = _YELLOW if level == "warn" else _DIM
        print(f"{stamp} {_DIM}|{_OFF} {color2}{d.get('message', '')}{_OFF}")
    elif ev.type == events.APPROVAL_REQUIRED:
        print(f"{stamp} {_YELLOW}{icon} 需要人工审批{_OFF} {d.get('question', '')}")
        for f in d.get("files") or []:
            print(f"{stamp}     {_DIM}- {f.get('path')} — {f.get('purpose')}{_OFF}")
    elif ev.type == events.APPROVAL_RESOLVED:
        verdict = _GREEN + "通过" + _OFF if d.get("approved") else _RED + "驳回" + _OFF
        print(f"{stamp} {icon} 审批结果：{verdict} {_DIM}{d.get('comment', '')}{_OFF}")
    elif ev.type == events.ARTIFACT:
        print(f"{stamp} {_CYAN}{icon}{_OFF} 产物 {d.get('path')} "
              f"{_DIM}{d.get('summary', '')}{_OFF}")
    elif ev.type == events.SANDBOX_RUN:
        status = _GREEN + "ok" + _OFF if d.get("ok") else _RED + "fail" + _OFF
        print(f"{stamp} {_BLUE}{icon}{_OFF} 沙箱执行 {status} "
              f"{_DIM}{d.get('ms', 0)}ms{_OFF}")
    elif ev.type == events.TASK_STATUS:
        print(f"{stamp} {_BLUE}{icon}{_OFF} 状态 → {_BOLD}{d.get('status')}{_OFF}")
    elif ev.type in (events.TASK_DONE, events.TASK_FAILED):
        res = d.get("result") or {}
        if ev.type == events.TASK_DONE:
            print(f"\n{_GREEN}{_BOLD}✓ 任务完成{_OFF}")
            print(f"  覆盖率      {res.get('coverage')}%（目标 {res.get('coverage_target')}%）")
            print(f"  澄清轮次    {res.get('clarify_rounds')}　"
                  f"编码轮次 {res.get('codegen_rounds')}")
            print(f"  关键路径    {' → '.join(res.get('critical_path') or [])}")
            usage = (res.get("usage") or {}).get("total") or {}
            print(f"  模型调用    {usage.get('calls', 0)} 次　"
                  f"token {usage.get('in', 0)}+{usage.get('out', 0)}")
            print(f"  产物        {len(res.get('files') or [])} 个文件")
            if res.get("gate_degraded"):
                print(f"  {_YELLOW}注意：{res.get('gate_reason') or '门禁未达标，已降级接受'}{_OFF}")
            if res.get("degraded"):
                print(f"  {_YELLOW}注意：部分节点走了降级（模型不可用）{_OFF}")
        else:
            print(f"\n{_RED}{_BOLD}✗ 任务失败{_OFF}\n  {d.get('error')}")


# ============================================================
# 子命令
# ============================================================

async def _drive(ctx: RunContext, *, ask: bool) -> dict:
    """跑任务并把事件流渲染出来。"""
    BUS.bind_loop(asyncio.get_running_loop())
    box: dict = {}

    def work() -> None:
        try:
            box["out"] = run_pipeline(ctx)
        except Exception as exc:  # noqa: BLE001
            box["out"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    worker = threading.Thread(target=work, daemon=True, name=f"task-{ctx.task_id}")
    worker.start()

    async for ev in BUS.stream(ctx.task_id):
        _render(ev)
        if ev.type == events.APPROVAL_REQUIRED and ask:
            # to_thread：input() 是阻塞的，直接在协程里调会把事件循环卡住，
            # 于是 approve() 的 resolve 永远送不进来 —— 自己等自己。
            try:
                ans = await asyncio.to_thread(input, f"{_YELLOW}批准？[y/N] {_OFF}")
            except EOFError:
                ans = ""
            ctx.gate.resolve(ctx.task_id, ans.strip().lower().startswith("y"), "CLI 输入")

    worker.join(timeout=5)
    return box.get("out") or {"ok": False, "error": "任务未返回结果"}


def cmd_run(args: argparse.Namespace) -> int:
    store = Store(SETTINGS.db_path)
    task_id = uuid.uuid4().hex[:12]
    auto = args.approve == "auto"
    store.create_task(task_id, args.requirement, {"auto_approve": auto})

    ctx = RunContext(
        task_id=task_id, requirement=args.requirement, store=store,
        sandbox=Sandbox(task_id), settings=SETTINGS, gate=ApprovalGate(),
        state={"auto_approve": auto},
    )
    print(f"{_DIM}任务 {task_id}｜{args.requirement}{_OFF}\n")
    out = asyncio.run(_drive(ctx, ask=(args.approve == "ask")))
    return 0 if out.get("ok") else 1


def cmd_ls(args: argparse.Namespace) -> int:
    store = Store(SETTINGS.db_path)
    rows = store.list_tasks(limit=args.limit)
    if not rows:
        print(f"{_DIM}（还没有任务）{_OFF}")
        return 0
    print(f"{'ID':<14}{'状态':<18}{'需求'}")
    for t in rows:
        color = _GREEN if t.status is TaskStatus.SUCCEEDED else (
            _RED if t.status is TaskStatus.FAILED else _YELLOW)
        req = t.requirement.replace("\n", " ")[:44]
        print(f"{t.task_id:<14}{color}{t.status.value:<18}{_OFF}{req}")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    store = Store(SETTINGS.db_path)
    rec = store.get_task(args.task_id)
    if rec is None:
        print(f"{_RED}任务不存在：{args.task_id}{_OFF}", file=sys.stderr)
        return 1
    print(f"{_BOLD}{rec.task_id}{_OFF}  {rec.status.value}")
    print(f"需求：{rec.requirement}")
    cp = store.latest_checkpoint(args.task_id)
    if cp:
        print(f"最新检查点：{cp['node']}（第 {cp['seq']} 个，steps={cp['steps']}）")
        print(f"  各节点执行次数：{cp['visits']}")
    print("检查点轨迹：")
    for c in store.list_checkpoints(args.task_id):
        print(f"  #{c['seq']} {c['node']}")
    if rec.result:
        res = rec.result
        print(f"覆盖率：{res.get('coverage')}%（目标 {res.get('coverage_target')}%）")
        print(f"关键路径：{' → '.join(res.get('critical_path') or [])}")
        print("产物：")
        for f in res.get("files") or []:
            print(f"  {f['path']}  {_DIM}{f['size']}B{_OFF}")
    if rec.error:
        print(f"{_RED}错误：{rec.error}{_OFF}")
    return 0


def cmd_resume(args: argparse.Namespace) -> int:
    """在内核进程里恢复任务（走 HTTP），因为审批门在内存里，CLI 碰不到。"""
    import httpx
    base = f"http://{SETTINGS.serve_host}:{SETTINGS.serve_port}"
    with httpx.Client(timeout=30) as c:
        r = c.post(f"{base}/v1/tasks/{args.task_id}/resume",
                   json={"auto_approve": args.auto_approve},
                   headers={"X-Internal-Token": SETTINGS.internal_token})
    if r.status_code != 200:
        print(f"{_RED}恢复失败（{r.status_code}）：{r.text}{_OFF}", file=sys.stderr)
        return 1
    d = r.json()
    print(f"{_GREEN}已创建续跑任务 {d['task_id']}{_OFF}"
          f"（原任务 {d['resumed_from']}，从 {d['from_node']} 继续）")
    print(f"{_DIM}订阅进度：curl -N {base}/v1/tasks/{d['task_id']}/events"
          f" -H 'X-Internal-Token: {SETTINGS.internal_token}'{_OFF}")
    return 0


def cmd_approve(args: argparse.Namespace) -> int:
    import httpx
    base = f"http://{SETTINGS.serve_host}:{SETTINGS.serve_port}"
    with httpx.Client(timeout=30) as c:
        r = c.post(f"{base}/v1/tasks/{args.task_id}/approve",
                   json={"approved": not args.reject, "comment": args.comment},
                   headers={"X-Internal-Token": SETTINGS.internal_token})
    if r.status_code != 200:
        print(f"{_RED}审批失败（{r.status_code}）：{r.text}{_OFF}", file=sys.stderr)
        return 1
    print(f"{_GREEN}已提交审批结果：{'通过' if not args.reject else '驳回'}{_OFF}")
    return 0


def cmd_pipeline(args: argparse.Namespace) -> int:
    g = build_pipeline(SETTINGS)
    print(g.describe())
    print()
    print(f"{_DIM}刹车：clarify 最多 {g.nodes['clarify'].max_visits} 次，"
          f"code 最多 {g.nodes['code'].max_visits} 次"
          f"（由 DEVPILOT_MAX_CLARIFY / DEVPILOT_MAX_CODEGEN 控制）{_OFF}")
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    """DAG 静态分析演示：对应一个 DAG 静态分析问题。"""
    deps = {
        "fetch_spec": [], "gen_schema": ["fetch_spec"],
        "gen_code": ["gen_schema"], "gen_tests": ["gen_schema"],
        "run_tests": ["gen_code", "gen_tests"], "report": ["run_tests"],
    }
    weights = {"fetch_spec": 2, "gen_schema": 30, "gen_code": 180,
               "gen_tests": 90, "run_tests": 40, "report": 3}
    plan = execution_plan(deps, weights, parallelism=args.parallelism)
    print(f"{_BOLD}合法执行顺序{_OFF}  {' → '.join(plan['order'])}")
    print(f"{_BOLD}关键路径{_OFF}      {' → '.join(plan['critical_path'])}")
    print(f"{_BOLD}最小完成时间{_OFF}  {plan['makespan_inf']}s（无限并行 = 关键路径长度）")
    print(f"{_BOLD}实际完成时间{_OFF}  {plan['makespan_real']}s（并行度 {args.parallelism}）")
    print(f"{_BOLD}瓶颈节点{_OFF}      {plan['bottleneck']}"
          f"　{_DIM}优化非关键路径上的节点，在关键路径改变前一无所获{_OFF}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="devpilot", description="多 Agent 交付流水线")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="跑一条交付流水线")
    r.add_argument("requirement", help="一句话需求")
    r.add_argument("--approve", choices=["auto", "ask", "skip"], default="auto",
                   help="审批方式：auto=自动通过（默认），ask=命令行交互，skip=直接通过不等")
    r.set_defaults(func=cmd_run)

    ls = sub.add_parser("ls", help="列出任务")
    ls.add_argument("--limit", type=int, default=20)
    ls.set_defaults(func=cmd_ls)

    sh = sub.add_parser("show", help="查看任务详情")
    sh.add_argument("task_id")
    sh.set_defaults(func=cmd_show)

    ap = sub.add_parser("approve", help="审批（需要内核在运行）")
    ap.add_argument("task_id")
    ap.add_argument("--reject", action="store_true", help="驳回")
    ap.add_argument("--comment", default="")
    ap.set_defaults(func=cmd_approve)

    rs = sub.add_parser("resume", help="从检查点续跑（需要内核在运行）")
    rs.add_argument("task_id")
    rs.add_argument("--auto-approve", action="store_true")
    rs.set_defaults(func=cmd_resume)

    pl = sub.add_parser("pipeline", help="打印流水线结构")
    pl.set_defaults(func=cmd_pipeline)

    pn = sub.add_parser("plan", help="DAG 依赖分析演示（关键路径 / 调度 / 重调度）")
    pn.add_argument("--parallelism", type=int, default=2)
    pn.set_defaults(func=cmd_plan)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
