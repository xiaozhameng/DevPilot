"""内核服务层：HTTP + SSE。内核对外的唯一入口。

## 为什么内核必须是个常驻进程，而不是"一次调用的脚本"

因为这批任务的**生命周期远长于一次 HTTP 请求**：
一次交付要跑几分钟（澄清 → PRD → 方案 → 审批 → 编码 → 沙箱跑测试），
中间还有一处要停下来等人审批（可能几十分钟）。
如果做成"请求进来就同步跑完再返回"，那：
  - 连接必然超时
  - 审批期间那个连接一直挂着
  - 进程重启后一切从头开始

所以形态必须是：**请求只负责创建任务并立刻返回，任务是内核自己并行跑的**，
调用方通过 SSE 订阅进度、通过审批接口放行、通过查询接口看状态。
Java 网关、网页、命令行都只是它的客户端。

## 鉴权为什么用一个静态内网 token，而不是 JWT

因为内核**不应该**出现在公网。它没有用户体系、没有租户隔离、没有配额 ——
这些是网关的职责。内核只认一个"你是不是我自己人"的凭证，
所有面向用户的东西（登录、鉴权、限流、审计）都在网关上做。

这不是偷懒，是刻意的边界：内核里每多一套权限逻辑，
就多一处需要单独测试、单独升级、单独审计的东西，
而它的信任模型本来就只有"内网可信"这一档。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel, Field

from . import events
from .config import SETTINGS, Settings
from .events import BUS, EventBus
from .pipeline import build_pipeline, resume_start, run_pipeline
from .runtime import GATE, RunContext
from .sandbox import Sandbox
from .state import TERMINAL_STATUSES, Store, TaskStatus


class TaskCreate(BaseModel):
    requirement: str = Field(min_length=1, max_length=4000)
    options: dict = Field(default_factory=dict)
    # 幂等键：同一个键重复提交，返回**已经存在的那条任务**，不新建。
    # 这是"网络重试不会导致重复执行"的实现方式 —— 客户端在第一次请求前生成键，
    # 重试时带同一个键。注意唯一约束在数据库层（见 state.py）。
    idempotency_key: str | None = None


class ApprovalIn(BaseModel):
    approved: bool
    comment: str = ""


class ResumeIn(BaseModel):
    auto_approve: bool = False


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    st = settings or SETTINGS
    db = store or Store(st.db_path)
    bus: EventBus = BUS

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 绑定事件循环：EventBus 需要它来做跨线程唤醒
        # （流水线跑在工作线程里，SSE 跑在事件循环里）。
        bus.bind_loop(asyncio.get_running_loop())
        yield

    app = FastAPI(title="DevPilot Kernel", version="0.1.0", lifespan=lifespan)
    app.state.store = db
    app.state.settings = st

    # ---------- 鉴权 ----------
    def auth(token: str | None) -> None:
        if not token or token != st.internal_token:
            raise HTTPException(status_code=401, detail="内核只接受内网调用（X-Internal-Token）")

    # ---------- 任务执行 ----------
    def _spawn(ctx: RunContext, *, resume: bool = False) -> None:
        """在工作线程里跑流水线。

        用线程而不是 asyncio 任务，是因为流水线是**同步阻塞**的：
        它调模型（HTTP）、跑子进程（pytest）、等审批（Event.wait）。
        硬塞进事件循环会把整个内核卡住 —— 包括正在推送的 SSE。
        一条任务一个线程，简单、可靠，而且天然隔离（一个任务炸了不影响别的）。
        """
        def work() -> None:
            try:
                run_pipeline(ctx, resume=resume)
            except Exception as exc:  # noqa: BLE001
                # run_pipeline 内部已经处理了常规失败；能走到这里说明是它自己炸了。
                err = f"{type(exc).__name__}: {exc}"
                try:
                    cur = db.get_task(ctx.task_id)
                    if cur and cur.status not in TERMINAL_STATUSES:
                        db.update_status(ctx.task_id, TaskStatus.FAILED,
                                         state=ctx.state, error=err)
                    ctx.emit(events.TASK_FAILED, error=err)
                except Exception:  # noqa: BLE001
                    pass

        threading.Thread(target=work, daemon=True, name=f"task-{ctx.task_id}").start()

    def _make_ctx(task_id: str, requirement: str, options: dict,
                  *, resume: bool = False) -> RunContext:
        # 恢复时的共享状态由 run_pipeline → restore_from_checkpoint 统一负责，
        # 这里**不重复合并**：两处都做会引入"谁先谁后"的隐性依赖，
        # 而那种依赖在改动其中一处时会静默失效。
        # 这里传进去的 state 只承载"本次运行的显式选项"（如 auto_approve）。
        return RunContext(
            task_id=task_id, requirement=requirement, store=db,
            sandbox=Sandbox(task_id, root=st.workspace_dir),
            settings=st, gate=GATE, state=dict(options),
        )

    # ============================================================
    # 健康检查
    # ============================================================
    @app.get("/health")
    def health(x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        tasks = db.list_tasks(limit=1000)
        by_status: dict[str, int] = {}
        for t in tasks:
            by_status[t.status.value] = by_status.get(t.status.value, 0) + 1
        waiting = [t.task_id for t in tasks if t.status is TaskStatus.WAITING_APPROVAL]
        return {
            "ok": True,
            "model": st.model,
            "llm_available": st.has_llm,
            "coverage_target": st.coverage_target,
            "brakes": {"max_clarify": st.max_clarify, "max_codegen": st.max_codegen},
            "tasks": {"total": len(tasks), "by_status": by_status},
            "waiting_approval": waiting,
        }

    # ============================================================
    # 流水线结构（给演示页画图用）
    # ============================================================
    @app.get("/v1/pipeline")
    def pipeline(x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        g = build_pipeline(st)
        return {
            "entry": g.entry,
            "nodes": [
                {"name": n.name, "description": n.description,
                 "avg_ms": n.avg_ms, "max_visits": n.max_visits,
                 "conditional": n.name in g._routers}
                for n in g.nodes.values()
            ],
            "edges": [
                {"from": src, "to": dst, "kind": "fixed"}
                for src, dst in g._fixed.items()
            ] + [
                {"from": src, "to": t, "kind": "conditional"}
                for src in g._routers for t in sorted(g._router_targets(src))
            ],
            "describe": g.describe(),
        }

    # ============================================================
    # 任务
    # ============================================================
    @app.post("/v1/tasks")
    def create_task(body: TaskCreate,
                    x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)

        if body.idempotency_key:
            existing = db.find_by_idempotency(body.idempotency_key)
            if existing:
                # 返回原任务，并显式告诉调用方"这是复用，不是新建"——
                # 不告诉的话，调用方无法区分"我的重试命中了"和"系统真的起了两个任务"。
                data = existing.to_dict()
                data["deduplicated"] = True
                return data

        task_id = uuid.uuid4().hex[:12]
        rec = db.create_task(task_id, body.requirement, body.options,
                             body.idempotency_key)
        bus.publish(task_id, events.TASK_CREATED, seq=None,
                    requirement=body.requirement, options=body.options)
        db.append_event(task_id, 1, events.TASK_CREATED,
                        {"requirement": body.requirement}, time.time())

        ctx = _make_ctx(task_id, body.requirement, body.options)
        _spawn(ctx)
        out = rec.to_dict()
        out["deduplicated"] = False
        return out

    @app.get("/v1/tasks")
    def list_tasks(limit: int = 50,
                   x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        return {"items": [t.to_dict() for t in db.list_tasks(limit)]}

    @app.get("/v1/tasks/{task_id}")
    def get_task(task_id: str,
                 x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        rec = db.get_task(task_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        data = rec.to_dict()
        cp = db.latest_checkpoint(task_id)
        data["checkpoint"] = cp
        data["approval_pending"] = GATE.pending(task_id) is not None and GATE.is_waiting(task_id)
        data["sandbox_files"] = Sandbox(task_id, root=st.workspace_dir).list_files()
        return data

    # ---------- SSE ----------
    @app.get("/v1/tasks/{task_id}/events")
    async def task_events(task_id: str, since: int = 0,
                          x_internal_token: str | None = Header(default=None)):
        auth(x_internal_token)
        if db.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")

        async def gen():
            # 先回放**持久化**的事件。这样即便内核重启过、内存里的事件没了，
            # 客户端也能拿到完整历史；也因为如此，事件才必须落库。
            history = db.load_events(task_id, since=since)
            cursor = since
            for e in history:
                cursor = max(cursor, e["seq"])
                yield f"event: {e['type']}\ndata: {json.dumps(e, ensure_ascii=False)}\n\n"
            if history and history[-1]["type"] in events.TERMINAL:
                return
            # 再追实时增量（内存总线），直到出现终态事件。
            try:
                async for ev in bus.stream(task_id, since=cursor):
                    yield ev.to_sse()
                    if ev.type in events.TERMINAL:
                        return
            except asyncio.CancelledError:
                return

        return StreamingResponse(gen(), media_type="text/event-stream", headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # 关掉 nginx/网关的响应缓冲，否则 SSE 会被攒成一坨再发，
            # 表现是"进度条一直不动，最后一次性全出来"。
            "X-Accel-Buffering": "no",
        })

    # ---------- 审批 ----------
    @app.post("/v1/tasks/{task_id}/approve")
    def approve(task_id: str, body: ApprovalIn,
                x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        rec = db.get_task(task_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        if rec.status is not TaskStatus.WAITING_APPROVAL:
            # 409 而不是 400：这是"状态冲突"，不是"参数错误"。
            # 网关会把它原样透出，客户端能据此提示"这个任务已经不在等审批了"。
            raise HTTPException(
                status_code=409,
                detail=f"任务当前状态是 {rec.status.value}，不在等待审批",
            )
        ok = GATE.resolve(task_id, body.approved, body.comment)
        if not ok:
            raise HTTPException(status_code=409, detail="审批请求已失效（可能已超时）")
        return {"ok": True, "task_id": task_id, "approved": body.approved}

    # ---------- 断点恢复 ----------
    @app.post("/v1/tasks/{task_id}/resume")
    def resume(task_id: str, body: ResumeIn | None = None,
               x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        rec = db.get_task(task_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        if rec.status in TERMINAL_STATUSES:
            raise HTTPException(status_code=409,
                                detail=f"任务已处于终态 {rec.status.value}，不可恢复")

        cp = db.latest_checkpoint(task_id)
        if not cp:
            raise HTTPException(status_code=409, detail="没有可用的检查点，无法恢复")

        # 把状态推回 RUNNING 需要走状态机：
        # FAILED 是终态、无出边，所以不能直接改 —— 这正是"终态不可逆"的设计意图。
        # 恢复的做法是**新建一个任务**，让历史保持只追加。
        new_id = uuid.uuid4().hex[:12]
        db.create_task(new_id, rec.requirement, rec.options)
        # 把检查点搬过去，让新任务从断点续跑。
        db.save_checkpoint(new_id, cp["node"], cp["visits"], cp["steps"], cp["state"])
        bus.publish(new_id, events.TASK_CREATED, requirement=rec.requirement,
                    resumed_from=task_id)
        db.append_event(new_id, 1, events.TASK_CREATED,
                        {"resumed_from": task_id}, time.time())

        opts = dict(rec.options)
        if body and body.auto_approve:
            opts["auto_approve"] = True
        ctx = _make_ctx(new_id, rec.requirement, opts, resume=True)
        _spawn(ctx, resume=True)
        return {"ok": True, "task_id": new_id, "resumed_from": task_id,
                "from_node": resume_start(build_pipeline(st), ctx)}

    # ---------- 取消 ----------
    @app.post("/v1/tasks/{task_id}/cancel")
    def cancel(task_id: str,
               x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        rec = db.get_task(task_id)
        if rec is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        if rec.status in TERMINAL_STATUSES:
            raise HTTPException(status_code=409,
                                detail=f"任务已处于终态 {rec.status.value}")
        db.update_status(task_id, TaskStatus.CANCELLED)
        bus.publish(task_id, events.TASK_STATUS, status=TaskStatus.CANCELLED.value)
        # 如果正挂在审批点上，必须把它唤醒 ——
        # 否则那个工作线程会一直阻塞到审批超时，白占一个线程。
        GATE.resolve(task_id, False, "任务已被取消")
        return {"ok": True, "task_id": task_id, "status": TaskStatus.CANCELLED.value}

    # ---------- 产物 ----------
    @app.get("/v1/tasks/{task_id}/artifacts")
    def list_artifacts(task_id: str,
                       x_internal_token: str | None = Header(default=None)) -> dict:
        auth(x_internal_token)
        if db.get_task(task_id) is None:
            raise HTTPException(status_code=404, detail=f"任务不存在：{task_id}")
        return {"items": Sandbox(task_id, root=st.workspace_dir).list_files()}

    @app.get("/v1/tasks/{task_id}/artifacts/raw")
    def read_artifact(task_id: str, path: str,
                      x_internal_token: str | None = Header(default=None)):
        auth(x_internal_token)
        sb = Sandbox(task_id, root=st.workspace_dir)
        try:
            # 这里必须走 Sandbox.read —— 它内部会做路径解析与越界检查。
            # 直接 open(root/path) 就等于把"../"开放给了任何调用方。
            return PlainTextResponse(sb.read(path), media_type="text/plain; charset=utf-8")
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"产物不存在：{path}")
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail=str(exc))

    return app
