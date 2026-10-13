"""状态机与检查点：任务的状态怎么流转、崩了怎么续。

## 为什么必须是显式状态机，而不是几个布尔字段

用 `is_running` / `is_done` / `is_failed` 三个布尔量表示任务状态，是这类系统里
最常见也最致命的写法。它有三个绕不过去的问题：

1. **组合爆炸且大多非法。** 三个布尔量有 8 种组合，其中 5 种是矛盾的
   （is_running=True 且 is_done=True 是什么意思？）。代码里没有任何地方
   阻止你去写这种状态，于是它一定会在并发下被写出来。
2. **没有"非法转移"的概念。** 一个 already-SUCCEEDED 的任务能不能被重试改回
   RUNNING？布尔量版本答不上来，因为它连"转移"这个东西都没有。
3. **查不了。** 运维问"现在有多少个卡在等待审批"，布尔量版本要写一堆
   与条件拼出来，而每个条件都可能漏掉一种组合。

显式状态机把这三件事一次性解决：状态是**有限枚举**，转移是**显式的表**，
任何不在表里的转移直接被拒绝并抛异常 —— 错误在发生的那一行暴露，
而不是在三天后的一堆脏数据里。

## 检查点

每个节点执行完落一次盘。落盘内容 = "执行到哪个节点 + 当前共享状态 + 各节点执行次数"。
有了它，进程被 kill、机器重启、代码升级，都能从断点续跑 ——
前面已经跑完的节点不会重新花钱调模型。

这就是为什么 checkpoint 里必须存 `visits`（各节点执行次数）：
不存的话，恢复后刹车计数器从 0 开始，本该在第 3 次收敛的回环会重新跑满，
轻则多花钱，重则触发不了刹车。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from .fsutil import ensure_dir


class TaskStatus(str, Enum):
    PENDING = "pending"                       # 已创建，还没开始
    RUNNING = "running"                       # 正在执行
    WAITING_APPROVAL = "waiting_approval"     # 挂在人工审批点（HITL）
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


# 合法的状态转移。**这张表就是状态机的全部定义**，别的代码不许绕过它改状态。
_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {TaskStatus.WAITING_APPROVAL, TaskStatus.SUCCEEDED,
                         TaskStatus.FAILED, TaskStatus.CANCELLED},
    # 等待审批只有两个出口：放行（回到 RUNNING）或被取消。
    # 特意**不允许** WAITING_APPROVAL -> SUCCEEDED：审批还没给结果就说成功，
    # 这是把人当摆设。要成功必须先回到 RUNNING 再跑完。
    TaskStatus.WAITING_APPROVAL: {TaskStatus.RUNNING, TaskStatus.CANCELLED,
                                  TaskStatus.FAILED},
    # 三个终态都没有出边 —— 终态不可逆。想重跑就新建任务，
    # 这保证了"任务历史"是只追加的，事后审计永远能还原真相。
    TaskStatus.SUCCEEDED: set(),
    TaskStatus.FAILED: set(),
    TaskStatus.CANCELLED: set(),
}

TERMINAL_STATUSES = {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}


class InvalidTransition(RuntimeError):
    pass


def can_transition(src: TaskStatus, dst: TaskStatus) -> bool:
    return dst in _TRANSITIONS.get(src, set())


def ensure_transition(src: TaskStatus, dst: TaskStatus) -> None:
    if not can_transition(src, dst):
        raise InvalidTransition(
            f"非法状态转移：{src.value} → {dst.value}。"
            f"允许的下一步是 {sorted(s.value for s in _TRANSITIONS.get(src, set())) or '无（终态）'}"
        )


@dataclass
class TaskRecord:
    task_id: str
    status: TaskStatus
    requirement: str
    options: dict = field(default_factory=dict)
    state: dict = field(default_factory=dict)
    result: dict | None = None
    error: str | None = None
    idempotency_key: str | None = None
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "requirement": self.requirement,
            "options": self.options,
            "state": self.state,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id         TEXT PRIMARY KEY,
    status          TEXT NOT NULL,
    requirement     TEXT NOT NULL,
    options_json    TEXT NOT NULL DEFAULT '{}',
    state_json      TEXT NOT NULL DEFAULT '{}',
    result_json     TEXT,
    error           TEXT,
    idempotency_key TEXT UNIQUE,
    created_at      REAL NOT NULL,
    updated_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_created ON tasks(created_at DESC);

CREATE TABLE IF NOT EXISTS checkpoints (
    task_id     TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    node        TEXT NOT NULL,
    visits_json TEXT NOT NULL,
    steps       INTEGER NOT NULL,
    state_json  TEXT NOT NULL,
    created_at  REAL NOT NULL,
    PRIMARY KEY (task_id, seq)
);

CREATE TABLE IF NOT EXISTS events (
    task_id   TEXT NOT NULL,
    seq       INTEGER NOT NULL,
    type      TEXT NOT NULL,
    data_json TEXT NOT NULL,
    ts        REAL NOT NULL,
    PRIMARY KEY (task_id, seq)
);
"""


class Store:
    """SQLite 存储：任务表 + 检查点 + 事件。

    连接策略：**每次操作开一个新连接**，而不是全进程共用一个。
    原因是 SQLite 的连接默认不能跨线程使用，而我们的写入来自工作线程、
    读取来自 asyncio 事件循环线程。共享一个连接要么加 check_same_thread=False
    然后自己保证串行（容易漏），要么就是偶发的 "SQLite objects created in a
    thread can only be used in that same thread" —— 这种错误只在并发下出现，
    本地跑不出来，上线才炸。
    每次开连接的代价在 WAL 模式下很小，换来的是"不存在线程问题"。
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        # 用 ensure_dir 而不是 mkdir(exist_ok=True)：见 fsutil.py 的说明 ——
        # 这个执行环境会把"对已存在目录的 mkdir"拒绝成 PermissionError(EEXIST)。
        ensure_dir(self.path.parent)
        self._lock = threading.Lock()
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        # WAL 允许"一写多读"并发 —— 任务在跑（写检查点）的同时，
        # SSE 客户端还能读事件，不会互相阻塞。
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ---------- 任务 ----------
    def create_task(self, task_id: str, requirement: str, options: dict | None = None,
                    idempotency_key: str | None = None) -> TaskRecord:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO tasks (task_id, status, requirement, options_json, "
                "idempotency_key, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (task_id, TaskStatus.PENDING.value, requirement,
                 json.dumps(options or {}, ensure_ascii=False),
                 idempotency_key, now, now),
            )
        return self.get_task(task_id)  # type: ignore[return-value]

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return _row_to_task(row) if row else None

    def find_by_idempotency(self, key: str) -> TaskRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE idempotency_key=?", (key,)).fetchone()
        return _row_to_task(row) if row else None

    def list_tasks(self, limit: int = 50) -> list[TaskRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_to_task(r) for r in rows]

    def update_status(self, task_id: str, status: TaskStatus, *,
                      state: dict | None = None, result: dict | None = None,
                      error: str | None = None) -> TaskRecord:
        """改状态。**唯一的入口** —— 并且这里会校验转移是否合法。

        为什么校验必须在这一层（存储层）而不是调用方？
        因为调用方有很多个（流水线、审批回调、取消接口、超时重派），
        你没法保证每个都记得校验。而这里是所有写入的必经之路。
        """
        cur = self.get_task(task_id)
        if cur is None:
            raise KeyError(f"任务不存在：{task_id}")
        ensure_transition(cur.status, status)

        sets = ["status=?", "updated_at=?"]
        vals: list[object] = [status.value, time.time()]
        if state is not None:
            sets.append("state_json=?")
            vals.append(json.dumps(state, ensure_ascii=False))
        if result is not None:
            sets.append("result_json=?")
            vals.append(json.dumps(result, ensure_ascii=False))
        if error is not None:
            sets.append("error=?")
            vals.append(error)
        vals.append(task_id)

        with self._lock, self._connect() as conn:
            conn.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id=?", vals)
        return self.get_task(task_id)  # type: ignore[return-value]

    def save_state(self, task_id: str, state: dict) -> None:
        """只更新共享状态，不动状态机（节点执行中途用）。"""
        with self._lock, self._connect() as conn:
            conn.execute("UPDATE tasks SET state_json=?, updated_at=? WHERE task_id=?",
                         (json.dumps(state, ensure_ascii=False), time.time(), task_id))

    # ---------- 检查点 ----------
    def save_checkpoint(self, task_id: str, node: str, visits: dict, steps: int,
                        state: dict) -> int:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM checkpoints WHERE task_id=?",
                (task_id,),
            ).fetchone()
            seq = int(row["m"]) + 1
            conn.execute(
                "INSERT INTO checkpoints (task_id, seq, node, visits_json, steps, "
                "state_json, created_at) VALUES (?,?,?,?,?,?,?)",
                (task_id, seq, node, json.dumps(visits, ensure_ascii=False),
                 steps, json.dumps(state, ensure_ascii=False), time.time()),
            )
        return seq

    def latest_checkpoint(self, task_id: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM checkpoints WHERE task_id=? ORDER BY seq DESC LIMIT 1",
                (task_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "seq": row["seq"],
            "node": row["node"],
            "visits": json.loads(row["visits_json"]),
            "steps": row["steps"],
            "state": json.loads(row["state_json"]),
            "created_at": row["created_at"],
        }

    def list_checkpoints(self, task_id: str) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT node, seq, created_at FROM checkpoints WHERE task_id=? ORDER BY seq",
                (task_id,),
            ).fetchall()
        return [{"seq": r["seq"], "node": r["node"], "created_at": r["created_at"]} for r in rows]

    # ---------- 事件 ----------
    def append_event(self, task_id: str, seq: int, type_: str, data: dict, ts: float) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO events (task_id, seq, type, data_json, ts) "
                "VALUES (?,?,?,?,?)",
                (task_id, seq, type_, json.dumps(data, ensure_ascii=False), ts),
            )

    def load_events(self, task_id: str, since: int = 0) -> list[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT seq, type, data_json, ts FROM events "
                "WHERE task_id=? AND seq>? ORDER BY seq",
                (task_id, since),
            ).fetchall()
        return [{"seq": r["seq"], "type": r["type"], "ts": r["ts"],
                 "data": json.loads(r["data_json"])} for r in rows]


def _row_to_task(row: sqlite3.Row) -> TaskRecord:
    return TaskRecord(
        task_id=row["task_id"],
        status=TaskStatus(row["status"]),
        requirement=row["requirement"],
        options=json.loads(row["options_json"]),
        state=json.loads(row["state_json"]),
        result=json.loads(row["result_json"]) if row["result_json"] else None,
        error=row["error"],
        idempotency_key=row["idempotency_key"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )
