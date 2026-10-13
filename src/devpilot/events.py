"""事件协议：内核与所有界面之间的唯一契约。

为什么要有这么一层？
因为内核是常驻进程，界面会有很多个（命令行、网页、将来的手机端、以及 Java 网关）。
如果每个界面各自去读内核的内部状态，那就等于把内核的私有结构暴露了 N 次，
改一次内核要改 N 个地方。

所以定一条规矩：**内核只往外发事件，界面只消费事件。**
界面永远不"查询内核在干什么"，它只是把收到的事件渲染出来。

事件的三条设计约束：
  1) 每个事件带 seq（单调递增）—— 断了重连后可以 `?since=N` 续传，不丢不重
  2) 每个事件带 task_id —— 一个流只关心一个任务，天然隔离
  3) 事件类型是**有限的枚举**，而不是自由文本 —— 界面可以穷举处理，
     新增一种事件必须显式加常量，编译器/测试能挡住拼写错误
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import AsyncIterator

# ---- 事件类型（有限枚举，不写裸字符串）----
TASK_CREATED = "task.created"
TASK_STATUS = "task.status"
NODE_STARTED = "node.started"
NODE_FINISHED = "node.finished"
NODE_FAILED = "node.failed"
NODE_SKIPPED = "node.skipped"
LOG = "log"
ARTIFACT = "artifact"
APPROVAL_REQUIRED = "approval.required"
APPROVAL_RESOLVED = "approval.resolved"
LLM_USAGE = "llm.usage"
SANDBOX_RUN = "sandbox.run"
TASK_DONE = "task.done"
TASK_FAILED = "task.failed"

# 终态：SSE 流看到这两个就该收尾了。
# 少了这个判断，事件流永远不结束，客户端会一直挂着 —— 这是 SSE 最经典的坑。
TERMINAL = frozenset({TASK_DONE, TASK_FAILED})


@dataclass
class Event:
    task_id: str
    seq: int
    type: str
    data: dict = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {"seq": self.seq, "type": self.type, "ts": self.ts, "data": self.data}

    def to_sse(self) -> str:
        """编成一帧 SSE。

        `event:` 这一行不能省。只发 data 的话，浏览器端 EventSource 只会
        触发 onmessage，无法按类型分发 —— 前端就得在 data 里再判一次 type，
        等于协议退化成了"一个字符串流"。
        """
        payload = json.dumps(self.to_dict(), ensure_ascii=False)
        return f"event: {self.type}\ndata: {payload}\n\n"


class EventBus:
    """按任务分桶的事件总线，支持历史回放 + 实时推送。

    两条必须同时成立，缺一个都会出问题：
      - **历史回放**：SSE 客户端连上来时，任务可能已经跑了一半。
        不给历史，界面就从中间开始显示，用户看到的是残缺的进度。
      - **实时推送**：跑完之后才连上来的客户端，只是回放也没关系；
        但正在跑的必须能收到后续事件。

    线程模型：流水线跑在工作线程里（见 tasks.py），SSE 端点跑在 asyncio 事件循环里。
    所以 publish() 必须**线程安全**，而且跨线程唤醒 await 中的协程只能通过
    `loop.call_soon_threadsafe` —— 直接在别的线程 set 一个 asyncio.Event 是未定义行为。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._events: dict[str, list[Event]] = defaultdict(list)
        self._seq: dict[str, int] = defaultdict(int)
        self._signals: dict[str, asyncio.Event] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """由 server 在启动时调用一次，记下事件循环，供跨线程唤醒使用。"""
        self._loop = loop

    # ---- 写侧：任何线程都能调 ----
    def publish(self, task_id: str, type_: str, seq: int | None = None, **data) -> Event:
        """发一个事件。

        调用方注意：`**data` 里的键**不能叫 task_id** ——
        它会和第一个位置参数撞车（TypeError: multiple values for argument）。
        事件本身已经带 task_id 了，不需要在 payload 里再传一份。

        `seq` 允许由调用方指定：因为事件同时会被持久化到 SQLite，
        而进程重启后内存里的计数器会从 0 重来 —— 如果这里无条件自增，
        重启后的 seq 会与库里的历史 seq 撞车（表主键是 (task_id, seq)），
        结果是新事件**覆盖掉**历史事件，审计链断掉。
        所以由持久化层（runtime.py）统一分配序号，这里只负责接受。
        """
        with self._lock:
            if seq is None:
                self._seq[task_id] += 1
                seq = self._seq[task_id]
            else:
                self._seq[task_id] = max(self._seq[task_id], seq)
            ev = Event(task_id=task_id, seq=seq, type=type_, data=data)
            self._events[task_id].append(ev)

        loop, sig = self._loop, self._signals.get(task_id)
        if loop is not None and sig is not None:
            # call_soon_threadsafe：把 set() 排进事件循环，而不是在当前线程直接调 ——
            # asyncio.Event 不是线程安全的，跨线程直接 set 会在高并发下丢唤醒。
            try:
                loop.call_soon_threadsafe(sig.set)
            except RuntimeError:
                # 事件循环已关闭（进程正在退出），静默忽略即可。
                pass
        return ev

    # ---- 读侧：只在事件循环线程里调 ----
    def snapshot(self, task_id: str, since: int = 0) -> list[Event]:
        with self._lock:
            return [e for e in self._events.get(task_id, ()) if e.seq > since]

    def _signal(self, task_id: str) -> asyncio.Event:
        sig = self._signals.get(task_id)
        if sig is None:
            sig = asyncio.Event()          # 只能在事件循环线程里创建
            self._signals[task_id] = sig
        return sig

    async def stream(self, task_id: str, since: int = 0) -> AsyncIterator[Event]:
        """异步迭代一个任务的事件流，直到出现终态事件。"""
        cursor = since
        while True:
            batch = self.snapshot(task_id, cursor)
            if batch:
                cursor = batch[-1].seq
                for ev in batch:
                    yield ev
                if any(e.type in TERMINAL for e in batch):
                    return
                continue          # 立刻再取一次，避免"取完就等"造成的事件积压

            sig = self._signal(task_id)
            sig.clear()
            # 双重检查：clear() 和上面的 snapshot() 之间存在窗口，
            # 若此刻刚好有事件写进来，我们会丢掉那次唤醒，然后永久 wait。
            if self.snapshot(task_id, cursor):
                continue
            await sig.wait()

    # ---- 给"任务已经结束、不想挂住"的调用方 ----
    def is_terminal(self, task_id: str) -> bool:
        with self._lock:
            return any(e.type in TERMINAL for e in self._events.get(task_id, ()))

    def last(self, task_id: str) -> Event | None:
        with self._lock:
            evs = self._events.get(task_id)
            return evs[-1] if evs else None


# 进程级单例。
# 之所以不做成依赖注入：事件总线是"基础设施"，不是"业务依赖"，
# 它没有需要在测试里替换的实现（不像 LLM 需要假模型）。
BUS = EventBus()
