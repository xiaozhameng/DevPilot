"""执行上下文：Agent 节点能看到的唯一对象。

## 为什么要收成一个 ctx，而不是让节点随便 import

节点函数如果自己去 `from .sandbox import Sandbox`、自己去 `Store()`，
会带来三个具体问题：
  1) **测不了**：想测"覆盖率不达标会不会打回"，就得先起一个真的 SQLite + 真的沙箱。
  2) **审计不了**：每条路径都可能绕开事件上报，于是"任务执行过程"永远拼不完整。
  3) **改不动**：将来换存储、换沙箱实现，要改 N 个节点。

收成一个 ctx 之后，节点只依赖这一个接口；测试里塞一个假的 ctx 就能跑。
这是很典型的依赖注入，只是注入的东西叫"运行时"。

## ApprovalGate：人工审批为什么用阻塞等待，而不是轮询状态

审批的本质是"任务暂停，等人"。实现上有两种：
  - 轮询：循环查数据库"审批给了吗"，sleep 一下再查
  - 等待：挂在一个事件上，外部给了结果就唤醒

轮询看起来很"简单"，但它有三个问题：延迟取决于轮询间隔（要么慢要么费）、
每个等待中的任务都要占一个循环、以及"查到了结果"和"用上结果"之间存在竞态。
这里的任务可能要审批几小时，轮询纯属浪费。

用 threading.Event 阻塞等待，配合**超时**（默认 30 分钟的审批窗口），
超时即视为拒绝 —— 这个默认值是安全的：宁可任务不做，也不能无限期挂着占资源。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from . import events
from .config import SETTINGS, Settings
from .events import BUS, EventBus
from .llm import LEDGER, LLM
from .sandbox import Sandbox
from .state import Store, TaskStatus


class ApprovalTimeout(RuntimeError):
    pass


@dataclass
class ApprovalRequest:
    task_id: str
    node: str
    question: str
    payload: dict = field(default_factory=dict)


class ApprovalGate:
    """任务级的人工审批门。进程内内存实现。

    为什么不做成数据库 + 轮询？因为"等待"这个动作本来就有进程内的自然载体：
    任务正跑在一个工作线程里，它就该在那个线程上等。把等待外置到数据库，
    等于把"一个正在运行的线程"的语义硬塞进持久层，只会更绕。
    （跨进程的场景才需要外置，那是另一个量级的问题。）
    """

    def __init__(self, default_timeout: int = 1800) -> None:
        self._lock = threading.Lock()
        self._gates: dict[str, threading.Event] = {}
        self._requests: dict[str, ApprovalRequest] = {}
        self._decisions: dict[str, dict] = {}
        self.default_timeout = default_timeout

    def request(self, task_id: str, node: str, question: str, payload: dict | None = None) -> None:
        with self._lock:
            self._requests[task_id] = ApprovalRequest(task_id, node, question, payload or {})
            self._gates[task_id] = threading.Event()
            self._decisions.pop(task_id, None)

    def wait(self, task_id: str, timeout: int | None = None) -> dict:
        """阻塞等待审批结果。超时视为拒绝。"""
        with self._lock:
            gate = self._gates.get(task_id)
            req = self._requests.get(task_id)
        if gate is None:
            raise KeyError(f"任务 {task_id} 没有待审批的请求")

        ok = gate.wait(timeout or self.default_timeout)
        with self._lock:
            decision = self._decisions.pop(task_id, None)
            self._gates.pop(task_id, None)
        if not ok:
            raise ApprovalTimeout(
                f"审批等待超时（{timeout or self.default_timeout}s）。"
                f"超时按拒绝处理 —— 见 ApprovalGate 的说明。"
            )
        return decision or {"approved": False, "comment": "未收到审批意见"}

    def resolve(self, task_id: str, approved: bool, comment: str = "",
                edited: dict | None = None) -> bool:
        """外部（网关/命令行/网页）给出审批结果。返回是否成功唤醒。"""
        with self._lock:
            gate = self._gates.get(task_id)
            if gate is None:
                return False
            self._decisions[task_id] = {"approved": approved, "comment": comment,
                                        "edited": edited or {}, "at": time.time()}
            gate.set()
        return True

    def pending(self, task_id: str) -> ApprovalRequest | None:
        with self._lock:
            return self._requests.get(task_id)

    def is_waiting(self, task_id: str) -> bool:
        with self._lock:
            gate = self._gates.get(task_id)
            return gate is not None and not gate.is_set()


GATE = ApprovalGate()


class RunContext:
    """一次任务执行的运行时。Agent 节点通过它读输入、写产物、报事件、落检查点。"""

    def __init__(self, *, task_id: str, requirement: str, store: Store,
                 bus: EventBus | None = None, sandbox: Sandbox | None = None,
                 settings: Settings | None = None, gate: ApprovalGate | None = None,
                 state: dict | None = None) -> None:
        self.task_id = task_id
        self.requirement = requirement
        self.store = store
        self.bus = bus or BUS
        self.settings = settings or SETTINGS
        self.gate = gate or GATE
        self.sandbox = sandbox or Sandbox(task_id)
        self.state: dict[str, Any] = state or {}

        # 事件序号从库里已有的事件接着往下排（见 EventBus.publish 的说明）
        history = self.store.load_events(task_id)
        self._seq = history[-1]["seq"] if history else 0

        self.artifacts: list[dict] = []      # 本次运行产出的文件（供落库）
        self._current_node = ""

    # ---------- 事件 ----------
    def emit(self, type_: str, **data) -> None:
        """上报一个事件：内存广播（给 SSE）+ 落库（给审计与重启后回放）。"""
        self._seq += 1
        self.bus.publish(self.task_id, type_, seq=self._seq, **data)
        # 落库失败不能影响主流程 —— 审计重要，但不该让一个磁盘写失败把任务搞挂。
        try:
            self.store.append_event(self.task_id, self._seq, type_, data, time.time())
        except Exception:  # noqa: BLE001
            pass

    def log(self, message: str, level: str = "info") -> None:
        self.emit(events.LOG, node=self._current_node, message=message, level=level)

    # ---------- 状态机 ----------
    def set_status(self, status: TaskStatus, **kw) -> None:
        """流转任务状态。走 Store.update_status，因此**非法转移会被拒绝**。

        节点不该直接改状态字段 —— 那样状态机就退化成了"几个约定俗成的字符串"。
        所有流转都经过这里，转移表的校验才有意义。
        """
        self.store.update_status(self.task_id, status, **kw)
        self.emit(events.TASK_STATUS, status=status.value, node=self._current_node)

    # ---------- 共享状态 ----------
    def merge(self, patch: dict) -> None:
        self.state.update(patch)

    def get(self, key: str, default: Any = None) -> Any:
        return self.state.get(key, default)

    def bump(self, key: str, step: int = 1) -> int:
        """计数器自增并返回新值。用于回环次数（澄清轮次、打回次数）。"""
        val = int(self.state.get(key, 0)) + step
        self.state[key] = val
        return val

    # ---------- 检查点 ----------
    def checkpoint(self, node: str, visits: dict, steps: int, status_note: str = "") -> None:
        self.store.save_checkpoint(self.task_id, node, visits, steps, self.state)
        self.store.save_state(self.task_id, self.state)
        self.emit(events.LOG, node=node, message=f"检查点已保存（steps={steps}{status_note}）",
                  level="debug")

    # ---------- 节点边界（由执行器调用）----------
    def enter_node(self, name: str) -> None:
        self._current_node = name

    # ---------- LLM ----------
    def llm_for(self, node: str) -> LLM:
        """按**本任务的设置**构造模型客户端。

        刻意不吃全局 SETTINGS：任务带着自己的 Settings 跑（见 self.settings），
        模型配置必须跟着任务走。否则"这个任务用的哪个模型/哪把 key"
        就变成了一个全局事实，多任务并发时无法区分，测试里也无法伪造。
        """
        return LLM(model=self.settings.model, api_key=self.settings.api_key,
                   api_base=self.settings.api_base, node=node)

    # ---------- 产物 ----------
    def add_artifact(self, path: str, kind: str, summary: str = "") -> dict:
        item = {"path": path, "kind": kind, "summary": summary,
                "task_id": self.task_id, "at": time.time()}
        self.artifacts.append(item)
        # 注意这里**不把 item 整个展开**到 emit 里：
        # item 含 `task_id`，而 publish 的第一个位置参数就叫 task_id，
        # `publish(task_id, type_, **item)` 会直接 TypeError（multiple values）。
        # 事件本身就带 task_id，不需要再传一次。
        self.emit(events.ARTIFACT, path=path, kind=kind, summary=summary)
        return item

    def usage(self) -> dict:
        return LEDGER.snapshot()
