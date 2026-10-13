"""DevPilot 内核：多 Agent 交付流水线。

分层（自顶向下）：
    cli / server      —— 界面层：命令行与 HTTP/SSE
    pipeline          —— 装配：把节点接成图，定义刹车
    graph / plan      —— 编排引擎：运行时路由 + 静态依赖分析
    agents            —— 6 个节点：各自独立的上下文与职责
    runtime/state     —— 执行上下文、状态机、检查点
    sandbox           —— 沙箱执行：让生成的东西真的跑起来，但跑不出目录
    llm               —— 模型调用：分类重试 + 按节点记账
"""

from .config import SETTINGS, Settings, load_settings

__all__ = ["SETTINGS", "Settings", "load_settings"]
__version__ = "0.1.0"
