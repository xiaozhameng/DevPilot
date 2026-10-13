"""6 个交付节点，一节点一文件。

为什么按节点分文件，而不是"一个大 agents.py"？

因为它们之间的**唯一**联系是流水线（pipeline.py）里的那几条边。
在文件层面保持隔离，让"一个节点改了会不会影响另一个"这个问题
有一个廉价的答案：看它有没有改公共的 state 键。

如果全挤在一个文件里，很容易顺手写个共享的私有函数，
然后两个节点的耦合就从"通过 state 通信"退化成"通过函数调用耦合"——
后者在多 Agent 系统里是危险的，因为它绕过了状态机与检查点：
恢复时共享函数里的临时状态全丢了，而你以为它在。
"""

from .approve import approve
from .clarify import clarify, clarify_router
from .code import code
from .design import design
from .gate import gate, gate_router
from .prd import prd

__all__ = ["clarify", "clarify_router", "prd", "design",
           "approve", "code", "gate", "gate_router"]
