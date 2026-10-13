"""配置：只从环境变量与 .env 读，代码里零硬编码密钥。

DevPilot 的配置比 CitePilot 多了一类东西：**刹车参数**。
因为流水线里有两处条件边会成环（澄清追问、覆盖率打回），
而"环"在没有上限时等于无限烧钱。所以 max_clarify / max_codegen
不是可调参数，是安全阀 —— 它们和并发数、超时一样，属于必须在配置里显式写死的东西。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# 项目根目录（本文件在 src/devpilot/config.py，往上三层）
ROOT = Path(__file__).resolve().parents[2]

load_dotenv(ROOT / ".env")

# 关掉 litellm 启动时的远程 model cost map 拉取。
# 不关的话，import litellm 会去 raw.githubusercontent.com 拉一份价格表；
# 网络不通时它会重试 3 次、总共约 10 秒。而这段 IO 发生在"准备调用模型"的
# 路径上，和本次调用毫无关系，纯粹是启动延迟 —— 实测把整个测试套件
# 从 200 秒拖到 20 秒。
os.environ.setdefault("LITELLM_LOCAL_MODEL_COST_MAP", "True")


def _env(key: str, default: str = "") -> str:
    return (os.environ.get(key) or default).strip()


def _path(key: str, default: str) -> Path:
    p = Path(_env(key, default))
    return p if p.is_absolute() else ROOT / p


@dataclass(frozen=True)
class Settings:
    # 生成模型
    api_key: str
    api_base: str
    model: str

    # 流水线刹车（见本模块 docstring）
    max_clarify: int
    max_codegen: int
    coverage_target: int
    approval_timeout: int      # 人工审批的等待窗口（秒），超时按拒绝处理

    # 沙箱
    workspace_dir: Path
    sandbox_timeout: int
    sandbox_output_limit: int

    # 服务层
    serve_host: str
    serve_port: int
    internal_token: str
    gateway_base: str

    # 路径
    root: Path
    db_path: Path          # 检查点 + 任务表（内核侧）

    @property
    def has_llm(self) -> bool:
        """没有有效 key 时，内核自动降级为「模板产出」模式。

        这一点很重要：整条流水线的骨架（编排、状态机、沙箱、断点恢复）
        不该依赖模型可用。模型挂了，任务照样能跑完，只是产出的是占位代码。
        —— 实践里这对应「降级链」：把最贵、最不稳定的那一环做成可插拔的。
        """
        return bool(self.api_key) and not self.api_key.startswith("sk-xxx")


def load_settings() -> Settings:
    return Settings(
        api_key=_env("DEVPILOT_API_KEY"),
        api_base=_env("DEVPILOT_API_BASE", "https://api.deepseek.com/v1"),
        model=_env("DEVPILOT_MODEL", "openai/deepseek-v4-flash"),
        max_clarify=int(_env("DEVPILOT_MAX_CLARIFY", "3")),
        max_codegen=int(_env("DEVPILOT_MAX_CODEGEN", "2")),
        coverage_target=int(_env("DEVPILOT_COVERAGE_TARGET", "80")),
        approval_timeout=int(_env("DEVPILOT_APPROVAL_TIMEOUT", "1800")),
        workspace_dir=_path("DEVPILOT_WORKSPACE_DIR", "var/workspaces"),
        sandbox_timeout=int(_env("DEVPILOT_SANDBOX_TIMEOUT", "120")),
        sandbox_output_limit=int(_env("DEVPILOT_SANDBOX_OUTPUT_LIMIT", "20000")),
        serve_host=_env("DEVPILOT_SERVE_HOST", "127.0.0.1"),
        serve_port=int(_env("DEVPILOT_SERVE_PORT", "8078")),
        internal_token=_env("DEVPILOT_INTERNAL_TOKEN", "dev-local-token-change-me"),
        gateway_base=_env("DEVPILOT_GATEWAY_BASE", "http://127.0.0.1:8091"),
        root=ROOT,
        db_path=_path("DEVPILOT_DB_PATH", "var/devpilot.db"),
    )


SETTINGS = load_settings()
