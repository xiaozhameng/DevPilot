#!/usr/bin/env bash
# 启动 DevPilot 内核（Python 侧）。
#
# 内核是一个常驻进程，只对外暴露 HTTP + SSE。
# 所有界面（命令行、Java 网关、网页、将来的手机端）都是它的薄客户端。
#
# 用法：
#   ./scripts/kernel.sh        # 前台运行，Ctrl-C 退出
#   ./scripts/kernel.sh &      # 后台运行
set -euo pipefail

# BASH_SOURCE[0] 是"这个脚本文件自身的路径"。
# 先 dirname 拿到 scripts/ 目录，再 /.. 回到项目根 ——
# 这样无论从哪个目录调用脚本，路径都是对的。
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# 让 python 能找到 src/devpilot 包。
# 用 editable 安装（pip install -e .）也可以，但显式设 PYTHONPATH
# 能保证在"没装过包"的机器上直接跑起来。
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export TMPDIR="$ROOT/var/tmp"
mkdir -p "$TMPDIR"

HOST="${DEVPILOT_SERVE_HOST:-127.0.0.1}"
PORT="${DEVPILOT_SERVE_PORT:-8078}"

echo "DevPilot 内核 → http://${HOST}:${PORT}"
echo "  接口文档  http://${HOST}:${PORT}/docs"
echo "  健康检查  http://${HOST}:${PORT}/health"
echo
echo "  提示：/health 需要 X-Internal-Token 头，不带会返回 401。"
echo "        「能返回 401」本身就说明服务已在监听，属于正常现象。"
echo

exec "$ROOT/.venv/bin/python" -m uvicorn \
  --factory devpilot.server:create_app \
  --host "$HOST" --port "$PORT"
