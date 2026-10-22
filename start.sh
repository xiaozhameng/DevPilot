#!/usr/bin/env bash
# 一键启动 DevPilot：内核 (:8078) + 网关 (:8091)，前台阻塞。
#
# 停止方式（三种都行）：
#   · 在这个窗口按 Ctrl-C
#   · 直接关掉这个终端窗口
#   · 另开一个终端 kill 掉这个前台进程
# 两个服务会被一起收走，不会留下孤儿进程。
#
# 它会自动处理三件容易忘的事：.env 没配会提示、venv 没建会建、jar 过期会重打包。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

info() { printf "\033[36m·\033[0m %s\n" "$1"; }
warn() { printf "\033[33m!\033[0m %s\n" "$1"; }
die()  { printf "\033[31m✗\033[0m %s\n" "$1" >&2; exit 1; }

# ── 1. 配置 ────────────────────────────────────────────────
if [ ! -f .env ]; then
  cp .env.example .env
  warn "已生成 .env，但里面有占位符，先把 key 填上再运行："
  warn "  $ROOT/.env  →  DEVPILOT_API_KEY=sk-..."
  exit 1
fi

KEY="$(grep -E '^DEVPILOT_API_KEY=' .env | head -1 | cut -d= -f2- || true)"
if [ -z "$KEY" ] || [ "${KEY#sk-xxx}" != "$KEY" ]; then
  die ".env 里的 DEVPILOT_API_KEY 还没填（当前值：${KEY:-空}）"
fi

# ── 2. Python 环境 ─────────────────────────────────────────
if [ ! -x .venv/bin/python ]; then
  info "首次运行，创建虚拟环境并安装依赖（约 1 分钟）…"
  python3 -m venv .venv
  .venv/bin/pip install -q -e . || die "依赖安装失败"
fi

# ── 3. 网关 jar ────────────────────────────────────────────
# 只在 jar 缺失、或 Java 源码 / pom 比 jar 新时才打包。
# 不做这步的话，改了 Java 却还在跑旧 jar —— 表现是"明明改对了却没生效"。
JAR="gateway/target/devpilot-gateway-0.1.0.jar"
if [ ! -f "$JAR" ] || [ -n "$(find gateway/src gateway/pom.xml -newer "$JAR" -print -quit 2>/dev/null)" ]; then
  info "打包网关（首次运行或 Java 代码有改动）…"
  if ! ./scripts/build.sh >/tmp/devpilot-build.log 2>&1; then
    tail -15 /tmp/devpilot-build.log
    die "打包失败，完整日志：/tmp/devpilot-build.log"
  fi
fi

# ── 4. 起服务 ──────────────────────────────────────────────
echo
info "内核 :8078  ·  网关 :8091"
info "演示看板  http://127.0.0.1:8091/"
info "按 Ctrl-C 或关闭本窗口即可全部停止"
echo

exec ./scripts/dev.sh
