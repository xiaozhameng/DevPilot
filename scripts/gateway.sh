#!/usr/bin/env bash
# 启动 Java 网关。它依赖内核已经在跑（内核是它唯一的上游）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

TOOLS="$(dirname "$ROOT")/.tools"
if [ -f "$TOOLS/env.sh" ]; then
  # shellcheck disable=SC1091
  source "$TOOLS/env.sh"
fi
if [ -d "$TOOLS/jdk21/Contents/Home" ]; then
  export JAVA_HOME="$TOOLS/jdk21/Contents/Home"
fi

JAR="$ROOT/gateway/target/devpilot-gateway-0.1.0.jar"
if [ ! -f "$JAR" ]; then
  echo "还没打包。先跑： ./scripts/build.sh" >&2
  exit 1
fi

PORT="${GATEWAY_PORT:-8091}"
echo "DevPilot 网关 → http://127.0.0.1:${PORT}"
echo "  演示页    http://127.0.0.1:${PORT}/"
echo

# exec 让 java 直接接管这个进程：Ctrl-C 能一次停干净，
# 不会留下一个"父脚本已经退出、java 还在跑"的孤儿进程。
exec java -jar "$JAR" --server.port="$PORT"
