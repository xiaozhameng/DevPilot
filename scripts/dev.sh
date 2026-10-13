#!/usr/bin/env bash
# 一键起两个服务：内核 + 网关。Ctrl-C 一起停。
#
# 顺序不能反 —— 网关启动时会去探内核的健康状态，
# 内核没起来的话网关的 /api/health 会显示内核离线（虽然不影响使用）。
#
# 用法：
#   ./scripts/dev.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KERNEL_PID=""
GATEWAY_PID=""

cleanup() {
  echo
  echo "正在停止…"
  # 只杀我们自己拉起来的进程，不用 pkill -f —— 那会误伤同名的其他进程。
  [ -n "$GATEWAY_PID" ] && kill "$GATEWAY_PID" 2>/dev/null || true
  [ -n "$KERNEL_PID" ] && kill "$KERNEL_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo "已停止。"
}
trap cleanup EXIT INT TERM

echo "──── 1/2 启动内核 ────"
"$ROOT/scripts/kernel.sh" &
KERNEL_PID=$!

# 等内核就绪：轮询健康检查。
# 为什么不用固定 sleep？因为首次启动要建库、加载配置，时间不固定；
# 固定 sleep 要么等太久、要么没等够。
#
# 注意 --noproxy '*'：健康检查是打本机端口的，
# 如果环境里设了 http_proxy（很常见），请求会被代理劫持到别处去，
# 表现是"服务明明起来了却探测不到"。本机探测一律绕过代理。
#
# 这里刻意**不判断 HTTP 状态码**：/health 需要 X-Internal-Token，
# 不带 token 会返回 401 —— 而"能返回 401"本身就说明服务已经在监听并响应了。
# 若改成判断 200，token 一旦没配好就会在这里死等到超时，反而更难排查。
PORT="${DEVPILOT_SERVE_PORT:-8078}"
for _ in $(seq 1 80); do
  if curl -s --noproxy '*' -o /dev/null "http://127.0.0.1:${PORT}/health" 2>/dev/null; then
    echo "✓ 内核就绪"
    break
  fi
  sleep 0.5
done

echo
echo "──── 2/2 启动网关 ────"
"$ROOT/scripts/gateway.sh" &
GATEWAY_PID=$!

GPORT="${GATEWAY_PORT:-8091}"
for _ in $(seq 1 80); do
  if curl -s --noproxy '*' -o /dev/null "http://127.0.0.1:${GPORT}/api/health" 2>/dev/null; then
    echo "✓ 网关就绪"
    break
  fi
  sleep 0.5
done

echo
echo "═══════════════════════════════════════════"
echo "  演示页： http://127.0.0.1:${GPORT}/"
echo "  按 Ctrl-C 停止全部服务"
echo "═══════════════════════════════════════════"
echo

wait
