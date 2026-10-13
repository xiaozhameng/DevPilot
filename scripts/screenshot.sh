#!/usr/bin/env bash
# 用本机 Chrome 无头模式给演示页截图（2 倍高清）。
#
# 为什么用 Chrome 无头 + --force-device-scale-factor=2：
#   截图要放进 README，普通 1x 图在视网膜屏上看是糊的。
#   device-scale-factor=2 让 CSS 像素 1:1 对应 2 个物理像素，文字边缘干净。
#
# 用法：
#   ./scripts/screenshot.sh                                  # 只截总览
#   ./scripts/screenshot.sh <gatewayTaskId>                  # 总览 + 该任务详情
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/docs/screenshots"
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PORT="${GATEWAY_PORT:-8091}"
BASE="http://127.0.0.1:${PORT}"
TASK_ID="${1:-}"

mkdir -p "$OUT"

# --virtual-time-budget：等页面把异步的 /api/tasks 拉回来再截。
# 不加这个参数会在 DOM 刚渲染、数据还没到时就按下快门，截出一堆空列表。
# --hide-scrollbars：无头模式下滚动条会占位，去掉更干净。
#
# --no-sandbox / --disable-dev-shm-usage / --user-data-dir：
# 这三个是"在受限环境里能跑起来"的必要条件，实测缺一个就崩
# （报 Trace/BPT trap: 5，看起来像 Chrome 坏了，其实是沙箱与共享内存的问题）。
# user-data-dir 必须给独立目录，否则会去抢用户日常配置的锁。
shot() {
  local url="$1" file="$2" w="$3" h="$4"
  local profile
  profile="$(mktemp -d)"
  "$CHROME" --headless=new --no-sandbox --disable-gpu --disable-dev-shm-usage \
    --user-data-dir="$profile" --hide-scrollbars \
    --force-device-scale-factor=2 \
    --window-size="${w},${h}" \
    --virtual-time-budget=8000 \
    --screenshot="$file" "$url" >/dev/null 2>&1 || true
  rm -rf "$profile"
  if [ -s "$file" ]; then echo "  ✓ $(basename "$file")  ($(wc -c < "$file") B)"; else echo "  ✗ $(basename "$file") 截图失败" >&2; fi
}

echo "截图 → $OUT"
shot "$BASE/" "$OUT/dashboard.png" 1440 900

if [ -n "$TASK_ID" ]; then
  shot "$BASE/?task=${TASK_ID}" "$OUT/task-detail.png" 1440 980
else
  echo "  （未传 taskId，跳过详情页截图）"
fi

echo "完成。"
