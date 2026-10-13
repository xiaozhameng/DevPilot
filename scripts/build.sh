#!/usr/bin/env bash
# 打包网关（Java 侧）。改了 Java 代码或演示页之后必须跑一次。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Java 工具链统一入口。和 CitePilot 共用同一份（在 Agent/.tools/ 下）。
# 用 if 而不是 `[ -f ] && source`：后者在文件不存在时返回非 0，
# 配合 set -e 会让整个脚本静默退出 —— 表现为"打包什么都没发生"。
TOOLS="$(dirname "$ROOT")/.tools"
if [ -f "$TOOLS/env.sh" ]; then
  # shellcheck disable=SC1091
  source "$TOOLS/env.sh"
fi

# 显式给 JAVA_HOME/PATH 兜一层：万一调用方的 shell 没加载 .tools/env.sh，
# 这里也能找到 java —— 否则报错是 "mvn: command not found"，
# 而真正的原因是 JAVA_HOME 没设，排查方向会跑偏。
if [ -d "$TOOLS/jdk21/Contents/Home" ]; then
  export JAVA_HOME="$TOOLS/jdk21/Contents/Home"
  export PATH="$JAVA_HOME/bin:$PATH"
fi
if [ -d "$TOOLS/maven" ]; then
  export PATH="$TOOLS/maven/bin:$PATH"
fi

if ! command -v mvn >/dev/null 2>&1; then
  echo "找不到 mvn。请先执行： source $TOOLS/env.sh" >&2
  exit 1
fi

cd "$ROOT/gateway"

# Maven 本地仓库：**复用 ~/.m2/repository**，不在项目内另开一个。
#
# 为什么改掉了原来"项目内 .m2repo"的做法：
# 上一个项目（CitePilot）用的是同一个 Spring Boot 3.3.5，
# 依赖已经完整缓存在 ~/.m2/repository 里了（约 100MB）。
# 若强行指向项目内的空目录，Maven 会把它们全部重新下一遍 ——
# 在网络受限的环境里这就是几分钟的纯等待，换来的只是"不污染 ~/.m2"
# 这个洁癖式的收益。
#
# 且 ~/.m2/settings.xml 里已经配好了阿里云镜像，
# 万一有缺失的依赖也会走国内源，不会卡在 central 上。
#
# 需要隔离时用 MAVEN_REPO 覆盖即可：
#   MAVEN_REPO=/tmp/m2repo ./scripts/build.sh
MAVEN_REPO="${MAVEN_REPO:-$HOME/.m2/repository}"
export MAVEN_OPTS="${MAVEN_OPTS:-} -Dmaven.repo.local=$MAVEN_REPO"

echo "──── 打包网关 ────"
mvn -B -q package -DskipTests
echo
echo "产物："
ls -lh target/*.jar 2>/dev/null | awk '{print "  " $9 "  " $5}'
