"""沙箱执行层：让 Agent 生成的东西真的跑起来，但跑不出这个目录。

## 危险分级（先分级，再谈方案）

四类动作的危险等级天差地别，用同一套措施去管是浪费，也是漏：

| 动作 | 危险等级 | 为什么 |
|---|---|---|
| 读文件 | 低 | 只读不会造成不可逆损害，但可能读到不该读的（密钥、别人的数据） |
| 写文件 | 中 | 会覆盖，但**可回滚** —— 前提是你提前存了快照 |
| 执行命令 | 高 | 不可逆。一条 `rm -rf` 之后没有"撤销"这个操作 |
| 外部 API / 数据库写 | 高 | 影响跳出本机，回滚要靠对方配合 |

DevPilot 主要涉及前三类，所以措施是：**读限范围、写存快照、执行限白名单 + 限时**。

## 最重要的一条：子进程的环境变量白名单

这一条最容易被忽略，但后果最严重。

Agent 会生成代码，代码要跑起来。而跑代码的常规做法是 `subprocess.run(..., env=os.environ)`
—— 这一行等于把**本进程的所有环境变量**（包括 DEVPILOT_API_KEY）交到模型生成的代码手里。
模型只要写一行 `os.environ["DEVPILOT_API_KEY"]` 或者发一个带 env 的请求，
密钥就出去了。而这段代码是模型写的，你没法审。

所以这里用的是**白名单**：只有明确列出的变量才传给子进程，其余一律不传。
不是"过滤掉看起来像密钥的"，而是"只放行已知需要的"——
黑名单永远会漏（谁能穷举所有密钥的命名习惯？），白名单的默认值是安全的。

## 诚实说明：这个沙箱挡得住什么、挡不住什么

**挡得住**：误操作（写错路径、删错文件）、越界访问（想读 ../.env）、
跑挂（死循环、等待输入）、密钥外泄（环境变量白名单）。

**挡不住**：蓄意逃逸。命令白名单允许 `python xxx.py`，而 python 能执行任意代码 ——
这是语言本身的特性，不是校验能解决的。真要防这个级别，需要容器 / gVisor /
受限系统账号 / seccomp，属于基础设施层的事，不是应用层能补的。

把这条写清楚比假装"我的沙箱很安全"重要：**说清边界在哪比宣称安全更有用。**
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from .config import SETTINGS
from .fsutil import ensure_dir


class SandboxError(RuntimeError):
    pass


class PathEscape(SandboxError):
    """试图访问工作目录之外的路径。"""


class CommandBlocked(SandboxError):
    """命令不在白名单里，或参数里有危险内容。"""


class CommandTimeout(SandboxError):
    """命令超时被强杀。"""


# 允许执行的命令（按 basename 匹配）。
# 刻意**不含** shell（sh/bash/zsh）—— 一旦允许 shell，白名单就形同虚设，
# 因为 `sh -c "任意东西"` 能绕过一切参数检查。
ALLOWED_COMMANDS = frozenset({
    "python", "python3", "pytest", "coverage",
    "ls", "cat", "head", "tail", "wc", "grep", "find", "echo",
    "mkdir", "touch", "cp", "mv", "rm", "diff", "sort", "uniq",
    "git", "ruff", "mypy",
})

# 传给子进程的环境变量（白名单，见模块 docstring）。
# 注意这里没有任何 *_API_KEY / *_TOKEN / *_SECRET。
_ALLOWED_ENV = ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TMPDIR", "TERM")

# 参数里出现这些片段就拒绝。它们共同的特征是"指向工作目录之外"。
_DANGEROUS_ARG = re.compile(r"(^|/)\.\.(/|$)|^/$|^~")

# 去掉解释器路径末尾的版本号："python3.13.12" -> "python"，"ruby3.2" -> "ruby"。
# 不这么做的话，同一台机器上 `python` 能用而 `python3.13` 被拒 ——
# 白名单拦的是"意图"，不是"版本写法"。
_VERSION_SUFFIX = re.compile(r"[\d.]+$")


@dataclass
class RunResult:
    argv: list[str]
    returncode: int
    stdout: str = ""
    stderr: str = ""
    ms: int = 0
    timed_out: bool = False
    truncated: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0 and not self.timed_out

    def tail(self, n: int = 2000) -> str:
        """末尾 n 个字符。报错信息几乎总在输出末尾，所以默认取尾不取头。"""
        body = (self.stdout + ("\n" + self.stderr if self.stderr else "")).strip()
        return body[-n:]


def _clip(text: str, limit: int) -> tuple[str, bool]:
    """输出截断：保留头 2/3 + 尾 1/3。

    只留头会丢掉报错，只留尾会丢掉上下文（比如"跑了哪些测试"）。
    头尾都留是折中，并且在中间显式写出被省略了多少字 ——
    让模型知道"这里少了东西"，而不是以为输出就这么短。
    """
    if len(text) <= limit:
        return text, False
    head = limit * 2 // 3
    tail = limit - head
    omitted = len(text) - limit
    return f"{text[:head]}\n…（已省略 {omitted} 字符）…\n{text[-tail:]}", True


class Sandbox:
    """一个任务一个沙箱。目录即边界。"""

    def __init__(self, task_id: str, root: Path | None = None,
                 timeout: int | None = None, output_limit: int | None = None) -> None:
        self.task_id = task_id
        self.timeout = timeout or SETTINGS.sandbox_timeout
        self.output_limit = output_limit or SETTINGS.sandbox_output_limit
        base = root or SETTINGS.workspace_dir
        # 任务 ID 进目录名之前先做一次净化：它可能来自外部输入（URL 参数）。
        # 一个带 ../ 的 task_id 能让沙箱根目录直接跑到项目外面去。
        safe_id = re.sub(r"[^A-Za-z0-9._-]", "_", task_id) or "task"
        self.root = Path(base) / safe_id
        ensure_dir(self.root)
        self._snap_dir = self.root / ".snapshots"
        self._snap_seq = 0
        self._undo: list[tuple[str, str | None]] = []   # (相对路径, 原内容或 None)
        self.runs: list[RunResult] = []                 # 执行记录，供审计

    # ============================================================
    # 路径与文件
    # ============================================================
    def resolve(self, relative: str) -> Path:
        """把相对路径解析成绝对路径，并保证它落在工作目录内。

        为什么不能只检查字符串里有没有 ".."？
        因为符号链接。工作区里如果有人（或模型）建了一个指向 /etc 的软链，
        那么 `link/passwd` 这种"看起来完全正常"的路径就能读到外面去。
        所以判定的依据必须是**解析之后的真实路径**（resolve 会解掉软链），
        而不是原始字符串。
        """
        raw = str(relative).strip()
        if not raw:
            raise PathEscape("路径不能为空")
        candidate = (self.root / raw).resolve()
        root = self.root.resolve()
        if candidate != root and root not in candidate.parents:
            raise PathEscape(
                f"路径越界：{relative} 解析到 {candidate}，"
                f"不在工作目录 {root} 内"
            )
        return candidate

    def write(self, relative: str, content: str) -> Path:
        """写文件。会先把原内容记进 undo log —— 这是"可回滚"的实现。"""
        target = self.resolve(relative)
        ensure_dir(target.parent)
        old = target.read_text(encoding="utf-8") if target.exists() else None
        self._undo.append((relative, old))
        target.write_text(content, encoding="utf-8")
        return target

    def read(self, relative: str) -> str:
        target = self.resolve(relative)
        if not target.exists():
            raise FileNotFoundError(f"文件不存在：{relative}")
        return target.read_text(encoding="utf-8", errors="replace")

    def exists(self, relative: str) -> bool:
        return self.resolve(relative).exists()

    def list_files(self, max_files: int = 200) -> list[dict]:
        """列出工作区内的文件（不含快照目录）。

        用 os.walk 而不是 Path.rglob 是因为要在遍历时**剪掉** .snapshots ——
        快照里是历史副本，列给模型看只会让它困惑，而且会占满文件配额。
        """
        out: list[dict] = []
        for dirpath, dirnames, filenames in os.walk(self.root):
            dirnames[:] = [d for d in dirnames if d != ".snapshots" and not d.startswith("__py")]
            for fn in filenames:
                if fn.endswith(".pyc"):
                    continue
                p = Path(dirpath) / fn
                out.append({
                    "path": str(p.relative_to(self.root)),
                    "size": p.stat().st_size,
                })
                if len(out) >= max_files:
                    return sorted(out, key=lambda x: x["path"])
        return sorted(out, key=lambda x: x["path"])

    # ============================================================
    # 回滚
    # ============================================================
    def begin(self) -> None:
        """开始一个回滚区间。区间内所有写入都记录在案。"""
        self._undo.clear()

    def rollback(self) -> list[str]:
        """撤销 begin() 之后的所有写入，返回被恢复的路径列表。

        为什么用 undo log 而不是"整目录快照 + 复制回去"？
        因为快照复制的代价随工作区大小线性增长，而这个动作会在
        "覆盖率不达标，打回重写"这条路上反复触发。undo log 只记录
        真正动过的文件，代价与改动量成正比。
        """
        restored: list[str] = []
        for rel, old in reversed(self._undo):
            target = self.resolve(rel)
            if old is None:
                if target.exists():
                    target.unlink()
                    restored.append(f"-{rel}")
            else:
                ensure_dir(target.parent)
                target.write_text(old, encoding="utf-8")
                restored.append(f"~{rel}")
        self._undo.clear()
        return restored

    def discard(self) -> None:
        """提交：丢掉 undo log，之后不能再回滚这些改动。"""
        self._undo.clear()

    def snapshot(self, label: str = "") -> str:
        """整目录快照（用于"版本回滚"这种粗粒度场景）。"""
        self._snap_seq += 1
        name = f"{self._snap_seq:03d}{('-' + label) if label else ''}"
        dest = self._snap_dir / name
        ensure_dir(dest)
        for item in self.root.iterdir():
            if item.name == ".snapshots":
                continue
            if item.is_dir():
                shutil.copytree(item, dest / item.name, dirs_exist_ok=True)
            else:
                shutil.copy2(item, dest / item.name)
        return name

    def restore(self, name: str) -> None:
        src = self._snap_dir / name
        if not src.is_dir():
            raise SandboxError(f"快照不存在：{name}")
        for item in list(self.root.iterdir()):
            if item.name == ".snapshots":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        for item in src.iterdir():
            if item.is_dir():
                shutil.copytree(item, self.root / item.name)
            else:
                shutil.copy2(item, self.root / item.name)

    def list_snapshots(self) -> list[str]:
        if not self._snap_dir.is_dir():
            return []
        return sorted(d.name for d in self._snap_dir.iterdir() if d.is_dir())

    # ============================================================
    # 执行
    # ============================================================
    def child_env(self) -> dict[str, str]:
        """给子进程的环境（白名单，见模块 docstring）。"""
        env = {k: os.environ[k] for k in _ALLOWED_ENV if k in os.environ}
        env["PYTHONPATH"] = str(self.root)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        env["HOME"] = str(self.root)      # 别让子进程往真实 HOME 里写缓存
        env["TMPDIR"] = str(self.root / ".tmp")
        ensure_dir(self.root / ".tmp")
        return env

    def check(self, argv: list[str]) -> str | None:
        """预检：命令是否允许。返回拒绝原因，通过则返回 None。

        单独抽出来是为了**能在不执行的前提下做测试** ——
        "这条命令会被拦吗"是个纯函数问题，不该需要真的跑一次才知道。
        """
        if not argv:
            return "空命令"
        prog = os.path.basename(argv[0])
        normalized = _VERSION_SUFFIX.sub("", prog) or prog
        if prog not in ALLOWED_COMMANDS and normalized not in ALLOWED_COMMANDS:
            return f"命令不在白名单：{prog}（允许：{', '.join(sorted(ALLOWED_COMMANDS))}）"
        for arg in argv[1:]:
            if _DANGEROUS_ARG.search(arg):
                return f"参数包含越界路径：{arg}"
        return None

    def run(self, command: str | list[str], timeout: int | None = None,
            cwd: str | None = None) -> RunResult:
        """执行一条命令。

        这里每一行都是踩过的：
          - `start_new_session=True` + `os.killpg`：只 kill 父进程的话，
            它 fork 出来的子进程会变成孤儿继续跑。必须整组杀。
          - `stdin=DEVNULL`：不接输入。否则 `input()` 会让命令永久挂着，
            看起来像"卡死"，实际是在等一个永远不会来的回车。
          - 绝不 shell=True：那等于把我们辛苦做的 argv 检查全部作废。
        """
        argv = shlex.split(command) if isinstance(command, str) else list(command)
        blocked = self.check(argv)
        if blocked:
            raise CommandBlocked(blocked)

        workdir = self.resolve(cwd) if cwd else self.root
        tmo = timeout or self.timeout
        t0 = time.perf_counter()

        proc = subprocess.Popen(
            argv,
            cwd=str(workdir),
            env=self.child_env(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            start_new_session=True,
        )
        timed_out = False
        try:
            out, err = proc.communicate(timeout=tmo)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            out, err = proc.communicate()
            err = (err or "") + f"\n[沙箱] 命令超过 {tmo}s，已被强制终止"

        ms = int((time.perf_counter() - t0) * 1000)
        out, t1 = _clip(out or "", self.output_limit)
        err, t2 = _clip(err or "", self.output_limit)

        res = RunResult(argv=argv, returncode=proc.returncode or 0, stdout=out,
                        stderr=err, ms=ms, timed_out=timed_out, truncated=t1 or t2)
        self.runs.append(res)
        return res

    # ---------- 组合动作（Agent 用得最多的一条）----------
    def run_pytest(self, target: str = "", coverage: bool = True,
                   timeout: int | None = None) -> tuple[RunResult, float | None]:
        """跑测试，并顺带解析覆盖率。返回 (结果, 覆盖率百分比或 None)。

        用 `sys.executable -m pytest` 而不是裸 `pytest`：
        裸命令依赖 PATH 里有 pytest，而它通常只装在虚拟环境的 bin 里 ——
        结果是"本机手动跑没问题、在沙箱里跑就说找不到命令"。
        用当前解释器 + -m 是唯一不依赖 PATH 的写法。

        覆盖率不从退出码里读（它只告诉你过没过），而是解析
        `--cov-report=term` 输出里的 TOTAL 行 —— 那一行是唯一能拿到的
        权威数字，也正好对应"覆盖率门禁"这个节点要判的东西。
        """
        parts = [sys.executable, "-m", "pytest", "-q", "--no-header",
                 "-p", "no:cacheprovider"]
        if coverage:
            parts += ["--cov=.", "--cov-report=term"]
        if target:
            parts.append(target)
        res = self.run(parts, timeout=timeout)
        cov = None
        for line in (res.stdout or "").splitlines():
            if line.startswith("TOTAL"):
                for token in reversed(line.split()):
                    if token.endswith("%"):
                        try:
                            cov = float(token.rstrip("%"))
                        except ValueError:
                            pass
                        break
        return res, cov

    def cleanup(self) -> None:
        """删掉整个工作区。任务结束、产物已归档之后调用。"""
        shutil.rmtree(self.root, ignore_errors=True)
