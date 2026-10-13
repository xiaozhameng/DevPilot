"""沙箱的测试。

最该被守住的两条，都不是"能不能跑通"，而是"能不能拦住"：
  1) **密钥不出沙箱** —— 子进程的环境里不能有任何 *_API_KEY
  2) **路径不出沙箱** —— 包括符号链接这种"看起来完全正常"的路径
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

from devpilot.sandbox import (
    CommandBlocked,
    PathEscape,
    Sandbox,
    _clip,
)

PY = sys.executable          # 真实解释器绝对路径（basename 带版本号，考验白名单归一化）


@pytest.fixture()
def sb(tmp_path: Path) -> Sandbox:
    return Sandbox("unit-test", root=tmp_path, timeout=20, output_limit=2000)


# ============================================================
# 路径边界
# ============================================================

def test_write_then_read_roundtrip(sb: Sandbox):
    sb.write("src/main.py", "print('hi')")
    assert sb.read("src/main.py") == "print('hi')"
    assert sb.exists("src/main.py")


def test_path_escape_with_dotdot_is_blocked(sb: Sandbox):
    with pytest.raises(PathEscape):
        sb.resolve("../../etc/passwd")


def test_path_escape_inside_nested_is_blocked(sb: Sandbox):
    with pytest.raises(PathEscape):
        sb.write("a/b/../../../outside.txt", "x")


def test_absolute_path_outside_is_blocked(sb: Sandbox):
    with pytest.raises(PathEscape):
        sb.resolve("/etc/passwd")


def test_symlink_escape_is_blocked(sb: Sandbox, tmp_path: Path):
    """这条是"只查字符串里有没有 .."的写法会漏掉的：

    先在工作区里建一个指向外面的软链，然后用一个**不含任何可疑字符**的路径
    去访问它。字符串检查完全看不出来，只有把路径 resolve 之后比对才拦得住。
    """
    outside = tmp_path.parent / "secret.txt"
    outside.write_text("不该被读到", encoding="utf-8")
    link = sb.root / "innocent"
    if link.exists():
        link.unlink()
    os.symlink(outside, link)

    with pytest.raises(PathEscape):
        sb.resolve("innocent")


def test_list_files_skips_snapshot_dir(sb: Sandbox):
    sb.write("a.py", "x")
    sb.snapshot("v1")
    paths = [f["path"] for f in sb.list_files()]
    assert "a.py" in paths
    assert not any(".snapshots" in p for p in paths)


# ============================================================
# 命令白名单
# ============================================================

def test_shell_is_not_whitelisted(sb: Sandbox):
    """不允许 shell —— 它一进来，白名单就形同虚设。"""
    for prog in ("bash", "sh", "zsh"):
        assert sb.check([prog, "-c", "echo hi"]) is not None


def test_unknown_command_is_rejected(sb: Sandbox):
    assert sb.check(["curl", "http://example.com"]) is not None
    with pytest.raises(CommandBlocked):
        sb.run("curl http://example.com")


def test_version_suffixed_python_is_accepted(sb: Sandbox):
    """`python3.13` 和 `python` 语义相同，不该因为写法不同被拒。"""
    assert sb.check(["/usr/bin/python3.13.12", "-c", "print(1)"]) is None


def test_arguments_pointing_outside_are_rejected(sb: Sandbox):
    assert sb.check(["cat", "../../etc/passwd"]) is not None
    assert sb.check(["rm", "-rf", "/"]) is not None
    assert sb.check(["ls", "~"]) is not None


def test_allowed_command_runs_and_captures_stdout(sb: Sandbox):
    res = sb.run([PY, "-c", "print('hello sandbox')"])
    assert res.ok, res.stderr
    assert "hello sandbox" in res.stdout
    assert res.ms >= 0
    assert not res.truncated


# ============================================================
# 环境变量隔离（最重要的一条）
# ============================================================

def test_child_env_has_no_api_key(sb: Sandbox):
    env = sb.child_env()
    leaked = [k for k in env if "API_KEY" in k.upper() or "TOKEN" in k.upper()]
    assert leaked == [], f"子进程环境里出现了疑似密钥：{leaked}"


def test_generated_code_cannot_steal_api_key(sb: Sandbox, monkeypatch):
    """端到端验证：让子进程把整个 environ 吐出来，逐项检查。

    这比只测 sb.child_env() 更硬 —— 因为 child_env() 是"我们算出来的"，
    而这里是"子进程真正看到的"。如果有人把 env=os.environ 写回去，
    这个测试会失败，而前者不会。
    """
    monkeypatch.setenv("DEVPILOT_API_KEY", "sk-must-not-leak-0123456789")
    code = "import os,json;print(json.dumps(dict(os.environ)))"
    res = sb.run([PY, "-c", code])
    assert res.ok, res.stderr
    assert "sk-must-not-leak" not in res.stdout
    assert "DEVPILOT_API_KEY" not in res.stdout


def test_child_env_home_is_redirected_into_workspace(sb: Sandbox):
    """子进程的 HOME 指向工作区，避免污染真实的 ~（缓存、配置）。"""
    assert sb.child_env()["HOME"] == str(sb.root)


# ============================================================
# 超时 / 截断
# ============================================================

def test_timeout_kills_the_whole_process_group(sb: Sandbox):
    """不只是父进程死掉 —— 它 fork 出来的子进程也要一起走。"""
    t0 = time.perf_counter()
    res = sb.run([PY, "-c", "import time; time.sleep(30)"], timeout=1)
    elapsed = time.perf_counter() - t0
    assert res.timed_out
    assert res.returncode != 0
    assert elapsed < 10, f"超时没生效，实际跑了 {elapsed:.1f}s"
    assert "强制终止" in res.stderr


def test_output_is_truncated_keeping_head_and_tail(sb: Sandbox):
    res = sb.run([PY, "-c", "print('A'*100 + 'B'*5000 + 'C'*100)"])
    assert res.truncated
    assert res.stdout.startswith("A")
    assert res.stdout.rstrip().endswith("C")
    assert "已省略" in res.stdout


def test_clip_marks_omitted_length():
    text, truncated = _clip("x" * 100, 30)
    assert truncated
    assert "70" in text            # 省略了多少字符要写清楚


def test_stdin_is_closed_so_input_does_not_hang(sb: Sandbox):
    """不接 stdin：否则 input() 会永久挂住，表现为"卡死"。"""
    res = sb.run([PY, "-c", "import sys; data=sys.stdin.read(); print('got', len(data))"],
                 timeout=10)
    assert res.ok
    assert "got 0" in res.stdout


# ============================================================
# 回滚
# ============================================================

def test_rollback_restores_overwritten_file(sb: Sandbox):
    sb.write("app.py", "v1")
    sb.discard()
    sb.begin()
    sb.write("app.py", "v2")
    sb.write("new.py", "brand new")
    restored = sb.rollback()

    assert sb.read("app.py") == "v1"
    assert not sb.exists("new.py")
    assert any(r.startswith("~app.py") for r in restored)
    assert any(r.startswith("-new.py") for r in restored)


def test_discard_commits_changes(sb: Sandbox):
    sb.begin()
    sb.write("keep.py", "值")
    sb.discard()
    assert sb.read("keep.py") == "值"


def test_snapshot_and_restore(sb: Sandbox):
    sb.write("v1.txt", "one")
    name = sb.snapshot("base")
    sb.write("v1.txt", "two")
    sb.write("v2.txt", "extra")
    sb.restore(name)

    assert sb.read("v1.txt") == "one"
    assert not sb.exists("v2.txt")
    assert sb.list_snapshots() == [name]


# ============================================================
# 覆盖率解析
# ============================================================

def test_pytest_coverage_parsing(tmp_path: Path):
    sb = Sandbox("cov-test", root=tmp_path, timeout=180)
    sb.write("calc.py", "def add(a, b):\n    return a + b\n")
    sb.write("test_calc.py", "from calc import add\n\ndef test_add():\n    assert add(1, 2) == 3\n")

    res, cov = sb.run_pytest("--cov=calc", coverage=True, timeout=180)
    assert res.ok, res.tail(600)
    assert cov is not None, f"没能从输出里解析出覆盖率：\n{res.tail(600)}"
    assert 0 <= cov <= 100
