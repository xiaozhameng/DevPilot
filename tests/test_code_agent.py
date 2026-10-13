"""编码节点的输出净化（剥围栏 + 清包裹标签）。

这一节测的是另一类**不会报错的错误**：模型把一段"看着没问题"的文本交出来，
但里面混进了不属于源码的东西。

真实案例（2026-10-13 实测，第四轮 CSV 任务 15dbab535d5a）：
  模型在 `csvsum/__init__.py` 正文末尾凭空多输出一行 `</output>`，
  这一行被原样写进 .py。pytest 在**收集阶段**就 SyntaxError 炸掉，
  报错行指向那一行污染文本，看不出根因。
  门禁把它当成"测试不通过"，于是编码节点反复重生成、反复撞同一个坑，
  三轮打回用尽后接受了一个根本跑不起来的交付，流水线却显示 succeeded。

处理原则与围栏一致：语义零歧义 → 就地剥掉，不值得重试一次模型调用。
"""

from __future__ import annotations

import re
from pathlib import Path

from devpilot.agents.code import (
    _is_trivial_marker,
    _pick_test_file,
    _strip_fence,
    code,
)
from devpilot.config import Settings
from devpilot.runtime import ApprovalGate, RunContext
from devpilot.sandbox import Sandbox
from devpilot.state import Store


# ============================================================
# 基线：正常输出不被改动
# ============================================================

def test_plain_source_is_untouched():
    src = '"""doc"""\n\n\ndef f():\n    return 1\n'
    assert _strip_fence(src).strip() == src.strip()


def test_surrounding_whitespace_is_trimmed():
    assert _strip_fence("\n\n   x = 1\n\n\n") == "x = 1"


# ============================================================
# 围栏：老行为不能退化
# ============================================================

def test_markdown_fence_is_stripped():
    assert _strip_fence("```python\nx = 1\n```") == "x = 1"


def test_language_less_fence_is_stripped():
    assert _strip_fence("```\nx = 1\n```") == "x = 1"


# ============================================================
# 包裹标签：本轮新增的修复点
# ============================================================

def test_trailing_output_tag_is_stripped():
    """实测命中的形态：正文后面跟一行 `</output>`。"""
    dirty = '"""csvsum."""\n\n__version__ = "0.1.0"\n\n__all__ = ["__version__"]\n</output>'
    clean = _strip_fence(dirty)
    assert "</output>" not in clean
    assert clean.endswith('__all__ = ["__version__"]')


def test_leading_output_tag_is_stripped():
    dirty = "<output>\nx = 1\n"
    assert _strip_fence(dirty) == "x = 1"


def test_both_ends_wrapped_are_stripped():
    dirty = "<output>\nx = 1\n</output>"
    assert _strip_fence(dirty) == "x = 1"


def test_fence_plus_trailing_tag_is_stripped():
    """两个问题叠加时也必须一次剥干净（先清标签、再剥围栏）。"""
    dirty = "```python\nx = 1\n```\n</output>"
    assert _strip_fence(dirty) == "x = 1"


def test_wrapped_fence_is_fully_stripped():
    dirty = "<output>\n```python\nx = 1\n```\n</output>"
    assert _strip_fence(dirty) == "x = 1"


def test_other_wrapper_tags_are_stripped():
    for tag in ("code", "file", "answer", "result", "artifact"):
        dirty = f"<{tag}>\nx = 1\n</{tag}>"
        assert _strip_fence(dirty) == "x = 1", tag


def test_wrapper_tag_variants_with_attrs_are_not_guessed():
    """带属性的标签不在处理范围 —— 宁可不动，也不能误伤真实代码。"""
    src = '<output type="py">\nx = 1\n'
    assert "<output" in _strip_fence(src)


# ============================================================
# 保守性：不能误伤"看起来像标签"的真实代码
# ============================================================

def test_indented_tag_like_string_is_stripped_only_if_whole_line():
    """只有"整行仅由标签构成"才剥；行内出现不算。"""
    src = 's = "</output>"\n'
    assert _strip_fence(src) == 's = "</output>"'


def test_char_literal_containing_tag_survives():
    src = 'a = 1  # 别把它当成 </code>\n'
    # 行首不是标签，整行内容必须原样保留（仅首尾空白被裁）。
    assert _strip_fence(src) == src.strip()


def test_none_and_empty_are_safe():
    assert _strip_fence("") == ""
    assert _strip_fence(None) == ""


# ============================================================
# 挑"测试写进哪个文件"
#
# 实测踩过的坑：方案里 tests/ 下第一个是 tests/__init__.py，
# 于是测试的提示词把内容写进了 __init__.py，真正的 test_xxx.py
# 反而走了"实现一个文件"的提示词。两边打架，覆盖率莫名上不去。
# ============================================================

def _f(*paths):
    return [{"path": p, "purpose": "x"} for p in paths]


def test_pick_test_file_skips_dunder_init():
    picked = _pick_test_file(_f("tests/__init__.py", "tests/test_core.py"))
    assert picked == "tests/test_core.py"


def test_pick_test_file_prefers_test_prefix():
    picked = _pick_test_file(_f("tests/helpers.py", "tests/test_api.py"))
    assert picked == "tests/test_api.py"


def test_pick_test_file_accepts_suffix_style():
    picked = _pick_test_file(_f("tests/__init__.py", "tests/core_test.py"))
    assert picked == "tests/core_test.py"


def test_pick_test_file_falls_back_when_only_dunder_init():
    # 一个测试模块都没有时，宁可给一个默认名字，也不要把断言塞进 __init__.py。
    assert _pick_test_file(_f("tests/__init__.py")) == "tests/test_main.py"


def test_pick_test_file_uses_non_init_when_no_test_naming():
    assert _pick_test_file(_f("tests/__init__.py", "tests/checks.py")) == "tests/checks.py"


def test_pick_test_file_default_when_no_tests_dir():
    assert _pick_test_file(_f("src/app.py", "src/core.py")) == "tests/test_main.py"


def test_pick_test_file_ignores_non_dict_entries():
    # 模型偶尔把 files 写成 ["a.py", {...}] 这种混合列表，不能因此崩掉。
    assert _pick_test_file(["a.py", {"path": "tests/test_x.py"}]) == "tests/test_x.py"


# ============================================================
# 标记文件不走模型
# ============================================================

def test_tests_init_is_a_trivial_marker():
    assert _is_trivial_marker("tests/__init__.py") is True
    assert _is_trivial_marker("test/__init__.py") is True


def test_source_init_is_not_trivial():
    # 源码包的 __init__.py 常要写 __version__ / re-export，那是有内容的。
    assert _is_trivial_marker("src/app/__init__.py") is False


def test_regular_module_is_not_trivial():
    assert _is_trivial_marker("tests/test_core.py") is False


# ============================================================
# 集成：编码节点真的把干净内容写进沙箱，而且测试真的能跑过
#
# 上面测的是"函数返回了什么"，这里测的是"落盘之后能不能跑" ——
# 因为真正的失败发生在 pytest 收集阶段，而不在净化函数里。
# 用桩模型确定性复现"模型漏标签"，比再赌一次真实模型行为可靠得多。
# ============================================================

_PKG_INIT = ('"""pkg."""\n\n__version__ = "0.1.0"\n'
             '__all__ = ["__version__"]\n</output>')      # ← 故意漏一个闭合标签
_PKG_CORE = "def add(a, b):\n    return a + b\n"
_MAIN_TESTS = "from pkg.core import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"


class _Res:
    """伪装成 LLMResult：编码节点只读 text / truncated / completion_tokens。"""

    def __init__(self, text: str) -> None:
        self.text = text
        self.truncated = False
        self.completion_tokens = max(1, len(text) // 4)
        self.finish_reason = "stop"


class _StubLLM:
    """按"当前在生成哪个文件"返回内容，不发任何网络请求。"""

    enabled = True

    def __init__(self) -> None:
        self.calls: list[str] = []

    def complete(self, messages, **kwargs):
        system, user = messages[0]["content"], messages[-1]["content"]
        if "测试工程师" in system:
            self.calls.append("<tests>")
            return _Res(_MAIN_TESTS)
        m = re.search(r"你要实现的是：`([^`]+)`", user)
        path = m.group(1) if m else ""
        self.calls.append(path)
        return _Res(_PKG_INIT if path.endswith("__init__.py") else _PKG_CORE)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        api_key="", api_base="", model="",          # 留空，用桩模型绕开真实调用
        max_clarify=1, max_codegen=1, coverage_target=0, approval_timeout=5,
        workspace_dir=tmp_path / "ws", sandbox_timeout=180,
        sandbox_output_limit=8000,
        serve_host="127.0.0.1", serve_port=0,
        internal_token="test-token", gateway_base="",
        root=tmp_path, db_path=tmp_path / "test.db",
    )


def test_code_node_output_is_clean_on_disk_and_runnable(tmp_path):
    st = _settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t-dirty", "做一个加法库")
    ctx = RunContext(task_id="t-dirty", requirement="做一个加法库", store=store,
                     sandbox=Sandbox("t-dirty", root=st.workspace_dir, timeout=180),
                     settings=st, gate=ApprovalGate())
    ctx.llm_for = lambda node: _StubLLM()        # 只在实例上遮蔽，不动全局
    ctx.state["spec"] = "做一个加法库"
    ctx.state["prd"] = {"title": "加法库"}
    ctx.state["design"] = {
        "files": [
            {"path": "pkg/__init__.py", "purpose": "包入口"},
            {"path": "pkg/core.py", "purpose": "核心实现"},
        ],
        "interfaces": [],
        "test_plan": ["add 的正常路径"],
    }

    res = code(ctx)

    # 1) 落盘的源码里不该再有那行标签
    init = ctx.sandbox.read("pkg/__init__.py")
    assert "</output>" not in init
    assert init.rstrip().endswith('__all__ = ["__version__"]')

    # 2) 而且真的能跑起来 —— 这才是"干净"的意义所在
    assert res.patch["test_passed"] is True, res.patch.get("test_output")
    assert res.patch["code_degraded"] is False


def test_marker_file_skipped_and_tests_land_in_the_right_place(tmp_path):
    """方案里带 `tests/__init__.py` 时：标记文件不问模型，测试写进 test_*.py。

    这是实测命中的形态：方案的文件表是 [src/..., tests/__init__.py, tests/test_x.py]。
    修复前会把测试内容写进 `__init__.py`，覆盖率因此上不去。
    """
    st = _settings(tmp_path)
    store = Store(st.db_path)
    store.create_task("t-marker", "做一个加法库")
    ctx = RunContext(task_id="t-marker", requirement="做一个加法库", store=store,
                     sandbox=Sandbox("t-marker", root=st.workspace_dir, timeout=180),
                     settings=st, gate=ApprovalGate())
    stub = _StubLLM()
    ctx.llm_for = lambda node: stub
    ctx.state["spec"] = "做一个加法库"
    ctx.state["prd"] = {"title": "加法库"}
    ctx.state["design"] = {
        "files": [
            {"path": "pkg/__init__.py", "purpose": "包入口"},
            {"path": "pkg/core.py", "purpose": "核心实现"},
            {"path": "tests/__init__.py", "purpose": "测试包标记"},
            {"path": "tests/test_core.py", "purpose": "单元测试"},
        ],
        "interfaces": [],
        "test_plan": ["add 的正常路径"],
    }

    res = code(ctx)

    # 标记文件是直接写死的，不该出现在模型调用记录里
    assert "tests/__init__.py" not in stub.calls, stub.calls
    assert "标记文件" in ctx.sandbox.read("tests/__init__.py")

    # 测试写进了 test_*.py，而不是 __init__.py
    assert res.patch["test_file"] == "tests/test_core.py"
    assert "assert" in ctx.sandbox.read("tests/test_core.py")
    assert "assert" not in ctx.sandbox.read("tests/__init__.py")

    # 测试文件只生成一次（不再被"实现文件"的提示词抢先写一遍）
    assert stub.calls.count("<tests>") == 1
    assert res.patch["test_passed"] is True, res.patch.get("test_output")
