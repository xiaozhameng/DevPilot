"""LLM 层的输出截断处理。

这一节测的核心是**一类不会报错的错误**：模型输出被 max_tokens 截断。
它不会抛异常、不会返回错误码，只会把一份"看起来正常、实际残缺"的内容交出来。
等到下游解析 JSON 或编译源码时才发现问题，而那时错误信息已经完全
指不出真正的原因了。

真实案例（2026-10-13 实测）：
  design 节点默认 2000 tokens，而它的产出是一份十几个文件的技术方案，
  必然截断。于是它连续三次"输出不合规"，最后静默降级成最小骨架 ——
  整条流水线**报告成功**，交付物质量掉了，日志里没有一处说"这里出过问题"。
"""

from __future__ import annotations

import json
import sys
import types

import pytest

from devpilot.llm import LLM, LLMError, LLMResult, MAX_OUTPUT_BUDGET, OutputTruncated


# ============================================================
# 假的 litellm：可控地返回"被截断"或"完整"的响应
# ============================================================

class _Msg:
    def __init__(self, content):
        self.content = content


class _Choice:
    def __init__(self, content, finish_reason):
        self.message = _Msg(content)
        self.finish_reason = finish_reason


class _Usage:
    def __init__(self, p, c):
        self.prompt_tokens = p
        self.completion_tokens = c


class _Resp:
    def __init__(self, content, finish_reason):
        self.choices = [_Choice(content, finish_reason)]
        self.usage = _Usage(10, 2000)


@pytest.fixture
def fake_litellm(monkeypatch):
    """替换掉 litellm.completion，并记录每次调用传入的 max_tokens。"""
    calls: list[int] = []
    scripted: list[tuple[str, str]] = []      # (content, finish_reason) 队列

    def completion(**kw):
        calls.append(kw["max_tokens"])
        if scripted:
            content, finish = scripted.pop(0)
        else:
            content, finish = ('{"ok": true}', "stop")
        return _Resp(content, finish)

    module = types.ModuleType("litellm")
    module.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", module)
    return calls, scripted


def make_llm() -> LLM:
    return LLM(model="test/model", api_key="sk-test", api_base="http://x",
               timeout=5, max_retries=1, node="design")


# ============================================================
# LLMResult 的判据
# ============================================================

def test_truncated_is_decided_by_finish_reason_not_by_empty_content():
    """判据必须是 finish_reason，不能是"内容为空"。

    只判"内容为空"会漏掉最难发现的一类：**写了一半就断**。
    它看起来像一份正常回答，直到下游解析时才炸。
    """
    empty = LLMResult(text="", finish_reason="length")
    half = LLMResult(text='{"files": [{"path": "a.py"', finish_reason="length")
    full = LLMResult(text='{"files": []}', finish_reason="stop")

    assert empty.truncated is True
    assert half.truncated is True, "内容非空但被切断，同样必须判定为截断"
    assert full.truncated is False


# ============================================================
# 自动放大预算
# ============================================================

def test_truncated_output_triggers_bigger_budget(fake_litellm):
    calls, scripted = fake_litellm
    scripted.append(('{"files": [', "length"))        # 第一次被截断
    scripted.append(('{"files": []}', "stop"))        # 放大预算后完整了

    res = make_llm().complete([{"role": "user", "content": "x"}], max_tokens=2000)

    assert res.text == '{"files": []}'
    assert res.truncated is False
    assert len(calls) == 2, "应当重试一次"
    assert calls[0] == 2000
    assert calls[1] > calls[0], "第二次必须用更大的预算，否则只会同样被截断"


def test_keeps_escalating_until_capped(fake_litellm):
    """一直截断就一直放大，直到顶格 —— 那时再试也没有意义。"""
    calls, scripted = fake_litellm
    for _ in range(5):
        scripted.append(('{"a":', "length"))

    res = make_llm().complete([{"role": "user", "content": "x"}], max_tokens=2000)

    assert res.truncated is True, "顶格预算仍被截断时，如实交还并标注"
    assert max(calls) == MAX_OUTPUT_BUDGET
    assert len(calls) <= 3, "不设次数上限就成了无限烧钱"


def test_non_truncated_output_returns_immediately(fake_litellm):
    calls, _ = fake_litellm
    res = make_llm().complete([{"role": "user", "content": "x"}], max_tokens=2000)
    assert res.truncated is False
    assert len(calls) == 1, "正常输出不该触发任何重试"


# ============================================================
# 错误信息必须指向真正的原因
# ============================================================

def test_complete_json_reports_truncation_not_bad_format(fake_litellm):
    """被截断时报的必须是"截断"，而不是"JSON 不合法"。

    这两个错误信息会把排查带向完全相反的方向：前者去调 max_tokens / 拆小任务，
    后者去怀疑模型不守格式约定（甚至去改提示词）。
    """
    _, scripted = fake_litellm
    for _ in range(5):
        scripted.append(('{"files": [{"path": "a.py", "purpose": "半句话', "length"))

    with pytest.raises(OutputTruncated) as ei:
        make_llm().complete_json([{"role": "user", "content": "x"}], max_tokens=2000)

    msg = str(ei.value)
    assert "截断" in msg
    assert "max_tokens" in msg
    assert ei.value.completion_tokens == 2000


def test_complete_json_still_reports_bad_format_for_real_format_errors(fake_litellm):
    """真的格式不对（没被截断）时，仍然报格式问题 —— 不能把两种情况混成一种。"""
    _, scripted = fake_litellm
    scripted.append(("我觉得这个需求可以这样做：先建个文件……", "stop"))

    with pytest.raises(LLMError) as ei:
        make_llm().complete_json([{"role": "user", "content": "x"}], max_tokens=2000)

    assert not isinstance(ei.value, OutputTruncated)
    assert "未返回合法 JSON" in str(ei.value)


def test_complete_json_parses_fenced_json(fake_litellm):
    """轻微违约（包了 Markdown 围栏）就地修正，不重试、不报错。"""
    _, scripted = fake_litellm
    scripted.append(('```json\n{"files": []}\n```', "stop"))

    data, res = make_llm().complete_json([{"role": "user", "content": "x"}], max_tokens=2000)
    assert data == {"files": []}
    assert res.truncated is False


def test_empty_output_is_distinguishable_from_truncation(fake_litellm):
    """空输出与截断是两回事，错误信息也要分开。"""
    _, scripted = fake_litellm
    scripted.append(("", "stop"))

    with pytest.raises(LLMError) as ei:
        make_llm().complete_json([{"role": "user", "content": "x"}], max_tokens=2000)

    assert not isinstance(ei.value, OutputTruncated)
    assert "空内容" in str(ei.value)
