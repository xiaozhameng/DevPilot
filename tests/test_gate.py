"""覆盖率门禁的三种判定。

这个节点只有几十行，但它决定"收不收"。它最容易出的错不是判错大小，
而是把**三种不同的事**压成两种：

  1. 跑过了 + 覆盖率达标        → 通过
  2. 跑过了 + 覆盖率不达标      → 打回（可以补测试解决）
  3. 测试根本没跑起来           → 这不是"覆盖率 0%"，是"验证失败"
                                    （补测试解决不了，得先让它能跑）

把第 3 种当成第 1 种（"没解析到覆盖率就当通过"）是这类系统里最危险的漏洞：
门禁永远放行、机制形同虚设，而且没有任何报错。

另外，打回次数用尽后要**降级接受并说明原因**。只给一个 `gate_degraded=True`
是不够的 —— 使用者要知道拿到的是"覆盖率差一点但能用"还是"测试根本没过"。

真实案例（2026-10-13 实测）：一次 CSV 任务的降级原因其实是"测试没跑起来"，
而当时的界面统一显示成"覆盖率门禁未达标"，把问题说轻了。
"""

from __future__ import annotations

from devpilot.agents.gate import gate, gate_router


class _Settings:
    def __init__(self, target: int = 80, max_codegen: int = 3) -> None:
        self.coverage_target = target
        self.max_codegen = max_codegen


class _Ctx:
    """只实现 gate 会碰到的那几个方法。"""

    def __init__(self, **state) -> None:
        self.settings = state.pop("settings", _Settings())
        self.state = dict(state)
        self.logs: list[tuple[str, str]] = []

    def get(self, key, default=None):
        return self.state.get(key, default)

    def log(self, message, level="info"):
        self.logs.append((level, message))


# ============================================================
# 情况 1：达标 → 通过，且不算降级
# ============================================================

def test_pass_when_coverage_meets_target():
    ctx = _Ctx(coverage=95.0, test_passed=True, codegen_round=1)
    r = gate(ctx)
    assert r.patch["gate_passed"] is True
    assert r.patch["gate_degraded"] is False
    # router 读的是**合并后的共享状态**，不是节点返回值 ——
    # 真实执行器也是先 merge(patch) 再算路由，这里照做才不会测出假象。
    ctx.state.update(r.patch)
    assert gate_router(ctx) is None


# ============================================================
# 情况 2：不达标 → 打回，并把"哪没覆盖"带回去
# ============================================================

def test_reject_when_coverage_below_target():
    ctx = _Ctx(coverage=40.0, test_passed=True, codegen_round=1,
               test_output="... TOTAL 40%")
    r = gate(ctx)
    assert r.patch["gate_passed"] is False
    assert "40" in r.patch["gate_feedback"]
    ctx.state.update(r.patch)
    assert gate_router(ctx) == "code"


# ============================================================
# 情况 3：测试没跑起来 → 也是打回，但反馈要指向"先让它能跑"
# ============================================================

def test_failed_run_is_not_a_pass_even_without_coverage():
    ctx = _Ctx(coverage=None, test_passed=False, codegen_round=1,
               test_output="SyntaxError: invalid syntax")
    r = gate(ctx)
    assert r.patch["gate_passed"] is False, "测试没跑起来绝不能被当成通过"
    assert "SyntaxError" in r.patch["gate_feedback"]


def test_failed_run_feedback_points_at_making_it_run():
    ctx = _Ctx(coverage=None, test_passed=False, codegen_round=2, test_output="boom")
    r = gate(ctx)
    # 反馈必须说"先修到能跑"，而不是"去补覆盖率" —— 后者对这种情况毫无帮助
    assert "修到测试能跑起来" in r.patch["gate_feedback"]


# ============================================================
# 同样是 test_passed=False，靠退出码分成两类
#
# 只看布尔是不够的：rc=2（收集/导入失败）和 rc=1（有断言失败）
# 修法完全相反 —— 前者要让代码能跑，后者要改实现或断言。
# ============================================================

def test_exit_code_2_means_collection_failure():
    ctx = _Ctx(coverage=None, test_passed=False, codegen_round=2,
               test_returncode=2, test_output="ImportError: no module named x")
    r = gate(ctx)
    assert "无法收集" in r.patch["gate_feedback"]
    assert "先修到测试能跑起来" in r.patch["gate_feedback"]


def test_exit_code_1_means_assertion_failure_not_collection():
    ctx = _Ctx(coverage=93.0, test_passed=False, codegen_round=2,
               test_returncode=1, test_output="FAILED tests/test_x.py::test_a")
    r = gate(ctx)
    # 覆盖率能解析出来，恰恰说明测试跑起来了 —— 反馈不该再说"让它能跑"
    assert "存在失败的用例" in r.patch["gate_feedback"]
    assert "先修到测试能跑起来" not in r.patch["gate_feedback"]


def test_timeout_is_reported_as_timeout():
    ctx = _Ctx(coverage=None, test_passed=False, codegen_round=2,
               test_returncode=124, test_timeout=True, test_output="")
    r = gate(ctx)
    assert "超时" in r.patch["gate_feedback"]


def test_degraded_reason_carries_exit_code_diagnosis():
    ctx = _Ctx(settings=_Settings(max_codegen=2), coverage=93.0, test_passed=False,
               codegen_round=3, test_returncode=1, test_output="FAILED ...")
    r = gate(ctx)
    assert r.patch["gate_degraded"] is True
    assert "存在失败的用例" in r.patch["gate_reason"]


def test_degraded_reason_has_no_duplicated_wording():
    # 退出码未知时不该出现"测试未通过（测试未通过）"这种读起来像 bug 的字符串
    ctx = _Ctx(settings=_Settings(max_codegen=2), coverage=None, test_passed=False,
               codegen_round=3, test_output="")
    r = gate(ctx)
    assert r.patch["gate_reason"] == "测试未通过，已降级接受"
    assert "测试未通过（测试未通过）" not in r.patch["gate_reason"]


def test_assertion_failure_still_does_not_pass_the_gate():
    # 覆盖率再高，只要测试没过就不能算通过 —— 这是门禁存在的意义
    ctx = _Ctx(coverage=99.0, test_passed=False, codegen_round=1, test_returncode=1,
               test_output="1 failed")
    r = gate(ctx)
    assert r.patch["gate_passed"] is False


# ============================================================
# 刹车用尽 → 降级接受，且必须说明原因
# ============================================================

def test_exhausted_brake_accepts_with_reason_for_low_coverage():
    # max_codegen=2 表示"1 次初稿 + 2 次打回"，所以第 3 轮才是刹车用尽。
    ctx = _Ctx(settings=_Settings(max_codegen=2),
               coverage=55.0, test_passed=True, codegen_round=3, test_output="")
    r = gate(ctx)
    assert r.patch["gate_passed"] is True
    assert r.patch["gate_degraded"] is True
    assert "覆盖率" in r.patch["gate_reason"]
    ctx.state.update(r.patch)
    assert gate_router(ctx) is None          # 达标收工，流水线正常结束


def test_exhausted_brake_accepts_with_reason_for_broken_tests():
    ctx = _Ctx(settings=_Settings(max_codegen=2),
               coverage=None, test_passed=False, codegen_round=3,
               test_output="ModuleNotFoundError")
    r = gate(ctx)
    assert r.patch["gate_passed"] is True
    assert r.patch["gate_degraded"] is True
    # 关键：原因必须说"测试没过"，不能笼统说成"覆盖率未达标"
    assert "测试未通过" in r.patch["gate_reason"]


def test_exhausted_brake_reason_distinguishes_unparsed_coverage():
    ctx = _Ctx(settings=_Settings(max_codegen=2),
               coverage=None, test_passed=True, codegen_round=3, test_output="ok")
    r = gate(ctx)
    assert r.patch["gate_degraded"] is True
    assert "覆盖率" in r.patch["gate_reason"]
    assert "测试未通过" not in r.patch["gate_reason"]


def test_degraded_flag_always_carries_a_reason():
    """凡是要置降级标记的路径，都必须同时给得出原因。"""
    settings = _Settings(max_codegen=2)
    cases = [
        dict(coverage=10.0, test_passed=True, test_output=""),
        dict(coverage=None, test_passed=True, test_output=""),
        dict(coverage=None, test_passed=False, test_output=""),
    ]
    for case in cases:
        r = gate(_Ctx(settings=settings, codegen_round=3, **case))
        assert r.patch["gate_degraded"] is True, case
        assert r.patch.get("gate_reason"), case
