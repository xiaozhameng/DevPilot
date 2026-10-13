"""⑤ 编码 + 单测节点：按方案生成代码，写进沙箱，然后**真的跑测试**。

## 这个节点会被执行多次（这是设计，不是 bug）

覆盖率不达标时，gate 节点会把控制权打回这里（条件边成环）。
所以这个节点必须满足两个要求：

1. **幂等** —— 重跑时覆盖同名文件，不留下上一版的一部分。
   混版本是最难查的 bug：一半是旧接口、一半是新实现，报错在第三个文件里。
2. **感知上一轮为什么失败** —— 收到 `gate_feedback`（具体哪个文件、哪几行没覆盖），
   否则模型会原样再生成一遍几乎相同的代码，然后再次不达标。
   循环两轮的成本不低（每轮 N 次模型调用），没有反馈的循环纯粹是浪费。

## 为什么"跑测试"这件事一定要真跑

模型对"我的代码能跑"的自信程度和事实无关。它写的代码可能：
少一个 import、函数名对不上、把字符串当列表用、缩进错一格。
这些都是**读不出来的错**——静态看 200 行代码，人眼也未必发现漏了一个 `import`。

跑一次 pytest 只要几秒，能一次性证明"这个模块能导入、这个断言成立"。
不跑，就等于把验证工作推给用户，而用户打开项目时看到的是 ImportError。

## conftest.py 为什么必须写

沙箱的子进程里 `PYTHONPATH` 指向工作区根目录，但 pytest 在导入测试模块时
用的是自己的 sys.path 逻辑。写一个空 conftest.py 显式把项目根插进 sys.path，
能让 `from app.main import ...` 这种导入**必然成立** ——
否则会随机出现"某台机器上跑得通、另一台上 ModuleNotFoundError"。
"""

from __future__ import annotations

import re
from pathlib import Path

from .. import events
from ..graph import NodeResult
from ..llm import LLMError, OutputTruncated
from .base import as_list, failed, ok, text_of

# 从模型输出里剥掉 Markdown 围栏。
# 即使提示词说了"不要围栏"，模型仍会时不时加上 —— 这是最高频的"轻微违约"。
# 处理原则是**就地修正**：语义零歧义的事不值得重试一次模型调用。
_FENCE = re.compile(r"^\s*```[A-Za-z0-9_+\-.]*\s*\n(.*?)\n?\s*```\s*$", re.S)

# 模型偶尔会把某个"伪标签"吐进正文：整段输出末尾凭空多一行 `</output>`。
# 这不是模型"没听提示词"，而是它的输出协议里带的闭合标记泄漏了。
# 危害被严重低估：这一行会原样写进 .py，然后在 pytest **收集阶段** 炸成
# SyntaxError（报错行就是被污染的那一行），错误信息指不出真正原因；
# 更坏的是门禁会把它当成"测试不通过"，于是编码节点反复重生成、
# 反复撞同一个坑，最后三轮打回用尽、接受一个根本跑不起来的交付，
# 而此时流水线仍显示 succeeded —— 属于最隐蔽的失败。
# 修法与围栏一致：语义零歧义 → 就地剥掉，不值得重试一次模型调用。
_WRAPPER_LINE = re.compile(
    r"^[ \t]*</?(?:output|code|file|answer|result|artifact|thinking|response)>[ \t]*$\n?",
    re.I | re.M,
)


def _strip_fence(text: str) -> str:
    # 先清包裹标签行，再剥围栏。
    # 顺序有讲究：`<output>```py ... ```</output>` 这种叠加形态，
    # 若先剥围栏会因标签挂在末尾而匹配不上，先清标签才能露出完整围栏。
    t = (text or "").strip()
    t = _WRAPPER_LINE.sub("", t)
    m = _FENCE.match(t)
    if m:
        t = m.group(1)
    return t.strip()


FILE_SYSTEM = """你是一个高级工程师，正在按既定方案实现**一个**文件。

铁律：
- 只输出这个文件的完整内容，不要任何解释、不要 Markdown 围栏、不要"以下是代码"。
- 代码必须能被直接写入并运行，不能有伪代码、不能有 TODO。
- 只用标准库，不引入第三方依赖。
- 如果收到"上一轮失败反馈"，必须针对性地修复，不要重写无关部分。"""

TEST_SYSTEM = """你是一个测试工程师，正在为一个模块写单元测试。

铁律：
- 只用 pytest（import pytest / 普通 assert），不要 unittest。
- 测试必须**能通过**，也必须**真的断言行为**，不要写 assert True 这种凑数的。
- 覆盖正常路径 + 至少两个边界（空输入、非法输入、极端值）。
- 只输出测试文件的完整内容，不要解释、不要围栏。"""


def _write_conftest(ctx) -> None:
    ctx.sandbox.write(
        "conftest.py",
        "# 让项目根进入 sys.path，保证被测模块一定可导入。\n"
        "# 不依赖 pytest 的 rootdir 推断 —— 那个在不同目录结构下表现不一致。\n"
        "import pathlib\n"
        "import sys\n\n"
        "sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))\n",
    )


def _is_test_module(path: str) -> bool:
    """这个路径看起来是"放测试的地方"吗。"""
    name = Path(path).name
    return name.startswith("test_") or name.endswith("_test.py")


def _pick_test_file(files: list[dict]) -> str:
    """从方案的文件表里挑一个"测试该写进哪个文件"。

    不能简单取 `tests/` 下的第一个 —— 实测踩过这个坑：
    方案里第一个是 `tests/__init__.py`，于是**测试的提示词把内容写进了 `__init__.py`**，
    而真正的 `tests/test_xxx.py` 走了"实现文件"的提示词（它不是测试工程师提示词，
    要求是"实现一个文件"，不是"写断言"）。两边内容互相打架，
    表现是覆盖率莫名其妙上不去、`__init__.py` 里出现一堆 assert。

    所以按可靠性排序：先找 `test_*.py` / `*_test.py`；
    找不到就退到"非 __init__ 的第一个"；再找不到才用默认名。
    """
    candidates = [text_of(f["path"]) for f in files
                  if isinstance(f, dict) and text_of(f.get("path")).startswith(("tests/", "test/"))]
    for path in candidates:
        if _is_test_module(path):
            return path
    for path in candidates:
        if Path(path).name != "__init__.py":
            return path
    return "tests/test_main.py"


def _is_trivial_marker(path: str) -> bool:
    """测试包里的 `__init__.py` 是"标记文件"，不该花一次模型调用去生成。

    实测代价很具体：为了一个空的 `tests/__init__.py`，模型循环输出了
    16000 tokens 才被截断 —— 一次调用白烧掉，然后降级成占位实现。

    语义零歧义（它就是"这个目录是个包"），直接写空文件即可。
    原则和"剥围栏 / 清标签"完全相同：**能确定的事不要问模型**。

    只对 `tests/` 下的 `__init__.py` 生效 —— 源码包里的 `__init__.py`
    常常要写 `__version__`、re-export 之类，那是有内容的，仍该交给模型。
    """
    p = Path(path)
    return p.name == "__init__.py" and p.parent.name in ("tests", "test")


_MARKER_BODY = '# 标记文件：让 tests/ 成为一个包。\n# 内容由代码节点直接写死 —— 这里没有任何需要模型判断的东西。\n'


def _fallback_module(path: str, purpose: str) -> str:
    name = Path(path).stem.replace("-", "_")
    return (
        f'"""{purpose}\n\n（模型不可用，这是结构完整的占位实现。）"""\n\n\n'
        f"def {name}() -> str:\n"
        f'    """占位实现：返回一个可被断言的确定值。"""\n'
        f'    return "{name}"\n'
    )


def _gen_file(ctx, *, path: str, purpose: str, design: dict, feedback: str,
              prd: dict) -> str:
    file_map = "\n".join(
        f"- {text_of(f.get('path'))} — {text_of(f.get('purpose'))}"
        for f in as_list(design.get("files")) if isinstance(f, dict)
    )
    ifaces = "\n".join(
        f"- {text_of(i.get('signature'))}：{text_of(i.get('behavior'))}"
        for i in as_list(design.get("interfaces")) if isinstance(i, dict)
    ) or "（未定义）"

    user = (
        f"项目：{text_of(prd.get('title'), '未命名')}\n\n"
        f"需求规格：\n{text_of(ctx.get('spec'))[:1200]}\n\n"
        f"文件树（共 {len(as_list(design.get('files')))} 个）：\n{file_map}\n\n"
        f"接口契约：\n{ifaces}\n\n"
        f"你要实现的是：`{path}`\n"
        f"它的职责：{purpose}\n"
    )
    if feedback:
        user += f"\n上一轮失败反馈（必须修复）：\n{feedback}\n"

    llm = ctx.llm_for("code")
    if not llm.enabled:
        raise LLMError("未配置可用的模型")

    res = llm.complete(
        [{"role": "system", "content": FILE_SYSTEM},
         {"role": "user", "content": user}],
        temperature=0.15, max_tokens=4000,
    )
    # 截断必须显式拦下。这里和第 ① 类结构化输出不同：源码没有"JSON 解析失败"
    # 这种天然哨兵 —— 一个被切断的 .py 文件照样能被写进沙箱，
    # 然后以 SyntaxError 的形式在 pytest 里炸掉，错误信息完全指不出真正原因。
    # 更坏的情况是切断处恰好语法合法：文件被"成功"写进去，静默少了一半实现。
    if res.truncated:
        raise OutputTruncated(
            f"生成 {path} 时输出被截断（已输出 {res.completion_tokens} tokens）。"
            f"通常说明这个文件太大，应当缩小单文件职责或拆分文件。",
            completion_tokens=res.completion_tokens,
        )
    return _strip_fence(res.text)


def _gen_tests(ctx, *, sources: list[str], test_file: str, design: dict,
               feedback: str) -> str:
    plan = "\n".join(f"- {text_of(p)}" for p in as_list(design.get("test_plan"))) or "（无）"
    # 导入提示必须给出**真实的模块名**，而不是一句抽象的"用绝对导入"。
    # 实测第 1 轮最常挂在导入上：模型猜了一个 `from app.main import ...`，
    # 而项目里根本没有 app/ 这个包。把它要导入的模块名直接摆出来，
    # 这类错误就消失了 —— 提示词里少一点需要模型推理的地方，就少一类失败。
    imports = "\n".join(f"  - `from {_module_of(s)} import ...`" for s in sources[:6]) or "  （无源文件）"
    user = (
        f"项目：{text_of((ctx.get('prd') or {}).get('title'), '未命名')}\n\n"
        f"需求规格：\n{text_of(ctx.get('spec'))[:1000]}\n\n"
        f"已实现的源文件：{', '.join(sources)}\n\n"
        f"测试计划：\n{plan}\n\n"
        f"请为这些源文件写一份测试，就放在 `{test_file}`。\n"
        f"被测模块的绝对导入写法（**必须**用这些，不要自己猜包名）：\n{imports}\n"
    )
    if feedback:
        user += f"\n上一轮失败反馈（必须修复）：\n{feedback}\n"

    llm = ctx.llm_for("code_tests")
    if not llm.enabled:
        raise LLMError("未配置可用的模型")
    res = llm.complete(
        [{"role": "system", "content": TEST_SYSTEM},
         {"role": "user", "content": user}],
        temperature=0.15, max_tokens=3500,
    )
    if res.truncated:
        raise OutputTruncated(
            f"生成测试时输出被截断（已输出 {res.completion_tokens} tokens）。"
            f"缺少测试就等于门禁永远不达标，会让整个流水线卡在回环里。",
            completion_tokens=res.completion_tokens,
        )
    return _strip_fence(res.text)


def code(ctx) -> NodeResult:
    design = ctx.get("design") or {}
    prd = ctx.get("prd") or {}
    files = [f for f in as_list(design.get("files"))
             if isinstance(f, dict) and text_of(f.get("path"))]
    feedback = text_of(ctx.get("gate_feedback"))
    attempt = ctx.bump("codegen_round")

    ctx.log(f"编码 · 第 {attempt} 轮（{len(files)} 个文件）"
            + ("，带着上一轮反馈" if feedback else ""))

    test_file = _pick_test_file(files)
    # "源文件"的定义是"**除测试文件之外**的所有文件"，而不是"不在 tests/ 下的文件"。
    # 两者在正常情况下等价；不等价时（比如方案把测试放在 test/ 或 src/test_x.py）
    # 用后者会把测试文件当成被测对象，让测试去 import 它自己。
    sources = [text_of(f["path"]) for f in files if text_of(f["path"]) != test_file]

    written: list[str] = []
    degraded = False

    ctx.sandbox.begin()
    try:
        _write_conftest(ctx)
        written.append("conftest.py")

        for f in files:
            path = text_of(f["path"])
            purpose = text_of(f.get("purpose"), "未说明用途")
            if _is_trivial_marker(path):
                # 标记文件不问模型：它没有需要判断的内容，问了只是浪费一次调用。
                ctx.sandbox.write(path, _MARKER_BODY)
                written.append(path)
                ctx.add_artifact(path, "source", "测试包标记文件")
                continue
            if path == test_file:
                # 这个文件马上由"测试工程师"提示词专门生成。
                # 不能在这里也用"实现文件"的提示词生成一遍 —— 两次生成抢同一个文件，
                # 后写的赢，中间那次调用白烧，而且两份内容风格不一，很难解释。
                continue
            try:
                content = _gen_file(ctx, path=path, purpose=purpose, design=design,
                                    feedback=feedback, prd=prd)
            except LLMError as exc:
                ctx.log(f"生成 {path} 降级：{exc}", level="warn")
                content = _fallback_module(path, purpose)
                degraded = True
            ctx.sandbox.write(path, content)
            written.append(path)
            ctx.add_artifact(path, "source", purpose)

        # 测试文件：方案里没规划就补一个 —— 没有测试就没有覆盖率，
        # 门禁会永远不达标，整个流水线卡在回环里。
        try:
            test_code = _gen_tests(ctx, sources=sources, test_file=test_file,
                                   design=design, feedback=feedback)
        except LLMError as exc:
            ctx.log(f"生成测试降级：{exc}", level="warn")
            test_code = _fallback_tests(sources)
            degraded = True
        ctx.sandbox.write(test_file, test_code)
        written.append(test_file)
        ctx.add_artifact(test_file, "test", "单元测试")

        # 真的跑。这一步是整个流水线里唯一能证明"代码可用"的动作。
        run, coverage = ctx.sandbox.run_pytest()
        ctx.sandbox.discard()          # 跑完了，无需再回滚这些写入
    except Exception:
        # 生成过程本身炸了 → 回滚到本轮开始前的状态，不留半成品。
        restored = ctx.sandbox.rollback()
        ctx.log(f"编码过程异常，已回滚 {len(restored)} 个文件", level="warn")
        raise

    ctx.emit(events.SANDBOX_RUN, node="code", attempt=attempt,
             ok=run.ok, returncode=run.returncode, ms=run.ms,
             timed_out=run.timed_out, truncated=run.truncated)

    test_output = run.tail(4000)
    passed = run.ok
    summary = (f"第 {attempt} 轮：生成 {len(sources)} 个源文件 + 测试，"
               f"pytest {'通过' if passed else '未通过'}"
               + (f"，覆盖率 {coverage:.0f}%" if coverage is not None else "，覆盖率未解析到"))

    ctx.log(summary, level="info" if passed else "warn")
    if not passed:
        ctx.log(f"pytest 输出（末尾）：\n{test_output[-1200:]}", level="warn")

    return ok("code", summary,
              code_files=written,
              sources=sources,
              test_file=test_file,
              test_passed=passed,
              # 退出码必须带出去，因为 `test_passed=False` 这一点信息**不足以定位原因**：
              # pytest 收集失败（rc=2，代码压根 import 不进来）和断言失败（rc=1，代码能跑但行为不对）
              # 是两类完全不同的问题，修法也不同。门禁只看布尔的话，
              # 给模型的反馈会说错方向 —— 实测就出现过"覆盖率 93% 却报测试没通过"。
              test_returncode=run.returncode,
              test_timeout=run.timed_out,
              test_output=test_output,
              coverage=coverage,
              code_degraded=degraded)


def _fallback_tests(sources: list[str]) -> str:
    body = ["import importlib", "", "import pytest", ""]
    for src in sources:
        mod = Path(src).stem.replace("-", "_")
        body += [
            "",
            f"def test_{mod}_importable():",
            f'    assert importlib.import_module("{_module_of(src)}") is not None',
            "",
            f"def test_{mod}_placeholder():",
            "    assert True",
        ]
    return "\n".join(body) + "\n"


def _module_of(path: str) -> str:
    """把 'app/main.py' 变成 'app.main'。"""
    p = path[:-3] if path.endswith(".py") else path
    return p.replace("/", ".").lstrip(".")
