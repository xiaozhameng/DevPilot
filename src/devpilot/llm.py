"""LLM 客户端：统一出口 + 分类重试 + token 记账。

（这一层与 CitePilot 同源 —— 踩过的坑是一样的，所以在两个仓库里都保留了同样的处理。）

为什么必须包一层？裸调模型 API 会遇到三类问题，处理方式完全不同：
  1) 可重试的（超时 / 429 / 5xx）—— 退避重试
  2) 不可重试的（400 参数错 / 401 鉴权错）—— 立即失败，重试只是浪费时间
  3) 需要降级的（配额耗尽 / 模型下线）—— 换模型或降级到模板产出
把它们混在一个 except 里是最常见的坑：对 400 重试三次，白白多等 10 秒。

DevPilot 相比 CitePilot 多了一件事：**按节点记账**。
因为多 Agent 的成本要能拆到"哪个 Agent 花了多少"，
否则被问到"token 成本怎么控"时只能说个总数，没法定位到成本黑洞。
"""

from __future__ import annotations

import json
import re
import threading
import time
from dataclasses import dataclass

from .config import SETTINGS


class LLMError(RuntimeError):
    pass


class OutputTruncated(LLMError):
    """模型输出被 max_tokens 截断，**不是**它没按格式答。

    为什么值得单独一个异常类型：这两种情况的处置方式完全相反。
    格式不对 → 把具体错误回灌给它，重试有意义；
    被截断 → 重试多少次都是同样的长度上限，只会重复截断、白花钱，
    正确的动作是**放宽预算**或**缩小请求**。

    把它们混在一个 LLMError 里，就会出现"重试三次都失败"然后报
    「模型未返回合法 JSON」—— 把排查方向指向模型，而真正的原因是我们没给它空间。
    这是本次真实踩到的坑：design 节点默认 2000 tokens，12 个文件的方案必然截断，
    于是它连续 3 次"不合规"，最后悄悄降级成最小骨架，
    整条流水线依然"成功"，只是交付物质量无声地掉了下去。
    """

    def __init__(self, message: str, *, completion_tokens: int = 0, budget: int = 0) -> None:
        super().__init__(message)
        self.completion_tokens = completion_tokens
        self.budget = budget


@dataclass
class LLMResult:
    text: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    ms: int = 0
    model: str = ""
    finish_reason: str = ""

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens

    @property
    def truncated(self) -> bool:
        """输出是否因为长度上限被切断。

        注意判据是 finish_reason，而**不是**"内容为空"。
        内容为空只是截断的一种极端表现（推理把预算全吃掉时）；
        更常见的是"内容非空但只写了一半" —— 那才是最难发现的：
        它看起来像一份正常的回答，直到下游解析 JSON 时才炸。
        """
        return self.finish_reason == "length"


# 单次调用的输出预算上限（token）。
# 到这个数还是被截断，就不是"预算不够"的问题了，而是请求本身太大
# （比如让它一次产出几十个文件的完整实现）—— 该做的是拆分任务，不是继续加钱。
MAX_OUTPUT_BUDGET = 16000


# 只重试这些：超时、限流、上游故障。参数错误重试一万次也不会成功。
_RETRYABLE = ("timeout", "timed out", "rate limit", "429", "500", "502", "503", "504",
              "overloaded", "connection", "temporarily")


def _retryable(msg: str) -> bool:
    m = msg.lower()
    return any(k in m for k in _RETRYABLE)


class UsageLedger:
    """按节点维度的 token 账本。

    为什么要按节点分？因为"这次任务花了多少钱"不是一个有用的问题，
    "钱花在哪个节点"才是。实测里编码节点的 token 通常是澄清节点的 5~10 倍，
    只报总数会让优化完全失去方向。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.by_node: dict[str, dict[str, int]] = {}

    def add(self, node: str, res: LLMResult) -> None:
        with self._lock:
            slot = self.by_node.setdefault(node, {"calls": 0, "in": 0, "out": 0, "ms": 0})
            slot["calls"] += 1
            slot["in"] += res.prompt_tokens
            slot["out"] += res.completion_tokens
            slot["ms"] += res.ms

    def snapshot(self) -> dict:
        with self._lock:
            total = {"calls": 0, "in": 0, "out": 0, "ms": 0}
            for slot in self.by_node.values():
                for k in total:
                    total[k] += slot[k]
            return {"total": total, "by_node": dict(self.by_node)}

    def reset(self) -> None:
        with self._lock:
            self.by_node.clear()


class LLM:
    def __init__(self, model: str | None = None, api_key: str | None = None,
                 api_base: str | None = None, timeout: float = 90.0,
                 max_retries: int = 3, node: str = "unknown") -> None:
        self.model = model if model is not None else SETTINGS.model
        # 用 `is None` 而不是 `or`：空字符串是"明确的空"，
        # 表示调用方**故意**要一个不带 key 的客户端（测试里就是这么用的）。
        # 写成 `api_key or SETTINGS.api_key` 会让 "" 被当成"没传"而回退到全局配置，
        # 于是"模拟一个没有模型的环境"这件事永远做不到 —— 见 enabled 的说明。
        self.api_key = api_key if api_key is not None else SETTINGS.api_key
        self.api_base = api_base if api_base is not None else SETTINGS.api_base
        self.timeout = timeout
        self.max_retries = max_retries
        self.node = node          # 调用方（哪个 Agent）的名字，用于记账

    @property
    def enabled(self) -> bool:
        """这个客户端能不能用 —— **只取决于它自己持有的 key**。

        这里原本写的是「如果 api_key 和全局 SETTINGS 一样，就返回 SETTINGS.has_llm」。
        后果非常隐蔽：

          测试里构造了一个 api_key="" 的 Settings，想验证"模型不可用时全链路降级"。
          但 LLM() 的 key 回退到了全局 SETTINGS 的真 key，于是 enabled 为 True，
          **测试自以为在跑降级分支，实际上每次都真的调了模型** ——
          慢（每轮多等十几秒）、花钱、而且那条降级分支从未被覆盖过。

        一个对象的行为依赖全局状态，就会出这类问题：
        你改的是它，生效的却是别处。
        """
        key = (self.api_key or "").strip()
        return bool(key) and not key.startswith("sk-xxx")

    def for_node(self, node: str) -> "LLM":
        """派生出属于某个 Agent 的客户端 —— 共享连接参数，只换记账标签。"""
        return LLM(model=self.model, api_key=self.api_key, api_base=self.api_base,
                   timeout=self.timeout, max_retries=self.max_retries, node=node)

    def complete(self, messages: list[dict], temperature: float = 0.2,
                 max_tokens: int | None = None, json_mode: bool = False,
                 timeout: float | None = None, retries: int | None = None) -> LLMResult:
        """调一次模型。

        两个很隐蔽的坑（两个仓库都踩过，值得留在这里）：

        **坑一：推理模型的 reasoning token 也计入 max_tokens。**
        deepseek-v4-flash 先在 reasoning_content 里想一大段，想完才输出 content。
        预算给小了就会得到 content=''、finish_reason='length' ——
        不抛异常、不报错，静默返回空字符串。

        **坑二：内容非空的截断更危险。**
        上面那种"空答案"至少还能一眼看出来。真正难查的是
        "写了一半就断了"：它看起来像一份正常回答，直到下游解析 JSON 时
        才以"格式不对"炸掉 —— 而那时错误信息里已经看不出是预算问题。
        design 节点默认 2000 tokens、要产出十几个文件的方案，就正好落在这个坑里。

        所以这里的判据是 **finish_reason == 'length'**，与内容是否为空无关；
        一旦确认被截断就**放大预算重来**，而不是把残缺结果当成成功返回。
        """
        import litellm

        ledger = LEDGER
        budget = max_tokens or 1200
        tmo = timeout or self.timeout
        tries = self.max_retries if retries is None else retries

        last_truncated: LLMResult | None = None
        last_budget_used = 0
        rounds = 0

        # 循环的终止条件只有一个：**已经用顶格预算试过一次**仍然被截断。
        # 注意这里不能写成"下一轮预算到顶了就退出" —— 那会导致
        # 算出 16000 却从没用过它（实测踩到：calls=[2000, 8000] 就结束了）。
        # 判断必须针对"本轮实际用掉的预算"，而不是"准备用的预算"。
        while rounds < 8:
            rounds += 1
            last_budget_used = budget

            for attempt in range(tries):
                t0 = time.perf_counter()
                try:
                    resp = litellm.completion(
                        model=self.model,
                        messages=messages,
                        temperature=temperature,
                        api_key=self.api_key,
                        api_base=self.api_base,
                        timeout=tmo,
                        max_tokens=budget,
                        # 必须显式关掉 litellm 的内置重试！它默认 num_retries=2（共 3 次），
                        # 会把 timeout 放大三倍：设 timeout=5s 实测跑了 17.1s。
                        # 重试策略必须只有一处 —— 归我们自己管，否则两层相乘延迟失控。
                        num_retries=0,
                        **({"response_format": {"type": "json_object"}} if json_mode else {}),
                    )
                    ms = int((time.perf_counter() - t0) * 1000)
                    usage = getattr(resp, "usage", None)
                    choice = resp.choices[0]
                    out = LLMResult(
                        text=(choice.message.content or "").strip(),
                        prompt_tokens=getattr(usage, "prompt_tokens", 0) or 0,
                        completion_tokens=getattr(usage, "completion_tokens", 0) or 0,
                        ms=ms,
                        model=self.model,
                        finish_reason=getattr(choice, "finish_reason", "") or "",
                    )
                    # 不管这一轮是成功还是失败，钱都已经花了，照记 ——
                    # 否则"因为输出被截断而重试"的成本会从账本里消失，
                    # 事后看到的总花费会比实际低，优化方向就错了。
                    ledger.add(self.node, out)

                    if not out.truncated:
                        return out

                    last_truncated = out
                    budget = min(budget * 4, MAX_OUTPUT_BUDGET)
                    break              # 跳出错误重试，进入下一轮（更大的预算）
                except Exception as exc:  # noqa: BLE001
                    msg = f"{type(exc).__name__}: {exc}"
                    if not _retryable(msg) or attempt == tries - 1:
                        raise LLMError(f"模型调用失败（{self.model}）：{msg}") from exc
                    time.sleep(1.2 * (2 ** attempt))

            if last_budget_used >= MAX_OUTPUT_BUDGET:
                break

        if last_truncated is not None:
            # 顶格预算还是被截断：把残缺结果交出去，但**明确标注**它被截断过。
            # 上层据此给出准确的错误信息，而不是含混地说"格式不对"。
            return last_truncated

        raise LLMError(f"模型调用失败（{self.model}）：{tries} 次尝试后仍未产出内容")

    def complete_json(self, messages: list[dict], **kw) -> tuple[dict, LLMResult]:
        """要求模型返回 JSON，并做容错。

        模型有时把 JSON 包在 ```json 围栏里，有时前后加一句解释。
        直接 json.loads 会炸 —— 先用正则抠出最外层花括号内容再解析。
        这就是"参数修正"在生产里最常见的形态：不指望模型 100% 守约，
        而是在它轻微违约时把它拉回来。

        但**截断**是修不回来的：残缺的 JSON 无论怎么抠都括不起来。
        所以先判截断，再走容错 —— 顺序反了的话，用户看到的永远是
        "未返回合法 JSON"，而真正的原因（预算不够）被永久掩盖。
        """
        res = self.complete(messages, json_mode=True, **kw)
        raw = res.text.strip()
        if raw:
            try:
                return json.loads(raw), res
            except json.JSONDecodeError:
                m = re.search(r"\{.*\}", raw, re.S)
                if m:
                    try:
                        return json.loads(m.group(0)), res
                    except json.JSONDecodeError:
                        pass

        if res.truncated:
            raise OutputTruncated(
                f"模型输出被 max_tokens 截断（已输出 {res.completion_tokens} tokens，"
                f"内容长度 {len(raw)} 字符）。这不是格式问题 —— "
                f"同样的预算重试只会得到同样的截断，需要放宽预算或缩小请求。"
                f"片段：{raw[:160]}",
                completion_tokens=res.completion_tokens,
            )
        if not raw:
            raise LLMError("模型返回了空内容（可能是推理预算耗尽或上游异常）")
        raise LLMError(f"模型未返回合法 JSON：{raw[:200]}")


# 进程级账本，跨节点共享。
LEDGER = UsageLedger()
