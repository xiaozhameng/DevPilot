#!/usr/bin/env python3
"""端到端演示驱动：提交任务 → 自动放行审批 → 等终态 → 打印判决。

只依赖标准库。刻意走 HTTP 网关（而不是直接调内核），
因为要验证的正是"两个服务串起来"这条链路。
"""
from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request

GW = "http://127.0.0.1:8091"
REQ = sys.argv[1] if len(sys.argv) > 1 else "写一个命令行小工具：读取 CSV 文件并输出每列的统计摘要，带单元测试"
IDEM = sys.argv[2] if len(sys.argv) > 2 else ""


def call(method: str, path: str, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(GW + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # 绕过 http_proxy
    with opener.open(req, timeout=30) as resp:
        return json.loads(resp.read().decode() or "{}")


def main() -> int:
    headers = {"Idempotency-Key": IDEM} if IDEM else {}
    created = call("POST", "/api/tasks", {"requirement": REQ}, headers)
    tid = created["taskId"]
    print(f"提交成功 网关任务 {tid}  deduplicated={created.get('deduplicated')}")
    print(f"内核任务 {created.get('kernelTaskId')}")

    approved = False
    deadline = time.time() + 900
    last_state = None
    while time.time() < deadline:
        t = call("GET", f"/api/tasks/{tid}")
        state = t.get("state")
        if state != last_state:
            print(f"  [{time.strftime('%H:%M:%S')}] state = {state}"
                  f"  attempts={t.get('attempts')}")
            last_state = state
        if state == "waiting_approval" and not approved:
            call("POST", f"/api/tasks/{tid}/approve", {"approved": True, "comment": "自动放行"})
            print("  → 已放行审批")
            approved = True
        if state in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(2)
    else:
        print("超时未到终态")
        return 1

    t = call("GET", f"/api/tasks/{tid}")
    print("\n======== 最终判决 ========")
    print("状态      ", t.get("state"))
    print("执行次数  ", t.get("attempts"))
    print("错误      ", t.get("error"))
    res = t.get("result") or {}
    if res:
        print("覆盖率    ", res.get("coverage"), "%  / 门禁", res.get("coverage_target"), "%")
        print("测试通过  ", res.get("test_passed"))
        print("门禁降级  ", res.get("gate_degraded"), "|", res.get("gate_reason"))
        print("节点降级  ", res.get("degraded"))
        print("编码轮数  ", res.get("codegen_rounds"), " 澄清轮数", res.get("clarify_rounds"))
    else:
        print("（账本里没有判决摘要）")
    print("\n演示页：", f"{GW}/?task={tid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
