"""文件系统小工具。

## 为什么要有 `ensure_dir` 这么个两行函数

因为 `path.mkdir(parents=True, exist_ok=True)` 在这个执行环境里**不能直接用**。

某些运行环境（容器、IDE 的沙箱 shim、加固过的解释器）会拦截文件系统调用，
把「对已存在目录的 mkdir」直接拒绝成 `PermissionError(EEXIST)` ——
而不是返回 EEXIST 让 Python 的 `exist_ok=True` 去处理。

于是同一段代码会出现很反直觉的现象：
  - 第一次调用成功（目录不存在 → 真的创建）
  - 第二次调用**抛异常**（目录已存在 → 被 shim 拒绝）
而错误信息是 `EEXIST: file already exists`，看上去像"重复创建"，
但 `exist_ok=True` 明明就是为了这个场景写的 —— 排查时极易走偏。

结论：**先判断再创建**。多一次 stat，换掉一整类环境相关的偶发失败。
"""

from __future__ import annotations

from pathlib import Path


def ensure_dir(path: Path) -> Path:
    """确保目录存在；已存在（且是目录）时静默返回。"""
    p = Path(path)
    if not p.is_dir():
        p.mkdir(parents=True, exist_ok=True)
    return p
