"""让测试能 import devpilot：把 src/ 加进 sys.path。

不用 pip install -e . 是因为那样在"没装过包的机器"上跑不了，
而显式改 sys.path 在任何环境下都成立。
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
