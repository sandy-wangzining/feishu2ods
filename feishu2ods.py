# -*- coding: utf-8 -*-
"""feishu2ods 兼容入口：等价于 python -m feishu2ods（代码已拆为 feishu2ods/ 包）。

保留本文件是为了兼容既有的调度命令 `python feishu2ods.py --job ...`。
"""

import sys

from feishu2ods.cli import main

if __name__ == "__main__":
    sys.exit(main())
