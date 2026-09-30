# -*- coding: utf-8 -*-
"""通用工具：日志（带时间戳+脱敏）、运行锁（flock/msvcrt）、_api_err。"""

from __future__ import annotations

import hashlib
import os
import pathlib
import socket
import sys
import tempfile
from datetime import datetime, timedelta, timezone

try:
    import fcntl  # Linux / macOS：进程级运行锁
except ImportError:  # pragma: no cover - Windows 没有 fcntl
    fcntl = None

try:
    import msvcrt  # Windows：用首字节锁实现同样的效果
except ImportError:  # pragma: no cover - Linux / macOS 没有 msvcrt
    msvcrt = None

CN_TZ = timezone(timedelta(hours=8))  # 运行日历日基准（固定 +08:00，无夏令时）
_SECRETS: list[str] = []  # 日志脱敏用（job 里读到的密钥值）
_console_patched = False


def setup_console() -> None:
    """stdout/stderr 切 UTF-8，避免 Windows 控制台中文乱码/报错（切不了就跳过）。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:  # noqa: BLE001 - 某些重定向流不支持 reconfigure
            pass


def log(msg: str) -> None:
    """带时间戳（北京时间）的日志；所有输出过一道密钥脱敏，编码异常时降级不中断。"""
    global _console_patched
    if not _console_patched:
        setup_console()
        _console_patched = True
    line = f"[{datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}] {redact(msg)}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
        print(line.encode(encoding, "replace").decode(encoding, "replace"), flush=True)


def redact(text) -> str:
    """把 job 里出现过的密钥值在文本里遮掉（接口报错回显时兜底）。"""
    out = str(text)
    for secret in _SECRETS:
        if secret and len(secret) >= 6:
            out = out.replace(secret, "***")
    return out


def _api_err(data) -> str:
    """接口返回里的 code/msg → 报错片段（data 不是对象时也能安全展示）。"""
    if isinstance(data, dict):
        return f"code={data.get('code')} msg={data.get('msg')}"
    return f"接口返回不是 JSON 对象：{str(data)[:200]}"


# =============================================================================
# 运行锁（与 api2ods 同款）：避免定时任务与手动重跑同时写同一个分区
# =============================================================================
class RunLock:
    """进程级运行锁：避免同一台机器上定时任务与手动执行（或两个实例）同时跑。

    - Linux / macOS：flock 排它锁；
    - Windows：msvcrt 首字节锁（同样是排它、非阻塞）；
    - 两种锁都没有的平台：退化为"不阻塞"，不挡运行；
    - 锁随进程退出自动释放，进程被 kill 也由内核释放，不会残留死锁；
    - 锁文件在各自机器的本地磁盘上：只在单机内互斥，本地与服务器同时跑同一作业没有保护，
      正式调度请固定跑在一台机器上。
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.fh = None

    def __enter__(self):
        """拿锁；已被别人持有就抛 SystemExit（不等待），拿不到直接让本次运行退出。"""
        if fcntl is None and msvcrt is None:
            return self
        try:
            # "a+" 而不是 "w"：w 会在打开时把文件截断，持锁进程刚写进去的 pid 就被抹掉了
            self.fh = open(self.path, "a+")
        except OSError as exc:
            raise SystemExit(
                f"无法创建运行锁文件 {self.path}（{exc}）；请检查该路径所在目录是否存在/可写，或用 --job 指定别处的作业"
            )
        if not _try_lock(self.fh):
            holder = ""
            try:
                self.fh.seek(0)
                holder = (self.fh.read(200) or "").strip()
            except OSError:
                pass
            self.fh.close()
            self.fh = None
            detail = f"（持有者：{holder}）" if holder else ""
            raise SystemExit(
                f"已有任务在运行{detail}（锁文件 {self.path}），本次退出。"
                f"该锁只在同一台机器上生效，跨机并发（如本地与服务器同时跑）请自行避免。"
            )
        try:
            # 拿到锁之后才截断+写标记：拿不到锁时绝不能动内容
            self.fh.seek(0)
            self.fh.truncate()
            self.fh.write(f"{os.getpid()} {socket.gethostname()} {datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
            self.fh.flush()
        except OSError:  # 写标记只是给人看，失败不影响加锁
            pass
        return self

    def __exit__(self, *exc_info):
        """解锁并关句柄；锁文件本身保留（不删文件，避免削掉别人的锁）。"""
        if self.fh is not None:
            try:
                _unlock(self.fh)
            finally:
                self.fh.close()


def _try_lock(fh) -> bool:
    """对已打开的文件加排它锁；别人拿着锁时返回 False（不阻塞等待）。"""
    if fcntl is not None:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
    if msvcrt is not None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    return True  # 两种锁都没有：不阻塞（退回"无锁"行为）


def _unlock(fh) -> None:
    """释放锁；释放失败也没关系——进程退出时内核会兜底释放，不该因此让任务报错。"""
    if fcntl is not None:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        except OSError:
            pass
    elif msvcrt is not None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass


def lock_path(job_path: pathlib.Path, root: pathlib.Path | None = None) -> pathlib.Path:
    """每个作业一把运行锁（不同作业可并行，同一作业不会重复跑）。

    优先放工具目录下 .run-locks/；工具目录不可写（如 pip 装在只读位置）时退回系统临时目录；
    root 显式指定锁目录（单元测试用，避免往工具目录写测试残留）。
    锁名带路径哈希：jobs/a/api.json 与 jobs/b/api.json 同名不同作业，只按文件名会互相阻塞。
    注意：锁文件在各机器本地磁盘上，只保证单机互斥（跨机并发仍会互相写坏，正式跑固定一台）。
    """
    stem = job_path.stem or "job"
    digest = hashlib.sha1(str(job_path).encode("utf-8")).hexdigest()[:8]
    name = f"{stem}-{digest}"
    if root is not None:
        base = pathlib.Path(root)
        base.mkdir(parents=True, exist_ok=True)
        return base / f"{name}.lock"
    candidates = [
        pathlib.Path(__file__).resolve().parent / ".run-locks",
        pathlib.Path(tempfile.gettempdir()) / "feishu2ods-locks",
    ]
    for base in candidates:
        try:
            base.mkdir(parents=True, exist_ok=True)
            # 探测文件名必须唯一（mkstemp）：并发启动时共享的探测文件会被别的进程删掉，
            # 导致"静默"落到下一个候选目录——同一作业的两个实例锁在不同路径上，互斥失效
            handle, probe = tempfile.mkstemp(prefix=".probe-", dir=str(base))
            os.close(handle)
            os.unlink(probe)
            return base / f"{name}.lock"
        except OSError:
            continue
    return pathlib.Path(tempfile.gettempdir()) / f"feishu2ods-{name}.lock"
