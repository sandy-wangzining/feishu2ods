# -*- coding: utf-8 -*-
"""ODS 公共运行逻辑；唯一维护源为 api2ods/shared/runtime.py。

由 tools/sync_common.py 同步到各包的 _common.py，不依赖其它 ODS 项目或第三方包。
"""

from __future__ import annotations

import os
import re
import stat
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")
_AUTH_SCHEMES = frozenset({"basic", "bearer", "token", "digest"})
DEFAULT_BIZDATE_TZ = "America/New_York"


def default_bizdate(now: datetime | None = None) -> date:
    """没传业务日时的统一默认值：美东时区（America/New_York）的昨天。

    三个 ODS 项目共用这一份；显式 --bizdate 与环境变量 bizdate/SKYNET_BIZDATE
    仍由各项目优先处理，本函数只负责最后的兜底口径。
    """
    tz = ZoneInfo(DEFAULT_BIZDATE_TZ)
    if now is None:
        moment = datetime.now(tz)
    elif now.tzinfo is None:
        moment = now.replace(tzinfo=tz)
    else:
        moment = now.astimezone(tz)
    return moment.date() - timedelta(days=1)


def private_lock_dir(tool: str) -> Path:
    """默认临时锁目录在 POSIX 按 UID 隔离，并拒绝软链、他人属主与公共可写目录。"""
    uid = os.getuid() if hasattr(os, "getuid") else None
    suffix = f"-{uid}" if uid is not None else ""
    path = Path(tempfile.gettempdir()) / f"{tool}-locks{suffix}"
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise PermissionError(f"运行锁目录不是普通目录：{path}")
    if uid is not None and (info.st_uid != uid or info.st_mode & 0o022):
        raise PermissionError(f"运行锁目录属主或权限不安全：{path}")
    return path


def safe_label(value, limit: int = 80) -> str:
    """外部文本展示前去控制字符、转义反引号并限长；不改变入库数据。"""
    text = _CONTROL_CHARS_RE.sub(" ", str(value)).replace("`", "'")
    return text if len(text) <= limit else text[:limit] + "…"


def leaf_strings(value) -> list[str]:
    """收集嵌套配置里的凭证值；数字 ID 也收集，布尔和空值除外。"""
    if isinstance(value, dict):
        return [item for sub in value.values() for item in leaf_strings(sub)]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [item for sub in value for item in leaf_strings(sub)]
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return [str(value)]
    return []


def with_scheme_bare(value: str) -> list[str]:
    """收集带认证方案的完整凭证及裸值，防止接口只回显裸 token 时漏遮。"""
    head, sep, tail = value.partition(" ")
    if sep and head.lower() in _AUTH_SCHEMES and tail.strip():
        return [value, tail.strip()]
    return [value]


def webhook_targets(value) -> list[str]:
    """告警地址去首尾空白、去空、保序去重；运行时忽略错误类型。"""
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            continue
        item = item.strip()
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def feishu_card(title: str, lines, footer: str = "") -> dict:
    """构造三个项目共用的飞书卡片；发送和失败策略由调用方保留。"""
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(str(x) for x in lines)}}],
        },
    }
    if footer:
        card["card"]["elements"].append({"tag": "hr"})
        card["card"]["elements"].append({"tag": "note", "elements": [{"tag": "plain_text", "content": footer}]})
    return card


def run_sql_with_timeout(o, sql: str, timeout, desc: str, *, clock, log, heartbeat):
    """提交前计时；SQL 成功才返回，其余退出路径尽力取消云端实例。

    调用方传入自身的时钟与脱敏日志函数，保留现有接口、测试替身和日志策略。
    同步的提交/轮询请求仍由 SDK 的 HTTP 超时兜底，不能在 Python 内强制中断。
    """
    started = clock.monotonic()
    instance = o.run_sql(sql)
    last_log = started
    try:
        while True:
            if instance.is_successful():
                return instance
            if instance.is_terminated():
                instance.wait_for_success(timeout=1)
                if not instance.is_successful():
                    raise RuntimeError(f"{desc} 已终止但未成功（实例 {getattr(instance, 'id', '?')}）")
                return instance
            now = clock.monotonic()
            if timeout and timeout > 0 and now - started > timeout:
                raise TimeoutError(f"{desc} 执行超过 {timeout} 秒，已请求停止")
            if timeout and timeout > 0 and now - last_log >= heartbeat:
                log(f"    {desc} 还在执行（已等待 {int(now - started)} 秒，超时阈值 {timeout} 秒）...")
                last_log = now
            clock.sleep(1)
    except BaseException:
        # 轮询失败或用户中断也要取消，避免旧 SQL 在调度重跑后继续修改同一分区。
        try:
            instance.stop()
        except Exception as exc:
            log(f"    {desc} 取消失败（{type(exc).__name__}: {exc}），云端实例可能仍在运行")
        raise
