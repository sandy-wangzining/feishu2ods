# -*- coding: utf-8 -*-
"""业务日（pt）解析与日期值规范化（新鲜度校验用）。"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta

from .utils import CN_TZ, log

# 业务日参数白名单：只认紧凑与 ISO 两种写法（与 api2ods 同口径，不用 fromisoformat 防版本差异）
_DAY_COMPACT_RE = re.compile(r"\A\d{8}\Z")
_DAY_ISO_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")
DATE_RE = re.compile(r"\A(\d{4})-(\d{2})-(\d{2})")
SLASH_DATE_RE = re.compile(r"\A(\d{4})/(\d{1,2})/(\d{1,2})")


# =============================================================================
# 业务日（pt）解析
# =============================================================================
def parse_day_arg(text: str) -> date:
    """--bizdate：只认 YYYYMMDD / YYYY-MM-DD（与 api2ods 同口径，不用 fromisoformat 防版本差异）。"""
    value = str(text or "").strip()
    if _DAY_COMPACT_RE.match(value):
        try:
            return date(int(value[:4]), int(value[4:6]), int(value[6:]))
        except ValueError as exc:
            raise SystemExit(f"--bizdate 日期不存在：{text!r}（{exc}）") from None
    if _DAY_ISO_RE.match(value):
        try:
            return date(int(value[:4]), int(value[5:7]), int(value[8:10]))
        except ValueError as exc:
            raise SystemExit(f"--bizdate 日期不存在：{text!r}（{exc}）") from None
    raise SystemExit(f"--bizdate 格式应为 YYYYMMDD 或 YYYY-MM-DD：{text!r}")


def env_bizdate(strict: bool = True) -> date | None:
    """DataWorks 环境变量 bizdate / SKYNET_BIZDATE；没设置返回 None。

    设置了却解析不出来时必须报错，不能静默回退"昨天"：那会把数据写进错的分区
    （写入会替换掉对的分区），而退出码还是 0，调度侧完全看不出来。

    strict=False 只给"只读体检"（--check）用：它不写库、不发告警，落哪个 pt 只是看一眼，
    没必要因为调度环境变量脏了就连体检都跑不起来（那时按默认业务日继续并打警告）。
    正式同步路径必须保持 strict=True（非法业务日要报错，绝不静默回退成"昨天"）。
    """
    raw = os.environ.get("bizdate") or os.environ.get("SKYNET_BIZDATE") or ""
    text = raw.strip()
    if not text:
        return None
    try:
        return parse_day_arg(text)
    except SystemExit as exc:
        if not strict:
            log(
                f"  警告：环境变量 bizdate/SKYNET_BIZDATE 的值不是合法日期：{raw!r}；"
                f"只读体检（--check）不写库，按默认业务日继续"
            )
            return None
        raise SystemExit(
            f"环境变量 bizdate/SKYNET_BIZDATE 的值不是合法日期：{raw!r}（应为 YYYYMMDD 或 YYYY-MM-DD）；"
            f"不打算用它请先 unset，或用 --bizdate 显式指定业务日"
        ) from exc


def resolve_bizdate(args, strict: bool = True) -> date:
    """业务日：--bizdate > 环境变量 bizdate/SKYNET_BIZDATE > 当天-1（CN）。

    pt（写入分区）与新鲜度校验都以这个业务日为准：调度传什么 bizdate，就校验数据里有没有这一天。

    strict=False 只给只读体检（--check）用：环境变量里的 bizdate 格式不对时按"未设置"处理、
    退回当天-1，而不是直接报错退出（正式同步路径仍为 strict=True）。
    """
    if args.bizdate:
        return parse_day_arg(args.bizdate)
    from_env = env_bizdate(strict=strict)
    return from_env if from_env is not None else datetime.now(CN_TZ).date() - timedelta(days=1)


# =============================================================================
# 新鲜度校验 / 飞书告警
# =============================================================================
def normalize_date_value(value) -> str | None:
    """日期值 → yyyy-MM-dd；认不出返回 None。

    支持：ISO 串（'2026-09-27T00:00:00.000+08:00' 取前 10 位）、'yyyy/MM/dd'、epoch 毫秒数字。
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value / 1000, CN_TZ).date().isoformat()
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value or "").strip()
    match = DATE_RE.match(text)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    match = SLASH_DATE_RE.match(text)
    if match:
        return f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return None


def freshness_problem(records, date_field: str, expected: str) -> tuple[str, str | None] | None:
    """新鲜度校验：预期日期（业务日 - lag_days）没出现时返回 (预期, 当前最新)，否则 None。

    records 两种形态都认：记录列表（旧调用方，内部按 date_field 提取日期）或
    已 normalize 过的日期集合（流式路径传 FetchStats.date_values，元素是 str）。
    """
    # 迭代器（生成器）先实体化：下面要"先看一条判类型、再整体遍历"，直接两次遍历生成器
    # 会消费掉第一条记录（它的日期不参与比较，可能误报"缺数据"）
    if records is None:
        records = []
    elif not isinstance(records, (list, tuple, set, frozenset)):
        records = list(records)
    first = next(iter(records), None)
    if isinstance(first, dict):
        seen = {normalize_date_value(record.get(date_field)) for record in records}
    else:
        seen = set(records)
    seen.discard(None)
    if expected in seen:
        return None
    return expected, (max(seen) if seen else None)
