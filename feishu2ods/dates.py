# -*- coding: utf-8 -*-
"""业务日（pt）解析与日期值规范化（新鲜度校验用）。"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta

from .utils import CN_TZ, log

# 业务日参数白名单：只认紧凑与 ISO 两种写法（与 api2ods 同口径，不用 fromisoformat 防版本差异）
_DAY_COMPACT_RE = re.compile(r"\A\d{8}\Z", re.ASCII)  # 只认 ASCII 数字：全角数字不该被当成业务日
_DAY_ISO_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")
DATE_RE = re.compile(r"\A(\d{4})-(\d{1,2})-(\d{1,2})")  # 允许未补零（2026-9-7）：与 SLASH 同口径
SLASH_DATE_RE = re.compile(r"\A(\d{4})/(\d{1,2})/(\d{1,2})")
# epoch 换算的合理区间：超出的一律按"认不出"处理（0 → 1970、14 位 yyyyMMddHHmmss 当毫秒
# → 26xx 年，这类假日期不该先进 date_values 再去污染新鲜度判断）
_EPOCH_MIN = date(2000, 1, 1)
_EPOCH_MAX = date(2100, 1, 1)


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
    for name in ("bizdate", "SKYNET_BIZDATE"):
        raw = os.environ.get(name)
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            if strict:
                raise SystemExit(f"环境变量 {name} 的值为空白，无法作为业务日；请 unset 或用 --bizdate 指定")
            log(f"  警告：环境变量 {name} 的值为空白；只读体检（--check）不写库，按默认业务日继续")
            continue
        try:
            return parse_day_arg(text)
        except SystemExit as exc:
            if not strict:
                # continue 而不是 return None：与上面"空白值"分支同口径——脏值按"未设置"
                # 处理，继续看下一个环境变量（bizdate 脏 + SKYNET_BIZDATE 合法时能落到后者），
                # 全都没有才回默认业务日
                log(f"  警告：环境变量 {name} 的值不是合法日期：{raw!r}；只读体检（--check）不写库，按未设置处理")
                continue
            raise SystemExit(
                f"环境变量 {name} 的值不是合法日期：{raw!r}（应为 YYYYMMDD 或 YYYY-MM-DD）；"
                f"不打算用它请先 unset，或用 --bizdate 显式指定业务日"
            ) from exc
    return None


def resolve_bizdate(args, strict: bool = True) -> date:
    """业务日：--bizdate > 环境变量 bizdate/SKYNET_BIZDATE > 当天-1（CN）。

    pt（写入分区）与新鲜度校验都以这个业务日为准：调度传什么 bizdate，就校验数据里有没有这一天。

    strict=False 只给只读体检（--check）用：环境变量里的 bizdate 格式不对时按"未设置"处理、
    退回当天-1，而不是直接报错退出（正式同步路径仍为 strict=True）。
    """
    if args.bizdate is not None:
        # 显式传参（含空串）都必须走格式校验：调度脚本 `--bizdate "$pt"` 且 $pt 未定义时
        # argv 里就是空串——空串非法要报错，不能按"未指定"静默回退成"昨天"写错分区
        return parse_day_arg(args.bizdate)
    from_env = env_bizdate(strict=strict)
    return from_env if from_env is not None else datetime.now(CN_TZ).date() - timedelta(days=1)


# =============================================================================
# 新鲜度校验 / 飞书告警
# =============================================================================
def _in_reasonable_range(moment: date) -> str | None:
    """解析出的日期必须落在 2000~2100，否则按"认不出"返回 None（哨兵值口径统一）。

    1970-01-01 / 9999-12-31（"未填/永久有效"的常见占位）冒充真实日期会写进新鲜度告警的
    "当前最新"，掩盖"日期列格式不支持或全是占位值"这个真正的问题；数字、紧凑文本、
    带分隔符文本、epoch 四个分支共用这一处口径，避免"同一业务值因书写格式不同得到
    相反结论"。
    """
    if not _EPOCH_MIN <= moment <= _EPOCH_MAX:
        return None
    return moment.isoformat()


def normalize_date_value(value) -> str | None:
    """日期值 → yyyy-MM-dd；认不出返回 None。

    支持：ISO 串（'2026-09-27T00:00:00.000+08:00' 取前 10 位）、'yyyy/MM/dd'、紧凑串
    'yyyymmdd'、epoch 时间戳（秒或毫秒，按量级自动识别）、8 位整数 yyyymmdd。
    认不出返回 None（由调用方按"缺数据"处理）。
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            # 秒级时间戳不能除以 1000——那会静默解析成 1970 年（而不是认不出）。阈值 1e11：
            # 毫秒要到 1973 年、秒要到 5138 年才越过，两段时间戳区间互不重叠
            if float(value).is_integer() and 10_000_000 <= abs(value) <= 99_999_999:
                # 8 位整数按 yyyymmdd 解读：源表把"业务日期"存成数字是常见写法
                text = str(int(value))
                return _in_reasonable_range(date(int(text[:4]), int(text[4:6]), int(text[6:8])))
            seconds = value / 1000 if abs(value) >= 1e11 else value
            return _in_reasonable_range(datetime.fromtimestamp(seconds, CN_TZ).date())
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value or "").strip()
    if re.fullmatch(r"\d{8}", text, re.ASCII):
        # 文本形态的 yyyymmdd（源表把业务日期存成文本列很常见）：与数字分支同口径，
        # 不能被 DATE_RE/SLASH 的"带分隔符"要求漏掉而返回 None（那会让新鲜度校验天天误报缺数据）
        try:
            return _in_reasonable_range(date(int(text[:4]), int(text[4:6]), int(text[6:8])))
        except ValueError:
            return None
    match = DATE_RE.match(text)
    if match:
        try:
            return _in_reasonable_range(date(int(match.group(1)), int(match.group(2)), int(match.group(3))))
        except ValueError:
            return None
    match = SLASH_DATE_RE.match(text)
    if match:
        try:
            return _in_reasonable_range(date(int(match.group(1)), int(match.group(2)), int(match.group(3))))
        except ValueError:
            return None
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
    # 按元素类型统一收集（只看首元素会在混合输入下选错分支：TypeError 或把一类元素
    # 整体丢一边）。dict 取 date_field、str 直接按日期值归一化，None/其它类型跳过——
    # 两种来源合并成一个 seen，不二选一
    seen = set()
    for record in records:
        if isinstance(record, dict):
            seen.add(normalize_date_value(record.get(date_field)))
        elif isinstance(record, str):
            seen.add(normalize_date_value(record))
    seen.discard(None)
    # expected 与 seen 两侧同口径归一化：调用方可能传 date 对象或紧凑串（库调用方），
    # 不归一化拿 date 与字符串集合比较恒为 False——天天误报"缺数据"
    expected_norm = normalize_date_value(expected)
    if expected_norm is None:
        expected_norm = str(expected)  # 认不出的值按原样比（保持旧行为，不误报）
    if expected_norm in seen:
        return None
    return expected_norm, (max(seen) if seen else None)
