# -*- coding: utf-8 -*-
"""job 配置：读取与校验（白名单键、标识符、必填项）。"""

from __future__ import annotations

import json
import pathlib
import re

JOB_KEYS = {"job", "description", "feishu", "maxcompute", "fields", "target", "freshness"}
FEISHU_KEYS = {"app_id", "app_secret", "base_token", "table_id", "base_url"}
MC_KEYS = {"project", "endpoint", "access_key_id", "access_key_secret"}
TARGET_KEYS = {"project", "table", "column", "comment", "allow_empty"}
FRESHNESS_KEYS = {"date_field", "lag_days", "webhook"}
DEFAULT_COLUMN = "json"
DEFAULT_FRESHNESS_LAG_DAYS = 0                  # 预期日期 = bizdate - N 天（0 = 必须有 bizdate 当天数据）
IDENT_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")

# =============================================================================
# job 配置：读取 / 校验
# =============================================================================
def load_job(path_text: str) -> dict:
    """读 job 文件：缺失/不是 JSON/顶层不是对象时给带路径的明确报错。"""
    path = pathlib.Path(path_text)
    if not path.is_file():
        raise SystemExit(f"找不到 job 配置文件：{path}")
    try:
        text = path.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SystemExit(f"job 文件不是 UTF-8 编码（{path}）：{exc}；请存成 UTF-8 后重试") from None
    try:
        job = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"job 文件不是合法 JSON（{path}）：{exc}") from None
    if not isinstance(job, dict):
        raise SystemExit(f"job 文件顶层必须是 JSON 对象：{path}")
    return job


def _warn_unknown_keys(obj: dict, allowed: set, where: str, warnings: list) -> None:
    """未知键告警（防拼写错误静默不生效）；以 // 或 # 开头的键当注释放过。"""
    for key in obj:
        if str(key).startswith("//") or str(key).startswith("#"):
            continue
        if key not in allowed:
            warnings.append(f"{where}.{key} 不是已知配置项（拼写错误？）——已忽略")


def _require_text(value, where: str) -> str:
    """必填字符串：空/非字符串直接报错（带字段路径）。"""
    if not isinstance(value, str) or not value.strip():
        raise SystemExit(f"{where} 必须是非空字符串，实际 {value!r}")
    return value.strip()


def _require_identifier(value, where: str) -> str:
    """MaxCompute 标识符校验（表名/列名/项目名会拼进 DDL，挡注入与拼错）。"""
    text = _require_text(value, where)
    if not IDENT_RE.match(text):
        raise SystemExit(f"{where} 不是合法标识符（字母/数字/下划线，字母开头）：{text!r}")
    return text


def validate_job(job: dict) -> list[str]:
    """校验 job 配置；返回未知键告警列表（不阻断）。校验失败直接 SystemExit 带字段路径。

    校验通过的部分会写回规范化后的值（如去掉首尾空白），调用方随后直接读 job 即可。
    """
    warnings: list[str] = []
    _warn_unknown_keys(job, JOB_KEYS, "作业", warnings)

    feishu = job.get("feishu")
    if not isinstance(feishu, dict):
        raise SystemExit("作业配置缺少 feishu 块（app_id / app_secret / base_token / table_id）")
    _warn_unknown_keys(feishu, FEISHU_KEYS, "feishu", warnings)
    for key in ("app_id", "app_secret", "base_token", "table_id"):
        feishu[key] = _require_text(feishu.get(key), f"feishu.{key}")
    if feishu.get("base_url") is not None:
        base_url = _require_text(feishu.get("base_url"), "feishu.base_url")
        if not base_url.startswith(("http://", "https://")):
            raise SystemExit(f"feishu.base_url 必须是 http(s) 开头的地址：{base_url!r}")
        feishu["base_url"] = base_url

    maxcompute = job.get("maxcompute")
    if not isinstance(maxcompute, dict):
        raise SystemExit("作业配置缺少 maxcompute 块（access_key_id / access_key_secret）")
    _warn_unknown_keys(maxcompute, MC_KEYS, "maxcompute", warnings)
    for key in ("access_key_id", "access_key_secret"):
        maxcompute[key] = _require_text(maxcompute.get(key), f"maxcompute.{key}")
    if maxcompute.get("endpoint") is not None:
        endpoint = _require_text(maxcompute.get("endpoint"), "maxcompute.endpoint")
        if not endpoint.startswith(("http://", "https://")):
            raise SystemExit(f"maxcompute.endpoint 必须是 http(s) 开头的地址：{endpoint!r}")
        maxcompute["endpoint"] = endpoint
    if maxcompute.get("project") is not None:
        maxcompute["project"] = _require_identifier(maxcompute.get("project"), "maxcompute.project")

    fields = job.get("fields")
    if not isinstance(fields, dict) or not fields:
        raise SystemExit("作业配置缺少 fields（Base 列名 → JSON 英文键 的映射，至少一条）")
    seen: dict[str, str] = {}
    for source, target in fields.items():
        _require_text(source, "fields 的列名")
        if not isinstance(target, str) or not IDENT_RE.match(target):
            raise SystemExit(f"fields[{source!r}] 的英文键不合法（字母/数字/下划线，字母开头）：{target!r}")
        if target == "record_id":
            raise SystemExit("fields 里不能用 record_id 作为英文键（record_id 固定为记录 ID，自动输出）")
        if target in seen:
            raise SystemExit(f"fields 的英文键重复：{target!r}（{seen[target]!r} 与 {source!r}）")
        seen[target] = source

    target_cfg = job.get("target")
    if not isinstance(target_cfg, dict):
        raise SystemExit("作业配置缺少 target 块（project / table / column）")
    _warn_unknown_keys(target_cfg, TARGET_KEYS, "target", warnings)
    # 校验通过的值写回去（首尾空白在 DDL/SQL 里都是坑）
    target_cfg["table"] = _require_identifier(target_cfg.get("table"), "target.table")
    if target_cfg.get("column") is not None:
        target_cfg["column"] = _require_identifier(target_cfg.get("column"), "target.column")
    if target_cfg.get("project") is not None:
        target_cfg["project"] = _require_identifier(target_cfg.get("project"), "target.project")
    if not (target_cfg.get("project") or maxcompute.get("project")):
        raise SystemExit("没有目标项目：请在 target.project 或 maxcompute.project 里指定")
    if target_cfg.get("comment") is not None and not isinstance(target_cfg.get("comment"), str):
        raise SystemExit("target.comment 必须是字符串")
    if "allow_empty" in target_cfg and not isinstance(target_cfg.get("allow_empty"), bool):
        raise SystemExit(f"target.allow_empty 必须是 true/false，实际 {target_cfg.get('allow_empty')!r}")

    freshness = job.get("freshness")
    if freshness is not None:
        if not isinstance(freshness, dict):
            raise SystemExit("freshness 必须是对象（date_field / lag_days / webhook）")
        _warn_unknown_keys(freshness, FRESHNESS_KEYS, "freshness", warnings)
        date_field = _require_text(freshness.get("date_field"), "freshness.date_field")
        freshness["date_field"] = date_field
        if date_field not in seen:
            raise SystemExit(f"freshness.date_field={date_field!r} 不在 fields 的英文键里：{sorted(seen)}")
        lag = freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS)
        if isinstance(lag, bool) or not isinstance(lag, int) or lag < 0:
            raise SystemExit(f"freshness.lag_days 必须是非负整数，实际 {lag!r}")
        if freshness.get("webhook") is not None:
            webhook = _require_text(freshness.get("webhook"), "freshness.webhook")
            if not webhook.startswith(("http://", "https://")):
                raise SystemExit(f"freshness.webhook 必须是 http(s) 开头的地址：{webhook!r}")
            freshness["webhook"] = webhook
    return warnings


