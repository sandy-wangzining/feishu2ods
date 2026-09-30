# -*- coding: utf-8 -*-
"""feishu2ods：飞书多维表格 → MaxCompute ODS（JSON 原样落库，json + pt 分区，写临时分区后原子替换）。

干什么：
    把任意飞书多维表格（Base）的数据表按记录全量拉下来，每条记录一行 JSON 写进 MaxCompute：
    - JSON 键 = job 配置里的英文映射（Base 列名 → 英文键），另固定含 record_id（放最前）；
    - 值原样保留（表格里显示什么就是什么，如 "$6,079.07"、日期 ISO 字符串），
      清洗与落表口径交给下游 DWD 用 get_json_object 解析；
    - 目标表固定「单列 string + pt 分区」：每次运行把全量快照写进 pt=<业务日>（先写 pt=<业务日>__tmp
      临时分区，核对行数后再「删旧分区 + rename」原子替换；重复跑幂等，写入期间旧快照仍可读）；
    - 写库前先校验目标表结构，结构不符直接报错、绝不先清表；
    - 同一作业有运行锁（flock/msvcrt，进程退出自动释放）：同一台机器上调度与手动重跑重叠时
      有一边会退出；锁只在单机内生效，本地与服务器同时跑同一作业没有保护（正式跑请固定在一台机器）；
    - 可选新鲜度校验：业务日（bizdate - lag_days，默认 lag_days=0 即 bizdate 当天）必须出现在
      指定字段里，缺失 / 表为空时发飞书告警并以非 0 退出、不写库；
    - 未映射的新增列（Base 里新加的列）不报错：忽略其值、其余字段照常同步，并发一条飞书提醒
      （含列名与处理步骤，人工决定是否加 fields 映射）；映射的列被改名/删除仍直接报错。

    pt（业务日）取值优先级：--bizdate > 环境变量 bizdate / SKYNET_BIZDATE（DataWorks）> 当天-1（CN）。

第一次接一张新表（复用流程）：
    1. 把飞书自建应用加为该表格的「可阅读」协作者；
    2. `python feishu2ods.py --init` 按提示生成 jobs/<作业名>.json（自动拉列名、逐列起英文键）；
    3. `python feishu2ods.py --job jobs/<作业名>.json --check` 体检；
    4. `--dry-run` 试跑后正式跑；调度命令：--job ... --bizdate "${bizdate}"。

job 配置（jobs/*.json，密钥直接写在文件里；jobs/ 已 gitignore，参考 example）：
    {
      "job": "feishu_ai_cost",
      "description": "一句话说明（可选）",
      "feishu": {
        "app_id": "cli_xxx",              // 飞书自建应用（需被加为目标表格的「可阅读」协作者）
        "app_secret": "<App Secret>",
        "base_token": "IC4...",           // 多维表格 token
        "table_id": "tble...",            // 数据表 ID
        "base_url": "https://xxx.feishu.cn/base/IC4..."   // 可选，告警里带链接
      },
      "maxcompute": {
        "project": "my_project",             // 默认项目（target.project 未给时用它）
        "endpoint": "http://service.us-west-1.maxcompute.aliyun.com/api",  // 可选
        "access_key_id": "LTAI...",
        "access_key_secret": "..."
      },
      "fields": {                         // Base 列名 → JSON 英文键（必填，英文唯一）
        "date（美国时间）": "cost_date",
        "每日LLM推理成本 折前": "llm_cost_before_discount"
      },
      "target": {
        "project": "my_project",             // 可选，覆盖 maxcompute.project
        "table": "ods_xxx_json_df",       // 必填
        "column": "json",                 // 可选，默认 json
        "comment": "表注释（可选）",
        "allow_empty": false              // 可选，默认 false：拉到 0 行时拒绝写库
      },
      "freshness": {                      // 可选；配了才做新鲜度校验
        "date_field": "cost_date",        // 用哪个 JSON 键判日期（必须是 fields 的英文值）
        "lag_days": 0,                    // 预期日期 = bizdate - N 天（默认 0 = 必须有 bizdate 当天数据）
        "webhook": "https://open.feishu.cn/open-apis/bot/v2/hook/xxx"   // 缺失时告警
      }
    }

用法：
    python feishu2ods.py --init                              # 交互式生成新作业（接新表用）
    python feishu2ods.py --job jobs/feishu_ai_cost.json --check         # 体检（不写库、不发告警）
    python feishu2ods.py --job jobs/feishu_ai_cost.json                 # 日常：拉数 → 校验 → 写 pt（临时分区 + 原子替换）
    python feishu2ods.py --job jobs/feishu_ai_cost.json --bizdate 20260928      # 指定业务日 pt（重跑/补数）
    python feishu2ods.py --job jobs/feishu_ai_cost.json --dry-run       # 只拉数打印统计，不写库
    python feishu2ods.py --job jobs/feishu_ai_cost.json --skip-freshness       # 跳过新鲜度校验（补数/排查）
    python feishu2ods.py --job jobs/feishu_ai_cost.json --no-notify     # 不发飞书通知（缺数据/新增列等）
    python feishu2ods.py --job jobs/feishu_ai_cost.json --project my_project_dev  # 改目标项目（测试用）
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import json
import os
import pathlib
import re
import socket
import sys
import tempfile
import time
import traceback
from collections import Counter
from datetime import date, datetime, timedelta, timezone

try:
    import requests
except ImportError:  # pragma: no cover - 离线测试环境可以不带 requests
    requests = None

try:
    from odps import ODPS
except ImportError:  # pragma: no cover - 离线测试环境可以不带 pyodps
    ODPS = None

try:
    import fcntl  # Linux / macOS：进程级运行锁
except ImportError:  # pragma: no cover - Windows 没有 fcntl
    fcntl = None

try:
    import msvcrt  # Windows：用首字节锁实现同样的效果
except ImportError:  # pragma: no cover - Linux / macOS 没有 msvcrt
    msvcrt = None


# =============================================================================
# 常量与全局
# =============================================================================
VERSION = "1.2.0"                               # --version 输出；服务器部署后可用它对照版本
JOB_KEYS = {"job", "description", "feishu", "maxcompute", "fields", "target", "freshness"}
FEISHU_KEYS = {"app_id", "app_secret", "base_token", "table_id", "base_url"}
MC_KEYS = {"project", "endpoint", "access_key_id", "access_key_secret"}
TARGET_KEYS = {"project", "table", "column", "comment", "allow_empty"}
FRESHNESS_KEYS = {"date_field", "lag_days", "webhook"}

FEISHU_HOST = "https://open.feishu.cn"
TOKEN_URL = f"{FEISHU_HOST}/open-apis/auth/v3/tenant_access_token/internal"
DEFAULT_COLUMN = "json"
DEFAULT_ENDPOINT = "http://service.us-west-1.maxcompute.aliyun.com/api"
DEFAULT_FRESHNESS_LAG_DAYS = 0                  # 预期日期 = bizdate - N 天（0 = 必须有 bizdate 当天数据）
PARTITION_COLUMN = "pt"                         # 分区字段：业务日 yyyyMMdd（该分区 = 当天抽取的全量快照）

PAGE_SIZE = 500                                 # 单页行数（接口上限 2000，超了报 800004006；500 兼顾请求数与单页体积）
MAX_PAGES = 10000                               # 防死循环的翻页上限（10000 页 × 500 行 = 500 万行，够用）
BATCH_SIZE = 500                                # Tunnel 每批写入行数
WRITE_ATTEMPTS = 3                              # 写分区的最大尝试次数（重试会清掉临时分区重写）
WRITE_RETRY_DELAY = 10                          # 写入重试间隔秒数
TMP_PARTITION_SUFFIX = "__tmp"                  # 写库用临时分区后缀；写完 rename 成正式分区（缩短下游可见窗口）
HTTP_ATTEMPTS = 3                               # 单次请求最大尝试次数（429/5xx/网络抖动才重试）
# access token 失效的错误码：命中后重新取一次 token 再重试当前页（正常 2 小时有效期足够，兜底用）
TOKEN_ERROR_CODES = {99991661, 99991663, 99991668}
RATE_LIMIT_CODES = {99991400}                   # 接口限流（HTTP 200 + 该 code）：等待后重试同一页
RATE_LIMIT_ATTEMPTS = 5                         # 同一页限流最多重试次数
RATE_LIMIT_WAIT = 5                             # 限流重试间隔秒数
MAX_ROW_BYTES = 7_000_000                       # 单行 JSON 上限（MaxCompute string 8MB，留余量）

CN_TZ = timezone(timedelta(hours=8))            # 运行日历日基准（固定 +08:00，无夏令时）
IDENT_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")
DATE_RE = re.compile(r"\A(\d{4})-(\d{2})-(\d{2})")
SLASH_DATE_RE = re.compile(r"\A(\d{4})/(\d{1,2})/(\d{1,2})")
# 业务日参数白名单：只认紧凑与 ISO 两种写法（与 api2ods 同口径，不用 fromisoformat 防版本差异）
_DAY_COMPACT_RE = re.compile(r"\A\d{8}\Z")
_DAY_ISO_RE = re.compile(r"\A\d{4}-\d{2}-\d{2}\Z")

_SECRETS: list[str] = []                        # 日志脱敏用（job 里读到的密钥值）
_console_patched = False


class ApiHttpError(Exception):
    """HTTP 4xx（登录/权限/参数类确定性错误）：不再重试，由调用方决定处置。"""

    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body


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


def dump_record(record: dict) -> str:
    """一条记录 → 单行 JSON（与 api2ods 同款序列化参数：不转义中文、紧凑、拒绝 NaN）。"""
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


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


def env_bizdate() -> date | None:
    """DataWorks 环境变量 bizdate / SKYNET_BIZDATE；没设置返回 None。

    设置了却解析不出来时必须报错，不能静默回退"昨天"：那会把数据写进错的分区
    （写入会替换掉对的分区），而退出码还是 0，调度侧完全看不出来。
    """
    raw = os.environ.get("bizdate") or os.environ.get("SKYNET_BIZDATE") or ""
    text = raw.strip()
    if not text:
        return None
    try:
        return parse_day_arg(text)
    except SystemExit as exc:
        raise SystemExit(
            f"环境变量 bizdate/SKYNET_BIZDATE 的值不是合法日期：{raw!r}（应为 YYYYMMDD 或 YYYY-MM-DD）；"
            f"不打算用它请先 unset，或用 --bizdate 显式指定业务日"
        ) from exc


def resolve_bizdate(args) -> date:
    """业务日：--bizdate > 环境变量 bizdate/SKYNET_BIZDATE > 当天-1（CN）。

    pt（写入分区）与新鲜度校验都以这个业务日为准：调度传什么 bizdate，就校验数据里有没有这一天。
    """
    if args.bizdate:
        return parse_day_arg(args.bizdate)
    from_env = env_bizdate()
    return from_env if from_env is not None else datetime.now(CN_TZ).date() - timedelta(days=1)


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


# =============================================================================
# 飞书：tenant_access_token + 全量拉记录
# =============================================================================
def request_json(method: str, url: str, desc: str, *, params=None, body=None, headers=None, tries: int = HTTP_ATTEMPTS):
    """带重试的 HTTP 请求 → JSON。

    只对「可能自己好起来」的失败重试：429 / 5xx / 连接超时 / 响应不是 JSON。
    其余 4xx 抛 ApiHttpError（密钥错、权限错等确定性错误，重试无意义）；报错文本过脱敏。
    """
    if requests is None:
        raise SystemExit("缺少 requests，请先 pip install requests")
    last: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            # allow_redirects=False：requests 默认跟随重定向，而 301/302/303 会把 POST 降级成
            # 不带 body 的 GET（请求参数全丢），且自定义鉴权头（Authorization）会被转发到重定向
            # 目标。这两种后果都比"直接失败"危险得多（与 api2ods 2.1.8 的修复同款）
            resp = requests.request(
                method, url, params=params, json=body, headers=headers, timeout=(10, 30), allow_redirects=False
            )
        except requests.RequestException as exc:
            last = exc
        else:
            if 300 <= resp.status_code < 400:
                # 不跟随重定向：把 Location 报出来让用户直接改成最终地址。
                # 抛 ApiHttpError（确定性错误、不重试），别让它掉进重试的退避里
                location = redact(str(resp.headers.get("Location") or ""))
                raise ApiHttpError(
                    resp.status_code,
                    f"接口返回重定向 HTTP {resp.status_code}（Location: {location}）：本工具不跟随重定向"
                    f"——301/302/303 会把 POST 降级成不带 body 的 GET（请求参数全丢），"
                    f"鉴权头也可能被转发到别的地址。请把地址改成最终地址",
                )
            if resp.status_code >= 500 or resp.status_code == 429:
                last = requests.RequestException(f"HTTP {resp.status_code}：{resp.text[:200]}")
            elif 400 <= resp.status_code < 500:
                raise ApiHttpError(resp.status_code, resp.text[:300])
            else:
                try:
                    return resp.json()
                except ValueError as exc:
                    last = exc
        if attempt < tries:
            wait = 5 * attempt
            log(f"  [{desc}] 第 {attempt} 次失败：{redact(last)}；{wait}s 后重试")
            time.sleep(wait)
    raise SystemExit(f"{desc} 失败（重试 {tries} 次）：{redact(last)}")


def get_tenant_token(app_id: str, app_secret: str) -> str:
    """app_id/app_secret → tenant_access_token（有效期 2 小时；失败时给明确原因）。"""
    try:
        data = request_json(
            "POST", TOKEN_URL, "获取 tenant_access_token", body={"app_id": app_id, "app_secret": app_secret}
        )
    except ApiHttpError as exc:
        raise SystemExit(f"获取 tenant_access_token 失败：HTTP {exc.status}：{redact(exc.body)}") from None
    if not isinstance(data, dict) or data.get("code") != 0:
        raise SystemExit(f"获取 tenant_access_token 失败：{_api_err(data)}（检查 app_id/app_secret）")
    token = str(data.get("tenant_access_token") or "")
    if not token:
        raise SystemExit("tenant_access_token 为空（接口返回异常）")
    log(f"已获取 tenant_access_token（有效期 {data.get('expire')} 秒）")
    return token


def _records_url(feishu: dict) -> str:
    return f"{FEISHU_HOST}/open-apis/base/v3/bases/{feishu['base_token']}/tables/{feishu['table_id']}/records"


def fetch_records(
    feishu: dict, mapping: dict, max_pages: int | None = None, extra_out: list[str] | None = None
) -> list[dict]:
    """全量拉取数据表记录 → 每行一个 dict（record_id + 英文键）。

    v3 records 接口：limit=PAGE_SIZE + offset 翻页，has_more=false 结束；翻页中途 token 失效
    自动重取一次并重试当前页，限流（99991400）自动退避重试；字段列表在翻页间变化
    （表格结构刚被改动）按异常中止；接口给的表格版本号（rev）在翻页间变化也中止——
    offset 翻页期间表格被编辑会静默漏行，宁可中止重跑。max_pages 只拉前 N 页（--check 体检用）；
    extra_out 传入时收集未映射的新增列名（由 run_sync 发一次飞书提醒用）。
    """
    url = _records_url(feishu)
    token = get_tenant_token(feishu["app_id"], feishu["app_secret"])
    fields: list[str] | None = None
    rev: object = None
    rev_seen = False
    record_ids: list[str] = []
    seen_ids: set[str] = set()
    raw_rows: list[list] = []
    offset = 0
    page = 1
    token_refreshed = False
    rate_tries = 0
    while True:
        try:
            data = request_json(
                "GET",
                url,
                f"拉取记录第 {page} 页",
                params={"limit": PAGE_SIZE, "offset": offset},
                headers={"Authorization": f"Bearer {token}"},
            )
        except ApiHttpError as exc:
            if exc.status == 401 and not token_refreshed:
                token_refreshed = True
                log("  access token 已失效，重新获取后重试本页")
                token = get_tenant_token(feishu["app_id"], feishu["app_secret"])
                continue
            raise SystemExit(f"拉取记录失败：HTTP {exc.status}：{redact(exc.body)}") from None
        if not isinstance(data, dict):
            raise SystemExit("拉取记录失败：接口返回不是 JSON 对象")
        if data.get("code") != 0:
            code = data.get("code")
            if code in TOKEN_ERROR_CODES and not token_refreshed:
                token_refreshed = True
                log(f"  access token 已失效（code={code}），重新获取后重试本页")
                token = get_tenant_token(feishu["app_id"], feishu["app_secret"])
                continue
            if code in RATE_LIMIT_CODES and rate_tries < RATE_LIMIT_ATTEMPTS:
                rate_tries += 1
                log(f"  接口限流（code={code}），{RATE_LIMIT_WAIT}s 后重试本页（{rate_tries}/{RATE_LIMIT_ATTEMPTS}）")
                time.sleep(RATE_LIMIT_WAIT)
                continue
            raise SystemExit(f"拉取记录失败：code={code} msg={data.get('msg')}")

        payload = data.get("data") or {}
        # 表格版本号一致性：offset 翻页期间被删行会静默漏数据，页间 rev 变了就中止
        page_rev = payload.get("rev")
        if rev_seen:
            if rev is not None and page_rev is not None and page_rev != rev:
                raise SystemExit(
                    f"翻页期间表格内容发生变化（rev {rev} → {page_rev}）：offset 翻页可能漏行，"
                    f"本次快照不可信，已中止；请稍后重跑"
                )
        else:
            rev = page_rev
            rev_seen = True
        page_fields = [str(item) for item in (payload.get("fields") or [])]
        if fields is None:
            fields = page_fields
        elif page_fields != fields:
            raise SystemExit("翻页期间字段列表发生变化（表格结构可能刚被改动），已中止，请重跑")
        page_rows = [list(row) for row in (payload.get("data") or [])]
        page_ids = [str(item) for item in (payload.get("record_id_list") or [])]
        if len(page_rows) != len(page_ids):
            raise SystemExit(f"第 {page} 页行数与记录 ID 数不一致（{len(page_rows)} ≠ {len(page_ids)}），已中止")
        # 记录 ID 全局唯一：出现重复说明接口忽略了 offset（重复返回同一页）或翻页窗口重叠，
        # 这样兜底能立刻中止，而不是白翻 MAX_PAGES 页后才发现是死循环
        overlap = seen_ids.intersection(page_ids)
        if overlap:
            raise SystemExit(f"第 {page} 页出现已拉取过的记录（如 {sorted(overlap)[0]}），接口返回异常，已中止")
        seen_ids.update(page_ids)
        raw_rows += page_rows
        record_ids += page_ids
        # 翻页日志节流：小表每页都打，大表每 20 页打一次（末页必打），调度日志不刷屏
        if page <= 10 or page % 20 == 0 or not payload.get("has_more"):
            log(f"  第 {page} 页：{len(page_rows)} 行（累计 {len(raw_rows)}）")

        if max_pages is not None and page >= max_pages:
            break
        if not payload.get("has_more"):
            break
        if not page_rows:
            raise SystemExit(f"第 {page} 页为空但仍标记 has_more，接口返回异常，已中止")
        offset += len(page_rows)
        page += 1
        rate_tries = 0
        if page > MAX_PAGES:
            raise SystemExit(f"翻页超过 {MAX_PAGES} 页，已中止")

    return build_records(fields or [], record_ids, raw_rows, mapping, extra_out=extra_out)


def build_records(
    fields: list[str],
    record_ids: list[str],
    raw_rows: list[list],
    mapping: dict,
    extra_out: list[str] | None = None,
) -> list[dict]:
    """列式行 + 记录 ID → 记录 dict：{record_id, 英文键: 原值, ...}。

    - 映射里的 Base 列名找不到（被改名/删除）→ 直接报错（宁可中止也不静默丢列）；
    - 未映射的 Base 列 → 忽略并打一条告警（提示新列，需要就加到 fields 里）；
      extra_out 传入时同时收集列名，由 run_sync 发一次飞书提醒（人工决定是否加映射）；
    - 值原样保留（含 $ 千分位、日期 ISO 串等），清洗留给下游 DWD。
    """
    index = {name: i for i, name in enumerate(fields)}
    if fields:
        # 空表也要校验映射：接口对空表可能不给字段列表，给了就说明列名可用，映射错了必须报错
        missing = [source for source in mapping if source not in index]
        if missing:
            raise SystemExit("Base 里找不到 fields 映射的列：" + "、".join(missing) + "（列名可能被改名/删除，请核对）")
        extra = [name for name in fields if name and name not in mapping]
        if extra:
            log(f"  警告：Base 里有 {len(extra)} 个列未映射、已忽略：{'、'.join(extra)}")
            if extra_out is not None:
                extra_out.extend(extra)
    if not raw_rows and not record_ids:
        return []
    if len(record_ids) != len(raw_rows):
        raise SystemExit(f"记录数与行数不一致（{len(record_ids)} ≠ {len(raw_rows)}），已中止")
    if not fields:
        raise SystemExit("接口返回了记录但没给字段列表，无法把值映射成英文键，已中止")
    records: list[dict] = []
    for record_id, row in zip(record_ids, raw_rows):
        record: dict = {"record_id": record_id}
        for source, target in mapping.items():
            i = index[source]
            record[target] = row[i] if i < len(row) else None
        records.append(record)
    return records


def fetch_field_sample(feishu: dict) -> tuple[list[str], dict]:
    """拉第一页记录 → (字段名列表, {字段名: 第一条样例值})；--init 向导展示列名用。"""
    token = get_tenant_token(feishu["app_id"], feishu["app_secret"])
    data = request_json(
        "GET",
        _records_url(feishu),
        "拉取字段样例",
        params={"limit": PAGE_SIZE, "offset": 0},
        headers={"Authorization": f"Bearer {token}"},
    )
    if not isinstance(data, dict) or data.get("code") != 0:
        raise SystemExit(f"拉取字段列表失败：{_api_err(data)}")
    payload = data.get("data") or {}
    fields = [str(item) for item in (payload.get("fields") or [])]
    samples: dict[str, str] = {}
    rows = payload.get("data") or []
    if rows:
        first = list(rows[0])
        for i, name in enumerate(fields):
            if i < len(first) and first[i] is not None:
                samples[name] = str(first[i])[:60]
    return fields, samples


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


def freshness_problem(records: list[dict], date_field: str, expected: str) -> tuple[str, str | None] | None:
    """新鲜度校验：预期日期（业务日 - lag_days）没出现时返回 (预期, 当前最新)，否则 None。"""
    seen = {normalize_date_value(record.get(date_field)) for record in records}
    seen.discard(None)
    if expected in seen:
        return None
    return expected, (max(seen) if seen else None)


def notify(webhook: str, title: str, lines: list[str], footer: str = "") -> None:
    """发飞书群卡片消息；未配 webhook 或发送失败只记日志（不影响主流程退出码）。"""
    if not webhook:
        log("  警告：未配置 freshness.webhook，跳过飞书告警")
        return
    if requests is None:
        log("  警告：缺少 requests，跳过飞书告警（pip install requests）")
        return
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}],
        },
    }
    if footer:
        card["card"]["elements"].append({"tag": "hr"})
        card["card"]["elements"].append({"tag": "note", "elements": [{"tag": "plain_text", "content": footer}]})
    try:
        resp = requests.post(webhook, json=card, timeout=15)
        data = resp.json()
    except Exception as exc:  # noqa: BLE001 - 告警失败不该改变主流程的退出码
        log(f"  警告：飞书通知发送失败：{type(exc).__name__}: {exc}")
        return
    if resp.status_code == 200 and isinstance(data, dict) and data.get("code", data.get("StatusCode", 0)) == 0:
        log("飞书通知已发送")
    else:
        log(f"  警告：飞书通知发送失败：HTTP {resp.status_code} {str(data)[:200]}")


# =============================================================================
# MaxCompute：建表 / 结构校验 / 写分区（临时分区 + 原子替换）/ 写后校验
# =============================================================================
def build_ddl(project: str, table: str, column: str, comment: str) -> str:
    """目标表 DDL：单列 string + pt 分区 + 表注释（标识符已在 validate_job 校验过）。"""
    table_comment = (comment or "飞书多维表格记录 JSON 原样落库（写临时分区后原子替换 pt=<业务日>）").replace("'", "''")
    return (
        f"create table if not exists {project}.{table} (\n"
        f"    {column} string comment '多维表格记录 JSON（英文键，值原样）'\n"
        f")\n"
        f"partitioned by ({PARTITION_COLUMN} string comment '业务日期 yyyyMMdd（该分区 = 当天抽取的全量快照）')\n"
        f"tblproperties ('comment' = '{table_comment}')"
    )


def connect_odps(maxcompute: dict, project: str):
    """按 job 的 AK/SK 连 MaxCompute。"""
    if ODPS is None:
        raise SystemExit("缺少 pyodps：pip install pyodps")
    endpoint = maxcompute.get("endpoint") or DEFAULT_ENDPOINT
    return ODPS(maxcompute["access_key_id"], maxcompute["access_key_secret"], project, endpoint=endpoint)


def ensure_table(o, project: str, table: str, column: str, comment: str):
    """不存在则建表；存在返回表对象（结构校验由 verify_schema 负责）。"""
    o.execute_sql(build_ddl(project, table, column, comment))
    return o.get_table(table)


def verify_schema(table, table_name: str, column: str) -> None:
    """写库前校验表结构 = 单列 string + pt 分区，不一致直接报错（绝不先清表）。"""
    if getattr(table, "is_virtual_view", False) or getattr(table, "is_materialized_view", False):
        raise SystemExit(f"{table_name} 是视图，不能作为写入目标")
    if getattr(table, "is_transactional", False):
        raise SystemExit(f"{table_name} 是事务表，本工具按「写普通分区」处理；请换表名或先重建")
    # pyodps 的 table_schema.columns 会把分区列一起列出来，比较前先剔除分区列（api2ods 同款处理）
    part_names = {str(part.name).lower() for part in table.table_schema.partitions}
    actual_cols = [
        (str(col.name).lower(), str(col.type).lower())
        for col in table.table_schema.columns
        if str(col.name).lower() not in part_names
    ]
    actual_parts = [(str(part.name).lower(), str(part.type).lower()) for part in table.table_schema.partitions]
    if actual_cols != [(column.lower(), "string")] or actual_parts != [(PARTITION_COLUMN, "string")]:
        raise SystemExit(
            f"{table_name} 表结构与工具要求不一致，拒绝写入（先人工核对，避免清错表）。\n"
            f"  要求：单列 [{column} string] + 分区 [{PARTITION_COLUMN} string]\n"
            f"  实际：列 {actual_cols}，分区 {actual_parts or '无'}"
        )


def _sql_spec(spec: str) -> str:
    """把 pyodps 风格分区串（pt=20260928）转成 DDL 里的带引号写法（pt='20260928'）。"""
    key, _, value = spec.partition("=")
    return f"{key}='{value}'" if key and value else spec


def rename_partition(o, project: str, table_name: str, old_spec: str, new_spec: str) -> None:
    """把临时分区改名为正式分区（MaxCompute DDL 元数据操作；目标分区必须不存在）。

    「删旧分区 + rename」之间只有两条 DDL 的空窗，远比「先删再填」整段写入短；
    写入期间旧快照一直可读，下游不会读到空/半截分区。
    """
    o.execute_sql(
        f"alter table {project}.{table_name} "
        f"partition ({_sql_spec(old_spec)}) rename to partition ({_sql_spec(new_spec)})"
    )


def write_partition(o, table, project: str, table_name: str, column: str, pt: str, records: list[dict]) -> None:
    """写一个分区：先写 <pt>__tmp 临时分区并核对内容，再删旧分区 + rename 原子替换。

    - 可见窗口只剩两条 DDL 之间：写入期间旧快照完整可读；
    - 写前先通读检查单行大小（不保存）：超长记录永远写不进去，必须在动分区之前报错；
    - 写后核对：行数 + record_id 去重数 + 最小/最大 id（不一致就不替换，重试同上）；
    - 重试会从「清临时分区」重新开始；失败后尽力清掉临时分区，残留（如进程被强杀）下次运行也会先清；
    - 空快照（target.allow_empty=true）走同样的替换流程（清空正式分区）；否则上游已拦下空表。
    """
    ids = [str(record.get("record_id") or "") for record in records]
    if records and not all(ids):
        raise SystemExit("有记录缺少 record_id，无法做写后核对（检查 Base 接口返回）")
    final_spec = f"{PARTITION_COLUMN}={pt}"
    tmp_spec = f"{PARTITION_COLUMN}={pt}{TMP_PARTITION_SUFFIX}"
    for index, record in enumerate(records, 1):
        try:
            line = dump_record(record)
        except ValueError as exc:
            raise SystemExit(f"第 {index} 条记录无法序列化为 JSON：{exc}（值里可能含 NaN/Infinity）") from None
        size = len(line.encode("utf-8"))
        if size > MAX_ROW_BYTES:
            raise SystemExit(
                f"第 {index} 条记录 JSON {size:,} 字节，超过单列上限（约 {MAX_ROW_BYTES:,} 字节）；"
                f"检查 Base 里是否有超大单元格（如附件/长文本）"
            )

    final_deleted = False

    def once() -> None:
        nonlocal final_deleted
        table.delete_partition(tmp_spec, if_exists=True)  # 清掉上次失败留下的临时分区
        table.create_partition(tmp_spec, if_not_exists=True)
        with table.open_writer(partition=tmp_spec, reopen=True) as writer:
            for start in range(0, len(records), BATCH_SIZE):
                writer.write([[dump_record(record)] for record in records[start : start + BATCH_SIZE]])
        actual, distinct, smallest, largest = verify_partition(
            o, project, table_name, column, f"{pt}{TMP_PARTITION_SUFFIX}"
        )
        if actual != len(records):
            raise RuntimeError(f"临时分区行数不一致：计划 {len(records):,} 行，实际 {actual:,} 行")
        if records:
            # 空快照没有 id 可核：只校验行数，去重/范围检查跳过
            if distinct != actual:
                raise RuntimeError(f"临时分区 record_id 重复：{actual:,} 行里去重后只剩 {distinct:,} 个")
            if (smallest, largest) != (min(ids), max(ids)):
                raise RuntimeError(
                    f"临时分区 record_id 范围不一致：实际 [{smallest}, {largest}]，预期 [{min(ids)}, {max(ids)}]"
                )
        # 原子替换：旧分区先让位，临时分区立刻改名顶上（中间空窗只有一条 DDL 的执行时间）。
        # 置位放在删之前：删除请求可能已经发到服务端才报错，宁可把后果描述得保守一点
        final_deleted = True
        table.delete_partition(final_spec, if_exists=True)
        rename_partition(o, project, table_name, tmp_spec, final_spec)

    last: Exception | None = None
    for attempt in range(1, WRITE_ATTEMPTS + 1):
        try:
            once()
            return
        except Exception as exc:  # noqa: BLE001 - 统一重试并给出「重跑可修复」的结论
            last = exc
            if attempt < WRITE_ATTEMPTS:
                log(f"  分区 {final_spec} 写入第 {attempt} 次失败：{redact(exc)}；{WRITE_RETRY_DELAY}s 后重试")
                time.sleep(WRITE_RETRY_DELAY)
    # 尽力清掉临时分区：留在表里的 tmp 值（如 20260928__tmp）字符串序大于正式分区，下游
    # 用 max_pt() 取最新分区时可能读到半成品；清理失败只多打一条告警，不改变失败结论
    tmp_cleaned = True
    try:
        table.delete_partition(tmp_spec, if_exists=True)
    except Exception as exc:  # noqa: BLE001 - 清理是尽力而为
        tmp_cleaned = False
        log(f"  警告：清理临时分区 {tmp_spec} 失败：{redact(exc)}")
    leftover = "" if tmp_cleaned else f"临时分区 {tmp_spec} 残留，"
    if final_deleted:
        raise SystemExit(
            f"{table_name} {final_spec} 写入失败（已尝试 {WRITE_ATTEMPTS} 次；正式分区可能已被删掉，"
            f"{leftover}重跑本作业即可恢复）：{redact(last)}"
        )
    raise SystemExit(
        f"{table_name} {final_spec} 写入失败（已尝试 {WRITE_ATTEMPTS} 次；正式分区未动，"
        f"{leftover}重跑本作业即可修复）：{redact(last)}"
    )


def count_partition(o, project: str, table_name: str, pt: str) -> int:
    """写后行数核对：select count(*)（与拉取条数不一致按失败处理）。"""
    sql = f"select count(*) as cnt from {project}.{table_name} where {PARTITION_COLUMN} = '{pt}'"
    with o.execute_sql(sql).open_reader() as reader:
        for row in reader:
            return int(row["cnt"])
    return 0


def purge_stale_tmp_partitions(table, table_name: str) -> None:
    """清掉历史失败残留的 __tmp 临时分区（正式写入之前调用）。

    残留的 tmp 分区值（如 20260927__tmp）在字符串序上大于同日期正式分区，会让下游
    用 max_pt() 时读到半成品；每次正式运行前统一清一遍，避免一次中断长时间污染下游。
    """
    for part in list(table.partitions):
        spec = str(part.name)
        if TMP_PARTITION_SUFFIX in spec:
            log(f"  清理残留临时分区：{table_name} {spec}")
            table.delete_partition(spec, if_exists=True)


def verify_partition(o, project: str, table_name: str, column: str, pt: str) -> tuple[int, int, str, str]:
    """核对分区内容（汇总级，不逐条比对）：行数、record_id 去重数、record_id 最小/最大值。

    三件套能覆盖漏行、重复、整段错写（Tunnel 是分批原子提交，配合足够）；逐条比对内容对大表成本太高，不做。
    返回 (行数, 去重 id 数, 最小 id, 最大 id)，空分区时后两项为空串。
    """
    sql = (
        f"select count(*) as cnt, count(distinct rid) as ucnt, min(rid) as mn, max(rid) as mx from ("
        f"select get_json_object({column}, '$.record_id') as rid "
        f"from {project}.{table_name} where {PARTITION_COLUMN} = '{pt}')"
    )
    with o.execute_sql(sql).open_reader() as reader:
        for row in reader:
            return (
                int(row["cnt"]),
                int(row["ucnt"]),
                "" if row["mn"] is None else str(row["mn"]),
                "" if row["mx"] is None else str(row["mx"]),
            )
    return (0, 0, "", "")


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


# =============================================================================
# 交互式建配置（--init）：生成新的 jobs/*.json
# =============================================================================
def _ask(ask, prompt: str, default: str = "") -> str:
    """问一个问题；空输入用默认值（与 api2ods 向导同款）。"""
    hint = f"（默认 {default}）" if default else ""
    return str(ask(f"{prompt}{hint}：") or "").strip() or default


def _split_base_ref(raw: str) -> tuple[str, str]:
    """把用户粘贴的 Base 链接拆成 (base_token, table_id)；不是链接就当裸 token。"""
    text = raw.strip()
    match = re.search(r"/base/([A-Za-z0-9]+)", text)
    base_token = match.group(1) if match else text
    table_match = re.search(r"[?&]table=(tbl[A-Za-z0-9]+)", text)
    return base_token, (table_match.group(1) if table_match else "")


def run_init(
    out_path: str = "",
    ask=input,
    echo=print,
    workdir: pathlib.Path | None = None,
    fetch_fields=None,
    ask_secret=None,
) -> int:
    """交互式生成作业配置，返回退出码（0 成功 / 1 取消）。

    fetch_fields(feishu) -> (fields, samples)：默认走真实接口拉第一页做列名/样例展示；
    ask_secret(prompt) -> str：密钥类输入默认用 getpass（不回显）；单元测试都可注入假实现（离线跑）。
    """
    root = pathlib.Path(workdir) if workdir else pathlib.Path.cwd()
    fetch_fields = fetch_fields or fetch_field_sample
    ask_secret = ask_secret or _default_ask_secret

    echo("=== feishu2ods 配置向导（直接回车用默认值；随时 Ctrl+C 取消）===")

    # ① 作业名
    job_name = _ask(ask, "① 作业名（英文/数字/下划线，用于文件名和日志）", "feishu_table")
    safe_name = re.sub(r"[^0-9A-Za-z_\-]", "_", job_name).strip("_") or "feishu_table"
    if safe_name != job_name:
        echo(f"   提示：特殊字符已替换为下划线，文件名用 {safe_name}")
    job_name = safe_name
    out = pathlib.Path(out_path) if out_path else root / "jobs" / f"{job_name}.json"
    if out.exists():
        # 防手滑覆盖生产作业（里面有密钥）：默认不覆盖，想覆盖要显式答 y
        answer = _ask(ask, f"⚠️ {out} 已存在，覆盖吗？（y/n）", "n").lower()
        if answer not in ("y", "yes", "1"):
            echo("已取消（原文件未动）。")
            return 1

    # ② Base 链接 / token
    base_token = table_id = ""
    base_url = ""
    for _ in range(3):
        raw = _ask(ask, "② 多维表格链接（直接粘贴整条 URL，或只填 base_token）")
        if not raw:
            echo("   不能为空。")
            continue
        if "/wiki/" in raw:
            echo("   wiki 知识库链接里是 wiki 节点 token、不是 base_token；请在浏览器里打开该表格后复制 /base/ 开头的链接，或直接填 base_token。")
            continue
        base_token, table_id = _split_base_ref(raw)
        if not base_token or "/" in base_token or " " in base_token:
            echo("   链接/token 看起来不对，示例：https://xxx.feishu.cn/base/IC4xxxxxxxx")
            continue
        url_match = re.search(r"(https?://[^/]+/base/[A-Za-z0-9]+)", raw)
        base_url = url_match.group(1) if url_match else ""
        if not table_id:
            table_id = _ask(ask, "   链接里没带 table 参数，请粘贴数据表 ID（tbl 开头）")
        if not re.match(r"\Atbl[A-Za-z0-9]+\Z", table_id or ""):
            echo("   数据表 ID 应以 tbl 开头，示例：tbleAbCdEfGh123456")
            continue
        break
    else:
        echo("❌ 连续三次无效，已取消。")
        return 1

    # ③ 应用凭证
    echo("   提示：应用需要先被加为这张表格的「可阅读」协作者。")
    app_id = ""
    for _ in range(3):
        app_id = _ask(ask, "③ 飞书自建应用 app_id（cli_ 开头）")
        if re.match(r"\Acli_[A-Za-z0-9]+\Z", app_id):
            break
        echo("   app_id 应以 cli_ 开头。")
    else:
        echo("❌ app_id 连续三次无效，已取消。")
        return 1
    app_secret = _ask(ask_secret, "   App Secret（输入不回显）")
    if not app_secret:
        echo("❌ App Secret 不能为空，已取消。")
        return 1

    # ④ 拉列名和样例
    feishu = {"app_id": app_id, "app_secret": app_secret, "base_token": base_token, "table_id": table_id}
    if base_url:
        feishu["base_url"] = base_url  # 告警卡片里带表格链接
    echo("   正在拉取表格列名 ...")
    try:
        fields, samples = fetch_fields(feishu)
    except SystemExit as exc:
        echo(f"❌ 拉取失败：{exc}")
        echo("   请确认：应用已加为协作者、app_id/app_secret 正确、table_id 正确；然后重新运行 --init")
        return 1
    except Exception as exc:  # noqa: BLE001 - 向导里的任何失败都给一条人话，不给裸堆栈
        echo(f"❌ 拉取失败（未预期错误）：{type(exc).__name__}: {exc}")
        echo("   请检查网络与凭证后重新运行 --init")
        return 1
    if not fields:
        echo("❌ 表格没有列（或接口没返回字段列表），请检查后重试。")
        return 1
    echo(f"   共 {len(fields)} 列：" + "、".join(fields[:10]) + ("……" if len(fields) > 10 else ""))

    # ⑤ 列名 → 英文键
    echo("")
    echo("⑤ 给要同步的列起英文键（JSON 键）；不需要的列直接回车跳过；至少映射一列。")
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for field in fields:
        sample = samples.get(field, "")
        if sample:
            echo(f"   「{field}」样例：{sample}")
        for _ in range(3):
            key = _ask(ask, f"   「{field}」的英文键（回车跳过）")
            if not key:
                break
            if not IDENT_RE.match(key):
                echo("   英文键须为 字母/数字/下划线、字母开头。")
                continue
            if key == "record_id":
                echo("   record_id 是保留键（记录 ID 自动输出），换一个。")
                continue
            if key in used:
                echo(f"   英文键 {key!r} 已用过，换一个。")
                continue
            mapping[field] = key
            used.add(key)
            break
        else:
            echo("   连续三次没填对，跳过该列。")
    if not mapping:
        echo("❌ 一列都没映射，已取消。")
        return 1

    # ⑥ 目标表（项目和表名会拼进 DDL，就地校验、错三次才取消，不用等最后一步）
    for _ in range(3):
        project = _ask(ask, "⑥ MaxCompute 项目", "my_project")
        if IDENT_RE.match(project):
            break
        echo("   项目名只能是 字母/数字/下划线、字母开头。")
    else:
        echo("❌ 项目名连续三次无效，已取消。")
        return 1
    default_table = f"ods_{job_name.replace('-', '_')}_json_df"  # 表名不允许 '-'，默认值先换掉
    for _ in range(3):
        table_name = _ask(ask, "   目标表名", default_table)
        if IDENT_RE.match(table_name):
            break
        echo("   表名只能是 字母/数字/下划线、字母开头。")
    else:
        echo("❌ 表名连续三次无效，已取消。")
        return 1
    comment = _ask(ask, "   表注释", "飞书多维表格同步（JSON 原样，写临时分区后原子替换 pt=业务日）")

    # ⑦ 阿里云凭证
    echo("⑦ MaxCompute 凭证（与其它作业相同即可）")
    ak = sk = ""
    for _ in range(3):
        ak = _ask(ask, "   access_key_id（LTAI 开头）")
        sk = _ask(ask_secret, "   access_key_secret（输入不回显）")
        if ak and sk:
            break
        echo("   两个都不能为空。")
    else:
        echo("❌ access_key_id / access_key_secret 连续三次为空，已取消。")
        return 1

    # ⑧ 新鲜度
    freshness = None
    need = _ask(ask, "⑧ 要不要「缺少业务日数据」飞书告警？（y/n）", "y").lower()
    if need in ("y", "yes", "1"):
        keys = list(mapping.values())
        date_field = ""
        for _ in range(3):
            date_field = _ask(ask, f"   用哪个键判日期（可选：{', '.join(keys)}）", keys[0])
            if date_field in keys:
                break
            echo("   必须是刚映射过的英文键之一。")
        else:
            date_field = keys[0]  # 三次没填对就用第一个键，不让整个向导白跑
            echo(f"   连续三次没填对，改用 {date_field}")
        webhook = ""
        for _ in range(3):
            webhook = _ask(ask, "   告警 webhook（飞书群机器人地址，可留空后补）")
            if not webhook or webhook.startswith(("http://", "https://")):
                break
            echo("   webhook 必须以 http(s):// 开头（或直接回车留空）。")
        else:
            webhook = ""
            echo("   连续三次格式不对，先留空（之后可在 job 文件里补 freshness.webhook）。")
        freshness = {"date_field": date_field, "lag_days": 0}
        if webhook:
            freshness["webhook"] = webhook

    job = {
        "job": job_name,
        "description": f"飞书多维表格 {base_token}/{table_id} → {project}.{table_name}（json + pt 分区，临时分区 + 原子替换）",
        "feishu": feishu,
        "maxcompute": {"project": project, "endpoint": DEFAULT_ENDPOINT, "access_key_id": ak, "access_key_secret": sk},
        "fields": mapping,
        "target": {
            "project": project,
            "table": table_name,
            "column": DEFAULT_COLUMN,
            "comment": comment,
            "allow_empty": False,
        },
    }
    if freshness:
        job["freshness"] = freshness

    # 先按校验器过一遍（就地规范化：去空白等），避免生成一份跑不起来的配置
    try:
        validate_job(job)
    except SystemExit as exc:
        echo(f"❌ 生成的配置没过校验（{exc}）；请重新运行 --init")
        return 1

    out = pathlib.Path(out_path) if out_path else root / "jobs" / f"{job_name}.json"
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        # newline="\n"：生成的文件固定 LF，Windows 上跑出来的也能直接给服务器用
        out.write_text(json.dumps(job, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    except OSError as exc:
        echo(f"❌ 写文件失败：{exc}")
        return 1
    echo("")
    echo(f"✅ 已生成：{out}")
    echo(f"   下一步：python feishu2ods.py --job {out} --check      # 体检（不写库）")
    echo(f"           python feishu2ods.py --job {out} --dry-run    # 试跑（不写库）")
    return 0


# =============================================================================
# 主流程：拉数 → 新鲜度校验 → 写 pt 分区（临时分区 + 原子替换）→ 行数核对
# =============================================================================
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="feishu2ods",
        description="飞书多维表格 → MaxCompute ODS（JSON 原样落库，json + pt 分区，临时分区 + 原子替换）",
    )
    parser.add_argument("--job", default="", help="job 配置文件（jobs/*.json）；--init 时可不填")
    parser.add_argument("--init", action="store_true", help="交互式生成作业配置（接新表用；生成后先 --check）")
    parser.add_argument("--init-out", default="", help="--init 的输出路径（默认 jobs/<作业名>.json）")
    parser.add_argument("--check", action="store_true", help="体检：配置 + API 连通 + 字段映射 + 目标表结构（不写库、不发告警）")
    parser.add_argument(
        "--bizdate",
        default="",
        help="业务日 pt（YYYYMMDD 或 YYYY-MM-DD）；默认取环境变量 bizdate/SKYNET_BIZDATE，再默认当天-1",
    )
    parser.add_argument("--dry-run", action="store_true", help="只拉数并打印统计，不写 MaxCompute")
    parser.add_argument(
        "--skip-freshness",
        "--no-check",
        dest="skip_freshness",
        action="store_true",
        help="跳过新鲜度校验（补数/排查用；--no-check 是旧写法，兼容保留）",
    )
    parser.add_argument("--no-notify", action="store_true", help="不发任何飞书通知（缺数据/新增列等只记日志）")
    parser.add_argument("--project", default="", help="覆盖 target.project（测试用）")
    parser.add_argument("--table", default="", help="覆盖 target.table（测试用）")
    parser.add_argument("--version", action="version", version=f"feishu2ods {VERSION}")
    return parser.parse_args(argv)


def run_check(job: dict, project: str, table_name: str, column: str, pt: str) -> int:
    """体检：配置 + API 连通 + 字段映射 + 目标表结构（不写库、不发告警、不校验新鲜度）。"""
    log("== 体检（--check，不写库、不发告警） ==")
    log(f"  映射字段 {len(job['fields'])} 个；目标 {project}.{table_name}（{column}，pt={pt}）")
    try:
        records = fetch_records(job["feishu"], job["fields"], max_pages=1)
    except SystemExit as exc:
        log(f"  ❌ 拉取/映射校验失败：{exc}")
        return 1
    log(f"  ✅ API 连通、字段映射通过：第 1 页 {len(records)} 条记录")
    try:
        o = connect_odps(job["maxcompute"], project)
        if not o.exist_table(table_name):
            log("  ⭕ 目标表不存在（正式运行时自动创建）")
        else:
            verify_schema(o.get_table(table_name), table_name, column)
            log(f"  ✅ 目标表结构符合（{column} string + pt 分区）")
    except SystemExit as exc:
        log(f"  ❌ {exc}")
        return 1
    except Exception as exc:  # noqa: BLE001 - 连不上/校验失败都给干净结论
        log(f"  ❌ 连接/校验失败：{redact(exc)}")
        return 1
    log("体检通过。")
    return 0


def run_sync(args, job: dict, project: str, table_name: str, column: str, pt: str, bizdate: date, started: float) -> int:
    """正式流程：拉数 → 空表/新鲜度校验 → 写 pt 分区（临时分区 + 原子替换）→ 行数核对。"""
    feishu = job["feishu"]
    maxcompute = job["maxcompute"]
    target_cfg = job["target"]

    # ---- ① 拉数 ----
    webhook = str((job.get("freshness") or {}).get("webhook") or "")
    base_url = str(feishu.get("base_url") or "")
    new_fields: list[str] = []
    records = fetch_records(feishu, job["fields"], extra_out=new_fields)
    log(f"拉取完成：{len(records):,} 条记录，映射 {len(job['fields'])} 个字段")
    if new_fields and not args.no_notify:
        uniq = sorted(set(new_fields))
        lines = [
            f"**作业**：{job.get('job')}",
            f"**新增列**：{'、'.join(f'`{name}`' for name in uniq)}",
        ]
        if base_url:
            lines.append(f"**数据表**：{base_url}")
        lines += [
            "本次已忽略新增列、其余字段照常同步（新增列数据暂未采集）。如需入库，请手动处理：",
            "① 作业 `fields` 补映射（Base 列名 → 英文键）；",
            "② 重跑本节点（写入幂等）。",
        ]
        notify(webhook, "飞书多维表格出现新增列", lines, footer=f"目标表 {project}.{table_name}")
    empty_rows = sum(
        1 for record in records if all(value is None for key, value in record.items() if key != "record_id")
    )
    if empty_rows:
        log(f"  警告：{empty_rows:,} 条记录除 record_id 外全为空（Base 里的空白行），已原样同步；下游按日期字段过滤")

    # ---- ② 空表 / 新鲜度校验（缺失 → 告警 + 非 0 退出，不写库）----
    allow_empty = bool(target_cfg.get("allow_empty", False))
    if not records and not allow_empty:
        log("❌ 拉取到 0 条记录，已中止（target.allow_empty=false，拒绝写入空分区）")
        if not args.no_notify:
            lines = [f"**作业**：{job.get('job')}", "**情况**：拉取到 0 条记录，未写入 MaxCompute"]
            if base_url:
                lines.append(f"**数据表**：{base_url}")
            notify(webhook, "飞书多维表格同步：0 条记录", lines, footer=f"目标表 {project}.{table_name}")
        return 1
    freshness = job.get("freshness")
    if freshness and not args.skip_freshness:
        lag_days = freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS)
        expected = (bizdate - timedelta(days=lag_days)).isoformat()
        problem = freshness_problem(records, freshness["date_field"], expected)
        if problem is not None:
            expected, latest = problem
            log(f"❌ 缺少 {expected} 的数据（当前最新 {latest or '无'}），本次不写库")
            if freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS):
                log(f"   预期日期 = 业务日 - {freshness.get('lag_days', DEFAULT_FRESHNESS_LAG_DAYS)} 天；"
                    f"如表格出数节奏不同，请调整 freshness.lag_days")
            else:
                log("   若表格本来就晚一天出数，可在 job 里把 freshness.lag_days 调成 1；补数可加 --skip-freshness")
            if not args.no_notify:
                lines = [
                    f"**作业**：{job.get('job')}",
                    f"**预期已有**：{expected}（{freshness['date_field']}）",
                    f"**当前最新**：{latest or '无'}",
                    f"**当前条数**：{len(records):,}",
                ]
                if base_url:
                    lines.append(f"**数据表**：{base_url}")
                notify(
                    webhook,
                    f"飞书表格同步缺少 {expected} 数据",
                    lines,
                    footer=f"目标表 {project}.{table_name} · 请检查飞书表格是否已更新，更新后重跑即可",
                )
            return 1
        dup_dates = [day for day, count in Counter(
            normalize_date_value(record.get(freshness["date_field"])) for record in records
        ).items() if day and count > 1]
        if dup_dates:
            log(f"  警告：以下日期在 Base 里出现多行：{'、'.join(sorted(dup_dates)[:10])}（DWD 同一天会落多行）")
        log(f"新鲜度校验通过：{freshness['date_field']} 已包含业务日 {expected}")

    # ---- ③ 写库（dry-run 跳过）----
    if args.dry_run:
        log(f"--dry-run：不写库；将把 {len(records):,} 行写进 {project}.{table_name} pt={pt}（写临时分区后原子替换）")
        return 0

    o = connect_odps(maxcompute, project)
    table = ensure_table(o, project, table_name, column, str(target_cfg.get("comment") or ""))
    verify_schema(table, table_name, column)
    purge_stale_tmp_partitions(table, table_name)
    log(f"写入 {project}.{table_name} pt={pt}（{len(records):,} 行：先写临时分区，核对后原子替换）...")
    write_partition(o, table, project, table_name, column, pt, records)
    actual = count_partition(o, project, table_name, pt)
    if actual != len(records):
        log(f"❌ 写后校验不一致：计划 {len(records):,} 行，实际 {actual:,} 行（重跑即可，写入幂等）")
        return 1
    log(f"完成：{project}.{table_name} pt={pt} 共 {actual:,} 行，耗时 {(time.time() - started) / 60:.1f} 分钟")
    return 0


def _wizard_ask(prompt: str = "") -> str:
    """向导的提问也走日志（带时间戳）；只记问题、不记回答（答案里可能有密钥）。"""
    log(prompt)
    try:
        return input()
    except EOFError:  # Ctrl+Z / Ctrl+D：按用户中断处理，走统一的 130 出口
        raise KeyboardInterrupt from None


def _default_ask_secret(prompt: str = "") -> str:
    """密钥输入：不回显；环境不支持隐藏输入时退回普通 input（照常可用）。"""
    try:
        return getpass.getpass(prompt)
    except Exception:  # noqa: BLE001 - 没有 tty 等场景退回普通输入
        return input(prompt)


def main(argv: list[str] | None = None) -> int:
    """命令行入口。返回退出码：0 成功 / 1 运行失败 / 2 参数问题 / 130 用户中断。"""
    setup_console()
    args = parse_args(argv)
    started = time.time()
    try:
        return _run(args, started)
    except SystemExit as exc:
        # 配置/运行类错误（ConfigError 风格）：走统一日志出口（带时间戳+脱敏）后退出
        if isinstance(exc.code, int):
            return exc.code
        log(f"❌ {redact(exc)}")
        return 1
    except KeyboardInterrupt:
        log("已中断（若在写入阶段中断，正式分区可能还没替换、临时分区可能残留；重跑同一命令即可，写入幂等）")
        return 130
    except Exception as exc:  # noqa: BLE001 - 未预期错误也给干净结论（附堆栈便于排查）
        log(f"❌ 运行失败（未预期错误）：{type(exc).__name__}: {redact(exc)}")
        log(redact(traceback.format_exc()))
        return 1


def _run(args, started: float) -> int:
    """一次运行的主体（--init / --check / --sync 三种模式的分发）。"""
    if args.init:
        return run_init(args.init_out, ask=_wizard_ask, echo=log)

    if not args.job:
        log("请用 --job 指定作业配置文件（第一次接新表可以先用 `python feishu2ods.py --init` 生成）")
        return 2

    job = load_job(args.job)
    # 先把 job 里疑似密钥的值登记进脱敏表，再校验/打日志：
    # 校验报错会回显非法值，密钥写错形态时也不该出现在日志里
    raw_feishu = job.get("feishu") if isinstance(job.get("feishu"), dict) else {}
    raw_maxcompute = job.get("maxcompute") if isinstance(job.get("maxcompute"), dict) else {}
    raw_freshness = job.get("freshness") if isinstance(job.get("freshness"), dict) else {}
    for secret in (
        raw_feishu.get("app_secret"),
        raw_maxcompute.get("access_key_secret"),
        raw_freshness.get("webhook"),
    ):
        if isinstance(secret, str) and len(secret) >= 6:
            _SECRETS.append(secret)

    for warning in validate_job(job):
        log(f"⚠️ {warning}")

    maxcompute = job["maxcompute"]
    target_cfg = job["target"]
    if args.project:
        args.project = _require_identifier(args.project, "--project")
    if args.table:
        args.table = _require_identifier(args.table, "--table")
    project = args.project or target_cfg.get("project") or maxcompute.get("project")
    table_name = args.table or target_cfg["table"]
    column = target_cfg.get("column") or DEFAULT_COLUMN
    bizdate = resolve_bizdate(args)
    pt = bizdate.strftime("%Y%m%d")

    log(
        f"作业 {job.get('job') or pathlib.Path(args.job).stem} 启动："
        + (f"{job['description']}；" if job.get("description") else "")
        + f"目标 {project}.{table_name}（{column}，pt={pt}：写临时分区后原子替换）"
    )

    if args.check:
        return run_check(job, project, table_name, column, pt)

    with RunLock(lock_path(pathlib.Path(args.job).resolve())):  # 同机同一作业互斥；不同作业可并行
        return run_sync(args, job, project, table_name, column, pt, bizdate, started)


if __name__ == "__main__":
    sys.exit(main())
