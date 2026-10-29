# -*- coding: utf-8 -*-
"""ODS 公共运行逻辑；唯一维护源为 api2ods/shared/runtime.py。

由 tools/sync_common.py 同步到各包的 _common.py，不依赖其它 ODS 项目或第三方包。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# 终端/日志控制字符：C0/C1、Unicode 行分隔符、零宽与 bidi 控制符都能伪造显示行
# 或做视觉伪装，外部文本展示前必须清掉
_CONTROL_CHARS_RE = re.compile("[\x00-\x1f\x7f-\x9f​-‏  ‪-‮⁦-⁩]")
_AUTH_SCHEMES = frozenset({"basic", "bearer", "token", "digest"})
DEFAULT_BIZDATE_TZ = "America/New_York"
BIZDATE_TZ_ENV = "ODS_BIZDATE_TZ"


def default_bizdate(now: datetime | None = None) -> date:
    """没传业务日时的统一默认值：业务日时区的昨天。

    时区取环境变量 ODS_BIZDATE_TZ，未设置时用 America/New_York。
    显式 --bizdate 与环境变量 bizdate/SKYNET_BIZDATE 仍由各项目优先处理。
    """
    name = (os.environ.get(BIZDATE_TZ_ENV) or "").strip() or DEFAULT_BIZDATE_TZ
    try:
        tz = ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise SystemExit(
            f"{BIZDATE_TZ_ENV} 时区不可用：{name!r}。请检查拼写；若名称正确，请安装时区数据库"
            f"（Windows/Linux 均可 pip install tzdata；Linux 也可装系统 tzdata 包）"
        ) from exc
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


def target_lock_path(project: str, table: str, root: Path | None = None) -> Path:
    """同一用户的三个工具、不同安装目录共用目标表锁；跨用户可显式配置共享目录。"""
    override = os.environ.get("ODS_TARGET_LOCK_DIR", "").strip()
    try:
        base = (
            Path(root)
            if root is not None
            else Path(override).expanduser()
            if override
            else private_lock_dir("ods-target")
        )
        base.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise SystemExit(f"目标表锁目录不可用：{exc}；请用 ODS_TARGET_LOCK_DIR 指定可写的固定目录") from exc
    # 不含 endpoint：同一项目的公网/内网入口也必须互斥；摘要避免长表名超过文件系统限制。
    key = f"{project.strip().lower()}.{table.strip().lower()}"
    return base / f"target-{hashlib.sha256(key.encode('utf-8')).hexdigest()[:32]}.lock"


def open_run_lock(path: Path):
    """打开运行锁；POSIX 的 setgid 目录显式启用组共享，其余目录保持 0600。"""

    def opener(name, flags):
        shared = bool(Path(name).parent.stat().st_mode & stat.S_ISGID)
        mode = 0o660 if shared else 0o600
        fd = os.open(name, flags | getattr(os, "O_NOFOLLOW", 0), mode)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise PermissionError(f"运行锁不是普通文件：{name}")
            if shared and info.st_uid == os.geteuid():
                # 不改全局 umask；兼容严格 umask，并由属主迁移遗留的 0600 锁。
                os.fchmod(fd, mode)
        except BaseException:
            os.close(fd)
            raise
        return fd

    return open(path, "a+", encoding="utf-8", errors="replace", opener=opener if os.name == "posix" else None)


def safe_label(value, limit: int = 80) -> str:
    """外部文本展示前去控制字符、转义反引号/Markdown 结构符并限长；不改变入库数据。"""
    text = _CONTROL_CHARS_RE.sub(" ", str(value)).replace("`", "'")
    # 外部值会被拼进 lark_md/日志：中和 Markdown 链接与 <at> 标签语法，避免
    # 告警卡片里出现可点击的钓鱼链接或 @所有人（全角字符仍可读，不影响数据）
    text = text.replace("[", "［").replace("]", "］").replace("<", "＜").replace(">", "＞")
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
    # split(None, 1) 按任意空白（tab、NBSP 等）切分；只认 ASCII 空格会漏掉
    # "Bearer\tabc123" 这类回显
    parts = value.split(None, 1)
    if len(parts) == 2 and parts[0].lower() in _AUTH_SCHEMES and parts[1].strip():
        return [value, parts[1].strip()]
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
    # 单个字符串不能被 join 拆成逐字符行；None 也不能让告警构造直接 TypeError
    if lines is None:
        lines = []
    elif isinstance(lines, str):
        lines = [lines]
    elif not isinstance(lines, (list, tuple)):
        lines = [lines]
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


class UnresolvedSqlError(SystemExit):
    """云端写操作尚未确认结束；不能被普通重试捕获。"""


def ensure_no_pending_publication(o, project: str, table: str) -> Path:
    """调用方持有目标表锁；只有确认旧实例已终止，才能清除持久化标记。"""
    path = target_lock_path(project, table).with_suffix(".pending.json")
    if not os.path.lexists(path):
        return path
    try:
        with open_run_lock(path) as handle:
            handle.seek(0)
            state = json.load(handle)
        instance_id = state.get("instance_id")
        if not isinstance(instance_id, str) or not instance_id:
            raise ValueError("未取得实例 ID，请按标记中的提交时间和 SQL 核对云端作业")
        if str(state.get("project", "")).lower() != project.lower():
            raise ValueError("标记中的项目与当前目标不一致")
        instance = o.get_instance(instance_id, project=state.get("instance_project") or project)
        if instance.is_terminated() is not True:
            raise ValueError(f"实例 {instance_id} 尚未确认终止")
        path.unlink()
    except Exception as exc:
        raise UnresolvedSqlError(
            f"{project}.{table} 有未决发布，已阻止覆盖：{exc}；标记 {path}。"
            "请先确认该目标的旧发布全部终止；无法自动查询时，人工核对后才可删除此标记并重跑"
        ) from exc
    return path


def _write_pending_publication(path: Path, state: dict, *, create: bool = False) -> None:
    """提交前独占创建并刷盘；实例 ID 更新采用原子替换，崩溃时仍保留阻断标记。"""
    mode = 0o660 if path.parent.stat().st_mode & stat.S_ISGID else 0o600
    temporary = None
    if create:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), mode)
    else:
        fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            if os.name == "posix":
                os.fchmod(handle.fileno(), mode)
            json.dump(state, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        if temporary is not None:
            os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _clear_pending_publication(path: Path | None, log) -> None:
    if path is not None:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            log(f"    已确认发布实例结束，但标记清理失败：{path}（{exc}）；下次运行会重新核对")


def run_sql_with_timeout(o, sql: str, timeout, desc: str, *, clock, log, heartbeat, publication=None):
    """写操作失败需确认实例终止；正式发布还持久化未决状态，阻止跨运行迟到覆盖。"""
    pending = state = None
    if publication is not None:
        project, table = publication
        pending = ensure_no_pending_publication(o, project, table)
        execution_project = getattr(o, "project", None)
        state = {
            "project": project,
            "instance_project": execution_project
            if isinstance(execution_project, str) and execution_project
            else project,
            "table": table,
            "instance_id": None,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "statement": sql,
        }
        _write_pending_publication(pending, state, create=True)
    started = clock.monotonic()
    instance = None
    mutation = publication is not None or re.match(r"\s*(create|alter|drop|insert|truncate)\b", sql, re.I) is not None
    try:
        instance = o.run_sql(sql)
        if pending is not None:
            instance_id = getattr(instance, "id", None)
            state["instance_id"] = instance_id if isinstance(instance_id, str) else None
            instance_project = getattr(getattr(instance, "project", None), "name", None)
            if isinstance(instance_project, str) and instance_project:
                state["instance_project"] = instance_project
            _write_pending_publication(pending, state)
        last_log = started
        while True:
            if instance.is_successful():
                _clear_pending_publication(pending, log)
                return instance
            if instance.is_terminated():
                instance.wait_for_success(timeout=1)
                if not instance.is_successful():
                    raise RuntimeError(f"{desc} 已终止但未成功（实例 {getattr(instance, 'id', '?')}）")
                _clear_pending_publication(pending, log)
                return instance
            now = clock.monotonic()
            if timeout and timeout > 0 and now - started > timeout:
                raise TimeoutError(f"{desc} 执行超过 {timeout} 秒，已请求停止")
            if heartbeat and now - last_log >= heartbeat:
                limit = f"{timeout} 秒" if timeout and timeout > 0 else "不限制"
                log(f"    {desc} 还在执行（已等待 {int(now - started)} 秒，超时阈值 {limit}）...")
                last_log = now
            clock.sleep(1)
    except BaseException as original:
        if instance is not None:
            try:
                instance.stop()
            except Exception as exc:
                log(f"    {desc} 取消失败（{type(exc).__name__}: {exc}），云端实例可能仍在运行")
        if not mutation:
            raise
        # stop() 只表示请求已受理；有限轮询确认终止后才允许上层重试。
        if instance is not None:
            for attempt in range(6):
                try:
                    terminated = instance.is_terminated() is True
                except Exception:
                    break
                if terminated:
                    _clear_pending_publication(pending, log)
                    raise
                if attempt < 5:
                    clock.sleep(1)
        instance_id = getattr(instance, "id", None) if instance is not None else None
        message = (
            f"{desc} 的写入状态未确认（实例 {instance_id or '未知'}），已停止自动重试；"
            + (f"未决发布标记 {pending}，后续发布将被阻止；" if pending is not None else "")
            + f"请先核对云端实例是否已结束后再重跑；原始错误：{original}"
        )
        if isinstance(original, (KeyboardInterrupt, SystemExit)):
            log(message)
            raise
        raise UnresolvedSqlError(message) from original
