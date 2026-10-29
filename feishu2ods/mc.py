# -*- coding: utf-8 -*-
"""MaxCompute：建表 / 结构校验 / 独立暂存与原子发布/ 写后核对。"""

from __future__ import annotations

import os
import re
import socket
import time
import uuid

from . import _common
from .config import IDENT_RE
from .spool import BATCH_SIZE, FetchStats, SpoolWriter
from .utils import log, redact

try:
    from odps import ODPS
except ImportError:  # pragma: no cover - 离线测试环境可以不带 pyodps
    ODPS = None

PARTITION_COLUMN = "pt"  # 分区字段：业务日 yyyyMMdd（该分区 = 当天抽取的全量快照）
WRITE_ATTEMPTS = 3  # 写分区的最大尝试次数（发布重试复用已核对暂存数据）
WRITE_RETRY_DELAY = 10  # 写入重试间隔秒数


# 本机主机名（与 tmp 分区命名同口径清洗）：残留清理靠它识别"这个 tmp 是不是本机进程在写"
_TMP_HOST = re.sub(r"[^A-Za-z0-9_]", "_", socket.gethostname() or "host")[:32] or "host"


def _tmp_run_id() -> str:
    """本进程的临时分区标记：主机名 + pid，避免两台机器共用同一个 __tmp 名互相覆盖。"""
    return f"{_TMP_HOST}_{os.getpid()}"


TMP_RUN_ID = _tmp_run_id()
TMP_PARTITION_SUFFIX = f"__tmp_{TMP_RUN_ID}"  # 写库用临时分区后缀（含本机本进程标记）
# 失败收尾时"已核对完整、可手工恢复"的保留分区分区后缀：带 __keep 标记后，
# 后续运行的残留清理（purge）会显式跳过它，唯一完整副本不会被下一次运行清掉
KEEP_PARTITION_SUFFIX = "__keep"
MAX_ROW_BYTES = 7_000_000  # 单行 JSON 上限（MaxCompute string 8MB，留余量）
SQL_TIMEOUT_SECONDS = 600  # 单条 SQL（建表 / 校验 / 暂存 / 发布 / 清理）最长等待秒数，0 = 不限制
SQL_HEARTBEAT_SECONDS = 30  # 长 SQL 的"还在执行"心跳日志间隔
# https：作业没写 endpoint 时 AK/SK 签名与查询结果不能走明文 HTTP
DEFAULT_ENDPOINT = "https://service.us-west-1.maxcompute.aliyun.com/api"
# 分区值白名单：pt 恒为 8 位业务日（用 [0-9] 而不是 \d：\d 还认全角/阿拉伯-印度数字），
# 写入期的临时分区在 pt 后带 __tmp 或 __tmp_<run> 后缀
_PT_RE = re.compile(r"\A[0-9]{8}\Z")
_TMP_PT_RE = re.compile(r"\A[0-9]{8}__tmp(?:_[A-Za-z0-9_]+)?\Z")


def _require_pt(pt, *, tmp_ok: bool = False) -> str:
    """分区值形态校验（写库、count、核对统一从这里过）。"""
    text = str(pt)
    if not (_PT_RE.match(text) or (tmp_ok and _TMP_PT_RE.match(text))):
        suffix = "（写入期可带 __tmp 或 __tmp_<主机_pid> 后缀）" if tmp_ok else ""
        raise SystemExit(f"分区值必须是 8 位业务日{suffix}：{text!r}")
    return text


def _require_identifier(value, where: str) -> str:
    """会拼进 DDL/SQL 的标识符（project/table/column）：白名单校验（与 config 同口径）。"""
    text = str(value)
    if not IDENT_RE.match(text):
        raise SystemExit(f"{where} 不是合法标识符（字母/数字/下划线，且不能以数字开头）：{text!r}")
    return text


def _partition_clause(pt) -> str:
    """拼进 SQL 的 `pt = '<值>'` 子句。

    MaxCompute 没有绑定参数，分区值只能拼进语句；白名单把"能拼什么"锁死
    （pt 恒为 8 位业务日、写入期临时分区带 __tmp 后缀），注入面归零。
    """
    return f"{PARTITION_COLUMN} = '{_require_pt(pt, tmp_ok=True)}'"


# =============================================================================
# SQL 执行（带超时：pyodps 默认不限时，云端卡住会一直干等、占着运行锁不放）
# =============================================================================
def run_sql_with_timeout(o, sql: str, timeout: int = SQL_TIMEOUT_SECONDS, desc: str = "SQL", *, publication=None):
    """通过公共实现等待 SQL，保留项目自身的时钟、脱敏日志与超时配置。"""
    return _common.run_sql_with_timeout(
        o,
        sql,
        timeout,
        desc,
        clock=time,
        log=lambda message: log(redact(message)),
        heartbeat=SQL_HEARTBEAT_SECONDS,
        publication=publication,
    )


# =============================================================================
# MaxCompute：建表 / 结构校验 / 独立暂存与原子发布/ 写后校验
# =============================================================================
def build_ddl(project: str, table: str, column: str, comment: str, *, staging: bool = False) -> str:
    """单列 string + pt 分区；暂存表额外设置分区与表元数据的生命周期回收。"""
    project = _require_identifier(project, "target.project")
    table = _require_identifier(table, "target.table")
    column = _require_identifier(column, "target.column")
    # comment 来自作业配置（用户可任意编辑）：反斜杠是 MaxCompute 字符串字面量里的转义符，
    # 只替换单引号不够——结尾的 "\" 会吃掉收尾引号、把后续内容当 SQL 解析。先转义反斜杠
    table_comment = comment or "飞书多维表格记录 JSON 原样落库（独立暂存表校验后原子发布 pt=<业务日>）"
    table_comment = table_comment.replace("\\", "\\\\").replace("'", "''")
    return (
        f"create table if not exists {project}.{table} (\n"
        f"    {column} string comment '多维表格记录 JSON（英文键，值原样）'\n"
        f")\n"
        f"partitioned by ({PARTITION_COLUMN} string comment '业务日期 yyyyMMdd（该分区 = 当天抽取的全量快照）')\n"
        f"tblproperties ('comment' = '{table_comment}'"
        + (", 'lifecycle.deletemeta'='true')\nlifecycle 1" if staging else ")")
    )


def connect_odps(maxcompute: dict, project: str):
    """按 job 的 AK/SK 连 MaxCompute。"""
    if ODPS is None:
        raise SystemExit("缺少 pyodps：pip install pyodps")
    endpoint = maxcompute.get("endpoint") or DEFAULT_ENDPOINT
    return ODPS(maxcompute["access_key_id"], maxcompute["access_key_secret"], project, endpoint=endpoint)


def ensure_table(o, project: str, table: str, column: str, comment: str, timeout: int = SQL_TIMEOUT_SECONDS):
    """不存在则建表；存在返回表对象（结构校验由 verify_schema 负责）。"""
    project = _require_identifier(project, "target.project")
    table = _require_identifier(table, "target.table")
    column = _require_identifier(column, "target.column")
    run_sql_with_timeout(o, build_ddl(project, table, column, comment), timeout=timeout, desc=f"建表 {table}")
    return o.get_table(f"{project}.{table}")


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
    """把 pyodps 风格分区串（pt=20260928）转成 DDL 里的带引号写法（pt='20260928'）。

    pyodps 的 partition.name 可能已带引号（pt='20260928'），统一先去掉再补引号，
    避免生成 pt=''20260928'' 这种非法写法。

    边界：当前只处理单级分区。多级分区（pt=x,region=y）会被整体当成一个值拼成
    pt='x,region=y'（非法），需要扩展。本工具的表结构被 verify_schema 强制为单列 pt，
    所有 spec 都由内部按 pt=<值> 构造，多级分支不可达——留此说明以免后人踩坑。
    """
    key, _, value = str(spec).partition("=")
    key = key.strip()
    if not IDENT_RE.match(key):
        raise SystemExit(f"分区字段名不是合法标识符：{key!r}（来自 {spec!r}）")
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    if not value:
        raise SystemExit(f"分区值不能为空：{spec!r}")
    # 防御性转义：spec 多数由内部按 pt=<值> 构造，但 purge_stale_tmp_partitions 的
    # spec 来自服务端的 partition.name——反斜杠/引号不能原样拼进 DDL
    escaped = value.replace("\\", "\\\\").replace("'", "''")
    return f"{key}='{escaped}'"


def drop_partition(o, project: str, table_name: str, spec: str, timeout: int = SQL_TIMEOUT_SECONDS) -> None:
    """删除分区（DDL，走超时保护）。

    走 DDL 而不是 pyodps 的 `table.delete_partition`（同步、不限时）：云端分区元数据操作
    卡住时同样会无限挂起、一直占着运行锁。`IF EXISTS` 提供与 `delete_partition(if_exists=True)`
    一致的幂等语义（分区不存在也不报错）。
    """
    project = _require_identifier(project, "target.project")
    table_name = _require_identifier(table_name, "target.table")
    run_sql_with_timeout(
        o,
        f"alter table {project}.{table_name} drop if exists partition ({_sql_spec(spec)})",
        timeout=timeout,
        desc=f"删分区 {table_name}",
    )


def add_partition(o, project: str, table_name: str, spec: str, timeout: int = SQL_TIMEOUT_SECONDS) -> None:
    """新增分区（DDL，走超时保护）。

    `IF NOT EXISTS` 提供与 `create_partition(if_not_exists=True)` 一致的幂等语义（分区已存在不报错）。
    """
    project = _require_identifier(project, "target.project")
    table_name = _require_identifier(table_name, "target.table")
    run_sql_with_timeout(
        o,
        f"alter table {project}.{table_name} add if not exists partition ({_sql_spec(spec)})",
        timeout=timeout,
        desc=f"建分区 {table_name}",
    )


def write_partition(
    o,
    table,
    project: str,
    table_name: str,
    column: str,
    pt: str,
    spool: SpoolWriter,
    stats: FetchStats | None = None,
    timeout: int = SQL_TIMEOUT_SECONDS,
) -> None:
    """独立暂存表 → 内容核对 → 单条 INSERT OVERWRITE 原子发布一个分区。

    兼容记录列表旧签名；重试复用已核对的暂存数据。失败保留暂存表一天，
    成功尽力清理；正式表中仅新增/覆盖业务日分区，不暴露临时分区。
    """
    project = _require_identifier(project, "target.project")
    table_name = _require_identifier(table_name, "target.table")
    _common.ensure_no_pending_publication(o, project, table_name)
    column = _require_identifier(column, "target.column")
    pt = _require_pt(pt)
    _temp_spool = None
    if stats is None:
        if not isinstance(spool, (list, tuple)):
            raise TypeError("write_partition 需要同时传 spool 与 stats（新签名）或记录列表（旧签名）")
        records = list(spool)
        spool = SpoolWriter()
        _temp_spool = spool  # 先归本函数管：下面写入抛错时也要保证被 close，否则临时文件泄漏
        try:
            stats = FetchStats()
            spool.write_records(records)
            stats.update(records)
        except BaseException:
            # 兼容分支的写入失败（磁盘满等）还没进下面的 try/finally，在这里先清理再上抛
            _temp_spool.close()
            _temp_spool = None
            raise
    try:
        if stats.distinct_ids() != stats.count:
            raise SystemExit("有记录的 record_id 重复或缺失，拒绝写库（检查 Base 接口返回）")
        if stats.count and stats.min_id in (None, ""):
            # 显式判空：不能写真值判断（record_id 恰为 0 这类 falsy 合法值会被误判成缺失）
            raise SystemExit("有记录缺少 record_id，无法做写后核对（检查 Base 接口返回）")
        final_spec = f"{PARTITION_COLUMN}={pt}"
        tmp_spec = f"{PARTITION_COLUMN}={pt}{TMP_PARTITION_SUFFIX}"
        for index, line in enumerate(spool.iter_rows(), 1):
            size = len(line.encode("utf-8"))
            if size > MAX_ROW_BYTES:
                raise SystemExit(
                    f"第 {index} 条记录 JSON {size:,} 字节，超过单列上限（约 {MAX_ROW_BYTES:,} 字节）；"
                    f"检查 Base 里是否有超大单元格（如附件/长文本）"
                )

        # 独立暂存表不参与正式表的 MAX_PT / 全分区扫描；生命周期兜底回收强杀残留。
        stage_name = f"tmp_feishu2ods_{uuid.uuid4().hex}"
        run_sql_with_timeout(
            o,
            build_ddl(project, stage_name, column, "feishu2ods 暂存数据，校验后发布", staging=True),
            timeout=timeout,
            desc=f"建暂存表 {stage_name}",
        )
        stage = o.get_table(f"{project}.{stage_name}")
        verify_schema(stage, stage_name, column)
        verified = False
        published = False
        try:
            for attempt in range(1, WRITE_ATTEMPTS + 1):
                try:
                    if not verified:
                        drop_partition(o, project, stage_name, tmp_spec, timeout=timeout)
                        add_partition(o, project, stage_name, tmp_spec, timeout=timeout)
                        with stage.open_writer(partition=tmp_spec, reopen=True) as writer:
                            for batch in spool.iter_batches(BATCH_SIZE):
                                writer.write([[row] for row in batch])
                        actual, distinct, smallest, largest = verify_partition(
                            o, project, stage_name, column, f"{pt}{TMP_PARTITION_SUFFIX}", timeout=timeout
                        )
                        if actual != stats.count:
                            raise RuntimeError(f"暂存行数不一致：计划 {stats.count:,} 行，实际 {actual:,} 行")
                        if stats.count and (distinct, smallest, largest) != (
                            stats.distinct_ids(),
                            stats.min_id,
                            stats.max_id,
                        ):
                            raise RuntimeError("暂存 record_id 去重数或范围与拉取结果不一致")
                        verified = True
                    # 单分区 OVERWRITE 原子提交；失败重试复用同一份已核对数据，不先删正式分区。
                    run_sql_with_timeout(
                        o,
                        f"insert overwrite table {project}.{table_name} partition ({_sql_spec(final_spec)}) "
                        f"select {column} from {project}.{stage_name} "
                        f"where {_partition_clause(f'{pt}{TMP_PARTITION_SUFFIX}')}",
                        timeout=timeout,
                        desc=f"发布分区 {table_name} pt={pt}",
                        publication=(project, table_name),
                    )
                    published = True
                    _drop_stale_keep(o, table, project, table_name, pt, timeout)
                    return
                except Exception as exc:
                    if attempt == WRITE_ATTEMPTS:
                        raise SystemExit(
                            f"{table_name} {final_spec} 写入失败（已尝试 {WRITE_ATTEMPTS} 次）；"
                            f"正式分区未执行删除，发布由单条 OVERWRITE 提交；"
                            f"暂存表 {project}.{stage_name} 已保留（生命周期 1 天），重跑可幂等恢复：{redact(exc)}"
                        ) from exc
                    log(f"  分区 {final_spec} 写入第 {attempt} 次失败：{redact(exc)}；{WRITE_RETRY_DELAY}s 后重试")
                    time.sleep(WRITE_RETRY_DELAY)
        finally:
            if published:
                try:
                    run_sql_with_timeout(
                        o, f"drop table if exists {project}.{stage_name}", timeout=timeout, desc="清理暂存表"
                    )
                except (Exception, _common.UnresolvedSqlError) as exc:
                    log(f"  警告：暂存表 {project}.{stage_name} 清理失败，将由生命周期回收：{redact(exc)}")
            else:
                log(f"  提示：暂存表 {project}.{stage_name} 已保留（生命周期 1 天）；正式表未创建临时分区")
    finally:
        if _temp_spool is not None:
            _temp_spool.close()


def count_partition(o, project: str, table_name: str, pt: str, timeout: int = SQL_TIMEOUT_SECONDS) -> int:
    """写后行数核对：select count(*)（与拉取条数不一致按失败处理）。"""
    project = _require_identifier(project, "target.project")
    table_name = _require_identifier(table_name, "target.table")
    sql = f"select count(*) as cnt from {project}.{table_name} where {_partition_clause(pt)}"
    instance = run_sql_with_timeout(o, sql, timeout=timeout, desc=f"校验 {table_name} 行数")
    with instance.open_reader() as reader:
        for row in reader:
            return int(row["cnt"])
    # count(*) 必然返回一行：读不到行说明 SQL 没真正执行 / reader 异常。返回 0 会把"没读到结果"
    # 伪装成"分区确实 0 行"——写前的 0 行保护会据此以为"分区本来就是空的"，接着把有数据的分区
    # 清空（写后核对还会变成 0 == 0、rc=0 静默丢数）。宁可失败（与 sftp2ods 同口径）。
    raise SystemExit(f"校验 SQL 未返回行，无法确认分区 {table_name} pt={pt} 的行数")


def _pid_alive(pid: int) -> bool:
    """探测本机进程是否还在（只探测，不发信号）。

    非 POSIX 平台无法安全探测（Windows 上 os.kill(pid, 0) 会真的把进程杀掉，绝不能用）：
    返回"可能还在"，让 purge 保守跳过同机 tmp——宁可残留半成品也不误删别人正在写的唯一副本。
    """
    if os.name != "posix":
        return True
    try:
        os.kill(pid, 0)  # 信号 0：只做存在性/权限检查，不会真的发信号
    except (ProcessLookupError, OverflowError, ValueError):
        return False
    except OSError:
        # PermissionError = 进程存在但无权限发信号（跨用户并发）；其它异常判不准，保守按"还在"
        return True
    return True


def _partition_exists(table, spec: str) -> bool | None:
    """查询旧版保留分区是否存在；探测失败返回 None，调用方不得据此删除唯一副本。"""
    try:
        return bool(table.exist_partition(_sql_spec(spec)))
    except Exception:  # noqa: BLE001 - 探测不了就交给调用方按未知处理
        return None


def _drop_stale_keep(o, table, project: str, table_name: str, pt: str, timeout: int) -> None:
    """正式分区就位后清掉同 pt 的 __keep（上一轮失败留下的旧快照）。

    __keep 的字符串序大于正式分区，max_pt() 会选中它；不清理会让下游一直读旧快照。
    探测不到是否存在时只告警不猜（正式分区已写入成功，不能因此把本轮判失败）。
    """
    keep_spec = f"{PARTITION_COLUMN}={pt}{KEEP_PARTITION_SUFFIX}"
    state = _partition_exists(table, keep_spec)
    if state is False:
        return
    if state is None:
        log(f"  警告：无法确认保留分区 {keep_spec} 是否存在，未清理；若下游 max_pt() 读到旧快照请手工删除")
        return
    try:
        drop_partition(o, project, table_name, keep_spec, timeout=timeout)
    except (Exception, _common.UnresolvedSqlError) as exc:  # 清理旧副本失败不能改变正式发布结果
        log(f"  警告：清理旧保留分区 {keep_spec} 失败（{redact(exc)}）；下游 max_pt() 可能读到旧快照，请手工删除")
        return
    log(f"  已清理同 pt 的保留分区 {keep_spec}（正式分区已恢复，避免 max_pt() 读到旧快照）")


def _tmp_writer_alive(value: str) -> bool:
    """分区值是否由「本机另一个还活着的进程」在写（残留清理要放行在途写入）。

    命名是 <pt>__tmp_<host>_<pid>（见 _tmp_run_id）。只有「同机 + pid 还在」才算在途；
    别的机器（跨机并发不受支持）与查不出 pid 的旧命名一律按残留处理。
    """
    index = value.rfind("__tmp_")
    if index < 0:
        return False
    host, sep, pid_text = value[index + len("__tmp_") :].rpartition("_")
    if not sep or not pid_text.isdigit() or host != _TMP_HOST or int(pid_text) == os.getpid():
        return False
    return _pid_alive(int(pid_text))


def purge_stale_tmp_partitions(o, table, project: str, table_name: str, timeout: int = SQL_TIMEOUT_SECONDS) -> None:
    """升级时处理旧版本遗留分区；只清理已有正式副本的残留，保留可能的唯一副本。"""
    # pyodps 的 table.partitions 每次访问都发一次全量分区元数据请求：先取一份列表复用
    partitions = list(table.partitions)
    present = {str(part.name).split("=", 1)[-1].strip().strip("'\"") for part in partitions}
    for part in partitions:
        spec = str(part.name)
        value = spec.split("=", 1)[-1].strip().strip("'\"")
        if KEEP_PARTITION_SUFFIX in value:
            # __keep 是"正式分区已被删、这是唯一完整副本"的保留分区，清它等于把那一分区的
            # 数据彻底弄丢——但当同 pt 的正式分区已经回来（重跑成功/人工恢复了），这份副本
            # 就只是冗余：它的字符串序大于正式分区，不清理会让 max_pt() 永远读到旧快照，
            # 且 pt='20260927__keep' 不是 8 位业务日、按 yyyyMMdd 解析的下游会取错日期。
            base = value.split(KEEP_PARTITION_SUFFIX, 1)[0]
            if base in present:
                log(f"  清理冗余保留分区（正式分区已就位）：{table_name} {spec}")
                drop_partition(o, project, table_name, spec, timeout=timeout)
                continue
            log(f"  提示：发现保留分区 {spec}（内容已核对），跳过清理；可手工 rename 到正式分区恢复数据")
            continue
        # 「是不是本进程的」用后缀结尾判断（endswith），不用子串包含：
        # pid 前缀相同时（本机 456 与 4567），子串判断会把别人的分区误认成自己的而跳过，
        # 残留永远清不掉。endswith 只放行与本进程后缀完全一致的分区。
        if "__tmp" in value:
            if _tmp_writer_alive(value):
                log(f"  跳过在途临时分区（本机另一进程正在写，清掉会毁掉它的校验后替换）：{spec}")
                continue
            if value.split("__tmp", 1)[0] not in present:
                raise SystemExit(
                    f"发现旧版本临时分区 {table_name} {spec}，对应正式分区不存在；"
                    "为保留可能的唯一副本，未删除。请先核对内容并手工恢复或删除，再重跑"
                )
            log(f"  清理残留临时分区：{table_name} {spec}")
            drop_partition(o, project, table_name, spec, timeout=timeout)


def verify_partition(
    o, project: str, table_name: str, column: str, pt: str, timeout: int = SQL_TIMEOUT_SECONDS
) -> tuple[int, int, str, str]:
    """核对分区内容（汇总级，不逐条比对）：行数、record_id 去重数、record_id 最小/最大值。

    三件套能覆盖漏行、重复、整段错写（Tunnel 是分批原子提交，配合足够）；逐条比对内容对大表成本太高，不做。
    返回 (行数, 去重 id 数, 最小 id, 最大 id)，空分区时后两项为空串。
    """
    project = _require_identifier(project, "target.project")
    table_name = _require_identifier(table_name, "target.table")
    column = _require_identifier(column, "target.column")
    sql = (
        f"select count(*) as cnt, count(distinct rid) as ucnt, min(rid) as mn, max(rid) as mx from ("
        f"select get_json_object({column}, '$.record_id') as rid "
        f"from {project}.{table_name} where {_partition_clause(pt)})"
    )
    instance = run_sql_with_timeout(o, sql, timeout=timeout, desc=f"核对 {table_name} 分区 {pt}")
    with instance.open_reader() as reader:
        for row in reader:
            return (
                int(row["cnt"]),
                int(row["ucnt"]),
                "" if row["mn"] is None else str(row["mn"]),
                "" if row["mx"] is None else str(row["mx"]),
            )
    # 与 count_partition 同口径：核对 SQL 必然返回一行，"读不到结果"不能当成"0 行、无 id"——
    # 那会让"计划 N 行、实际 0 行"这类真异常看起来像"空分区，正好对上"
    raise SystemExit(f"核对 SQL 未返回行，无法确认分区 {table_name} pt={pt} 的内容")
