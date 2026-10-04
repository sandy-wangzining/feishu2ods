# -*- coding: utf-8 -*-
"""MaxCompute：建表 / 结构校验 / 写分区（临时分区 + 原子替换）/ 写后核对。"""

from __future__ import annotations

import re
import time

from .config import IDENT_RE
from .spool import BATCH_SIZE, FetchStats, SpoolWriter
from .utils import log, redact

try:
    from odps import ODPS
except ImportError:  # pragma: no cover - 离线测试环境可以不带 pyodps
    ODPS = None

PARTITION_COLUMN = "pt"  # 分区字段：业务日 yyyyMMdd（该分区 = 当天抽取的全量快照）
WRITE_ATTEMPTS = 3  # 写分区的最大尝试次数（重试会清掉临时分区重写）
WRITE_RETRY_DELAY = 10  # 写入重试间隔秒数
TMP_PARTITION_SUFFIX = "__tmp"  # 写库用临时分区后缀；写完 rename 成正式分区（缩短下游可见窗口）
MAX_ROW_BYTES = 7_000_000  # 单行 JSON 上限（MaxCompute string 8MB，留余量）
SQL_TIMEOUT_SECONDS = 600  # 单条 SQL（建表 / 校验 / 分区增删 / rename）最长等待秒数，0 = 不限制
SQL_HEARTBEAT_SECONDS = 30  # 长 SQL 的"还在执行"心跳日志间隔
# https：作业没写 endpoint 时 AK/SK 签名与查询结果不能走明文 HTTP
DEFAULT_ENDPOINT = "https://service.us-west-1.maxcompute.aliyun.com/api"
# 分区值白名单：pt 恒为 8 位业务日（用 [0-9] 而不是 \d：\d 还认全角/阿拉伯-印度数字），
# 写入期的临时分区在 pt 后带 __tmp 后缀
_PT_RE = re.compile(r"\A[0-9]{8}\Z")
_TMP_PT_RE = re.compile(r"\A[0-9]{8}__tmp\Z")


def _require_pt(pt, *, tmp_ok: bool = False) -> str:
    """分区值形态校验（写库、count、核对统一从这里过）。"""
    text = str(pt)
    if not (_PT_RE.match(text) or (tmp_ok and _TMP_PT_RE.match(text))):
        suffix = "（写入期可带 __tmp 后缀）" if tmp_ok else ""
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
def run_sql_with_timeout(o, sql: str, timeout: int = SQL_TIMEOUT_SECONDS, desc: str = "SQL"):
    """提交 SQL 并等待完成：成功返回 instance / 失败抛错 / 超时主动 stop() 取消并抛 TimeoutError。

    用 o.run_sql（异步提交）而不是 o.execute_sql（阻塞到底）：只有异步实例才能被轮询、
    超时后 stop() 取消。MaxCompute 侧卡住时，run_sql_with_timeout 会按 timeout 主动取消，
    避免调度任务无限挂起、一直占着运行锁把后续调度全挡掉。
    """
    instance = o.run_sql(sql)
    started = time.time()
    last_log = started
    while True:
        if instance.is_successful():
            return instance
        if instance.is_terminated():
            instance.wait_for_success(timeout=1)  # 触发一次，抛出带错误信息的异常
            return instance
        now = time.time()
        if timeout and timeout > 0 and now - started > timeout:
            try:
                instance.stop()
            except Exception as exc:  # noqa: BLE001 - 取消失败不影响报错
                # 不静默：运维看到"已主动停止"会以为云端 SQL 真的停了（可能仍在跑、占着运行锁），
                # 排障线索不能丢
                log(f"    {desc} 取消失败（云端可能仍在执行）：{redact(exc)}")
            raise TimeoutError(f"{desc} 执行超过 {timeout} 秒，已主动停止")
        if timeout and timeout > 0 and now - last_log >= SQL_HEARTBEAT_SECONDS:
            log(f"    {desc} 还在执行（已等待 {int(now - started)} 秒，超时阈值 {timeout} 秒）...")
            last_log = now
        time.sleep(1)


# =============================================================================
# MaxCompute：建表 / 结构校验 / 写分区（临时分区 + 原子替换）/ 写后校验
# =============================================================================
def build_ddl(project: str, table: str, column: str, comment: str) -> str:
    """目标表 DDL：单列 string + pt 分区 + 表注释（标识符已在 validate_job 校验过）。"""
    # comment 来自作业配置（用户可任意编辑）：反斜杠是 MaxCompute 字符串字面量里的转义符，
    # 只替换单引号不够——结尾的 "\" 会吃掉收尾引号、把后续内容当 SQL 解析。先转义反斜杠
    table_comment = comment or "飞书多维表格记录 JSON 原样落库（写临时分区后原子替换 pt=<业务日>）"
    table_comment = table_comment.replace("\\", "\\\\").replace("'", "''")
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


def ensure_table(o, project: str, table: str, column: str, comment: str, timeout: int = SQL_TIMEOUT_SECONDS):
    """不存在则建表；存在返回表对象（结构校验由 verify_schema 负责）。"""
    run_sql_with_timeout(o, build_ddl(project, table, column, comment), timeout=timeout, desc=f"建表 {table}")
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
    """把 pyodps 风格分区串（pt=20260928）转成 DDL 里的带引号写法（pt='20260928'）。

    pyodps 的 partition.name 可能已带引号（pt='20260928'），统一先去掉再补引号，
    避免生成 pt=''20260928'' 这种非法写法。

    边界：当前只处理单级分区。多级分区（pt=x,region=y）会被整体当成一个值拼成
    pt='x,region=y'（非法），需要扩展。本工具的表结构被 verify_schema 强制为单列 pt，
    所有 spec 都由内部按 pt=<值> 构造，多级分支不可达——留此说明以免后人踩坑。
    """
    key, _, value = str(spec).partition("=")
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1]
    if key and value:
        # 防御性转义：spec 多数由内部按 pt=<值> 构造，但 purge_stale_tmp_partitions 的
        # spec 来自服务端的 partition.name——反斜杠/引号不能原样拼进 DDL
        escaped = value.replace("\\", "\\\\").replace("'", "''")
        return f"{key}='{escaped}'"
    return spec


def drop_partition(o, project: str, table_name: str, spec: str, timeout: int = SQL_TIMEOUT_SECONDS) -> None:
    """删除分区（DDL，走超时保护）。

    走 DDL 而不是 pyodps 的 `table.delete_partition`（同步、不限时）：云端分区元数据操作
    卡住时同样会无限挂起、一直占着运行锁。`IF EXISTS` 提供与 `delete_partition(if_exists=True)`
    一致的幂等语义（分区不存在也不报错）。
    """
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
    run_sql_with_timeout(
        o,
        f"alter table {project}.{table_name} add if not exists partition ({_sql_spec(spec)})",
        timeout=timeout,
        desc=f"建分区 {table_name}",
    )


def rename_partition(
    o, project: str, table_name: str, old_spec: str, new_spec: str, timeout: int = SQL_TIMEOUT_SECONDS
) -> None:
    """把临时分区改名为正式分区（MaxCompute DDL 元数据操作；目标分区必须不存在）。

    「删旧分区 + rename」之间只有两条 DDL 的空窗，远比「先删再填」整段写入短；
    写入期间旧快照一直可读，下游不会读到空/半截分区。
    """
    run_sql_with_timeout(
        o,
        f"alter table {project}.{table_name} "
        f"partition ({_sql_spec(old_spec)}) rename to partition ({_sql_spec(new_spec)})",
        timeout=timeout,
        desc=f"替换分区 {table_name}",
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
    """写一个分区：先写 <pt>__tmp 临时分区并核对内容，再删旧分区 + rename 原子替换。

    - 可见窗口只剩两条 DDL 之间：写入期间旧快照完整可读；
    - 数据从 spool（流式落盘的临时 JSONL）逐批读回，不要求全量记录驻留内存；
    - 写前先通读检查单行大小（不保存）：超长记录永远写不进去，必须在动分区之前报错；
    - 写后核对：行数 + record_id 去重数 + 最小/最大 id（与拉取阶段的 stats 对比，
      不一致就不替换，重试同上）；
    - 重试会从「清临时分区」重新开始；失败后尽力清掉临时分区，残留（如进程被强杀）下次运行也会先清；
    - 空快照（target.allow_empty=true）走同样的替换流程（清空正式分区）；否则上游已拦下空表。
    - 兼容旧调用方：第 7 参传记录列表、stats 不给时，就地装进临时 spool + stats。
    """
    pt = _require_pt(pt)  # 最终分区名直接由它派生（pt / pt__tmp），先过形态白名单
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
        if stats.count and not stats.min_id:
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

        final_deleted = False

        def once() -> None:
            nonlocal final_deleted
            # 分区增删都走 run_sql_with_timeout（DDL）：pyodps 的 table.delete_partition /
            # create_partition 是同步不限时的，云端卡住会把整轮任务连同运行锁一起挂死
            drop_partition(o, project, table_name, tmp_spec, timeout=timeout)  # 清掉上次失败留下的临时分区
            add_partition(o, project, table_name, tmp_spec, timeout=timeout)
            with table.open_writer(partition=tmp_spec, reopen=True) as writer:
                for batch in spool.iter_batches(BATCH_SIZE):
                    writer.write([[row] for row in batch])
            actual, distinct, smallest, largest = verify_partition(
                o, project, table_name, column, f"{pt}{TMP_PARTITION_SUFFIX}", timeout=timeout
            )
            if actual != stats.count:
                raise RuntimeError(f"临时分区行数不一致：计划 {stats.count:,} 行，实际 {actual:,} 行")
            if stats.count:
                # 空快照没有 id 可核：只校验行数，去重/范围检查跳过
                if distinct != stats.distinct_ids():
                    raise RuntimeError(f"临时分区 record_id 重复：{actual:,} 行里去重后只剩 {distinct:,} 个")
                if (smallest, largest) != (stats.min_id, stats.max_id):
                    raise RuntimeError(
                        f"临时分区 record_id 范围不一致：实际 [{smallest}, {largest}]，"
                        f"预期 [{stats.min_id}, {stats.max_id}]"
                    )
            # 原子替换：旧分区先让位，临时分区立刻改名顶上（中间空窗只有一条 DDL 的执行时间）。
            # 置位放在删之前：删除请求可能已经发到服务端才报错，宁可把后果描述得保守一点
            final_deleted = True
            drop_partition(o, project, table_name, final_spec, timeout=timeout)
            rename_partition(o, project, table_name, tmp_spec, final_spec, timeout=timeout)

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
            drop_partition(o, project, table_name, tmp_spec, timeout=timeout)
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
    return 0


def purge_stale_tmp_partitions(o, table, project: str, table_name: str, timeout: int = SQL_TIMEOUT_SECONDS) -> None:
    """清掉历史失败残留的 __tmp 临时分区（正式写入之前调用）。

    残留的 tmp 分区值（如 20260927__tmp）在字符串序上大于同日期正式分区，会让下游
    用 max_pt() 时读到半成品；每次正式运行前统一清一遍，避免一次中断长时间污染下游。
    删除同样走带超时的 DDL（pyodps 的 table.delete_partition 不限时，卡住会挂死整个任务）。
    """
    for part in list(table.partitions):
        spec = str(part.name)
        if TMP_PARTITION_SUFFIX in spec:
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
    return (0, 0, "", "")
