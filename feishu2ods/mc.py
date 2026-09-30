# -*- coding: utf-8 -*-
"""MaxCompute：建表 / 结构校验 / 写分区（临时分区 + 原子替换）/ 写后核对。"""

from __future__ import annotations

import time

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
DEFAULT_ENDPOINT = "http://service.us-west-1.maxcompute.aliyun.com/api"


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


def write_partition(
    o, table, project: str, table_name: str, column: str, pt: str, spool: SpoolWriter, stats: FetchStats | None = None
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
    _temp_spool = None
    if stats is None:
        if not isinstance(spool, (list, tuple)):
            raise TypeError("write_partition 需要同时传 spool 与 stats（新签名）或记录列表（旧签名）")
        records = list(spool)
        spool = SpoolWriter()
        stats = FetchStats()
        spool.write_records(records)
        stats.update(records)
        _temp_spool = spool  # 兼容分支的临时 spool 归本函数管：结束时统一清理
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
            table.delete_partition(tmp_spec, if_exists=True)  # 清掉上次失败留下的临时分区
            table.create_partition(tmp_spec, if_not_exists=True)
            with table.open_writer(partition=tmp_spec, reopen=True) as writer:
                for batch in spool.iter_batches(BATCH_SIZE):
                    writer.write([[row] for row in batch])
            actual, distinct, smallest, largest = verify_partition(
                o, project, table_name, column, f"{pt}{TMP_PARTITION_SUFFIX}"
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
    finally:
        if _temp_spool is not None:
            _temp_spool.close()


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
