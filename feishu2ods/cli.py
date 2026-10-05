# -*- coding: utf-8 -*-
"""命令行入口：体检（--check）、正式同步、交互式建配置（--init）的分发。"""

from __future__ import annotations

import argparse
import pathlib
import sys
import time
import traceback
from datetime import date, timedelta

from . import VERSION
from .config import DEFAULT_COLUMN, DEFAULT_FRESHNESS_LAG_DAYS, _require_identifier, load_job, validate_job
from .dates import freshness_problem, resolve_bizdate
from .fetch import fetch_records
from .mc import (
    SQL_TIMEOUT_SECONDS,
    connect_odps,
    count_partition,
    ensure_table,
    purge_stale_tmp_partitions,
    verify_schema,
    write_partition,
)
from .notify import notify
from .spool import FetchStats, SpoolWriter
from .utils import (
    _SECRETS,
    RunLock,
    add_log_sink,
    collect_secret_values,
    lock_path,
    log,
    redact,
    redact_secrets,
    remove_log_sink,
    reset_lock_warning,
    setup_console,
    table_lock_path,
)
from .wizard import run_init


# =============================================================================
# 主流程：拉数 → 新鲜度校验 → 写 pt 分区（临时分区 + 原子替换）→ 行数核对
# =============================================================================
def _sql_timeout_arg(value: str) -> int:
    """--sql-timeout 参数校验：非负整数。

    负数会被 run_sql_with_timeout 当成"0=不限制"，与用户直觉相反（想调小却等成无限）；
    与 api2ods / sftp2ods 的口径一致：命令行参数问题在 argparse 阶段就报错（退出码 2，
    不发起任何远端操作）。
    """
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise argparse.ArgumentTypeError(f"必须是整数（0 表示不限制），实际 {value!r}") from None
    if number < 0:
        raise argparse.ArgumentTypeError(f"不能为负（0 表示不限制），实际 {number}")
    return number


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="feishu2ods",
        description="飞书多维表格 → MaxCompute ODS（JSON 原样落库，json + pt 分区，临时分区 + 原子替换）",
    )
    parser.add_argument("--job", default="", help="job 配置文件（jobs/*.json）；--init 时可不填")
    parser.add_argument("--init", action="store_true", help="交互式生成作业配置（接新表用；生成后先 --check）")
    parser.add_argument("--init-out", default="", help="--init 的输出路径（默认 jobs/<作业名>.json）")
    parser.add_argument(
        "--check", action="store_true", help="体检：配置 + API 连通 + 字段映射 + 目标表结构（不写库、不发告警）"
    )
    parser.add_argument(
        "--bizdate",
        default=None,
        # 默认必须是 None 而不是 ""：空串无法区分"没传 --bizdate"与"显式传了空值"
        # （调度脚本 `--bizdate "$pt"` 且 $pt 未定义）——后者要报错，不能静默回退成"昨天"
        help="业务日 pt（YYYYMMDD 或 YYYY-MM-DD）；默认取环境变量 bizdate/SKYNET_BIZDATE，再默认当天-1",
    )
    parser.add_argument("--dry-run", action="store_true", help="只拉数并打印统计，不写 MaxCompute")
    parser.add_argument(
        "--force",
        action="store_true",
        help="0 行且 target.allow_empty=true 时，跳过「先查现有分区行数」的保护，允许把已有分区覆盖成空分区",
    )
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
    parser.add_argument(
        "--sql-timeout",
        type=_sql_timeout_arg,
        default=SQL_TIMEOUT_SECONDS,
        help=f"单条 MaxCompute SQL 最长等待秒数，默认 {SQL_TIMEOUT_SECONDS}；0 表示不限制",
    )
    parser.add_argument("--log-file", default="", help="日志同时写一份到该文件（追加，UTF-8）")
    parser.add_argument("--version", action="version", version=f"feishu2ods {VERSION}")
    return parser.parse_args(argv)


def _open_log_file(path_text: str):
    """打开 --log-file 指定的日志文件（追加、UTF-8、父目录自动创建）；没指定返回 None。

    用户给成目录名（--log-file logs）时给一句人话，而不是 IsADirectoryError 的裸 traceback。
    """
    if not path_text:
        return None
    path = pathlib.Path(path_text)
    if path.is_dir():
        raise SystemExit(f"--log-file 指向的是目录，需要给文件名：{path}（如 {path / 'run.log'}）")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, "a", encoding="utf-8")
    except OSError as exc:
        raise SystemExit(f"--log-file 打不开：{path}（{exc}）") from exc


def _redact_job(job: dict, text) -> str:
    """作业上下文下的脱敏：先按配置里的密钥值遮（形态规则盖不住的自由文本回显），
    再走形态兜底。错误只在真出错时走这里，每次重收密钥值的开销可忽略。"""
    return redact_secrets(collect_secret_values(job), str(text))


def run_check(job: dict, project: str, table_name: str, column: str, pt: str) -> int:
    """体检：配置 + API 连通 + 字段映射 + 目标表结构（不写库、不发告警、不校验新鲜度）。"""
    log("== 体检（--check，不写库、不发告警） ==")
    log(f"  映射字段 {len(job['fields'])} 个；目标 {project}.{table_name}（{column}，pt={pt}）")
    try:
        records = fetch_records(job["feishu"], job["fields"], max_pages=1)
    except SystemExit as exc:
        log(f"  ❌ 拉取/映射校验失败：{_redact_job(job, exc)}")
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
        log(f"  ❌ {_redact_job(job, exc)}")
        return 1
    except Exception as exc:  # noqa: BLE001 - 连不上/校验失败都给干净结论
        log(f"  ❌ 连接/校验失败：{_redact_job(job, exc)}")
        return 1
    log("体检通过。")
    return 0


def run_sync(
    args, job: dict, project: str, table_name: str, column: str, pt: str, bizdate: date, started: float
) -> int:
    """正式流程：拉数（流式落盘）→ 空表/新鲜度校验 → 写 pt 分区（临时分区 + 原子替换）→ 行数核对。"""
    feishu = job["feishu"]
    maxcompute = job["maxcompute"]
    target_cfg = job["target"]
    freshness = job.get("freshness")
    if freshness is not None and not isinstance(freshness, dict):
        # validate_job 会拦；库调用方没走校验时给干净的配置错，而不是 'str' object has no attribute 'get'
        raise SystemExit("freshness 必须是对象（date_field / lag_days / webhook）")

    # ---- ① 拉数（流式：记录边拉边落盘，全量不驻留内存）----
    webhook = str((freshness or {}).get("webhook") or "")
    base_url = str(feishu.get("base_url") or "")
    new_fields: list[str] = []
    date_field = str((freshness or {}).get("date_field") or "")
    lag_days = DEFAULT_FRESHNESS_LAG_DAYS
    if freshness:
        lag_days = freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS)
        if isinstance(lag_days, bool) or not isinstance(lag_days, int) or lag_days < 0:
            raise SystemExit(f"freshness.lag_days 必须是 >= 0 的整数，实际 {lag_days!r}")
    if freshness and not args.skip_freshness and not date_field:
        # validate_job 要求 freshness 必带 date_field；没走校验的调用方在这里得到干净的配置错，
        # 而不是 KeyError（会被 main 的兜底 except 变成"未预期错误"+traceback）。
        # 校验放在建 spool 之前：失败退出时没有已打开的临时文件要收口
        raise SystemExit("freshness.date_field 缺失：freshness 开启时必须指定用哪个键判日期")
    try:
        spool = SpoolWriter()
    except OSError as exc:
        # 临时目录不可写/磁盘满：给一句人话而不是裸 traceback（否则 --log-file 里一个字都没有）
        log(f"❌ 无法创建落盘临时文件（检查系统临时目录是否可写/磁盘是否已满）：{_redact_job(job, exc)}")
        return 1
    stats = FetchStats(date_field=date_field)
    keep_spool = True
    try:
        fetch_records(feishu, job["fields"], extra_out=new_fields, sink=spool, stats=stats)
        count = stats.count
        log(f"拉取完成：{count:,} 条记录，映射 {len(job['fields'])} 个字段")
        if new_fields:
            uniq = sorted(set(new_fields))
            # 日志无条件打：--no-notify 只关飞书提醒，不关日志——新增列完全不可见会让
            # 用户以为"没出问题"（与 api2ods 的字段漂移提醒口径一致：先 log 再 notify）
            log(
                f"⚠️ Base 出现 {len(uniq)} 个未映射的新增列：{'、'.join(f'`{name}`' for name in uniq)}（本次忽略其值、其余字段照常同步）"
            )
            if not args.no_notify:
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
        empty_rows = stats.empty_rows
        if empty_rows:
            log(
                f"  警告：{empty_rows:,} 条记录除 record_id 外全为空（Base 里的空白行），已原样同步；下游按日期字段过滤"
            )

        # ---- ② 空表 / 新鲜度校验（缺失 → 告警 + 非 0 退出，不写库）----
        allow_empty = target_cfg.get("allow_empty", False)
        if not isinstance(allow_empty, bool):
            # validate_job 会拦；库调用方没走校验时，bool("false") 会静默变成 True、跳过 0 行保护
            # （源表被截断成 0 行时照样写空分区、退出码 0）。与 lag_days 同口径：显式校验类型。
            raise SystemExit(f"target.allow_empty 必须是 true/false，实际 {allow_empty!r}")
        if count == 0 and not allow_empty:
            log("❌ 拉取到 0 条记录，已中止（target.allow_empty=false，拒绝写入空分区）")
            if not args.no_notify:
                lines = [f"**作业**：{job.get('job')}", "**情况**：拉取到 0 条记录，未写入 MaxCompute"]
                if base_url:
                    lines.append(f"**数据表**：{base_url}")
                notify(webhook, "飞书多维表格同步：0 条记录", lines, footer=f"目标表 {project}.{table_name}")
            return 1
        if freshness and not args.skip_freshness:
            expected = (bizdate - timedelta(days=lag_days)).isoformat()
            problem = freshness_problem(stats.date_values, date_field, expected)
            if problem is not None:
                # 缺数据只告警、不失败：很多表是人填的（节假日/休假没人填是常态），
                # 缺一天不等于任务失败——快照照常写入（DWD 按源数据日期字段重新分区，
                # 缺的那天只是没有数据行），下游任务照常跑。真异常（表被清空=0 行）
                # 仍由上面的 0 行保护拦截。
                expected, latest = problem
                log(f"⚠️ 缺少 {expected} 的数据（当前最新 {latest or '无'}），照常写入并告警")
                if lag_days:
                    log(f"   预期日期 = 业务日 - {lag_days} 天；如表格出数节奏不同，请调整 freshness.lag_days")
                else:
                    log("   若表格本来就晚一天出数，可在 job 里把 freshness.lag_days 调成 1；补数可加 --skip-freshness")
                if not args.no_notify:
                    lines = [
                        f"**作业**：{job.get('job')}",
                        f"**预期已有**：{expected}（{date_field}）",
                        f"**当前最新**：{latest or '无'}",
                        f"**当前条数**：{count:,}",
                        "本次已照常写入快照（缺的那天只是没有数据行），下游任务不受影响；",
                        "请人工确认表格是否还需要更新，更新后重跑即可。",
                    ]
                    if base_url:
                        lines.append(f"**数据表**：{base_url}")
                    notify(
                        webhook,
                        f"飞书表格同步缺少 {expected} 数据（已照常写入）",
                        lines,
                        footer=f"目标表 {project}.{table_name}",
                    )
            else:
                dup_dates = [day for day, dup_count in stats.date_counter.items() if day and dup_count > 1]
                if dup_dates:
                    log(
                        f"  警告：以下日期在 Base 里出现多行：{'、'.join(sorted(dup_dates)[:10])}（DWD 同一天会落多行）"
                    )
                log(f"新鲜度校验通过：{date_field} 已包含业务日 {expected}")

        # ---- ③ 写库（dry-run 跳过）----
        if args.dry_run:
            keep_spool = False
            log(f"--dry-run：不写库；将把 {count:,} 行写进 {project}.{table_name} pt={pt}（写临时分区后原子替换）")
            return 0

        o = connect_odps(maxcompute, project)
        table = ensure_table(
            o, project, table_name, column, str(target_cfg.get("comment") or ""), timeout=args.sql_timeout
        )
        verify_schema(table, table_name, column)
        purge_stale_tmp_partitions(o, table, project, table_name, timeout=args.sql_timeout)

        # 0 行 + allow_empty=true（否则上面已拦截）→ 写空分区会清空已有分区：「删旧分区 + rename」
        # 不可逆，而"源端被截断成 0 行"几乎总是异常。写前先查现有分区行数，非 0 则拒绝写库，
        # 只有显式 --force 才放行（与 sftp2ods 同款保护）。
        if count == 0 and not args.force:
            existing = count_partition(o, project, table_name, pt, timeout=args.sql_timeout)
            if existing:
                log(
                    f"❌ {project}.{table_name} pt={pt} 本次拉到 0 行，但该分区现有 {existing:,} 行；"
                    f"为避免清空已有数据，本次未写库（若确认就是要写空分区，请加 --force）。"
                    f"源端被截断/清空时请先排查，数据无误后重跑即可"
                )
                return 1

        log(f"写入 {project}.{table_name} pt={pt}（{count:,} 行：先写临时分区，核对后原子替换）...")
        write_partition(o, table, project, table_name, column, pt, spool, stats, timeout=args.sql_timeout)
        actual = count_partition(o, project, table_name, pt, timeout=args.sql_timeout)
        if actual != count:
            log(f"❌ 写后校验不一致：计划 {count:,} 行，实际 {actual:,} 行（重跑即可，写入幂等）")
            return 1
        keep_spool = False
        log(f"完成：{project}.{table_name} pt={pt} 共 {actual:,} 行，耗时 {(time.time() - started) / 60:.1f} 分钟")
        return 0
    finally:
        # 拉数之后任何失败（含 notify / stats 异常、fetch 的 SystemExit）都在这里收口句柄。
        # 约定：失败路径一律保留落盘文件便于事后排查，只有写库并核对成功 / dry-run 才删除。
        if keep_spool:
            log(f"已保留本次落盘的数据文件（排查用）：{spool.path}")
        spool.close(keep=keep_spool)


def _wizard_ask(prompt: str = "") -> str:
    """向导的提问也走日志（带时间戳）；只记问题、不记回答（答案里可能有密钥）。"""
    log(prompt)
    try:
        return input()
    except EOFError:  # Ctrl+Z / Ctrl+D：按用户中断处理，走统一的 130 出口
        raise KeyboardInterrupt from None


def main(argv: list[str] | None = None) -> int:
    """命令行入口。返回退出码：0 成功 / 1 运行失败 / 2 参数问题 / 130 用户中断。"""
    setup_console()
    started = time.time()
    log_handle = None
    try:
        args = parse_args(argv)
        # 日志文件先挂上：--init 的问答、--check 的概要都值得留痕（Linux 上跑 cron/调度时
        # stdout 会被截断，落盘是唯一能事后翻查的途径）。挂载点要在 _run 之前。
        log_handle = _open_log_file(args.log_file)
        if log_handle is not None:
            add_log_sink(log_handle)
        return _run(args, started)
    except SystemExit as exc:
        # 配置/运行类错误（统一以 SystemExit 抛出）：走统一日志出口（带时间戳+脱敏）后退出
        # parse_args / --log-file 的 SystemExit 也走这里，避免冒泡成裸 traceback
        if isinstance(exc.code, int):
            if exc.code == 0:
                raise  # --help / --version：argparse 已打印，保持原出口
            # 没带消息的 int 退出码也要留痕（argparse 的参数错走这里）：原来既不打印也不写
            # 日志，cron/调度场景下 --log-file 里完全查不到这次为什么失败
            log(f"❌ 以退出码 {exc.code} 结束（未附带错误信息）")
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
    finally:
        # 摘掉日志 sink 并关句柄：同一进程里多次调用 main 时，残留句柄会继续写已关闭的文件
        remove_log_sink(log_handle)
        if log_handle is not None:
            try:
                log_handle.close()
            except (OSError, ValueError):
                pass


def _run(args, started: float) -> int:
    """一次运行的主体（--init / --check / --sync 三种模式的分发）。"""
    if args.init:
        return run_init(args.init_out, ask=_wizard_ask, echo=log)

    if not args.job:
        log("请用 --job 指定作业配置文件（第一次接新表可以先用 `python feishu2ods.py --init` 生成）")
        return 2

    job = load_job(args.job)
    # 脱敏表是模块级状态：每次运行前先清空再登记，避免同一进程里多次调用 main() 时
    # 上一轮的密钥值残留（值级替换会一直带着它，且下一轮日志脱敏口径被污染）。
    _SECRETS.clear()
    reset_lock_warning()  # 同上："文件系统不支持锁"的告警去重也按每次运行重来
    # 先把 job 里疑似密钥的值登记进脱敏表，再校验/打日志：
    # 校验报错会回显非法值，密钥写错形态时也不该出现在日志里
    for secret in collect_secret_values(job):
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
    # validate_job 已保证 target.table 存在并写回；这里仍用 .get + 明确报错做防御：
    # 未来调用顺序若变化，KeyError 会被当成"未预期错误"，而这句能直接指出缺什么
    table_name = args.table or target_cfg.get("table")
    if not table_name:
        raise SystemExit("没有目标表名：请在作业文件的 target.table 里指定（或用 --table 覆盖）")
    column = target_cfg.get("column") or DEFAULT_COLUMN
    # --check 是只读体检：环境变量 bizdate 格式不对时不该把体检也拖垮（按默认业务日继续）；
    # 正式同步路径保持严格（非法业务日必须报错，绝不静默回退成"昨天"写错分区）
    bizdate = resolve_bizdate(args, strict=not args.check)
    pt = bizdate.strftime("%Y%m%d")

    log(
        f"作业 {job.get('job') or pathlib.Path(args.job).stem} 启动："
        + (f"{job['description']}；" if job.get("description") else "")
        + f"目标 {project}.{table_name}（{column}，pt={pt}：写临时分区后原子替换）"
    )

    if args.check:
        return run_check(job, project, table_name, column, pt)

    with RunLock(lock_path(pathlib.Path(args.job).resolve())):  # 同机同一作业互斥；不同作业可并行
        # 表级锁再兜一层：不同作业（两份配置）指向同一张表时，上面的作业锁互不阻塞，
        # purge/rename 会互相拆台、一方数据被静默覆盖——同机同表也必须串行
        with RunLock(table_lock_path(project, table_name)):
            return run_sync(args, job, project, table_name, column, pt, bizdate, started)


if __name__ == "__main__":
    sys.exit(main())
