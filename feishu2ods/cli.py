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
    connect_odps,
    count_partition,
    ensure_table,
    purge_stale_tmp_partitions,
    verify_schema,
    write_partition,
)
from .notify import notify
from .spool import FetchStats, SpoolWriter
from .utils import _SECRETS, RunLock, lock_path, log, redact, setup_console
from .wizard import run_init


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
    parser.add_argument(
        "--check", action="store_true", help="体检：配置 + API 连通 + 字段映射 + 目标表结构（不写库、不发告警）"
    )
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


def run_sync(
    args, job: dict, project: str, table_name: str, column: str, pt: str, bizdate: date, started: float
) -> int:
    """正式流程：拉数（流式落盘）→ 空表/新鲜度校验 → 写 pt 分区（临时分区 + 原子替换）→ 行数核对。"""
    feishu = job["feishu"]
    maxcompute = job["maxcompute"]
    target_cfg = job["target"]

    # ---- ① 拉数（流式：记录边拉边落盘，全量不驻留内存）----
    webhook = str((job.get("freshness") or {}).get("webhook") or "")
    base_url = str(feishu.get("base_url") or "")
    new_fields: list[str] = []
    date_field = str((job.get("freshness") or {}).get("date_field") or "")
    spool = SpoolWriter()
    stats = FetchStats(date_field=date_field)
    try:
        fetch_records(feishu, job["fields"], extra_out=new_fields, sink=spool, stats=stats)
    except Exception:
        spool.close(keep=True)  # 失败保留临时文件供排查（系统 temp 会自行清理）
        raise
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
        log(f"  警告：{empty_rows:,} 条记录除 record_id 外全为空（Base 里的空白行），已原样同步；下游按日期字段过滤")

    # ---- ② 空表 / 新鲜度校验（缺失 → 告警 + 非 0 退出，不写库）----
    allow_empty = bool(target_cfg.get("allow_empty", False))
    if count == 0 and not allow_empty:
        log("❌ 拉取到 0 条记录，已中止（target.allow_empty=false，拒绝写入空分区）")
        if not args.no_notify:
            lines = [f"**作业**：{job.get('job')}", "**情况**：拉取到 0 条记录，未写入 MaxCompute"]
            if base_url:
                lines.append(f"**数据表**：{base_url}")
            notify(webhook, "飞书多维表格同步：0 条记录", lines, footer=f"目标表 {project}.{table_name}")
        spool.close()
        return 1
    freshness = job.get("freshness")
    if freshness and not args.skip_freshness:
        lag_days = freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS)
        expected = (bizdate - timedelta(days=lag_days)).isoformat()
        problem = freshness_problem(stats.date_values, freshness["date_field"], expected)
        if problem is not None:
            # 缺数据只告警、不失败：很多表是人填的（节假日/休假没人填是常态），
            # 缺一天不等于任务失败——快照照常写入（DWD 按源数据日期字段重新分区，
            # 缺的那天只是没有数据行），下游任务照常跑。真异常（表被清空=0 行）
            # 仍由上面的 0 行保护拦截。
            expected, latest = problem
            log(f"⚠️ 缺少 {expected} 的数据（当前最新 {latest or '无'}），照常写入并告警")
            if freshness.get("lag_days", DEFAULT_FRESHNESS_LAG_DAYS):
                log(
                    f"   预期日期 = 业务日 - {freshness.get('lag_days', DEFAULT_FRESHNESS_LAG_DAYS)} 天；"
                    f"如表格出数节奏不同，请调整 freshness.lag_days"
                )
            else:
                log("   若表格本来就晚一天出数，可在 job 里把 freshness.lag_days 调成 1；补数可加 --skip-freshness")
            if not args.no_notify:
                lines = [
                    f"**作业**：{job.get('job')}",
                    f"**预期已有**：{expected}（{freshness['date_field']}）",
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
                log(f"  警告：以下日期在 Base 里出现多行：{'、'.join(sorted(dup_dates)[:10])}（DWD 同一天会落多行）")
            log(f"新鲜度校验通过：{freshness['date_field']} 已包含业务日 {expected}")

    # ---- ③ 写库（dry-run 跳过）----
    if args.dry_run:
        spool.close()
        log(f"--dry-run：不写库；将把 {count:,} 行写进 {project}.{table_name} pt={pt}（写临时分区后原子替换）")
        return 0

    o = connect_odps(maxcompute, project)
    table = ensure_table(o, project, table_name, column, str(target_cfg.get("comment") or ""))
    verify_schema(table, table_name, column)
    purge_stale_tmp_partitions(table, table_name)
    log(f"写入 {project}.{table_name} pt={pt}（{count:,} 行：先写临时分区，核对后原子替换）...")
    write_partition(o, table, project, table_name, column, pt, spool, stats)
    actual = count_partition(o, project, table_name, pt)
    if actual != count:
        spool.close()
        log(f"❌ 写后校验不一致：计划 {count:,} 行，实际 {actual:,} 行（重跑即可，写入幂等）")
        return 1
    spool.close()
    log(f"完成：{project}.{table_name} pt={pt} 共 {actual:,} 行，耗时 {(time.time() - started) / 60:.1f} 分钟")
    return 0


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
