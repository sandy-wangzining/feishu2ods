# -*- coding: utf-8 -*-
"""飞书多维表格拉取：offset 翻页 + 一致性保护 + 流式落盘/全量两种模式。"""

from __future__ import annotations

import time

from .auth import FEISHU_HOST, ApiHttpError, get_tenant_token, request_json
from .spool import FetchStats, SpoolWriter
from .utils import _api_err, log, redact

PAGE_SIZE = 500  # 单页行数（接口上限 2000，超了报 800004006；500 兼顾请求数与单页体积）
MAX_PAGES = 10000  # 防死循环的翻页上限（10000 页 × 500 行 = 500 万行，够用）
# access token 失效的错误码：命中后重新取一次 token 再重试当前页（正常 2 小时有效期足够，兜底用）
TOKEN_ERROR_CODES = {99991661, 99991663, 99991668}
RATE_LIMIT_CODES = {99991400}  # 接口限流（HTTP 200 + 该 code）：等待后重试同一页
RATE_LIMIT_ATTEMPTS = 5  # 同一页限流最多重试次数
RATE_LIMIT_WAIT = 5  # 限流重试间隔秒数


def _records_url(feishu: dict) -> str:
    return f"{FEISHU_HOST}/open-apis/base/v3/bases/{feishu['base_token']}/tables/{feishu['table_id']}/records"


def fetch_records(
    feishu: dict,
    mapping: dict,
    max_pages: int | None = None,
    extra_out: list[str] | None = None,
    sink: SpoolWriter | None = None,
    stats: FetchStats | None = None,
) -> list[dict] | FetchStats:
    """全量拉取数据表记录 → 每行一个 dict（record_id + 英文键）。

    v3 records 接口：limit=PAGE_SIZE + offset 翻页，has_more=false 结束；翻页中途 token 失效
    自动重取一次并重试当前页，限流（99991400）自动退避重试；字段列表在翻页间变化
    （表格结构刚被改动）按异常中止；接口给的表格版本号（rev）在翻页间变化也中止——
    offset 翻页期间表格被编辑会静默漏行，宁可中止重跑。max_pages 只拉前 N 页（--check 体检用）；
    extra_out 传入时收集未映射的新增列名（由 run_sync 发一次飞书提醒用）。

    流式模式（sink 与 stats 都给）：每页记录立即写进 sink 落盘、stats 逐页累积，
    返回 stats——峰值内存只与单页数据量有关，大表不再全量驻留内存（run_sync 用）。
    两者都不给：保持旧行为，全量累积后返回记录列表（--check / 小表调用方用）。
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
    stream = sink is not None and stats is not None
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
            # 首轮就校验映射与收集新增列（与 build_records 同口径，但只跑一次，
            # 流式模式下不能靠 build_records 的逐页检查——警告会每页重复打）
            if fields:
                index = {name: i for i, name in enumerate(fields)}
                missing = [source for source in mapping if source not in index]
                if missing:
                    raise SystemExit(
                        "Base 里找不到 fields 映射的列：" + "、".join(missing) + "（列名可能被改名/删除，请核对）"
                    )
                extra = [name for name in fields if name and name not in mapping]
                if extra:
                    log(f"  警告：Base 里有 {len(extra)} 个列未映射、已忽略：{'、'.join(extra)}")
                    if extra_out is not None:
                        extra_out.extend(extra)
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
        accumulated = len(raw_rows) + len(page_rows)
        if stream:
            page_records = _page_records(fields or [], page_ids, page_rows, mapping)
            sink.write_records(page_records)
            stats.update(page_records)
            del page_records
        else:
            raw_rows += page_rows
            record_ids += page_ids
        # 翻页日志节流：小表每页都打，大表每 20 页打一次（末页必打），调度日志不刷屏
        if page <= 10 or page % 20 == 0 or not payload.get("has_more"):
            log(f"  第 {page} 页：{len(page_rows)} 行（累计 {accumulated}）")

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

    if stream:
        # 空表且接口没给字段列表：映射校验已无从谈起（与 build_records 的空表口径一致）
        return stats
    return build_records(fields or [], record_ids, raw_rows, mapping, extra_out=extra_out)


def _page_records(fields: list[str], page_ids: list[str], page_rows: list[list], mapping: dict) -> list[dict]:
    """一页的列式行 + 记录 ID → 记录 dict 列表（流式模式用；映射校验已在首轮做过）。"""
    index = {name: i for i, name in enumerate(fields)}
    records: list[dict] = []
    for record_id, row in zip(page_ids, page_rows):
        record: dict = {"record_id": record_id}
        for source, target in mapping.items():
            i = index[source]
            record[target] = row[i] if i < len(row) else None
        records.append(record)
    return records


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
