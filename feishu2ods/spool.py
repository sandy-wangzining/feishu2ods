# -*- coding: utf-8 -*-
"""Spool：流式落盘（临时 JSONL）+ 拉取统计（FetchStats）。"""

from __future__ import annotations

import json
import os
import pathlib
import tempfile
from collections import Counter

from .dates import normalize_date_value

BATCH_SIZE = 500  # Tunnel 每批写入行数


def dump_record(record: dict) -> str:
    """一条记录 → 单行 JSON（与 api2ods 同款序列化参数：不转义中文、紧凑、拒绝 NaN）。"""
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class SpoolWriter:
    """流式落盘：记录边拉边写本地临时 JSONL，写库时逐批读回。

    为什么要它：表格可能很大（十几万行 × 大单元格），全量记录放内存既慢又危险。
    拉取阶段边拉边写盘（顺序写很便宜），写 MaxCompute 时再逐批读回送进 Tunnel——
    峰值内存只与"单页 + 一个批次"的数据量有关，与总行数无关。
    """

    def __init__(self, path: pathlib.Path | None = None):
        created = None
        try:
            if path is None:
                handle, name = tempfile.mkstemp(prefix="feishu2ods-", suffix=".jsonl")
                os.close(handle)
                created = pathlib.Path(name)
                path = created
            self.path = pathlib.Path(path)
            self._handle = open(self.path, "w", encoding="utf-8", newline="\n")
        except OSError as exc:
            # mkstemp 已经把文件创建出来了：后续 open 失败（句柄用尽/磁盘满）时不能把
            # 临时文件留在系统 temp 里，best-effort 清掉
            if created is not None:
                try:
                    created.unlink(missing_ok=True)
                except OSError:
                    pass
            # 临时目录不可写/磁盘满/路径不存在：转成"带原因的人话"再抛出，由调用方（cli）
            # 记日志并按运行失败（退出码 1）结束——不让裸 OSError/FileNotFoundError 糊在用户脸上
            target = path if path is not None else "系统临时目录"
            raise OSError(f"建不了落盘临时文件（{target}）：{exc}") from exc
        self.count = 0

    def write_records(self, records: list[dict]) -> int:
        """把一批记录序列化后写入文件（返回本批条数）。"""
        for record in records:
            self._handle.write(dump_record(record) + "\n")
            self.count += 1
        return len(records)

    def iter_rows(self):
        """重新从头逐行读出（可多次调用：写库失败重试时会重新读一遍）。"""
        self._handle.flush()
        with open(self.path, "r", encoding="utf-8", newline="") as handle:
            for line in handle:
                line = line.rstrip("\n")
                if line:
                    yield line

    def iter_batches(self, batch_size: int = BATCH_SIZE):
        """按批读回（写 MaxCompute 用）。"""
        batch: list[str] = []
        for row in self.iter_rows():
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def close(self, keep: bool = False) -> None:
        """关闭并（默认）删除临时文件；keep=True 时保留（排障用）。"""
        try:
            self._handle.close()
        finally:
            if not keep:
                try:
                    self.path.unlink()
                except OSError:
                    pass


class FetchStats:
    """一次流式拉取的统计（内存只与字段数/去重 id 数有关，与总行数无关）。"""

    def __init__(self, date_field: str = ""):
        self.count = 0
        self.empty_rows = 0
        self.ids: set[str] = set()
        self.min_id = ""
        self.max_id = ""
        self.date_values: set[str] = set()
        self.date_counter: Counter = Counter()
        self.date_field = date_field

    def update(self, records: list[dict]) -> None:
        """拿一页记录更新统计。"""
        for record in records:
            self.count += 1
            record_id = str(record.get("record_id") or "")
            if record_id:
                self.ids.add(record_id)
                if not self.min_id or record_id < self.min_id:
                    self.min_id = record_id
                if not self.max_id or record_id > self.max_id:
                    self.max_id = record_id
            values = [value for key, value in record.items() if key != "record_id"]
            if values and all(value is None for value in values):
                # all() 对空序列恒为 True：只有 record_id 的记录（字段映射为空/接口没返回字段）
                # 不该被算成"空白行"，否则 empty_rows 的数据质量提示会失真
                self.empty_rows += 1
            if self.date_field:
                day = normalize_date_value(record.get(self.date_field))
                if day:
                    self.date_values.add(day)
                    self.date_counter[day] += 1

    def distinct_ids(self) -> int:
        return len(self.ids)
