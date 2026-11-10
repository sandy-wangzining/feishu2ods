# -*- coding: utf-8 -*-
"""feishu2ods 测试公共设施：离线基类、夹具与替身。"""

from __future__ import annotations

import argparse
import os
import pathlib
import shutil
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402
from feishu2ods import utils as utils_mod  # noqa: E402


class OfflineTestCase(unittest.TestCase):
    """基类：禁止真实 sleep、跳过控制台重配（与 api2ods 测试同款）。"""

    def setUp(self):
        utils_mod.setup_console()
        patcher = mock.patch.object(utils_mod, "_console_patched", True)
        patcher.start()
        self.addCleanup(patcher.stop)
        sleep_patcher = mock.patch.object(time, "sleep", lambda *args, **kwargs: None)
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)
        # 脱敏表是模块级状态：每个测试用独立列表，防跨测试污染
        secrets_patcher = mock.patch.object(utils_mod, "_SECRETS", [])
        secrets_patcher.start()
        self.addCleanup(secrets_patcher.stop)
        warned_patcher = mock.patch.object(utils_mod, "_sink_write_warned", False)
        warned_patcher.start()
        self.addCleanup(warned_patcher.stop)
        # 调度环境可能带着 bizdate / SKYNET_BIZDATE / ODS_BIZDATE_TZ：不隔离的话默认业务日用例会吃到脏值。
        # 只摘掉这三个键，不动 PATH/TEMP（clear=True 会弄坏临时目录/外部命令）。
        env_patcher = mock.patch.dict(os.environ, clear=False)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        for name in ("bizdate", "SKYNET_BIZDATE", "ODS_BIZDATE_TZ"):
            os.environ.pop(name, None)
        # 运行锁落到临时目录：lock_path 默认把锁写到工具目录的 .run-locks/，而单测用的是临时
        # 作业路径（锁名哈希每次不同），不重定向会在仓库里无限累积锁文件、污染工作区。
        # 只改测试侧的锁根路径，生产行为不变。
        lock_dir = tempfile.mkdtemp(prefix="feishu2ods-test-locks-")
        self.addCleanup(shutil.rmtree, lock_dir, ignore_errors=True)
        os.environ["ODS_TARGET_LOCK_DIR"] = str(pathlib.Path(lock_dir) / "targets")
        file_patcher = mock.patch.object(utils_mod, "__file__", str(pathlib.Path(lock_dir) / "utils.py"))
        file_patcher.start()
        self.addCleanup(file_patcher.stop)


def make_job(**overrides) -> dict:
    job = {
        "job": "t",
        "feishu": {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx", "table_id": "tblx"},
        "maxcompute": {"project": "test_project", "access_key_id": "ak" * 2, "access_key_secret": "sk" * 4},
        "fields": {"日期": "biz_date", "金额": "amount"},
        "target": {
            "project": "test_project",
            "table": "ods_t_json_df",
            "column": "json",
            "comment": "c",
            "allow_empty": False,
        },
        "freshness": {"date_field": "biz_date", "lag_days": 0, "webhook": "https://x/hook/1"},
    }
    for key, value in overrides.items():
        job[key] = value
    return job


def make_args(**overrides) -> argparse.Namespace:
    """cli 参数默认值（字段集合与 cli.parse_args 的真实结果对齐；测试要什么改什么）。

    用统一构造函数而不是各处手写 argparse.Namespace：字段集合不一致时，
    实现新增 args.xxx 的访问会让用例以 AttributeError 而非真实行为失败/掩盖问题。
    """
    base = dict(
        job="jobs/x.json",
        init=False,
        init_out="",
        check=False,
        bizdate=None,  # argparse 未传 --bizdate 时是 None；空串表示"显式传了空值"（要报错）
        dry_run=False,
        force=False,
        skip_freshness=False,
        no_notify=False,
        project="",
        table="",
        sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS,
        log_file="",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


class _Col:
    def __init__(self, name, col_type):
        self.name = name
        self.type = col_type


class _Schema:
    def __init__(self, columns, partitions):
        self.columns = columns
        self.partitions = partitions


class _Table:
    def __init__(self, columns, partitions, **flags):
        self.table_schema = _Schema(columns, partitions)
        self.is_virtual_view = flags.get("is_virtual_view", False)
        self.is_materialized_view = False
        self.is_transactional = flags.get("is_transactional", False)


class _FakeWriter:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def write(self, rows):
        self.sink.extend(rows)


class _Part:
    """假分区（只带 name，purge 用）。"""

    def __init__(self, name):
        self.name = name


class _FakeTable:
    """记录 delete/create/write 调用序列的假表。"""

    def __init__(self, fail_first_write=False):
        self.table_schema = _Schema([_Col("json", "string")], [_Col("pt", "string")])
        self.is_virtual_view = self.is_materialized_view = self.is_transactional = False
        self.calls = []
        self.rows = []
        self.partitions = []
        self.fail_first_write = fail_first_write
        self.write_attempts = 0
        self.reopens: list[bool] = []  # 记录 open_writer 的 reopen 实参：去掉它不该悄悄通过

    def delete_partition(self, spec, if_exists=False):
        self.calls.append(("delete", spec))

    def create_partition(self, spec, if_not_exists=False):
        self.calls.append(("create", spec))

    def exist_partition(self, spec):
        # 只有显式种下的分区算存在（write_partition 的 DDL 由 _FakeOdps 记录，
        # 不经过这里）；用例按需往 self.partitions 里放 spec 来模拟残留 __keep。
        return spec in self.partitions

    def open_writer(self, partition=None, reopen=False):
        self.write_attempts += 1
        if self.fail_first_write and self.write_attempts == 1:
            raise RuntimeError("tunnel 断了一次")
        self.calls.append(("write", partition))
        self.reopens.append(reopen)
        return _FakeWriter(self.rows)


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = {} if payload is None else payload
        self.text = text
        self.headers = headers if headers is not None else {}
        self.closed = False

    def json(self, **kwargs):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload

    def close(self):
        # 与 requests.Response.close 对齐：重试/失败分支必须显式关掉，否则连接不归还连接池
        self.closed = True


class _FakeReader:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return iter(self._rows)

    def __exit__(self, *exc_info):
        return False


class _FakeInstance:
    """假 SQL 实例：默认立即成功；never_success=True 时用于测试超时保护。"""

    def __init__(self, rows, never_success=False):
        self._rows = rows
        self._never_success = never_success
        self.stopped = False

    def is_successful(self):
        return not self._never_success

    def is_terminated(self):
        return False

    def wait_for_success(self, timeout=None):
        return self

    def stop(self):
        self.stopped = True

    def open_reader(self):
        return _FakeReader(self._rows)


class _FakeOdps:
    """记录 SQL 的假 ODPS。

    run_sql 才是生产的真实入口（带超时需要异步实例才能轮询/取消）；execute_sql 保留一份，
    以防有调用方仍走阻塞式接口。
    """

    def __init__(self, rows):
        self._rows = rows
        self.sqls = []
        self.stage = _FakeTable()

    def get_table(self, name):
        if ".tmp_feishu2ods_" not in name:
            raise AssertionError(f"不应读取其它表：{name}")
        return self.stage

    def run_sql(self, sql):
        self.sqls.append(sql)
        if ".tmp_feishu2ods_" in sql and "drop if exists partition" in sql:
            self.stage.rows.clear()
        return _FakeInstance(self._rows)

    def execute_sql(self, sql):
        self.sqls.append(sql)
        return _FakeInstance(self._rows)

    @property
    def sql(self):
        """最近一条 SQL（方便直接断言）。"""
        return self.sqls[-1] if self.sqls else None


def make_spool_stats(records):
    """把记录列表装进 Spool + FetchStats（write_partition 新签名的测试辅助）。"""
    spool = cli_mod.SpoolWriter()
    stats = cli_mod.FetchStats()
    spool.write_records(records)
    stats.update(records)
    return spool, stats


def fetch_stub(records):
    """fetch_records 的流式替身：像真实实现一样写 sink、累积 stats、返回 stats。"""

    def _stub(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
        if sink is not None and stats is not None:
            for record in records:
                sink.write_records([record])
                stats.update([record])
            return stats
        return list(records)

    return _stub
