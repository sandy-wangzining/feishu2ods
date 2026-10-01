# -*- coding: utf-8 -*-
"""feishu2ods 离线单元测试：不访问网络、不连 MaxCompute（requests/pyodps 没装也能跑）。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import argparse
import io
import json
import os
import pathlib
import shutil
import sys
import tempfile
import time
import unittest
from collections import Counter
from datetime import date, datetime
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import config as config_mod  # noqa: E402
from feishu2ods import dates as dates_mod  # noqa: E402
from feishu2ods import fetch as fetch_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402
from feishu2ods import notify as notify_mod  # noqa: E402
from feishu2ods import utils as utils_mod  # noqa: E402
from feishu2ods import wizard as wizard_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None


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
        # 运行锁落到临时目录：lock_path 默认把锁写到工具目录的 .run-locks/，而单测用的是临时
        # 作业路径（锁名哈希每次不同），不重定向会在仓库里无限累积锁文件、污染工作区。
        # 只改测试侧的锁根路径，生产行为不变。
        lock_dir = tempfile.mkdtemp(prefix="feishu2ods-test-locks-")
        self.addCleanup(shutil.rmtree, lock_dir, ignore_errors=True)
        file_patcher = mock.patch.object(utils_mod, "__file__", str(pathlib.Path(lock_dir) / "utils.py"))
        file_patcher.start()
        self.addCleanup(file_patcher.stop)


# ---------------------------------------------------------------------------
# 夹具
# ---------------------------------------------------------------------------
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
        self.calls = []
        self.rows = []
        self.partitions = []
        self.fail_first_write = fail_first_write
        self.write_attempts = 0

    def delete_partition(self, spec, if_exists=False):
        self.calls.append(("delete", spec))

    def create_partition(self, spec, if_not_exists=False):
        self.calls.append(("create", spec))

    def open_writer(self, partition=None, reopen=False):
        self.write_attempts += 1
        if self.fail_first_write and self.write_attempts == 1:
            raise RuntimeError("tunnel 断了一次")
        self.calls.append(("write", partition))
        return _FakeWriter(self.rows)


class _FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = {} if payload is None else payload
        self.text = text
        self.headers = headers if headers is not None else {}

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


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

    def run_sql(self, sql):
        self.sqls.append(sql)
        return _FakeInstance(self._rows)

    def execute_sql(self, sql):
        self.sqls.append(sql)
        return _FakeInstance(self._rows)

    @property
    def sql(self):
        """最近一条 SQL（方便直接断言）。"""
        return self.sqls[-1] if self.sqls else None


# ---------------------------------------------------------------------------
# 业务日（pt）解析
# ---------------------------------------------------------------------------
class TestBizdate(OfflineTestCase):
    def test_parse_compact(self):
        self.assertEqual(dates_mod.parse_day_arg("20260927"), date(2026, 9, 27))

    def test_parse_iso(self):
        self.assertEqual(dates_mod.parse_day_arg("2026-09-27"), date(2026, 9, 27))

    def test_parse_bad_format(self):
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("2026/09/27")

    def test_parse_bad_date(self):
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("20260230")

    def test_env_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate())

    def test_env_value(self):
        with mock.patch.dict(os.environ, {"bizdate": "20260927"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 27))

    def test_env_bad_value(self):
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "2026-9-7"}, clear=True):
            with self.assertRaises(SystemExit):
                dates_mod.env_bizdate()

    def test_resolve_cli_wins(self):
        args = argparse.Namespace(bizdate="20260101")
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 1, 1))

    def test_resolve_env(self):
        args = argparse.Namespace(bizdate="")
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 2, 2))

    def test_resolve_default_yesterday(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 29, 10, 0, tzinfo=utils_mod.CN_TZ).astimezone(tz) if tz else cls(2026, 9, 29, 10, 0)

        args = argparse.Namespace(bizdate="")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(dates_mod, "datetime", _FixedDatetime):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 9, 28))


# ---------------------------------------------------------------------------
# job 校验
# ---------------------------------------------------------------------------
class TestValidateJob(OfflineTestCase):
    def test_valid_no_warnings(self):
        self.assertEqual(config_mod.validate_job(make_job()), [])

    def test_unknown_key_warning(self):
        job = make_job()
        job["extra_key"] = 1
        job["feishu"]["base_ur1"] = "typo"
        warnings = config_mod.validate_job(job)
        self.assertTrue(any("作业.extra_key" in w for w in warnings))
        self.assertTrue(any("feishu.base_ur1" in w for w in warnings))

    def test_missing_feishu_block(self):
        job = make_job()
        job.pop("feishu")
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_reserved_record_id(self):
        job = make_job(fields={"日期": "record_id"})
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_duplicate_english_keys(self):
        job = make_job(fields={"日期": "x", "金额": "x"})
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_whitespace_normalized(self):
        job = make_job(maxcompute={"project": " test_project ", "access_key_id": "ak", "access_key_secret": "sk"})
        config_mod.validate_job(job)
        self.assertEqual(job["maxcompute"]["project"], "test_project")

    def test_freshness_date_field_must_exist(self):
        job = make_job(freshness={"date_field": "nope"})
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_lag_days_bool_rejected(self):
        job = make_job(freshness={"date_field": "biz_date", "lag_days": True})
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_allow_empty_must_be_bool(self):
        job = make_job(target={"table": "t", "allow_empty": "false"})
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)


# ---------------------------------------------------------------------------
# 记录组装
# ---------------------------------------------------------------------------
class TestBuildRecords(OfflineTestCase):
    def test_basic_mapping(self):
        records = fetch_mod.build_records(
            ["日期", "金额", "备注"],
            ["r1"],
            [["2026-09-27", "$1", "x"]],
            {"日期": "biz_date", "金额": "amount"},
        )
        self.assertEqual(records, [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}])
        self.assertEqual(list(records[0].keys())[0], "record_id")

    def test_missing_column(self):
        with self.assertRaises(SystemExit):
            fetch_mod.build_records(["日期"], ["r1"], [["x"]], {"不存在的列": "v"})

    def test_empty_table(self):
        self.assertEqual(fetch_mod.build_records([], [], [], {"日期": "biz_date"}), [])

    def test_empty_table_still_checks_mapping(self):
        # 接口给了字段列表但 0 行：映射错了也要报错，不能「空表假通过」
        with self.assertRaises(SystemExit):
            fetch_mod.build_records(["日期"], [], [], {"不存在的列": "v"})

    def test_short_row_fills_none(self):
        records = fetch_mod.build_records(["a", "b"], ["r1"], [["1"]], {"b": "bbb"})
        self.assertIsNone(records[0]["bbb"])

    def test_ids_without_rows_rejected(self):
        # 接口异常：给了记录 ID 却没有行数据，宁可报错也不能当空表静默跳过
        with self.assertRaises(SystemExit):
            fetch_mod.build_records(["a"], ["r1"], [], {"a": "aaa"})

    def test_rows_without_fields_rejected(self):
        with self.assertRaises(SystemExit):
            fetch_mod.build_records([], ["r1"], [["1"]], {"a": "aaa"})

    def test_extra_columns_collected(self):
        extras: list[str] = []
        records = fetch_mod.build_records(
            ["日期", "金额", "新列A", "新列B"],
            ["r1"],
            [["2026-09-27", "$1", "x", "y"]],
            {"日期": "biz_date", "金额": "amount"},
            extra_out=extras,
        )
        self.assertEqual(records, [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}])
        self.assertEqual(extras, ["新列A", "新列B"])


# ---------------------------------------------------------------------------
# 新鲜度
# ---------------------------------------------------------------------------
class TestFreshness(OfflineTestCase):
    def test_normalize_iso(self):
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27T00:00:00.000+08:00"), "2026-09-27")

    def test_normalize_slash(self):
        self.assertEqual(dates_mod.normalize_date_value("2026/9/7"), "2026-09-07")

    def test_normalize_epoch_ms(self):
        self.assertEqual(dates_mod.normalize_date_value(0), "1970-01-01")

    def test_normalize_garbage(self):
        self.assertIsNone(dates_mod.normalize_date_value(True))
        self.assertIsNone(dates_mod.normalize_date_value("abc"))
        self.assertIsNone(dates_mod.normalize_date_value(10**30))

    def test_found(self):
        records = [{"biz_date": "2026-09-27T00:00:00.000+08:00"}]
        self.assertIsNone(dates_mod.freshness_problem(records, "biz_date", "2026-09-27"))

    def test_missing(self):
        records = [{"biz_date": "2026-09-26T00:00:00.000+08:00"}]
        self.assertEqual(dates_mod.freshness_problem(records, "biz_date", "2026-09-27"), ("2026-09-27", "2026-09-26"))

    def test_empty(self):
        self.assertEqual(dates_mod.freshness_problem([], "biz_date", "2026-09-27"), ("2026-09-27", None))


# ---------------------------------------------------------------------------
# HTTP 与拉取
# ---------------------------------------------------------------------------
@unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
class TestRequestJson(OfflineTestCase):
    def test_ok(self):
        with mock.patch.object(auth_mod.requests, "request", return_value=_FakeResponse(200, {"code": 0})):
            self.assertEqual(fetch_mod.request_json("GET", "https://x", "t"), {"code": 0})

    def test_retry_500_then_ok(self):
        responses = [_FakeResponse(500, text="boom"), _FakeResponse(200, {"code": 0})]
        with mock.patch.object(auth_mod.requests, "request", side_effect=responses) as call:
            self.assertEqual(fetch_mod.request_json("GET", "https://x", "t"), {"code": 0})
        self.assertEqual(call.call_count, 2)

    def test_4xx_raises_api_error(self):
        with mock.patch.object(auth_mod.requests, "request", return_value=_FakeResponse(403, text="nope")):
            with self.assertRaises(fetch_mod.ApiHttpError) as ctx:
                fetch_mod.request_json("GET", "https://x", "t")
        self.assertEqual(ctx.exception.status, 403)

    def test_exhausted(self):
        with mock.patch.object(auth_mod.requests, "request", return_value=_FakeResponse(500, text="boom")):
            with self.assertRaises(SystemExit):
                fetch_mod.request_json("GET", "https://x", "t")

    def test_retry_429_then_ok(self):
        responses = [_FakeResponse(429, text="slow down"), _FakeResponse(200, {"code": 0})]
        with mock.patch.object(auth_mod.requests, "request", side_effect=responses) as call:
            self.assertEqual(fetch_mod.request_json("GET", "https://x", "t"), {"code": 0})
        self.assertEqual(call.call_count, 2)

    def test_non_dict_body_raises_api_error(self):
        with mock.patch.object(auth_mod.requests, "request", return_value=_FakeResponse(403, text="no")):
            with self.assertRaises(fetch_mod.ApiHttpError):
                fetch_mod.request_json("GET", "https://x", "t")

    def test_redirect_raises_api_error_without_retry(self):
        # 301/302/303 会把 POST 降级成不带 body 的 GET（请求参数全丢），
        # 鉴权头也可能被转发到别的地址——直接失败，不重试
        resp = _FakeResponse(302, text="", headers={"Location": "https://other.example.com/new"})
        with mock.patch.object(auth_mod.requests, "request", return_value=resp) as call:
            with self.assertRaises(fetch_mod.ApiHttpError) as ctx:
                fetch_mod.request_json("POST", "https://x", "t", body={"app_id": "a", "app_secret": "s"})
        self.assertEqual(ctx.exception.status, 302)
        self.assertEqual(call.call_count, 1)  # 确定性错误不重试

    def test_request_disables_redirects(self):
        with mock.patch.object(auth_mod.requests, "request", return_value=_FakeResponse(200, {"code": 0})) as call:
            fetch_mod.request_json("GET", "https://x", "t")
        kwargs = call.call_args.kwargs
        self.assertIs(kwargs.get("allow_redirects"), False)


@unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
class TestNotify(OfflineTestCase):
    def test_notify_ok(self):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"code": 0}
        messages: list[str] = []
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.notify("https://x/hook/1", "标题", ["第一行"], footer="尾部")
        self.assertIn("飞书通知已发送", "\n".join(messages))

    def test_notify_error_swallowed(self):
        messages: list[str] = []
        with (
            mock.patch.object(notify_mod.requests, "post", side_effect=RuntimeError("网络坏了")),
            mock.patch.object(notify_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.notify("https://x/hook/1", "标题", ["第一行"])
        self.assertIn("飞书通知发送失败", "\n".join(messages))


class TestFetchRecords(OfflineTestCase):
    def setUp(self):
        super().setUp()
        self.mapping = {"日期": "biz_date", "金额": "amount"}
        self.job_feishu = {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx", "table_id": "tblx"}

    @staticmethod
    def _page(rows, ids, has_more, fields=("日期", "金额"), rev=None):
        data = {"fields": list(fields), "data": rows, "record_id_list": ids, "has_more": has_more}
        if rev is not None:
            data["rev"] = rev
        return {"code": 0, "data": data}

    def test_two_pages(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True),
            self._page([["2026-09-26", "$2"]], ["r2"], False),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            records = cli_mod.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual([r["record_id"] for r in records], ["r1", "r2"])
        self.assertEqual(records[0]["biz_date"], "2026-09-27")

    def test_max_pages_one(self):
        pages = [self._page([["2026-09-27", "$1"]], ["r1"], True)]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            records = cli_mod.fetch_records(self.job_feishu, self.mapping, max_pages=1)
        self.assertEqual(len(records), 1)

    def test_token_refresh(self):
        pages = [{"code": 99991663}, self._page([["2026-09-27", "$1"]], ["r1"], False)]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", side_effect=["tok1", "tok2"]),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            records = cli_mod.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual(len(records), 1)

    def test_rate_limit_retry(self):
        pages = [{"code": 99991400}, self._page([["2026-09-27", "$1"]], ["r1"], False)]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            records = cli_mod.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual(len(records), 1)

    def test_fields_changed_mid_pagination(self):
        pages = [
            self._page([["a", "b"]], ["r1"], True),
            self._page([["a", "b"]], ["r2"], False, fields=("日期", "换列")),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.fetch_records(self.job_feishu, self.mapping)

    def test_duplicate_record_ids(self):
        pages = [
            self._page([["a", "b"]], ["r1"], True),
            self._page([["a", "b"]], ["r1"], False),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.fetch_records(self.job_feishu, self.mapping)

    def test_row_id_count_mismatch(self):
        pages = [
            {
                "code": 0,
                "data": {"fields": ["日期"], "data": [["x"]], "record_id_list": ["r1", "r2"], "has_more": False},
            }
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.fetch_records(self.job_feishu, self.mapping)

    def test_rev_changed_mid_pagination(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True, rev=12),
            self._page([["2026-09-26", "$2"]], ["r2"], False, rev=13),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.fetch_records(self.job_feishu, self.mapping)

    def test_rev_same_mid_pagination_ok(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True, rev=12),
            self._page([["2026-09-26", "$2"]], ["r2"], False, rev=12),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            self.assertEqual(len(cli_mod.fetch_records(self.job_feishu, self.mapping)), 2)


# ---------------------------------------------------------------------------
# MaxCompute：DDL / 结构校验 / 写入 / 计数
# ---------------------------------------------------------------------------
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


class TestMc(OfflineTestCase):
    def test_build_ddl(self):
        ddl = mc_mod.build_ddl("test_project", "t", "json", "a'b")
        self.assertIn("partitioned by (pt string", ddl)
        self.assertIn("a''b", ddl)

    def test_verify_schema_ok(self):
        table = _Table([_Col("json", "string"), _Col("pt", "string")], [_Col("pt", "string")])
        cli_mod.verify_schema(table, "t", "json")

    def test_verify_schema_mismatch(self):
        table = _Table([_Col("payload", "string"), _Col("pt", "string")], [_Col("pt", "string")])
        with self.assertRaises(SystemExit):
            cli_mod.verify_schema(table, "t", "json")

    def test_verify_schema_view(self):
        table = _Table([_Col("json", "string")], [], is_virtual_view=True)
        with self.assertRaises(SystemExit):
            cli_mod.verify_schema(table, "t", "json")

    @staticmethod
    def _verify_row(cnt, ucnt=None, mn="r1", mx=None):
        """verify_partition 的假返回值（cnt/ucnt/mn/mx）。"""
        return {"cnt": cnt, "ucnt": cnt if ucnt is None else ucnt, "mn": mn, "mx": mx or mn}

    def test_write_partition_calls_and_rows(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, mn="r1", mx="r2")])
        records = [{"record_id": "r1", "a": 1}, {"record_id": "r2", "a": 2}]
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", records)
        # 先写临时分区（删残留 tmp + 建 tmp），再删正式分区、rename 顶上；
        # 分区增删走带超时的 DDL（run_sql），表对象只负责 Tunnel 写入
        self.assertEqual([c[0] for c in table.calls], ["write"])
        self.assertEqual(table.calls[0][1], "pt=20260928__tmp")
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260928__tmp')", sqls)
        self.assertIn("add if not exists partition (pt='20260928__tmp')", sqls)
        self.assertIn("drop if exists partition (pt='20260928')", sqls)
        self.assertIn("rename to partition (pt='20260928')", sqls)
        self.assertIn("pt='20260928__tmp'", odps.sql)

    def test_write_partition_retry(self):
        table = _FakeTable(fail_first_write=True)
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 2)
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}']])

    def test_write_partition_row_too_big(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "MAX_ROW_BYTES", 10):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(
                    odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "big": "x" * 100}]
                )
        self.assertEqual(table.calls, [])  # 动分区之前就该报错

    def test_write_partition_count_mismatch_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])  # 计划 2 行，实际 1 行
        with self.assertRaises(SystemExit):
            cli_mod.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(table.write_attempts, mc_mod.WRITE_ATTEMPTS)
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_duplicate_ids_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, ucnt=1, mn="r1", mx="r1")])
        with self.assertRaises(SystemExit):
            cli_mod.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(table.write_attempts, mc_mod.WRITE_ATTEMPTS)
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_id_range_mismatch_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, mn="rx", mx="ry")])
        with self.assertRaises(SystemExit):
            cli_mod.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_empty_replaces_with_empty(self):
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 0, "ucnt": 0, "mn": None, "mx": None}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [])
        self.assertEqual([c[0] for c in table.calls], ["write"])
        self.assertEqual(table.rows, [])
        self.assertIn("rename to partition (pt='20260928')", odps.sql)

    def test_write_partition_rename_failure_mentions_deleted_final(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("ddl boom")):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区可能已被删掉", str(ctx.exception))
        # 失败后尽力清掉临时分区（最后一步是 drop tmp 的分区 DDL）
        self.assertIn("drop if exists partition (pt='20260928__tmp')", odps.sqls[-1])

    def test_write_partition_cleanup_failure_keeps_error(self):
        table = mock.Mock()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])

        def drop(o, project, table_name, spec, timeout=None):
            if spec.endswith("__tmp"):
                raise RuntimeError("delete boom")

        with mock.patch.object(mc_mod, "drop_partition", side_effect=drop):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区未动", str(ctx.exception))
        self.assertIn("残留", str(ctx.exception))

    def test_purge_stale_tmp_partitions(self):
        table = _FakeTable()
        table.partitions = [_Part("pt='20260927__tmp'"), _Part("pt='20260928'"), _Part("pt='20260929__tmp'")]
        odps = _FakeOdps([])
        cli_mod.purge_stale_tmp_partitions(odps, table, "p", "t")
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260927__tmp')", sqls)
        self.assertIn("drop if exists partition (pt='20260929__tmp')", sqls)
        self.assertNotIn("20260928", sqls)  # 正式分区不动

    def test_sql_spec_normalizes_quotes(self):
        self.assertEqual(mc_mod._sql_spec("pt=20260928"), "pt='20260928'")
        self.assertEqual(mc_mod._sql_spec("pt='20260928'"), "pt='20260928'")
        self.assertEqual(mc_mod._sql_spec('pt="20260928"'), "pt='20260928'")

    def test_verify_partition(self):
        odps = _FakeOdps([{"cnt": 3, "ucnt": 3, "mn": "a", "mx": "c"}])
        self.assertEqual(mc_mod.verify_partition(odps, "p", "t", "json", "20260928"), (3, 3, "a", "c"))
        self.assertIn("get_json_object(json, '$.record_id')", odps.sql)
        self.assertIn("where pt = '20260928'", odps.sql)

    def test_write_partition_rename_failure_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("ddl boom")):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, mc_mod.WRITE_ATTEMPTS)

    def test_count_partition(self):
        odps = _FakeOdps([{"cnt": 7}])
        self.assertEqual(cli_mod.count_partition(odps, "p", "t", "20260928"), 7)
        self.assertIn("where pt = '20260928'", odps.sql)


# ---------------------------------------------------------------------------
# 运行锁
# ---------------------------------------------------------------------------
class TestRunLock(OfflineTestCase):
    def test_lock_path_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = cli_mod.lock_path(pathlib.Path(tmp) / "x.json", root=pathlib.Path(tmp) / "locks")
            self.assertTrue(str(path).endswith(".lock"))
            self.assertEqual(path.parent, pathlib.Path(tmp) / "locks")
            self.assertTrue(path.parent.is_dir())

    def test_mutual_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = cli_mod.lock_path(pathlib.Path(tmp) / "x.json", root=pathlib.Path(tmp))
            with utils_mod.RunLock(path):
                with self.assertRaises(SystemExit):
                    with utils_mod.RunLock(path):
                        pass
            with utils_mod.RunLock(path):  # 释放之后可以再次拿到
                pass


# ---------------------------------------------------------------------------
# --init 向导
# ---------------------------------------------------------------------------
class TestRunInit(OfflineTestCase):
    def _run(self, answers, fetch_fields):
        echoed: list[str] = []
        answers = list(answers)

        def ask(prompt: str = "") -> str:
            return answers.pop(0) if answers else ""

        code = wizard_mod.run_init(
            out_path="",
            ask=ask,
            echo=lambda line="": echoed.append(str(line)),
            workdir=pathlib.Path(self._tmp.name),
            fetch_fields=fetch_fields,
            ask_secret=ask,
        )
        return code, echoed

    def setUp(self):
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_generate_job(self):
        fields = ["日期", "金额", "备注"]

        def fake_fetch(feishu):
            return fields, {"日期": "2026-09-27", "金额": "$1"}

        answers = [
            "probe_job",  # ① 作业名
            "https://example.feishu.cn/base/IC4TEST?table=tblTEST&view=v",  # ② 链接
            "cli_test123",  # ③ app_id
            "secret-abc-123",  # App Secret
            "biz_date",  # 日期 → 英文键
            "amount",  # 金额 → 英文键
            "",  # 备注 跳过
            "",  # 项目（默认）
            "",  # 表名（默认）
            "",  # 注释（默认）
            "LTAI_TEST",  # AK
            "SK_SECRET_XYZ",  # SK
            "y",  # 新鲜度
            "biz_date",  # date_field
            "",  # webhook
        ]
        code, echoed = self._run(answers, fake_fetch)
        self.assertEqual(code, 0)
        out = pathlib.Path(self._tmp.name) / "jobs" / "probe_job.json"
        self.assertTrue(out.is_file())
        job = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(job["fields"], {"日期": "biz_date", "金额": "amount"})
        self.assertEqual(job["target"]["table"], "ods_probe_job_json_df")
        self.assertEqual(job["freshness"], {"date_field": "biz_date", "lag_days": 0})
        config_mod.validate_job(json.loads(json.dumps(job)))  # 生成的配置必须能过校验
        # 密钥不能出现在任何回显里
        text = "\n".join(echoed)
        self.assertNotIn("secret-abc-123", text)
        self.assertNotIn("SK_SECRET_XYZ", text)

    def test_refuse_overwrite(self):
        jobs_dir = pathlib.Path(self._tmp.name) / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        existing = jobs_dir / "probe_job.json"
        existing.write_text("OLD", encoding="utf-8")
        code, _ = self._run(["probe_job", "n"], lambda feishu: (["日期"], {}))
        self.assertEqual(code, 1)
        self.assertEqual(existing.read_text(encoding="utf-8"), "OLD")

    def test_cancel_when_nothing_mapped(self):
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {}

        answers = [
            "probe_job",
            "IC4TEST",
            "tblTEST",
            "cli_test123",
            "secret-abc-123",
            "",  # 日期 跳过
        ]
        code, _ = self._run(answers, fake_fetch)
        self.assertEqual(code, 1)

    def test_wiki_link_rejected_then_base_ok(self):
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {"日期": "2026-09-27"}

        answers = [
            "probe_job",
            "https://example.feishu.cn/wiki/AbCdEfGh?table=tblTEST",  # wiki 链接 → 提示重问
            "https://example.feishu.cn/base/IC4TEST?table=tblTEST",  # 第二次给 base 链接
            "cli_test123",
            "secret-abc-123",
            "biz_date",
            "",  # 项目（默认）
            "",  # 表名（默认）
            "",  # 注释（默认）
            "LTAI_TEST",
            "SK_SECRET_XYZ",
            "n",  # 不要新鲜度
        ]
        code, echoed = self._run(answers, fake_fetch)
        self.assertEqual(code, 0)
        self.assertTrue(any("wiki" in line for line in echoed))

    def test_job_name_dash_gets_legal_table_default(self):
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {"日期": "2026-09-27"}

        answers = [
            "probe-job",  # 作业名带 '-'（文件名可以，表名不行）
            "IC4TEST",
            "tblTEST",
            "cli_test123",
            "secret-abc-123",
            "biz_date",
            "",
            "",
            "",
            "LTAI_TEST",
            "SK_SECRET_XYZ",
            "n",
        ]
        code, _ = self._run(answers, fake_fetch)
        self.assertEqual(code, 0)
        out = pathlib.Path(self._tmp.name) / "jobs" / "probe-job.json"
        job = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(job["target"]["table"], "ods_probe_job_json_df")


# ---------------------------------------------------------------------------
# run_check / run_sync 接线
# ---------------------------------------------------------------------------
class TestWiring(OfflineTestCase):
    def test_parse_args_alias(self):
        args = cli_mod.parse_args(["--job", "x", "--no-check"])
        self.assertTrue(args.skip_freshness)
        args = cli_mod.parse_args(["--job", "x", "--skip-freshness"])
        self.assertTrue(args.skip_freshness)

    def test_main_requires_job(self):
        self.assertEqual(cli_mod.main([]), 2)

    def test_main_missing_job_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(cli_mod.main(["--job", str(pathlib.Path(tmp) / "nope.json")]), 1)

    def test_run_check_ok(self):
        o = mock.Mock()
        o.exist_table.return_value = False
        with (
            mock.patch.object(cli_mod, "fetch_records", return_value=[{"a": 1}]),
            mock.patch.object(cli_mod, "connect_odps", return_value=o),
        ):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 0)

    def test_run_check_api_failure(self):
        with mock.patch.object(cli_mod, "fetch_records", side_effect=SystemExit("boom")):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 1)

    def test_run_sync_dry_run_pass(self):
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-28", "amount": "$1"}]
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_freshness_warns_but_continues(self):
        # 缺数据只告警不失败（人填的表，节假日没人填是常态）：rc=0、照常写、飞书告警一次
        args = argparse.Namespace(skip_freshness=False, no_notify=False, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_called_once()
        self.assertIn("已照常写入", notifier.call_args.args[1])

    def test_run_sync_freshness_warns_but_no_notify_flag_silences_webhook(self):
        # --no-notify 只关飞书提醒：rc 仍是 0（缺数据不阻塞）
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_not_called()

    def test_run_sync_zero_records_still_fails(self):
        # 真异常（表被清空 = 0 行）仍然失败：缺数放行不等于放行空表
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub([])),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)
        notifier.assert_not_called()

    def test_run_sync_new_fields_notifies_once(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=False, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            if extra_out is not None:
                extra_out.extend(["新列A", "新列A", "新列B"])
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_called_once()
        joined = "\n".join(notifier.call_args.args[2])
        self.assertIn("新增列", notifier.call_args.args[1])
        self.assertIn("新列A", joined)
        self.assertIn("新列B", joined)

    def test_run_sync_new_fields_no_notify_flag(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            if extra_out is not None:
                extra_out.append("新列A")
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_not_called()

    def test_run_sync_skip_freshness(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_warns_empty_rows(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": None, "amount": None}]
        messages: list[str] = []
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        self.assertTrue(any("全为空" in m for m in messages))

    def test_run_sync_writes_and_counts(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=600)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        table = mock.Mock()
        odps = mock.Mock()
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "connect_odps", return_value=odps),
            mock.patch.object(cli_mod, "ensure_table", return_value=table) as ensure,
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition") as writer,
            mock.patch.object(cli_mod, "count_partition", return_value=1) as counter,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        writer.assert_called_once()
        self.assertEqual(writer.call_args.args[0], odps)
        self.assertEqual(writer.call_args.args[7].count, 1)  # stats 带 1 条记录
        # 建表 / 写分区 / 写后核对三条 SQL 路径都要带上 --sql-timeout 的口径
        self.assertEqual(ensure.call_args.kwargs["timeout"], 600)
        self.assertEqual(writer.call_args.kwargs["timeout"], 600)
        self.assertEqual(counter.call_args.kwargs["timeout"], 600)

    def test_run_sync_count_mismatch(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=600)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition"),
            mock.patch.object(cli_mod, "count_partition", return_value=0),
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)

    def test_main_keyboard_interrupt(self):
        with mock.patch.object(cli_mod, "_run", side_effect=KeyboardInterrupt):
            self.assertEqual(cli_mod.main(["--job", "x"]), 130)

    def test_main_unexpected_error(self):
        with mock.patch.object(cli_mod, "_run", side_effect=RuntimeError("boom")):
            self.assertEqual(cli_mod.main(["--job", "x"]), 1)


# ===========================================================================
# 流式改造（v1.4）与边界补充：FetchStats / SpoolWriter / 流式拉取 / 双形态新鲜度
# ===========================================================================
class TestFetchStats(OfflineTestCase):
    def test_empty_stats(self):
        stats = cli_mod.FetchStats()
        self.assertEqual(stats.count, 0)
        self.assertEqual(stats.distinct_ids(), 0)
        self.assertEqual((stats.min_id, stats.max_id), ("", ""))
        self.assertEqual(stats.empty_rows, 0)

    def test_count_and_ids(self):
        stats = cli_mod.FetchStats()
        stats.update([{"record_id": "r1", "a": 1}, {"record_id": "r2", "a": 2}])
        self.assertEqual(stats.count, 2)
        self.assertEqual(stats.distinct_ids(), 2)
        self.assertEqual((stats.min_id, stats.max_id), ("r1", "r2"))

    def test_min_max_tracks_order_independently(self):
        stats = cli_mod.FetchStats()
        stats.update([{"record_id": "r5"}, {"record_id": "r1"}, {"record_id": "r9"}])
        self.assertEqual((stats.min_id, stats.max_id), ("r1", "r9"))

    def test_duplicate_ids_distinct(self):
        stats = cli_mod.FetchStats()
        stats.update([{"record_id": "r1"}, {"record_id": "r1"}])
        self.assertEqual(stats.count, 2)
        self.assertEqual(stats.distinct_ids(), 1)

    def test_missing_record_id_not_in_ids(self):
        stats = cli_mod.FetchStats()
        stats.update([{"record_id": "", "a": 1}, {"record_id": "r1"}])
        self.assertEqual(stats.distinct_ids(), 1)
        self.assertEqual(stats.min_id, "r1")

    def test_empty_rows_detection(self):
        stats = cli_mod.FetchStats()
        stats.update(
            [
                {"record_id": "r1", "a": None, "b": None},  # 全空
                {"record_id": "r2", "a": 1},  # 有值
                {"record_id": "r3"},  # 只有 record_id（也全空）
            ]
        )
        self.assertEqual(stats.empty_rows, 2)

    def test_date_values_and_counter(self):
        stats = cli_mod.FetchStats(date_field="biz_date")
        stats.update(
            [
                {"biz_date": "2026-09-27T00:00:00+08:00"},
                {"biz_date": "2026-09-27"},
                {"biz_date": "2026/9/26"},
                {"biz_date": "垃圾值"},
            ]
        )
        self.assertEqual(stats.date_values, {"2026-09-27", "2026-09-26"})
        self.assertEqual(stats.date_counter["2026-09-27"], 2)
        self.assertEqual(stats.date_counter["2026-09-26"], 1)

    def test_no_date_field_collects_nothing(self):
        stats = cli_mod.FetchStats()
        stats.update([{"biz_date": "2026-09-27"}])
        self.assertEqual(stats.date_values, set())
        self.assertEqual(stats.date_counter, Counter())

    def test_none_date_ignored(self):
        stats = cli_mod.FetchStats(date_field="d")
        stats.update([{"d": None}])
        self.assertEqual(stats.date_values, set())


class TestSpoolWriter(OfflineTestCase):
    def test_roundtrip_rows(self):
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"a": 1}, {"b": "中文"}])
        rows = list(spool.iter_rows())
        self.assertEqual(rows, ['{"a":1}', '{"b":"中文"}'])
        self.assertEqual(spool.count, 2)
        spool.close()

    def test_write_returns_count(self):
        spool = cli_mod.SpoolWriter()
        self.assertEqual(spool.write_records([{"a": 1}, {"a": 2}]), 2)
        spool.close()

    def test_batches(self):
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"i": i} for i in range(5)])
        batches = list(spool.iter_batches(batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 2, 1])
        spool.close()

    def test_iter_rows_rerunnable(self):
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"a": 1}])
        self.assertEqual(len(list(spool.iter_rows())), 1)
        self.assertEqual(len(list(spool.iter_rows())), 1)  # 重试场景：重新读一遍
        spool.close()

    def test_empty_spool(self):
        spool = cli_mod.SpoolWriter()
        self.assertEqual(list(spool.iter_rows()), [])
        self.assertEqual(list(spool.iter_batches()), [])
        spool.close()

    def test_close_deletes_file(self):
        spool = cli_mod.SpoolWriter()
        path = spool.path
        spool.write_records([{"a": 1}])
        spool.close()
        self.assertFalse(path.exists())

    def test_close_keep(self):
        spool = cli_mod.SpoolWriter()
        path = spool.path
        spool.write_records([{"a": 1}])
        spool.close(keep=True)
        self.assertTrue(path.exists())
        path.unlink()

    def test_iter_rows_strips_newline(self):
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"a": "x\ny"}])  # 值里含换行会被 JSON 转义
        rows = list(spool.iter_rows())
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].startswith('{"a":"x\\ny"}'))
        spool.close()

    def test_custom_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "spool.jsonl"
            spool = cli_mod.SpoolWriter(path)
            spool.write_records([{"a": 1}])
            spool.close(keep=True)
            self.assertEqual(path.read_text(encoding="utf-8"), '{"a":1}\n')


class TestFetchRecordsStreaming(OfflineTestCase):
    """流式模式：每页落盘 + stats 累积；映射校验只打一次告警。"""

    def setUp(self):
        super().setUp()
        self.mapping = {"日期": "biz_date", "金额": "amount"}
        self.job_feishu = {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx", "table_id": "tblx"}

    @staticmethod
    def _page(rows, ids, has_more):
        return {
            "code": 0,
            "data": {"fields": ["日期", "金额"], "data": rows, "record_id_list": ids, "has_more": has_more},
        }

    def test_stream_writes_and_stats(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True),
            self._page([["2026-09-26", "$2"]], ["r2"], False),
        ]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats(date_field="biz_date")
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            result = cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertIs(result, stats)
        self.assertEqual(stats.count, 2)
        self.assertEqual(spool.count, 2)
        self.assertEqual(
            list(spool.iter_rows()),
            [
                '{"record_id":"r1","biz_date":"2026-09-27","amount":"$1"}',
                '{"record_id":"r2","biz_date":"2026-09-26","amount":"$2"}',
            ],
        )
        spool.close()

    def test_stream_empty_table(self):
        pages = [self._page([], [], False)]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertEqual(stats.count, 0)
        self.assertEqual(list(spool.iter_rows()), [])
        spool.close()

    def test_stream_mapping_missing_aborts(self):
        pages = [
            {"code": 0, "data": {"fields": ["别的列"], "data": [["x"]], "record_id_list": ["r1"], "has_more": False}}
        ]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertIn("找不到", str(ctx.exception))
        spool.close()

    def test_stream_extra_column_warns_once_and_collects(self):
        pages = [
            {
                "code": 0,
                "data": {
                    "fields": ["日期", "金额", "新列"],
                    "data": [["a", "b", "c"]],
                    "record_id_list": ["r1"],
                    "has_more": True,
                },
            },
            {
                "code": 0,
                "data": {
                    "fields": ["日期", "金额", "新列"],
                    "data": [["d", "e", "f"]],
                    "record_id_list": ["r2"],
                    "has_more": False,
                },
            },
        ]
        extra: list[str] = []
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        warnings: list[str] = []
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
            mock.patch.object(fetch_mod, "log", side_effect=lambda msg: warnings.append(str(msg))),
        ):
            cli_mod.fetch_records(self.job_feishu, self.mapping, extra_out=extra, sink=spool, stats=stats)
        self.assertEqual(extra, ["新列"])
        # 两页只打一次"未映射"警告（旧实现逐页重复）
        self.assertEqual(sum(1 for w in warnings if "未映射" in w), 1)
        self.assertEqual(stats.count, 2)
        spool.close()

    def test_stream_extra_out_none_ok(self):
        pages = [
            {
                "code": 0,
                "data": {
                    "fields": ["日期", "金额", "新列"],
                    "data": [["a", "b", "c"]],
                    "record_id_list": ["r1"],
                    "has_more": False,
                },
            },
        ]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertEqual(stats.count, 1)
        spool.close()

    def test_stream_accumulated_log_is_cumulative(self):
        # 回归：流式模式 raw_rows 不再累积，翻页日志的「累计」必须取 stats.count，否则每页都只显示本页行数
        pages = [
            self._page([["2026-09-27", "$1"], ["2026-09-26", "$2"]], ["r1", "r2"], True),
            self._page([["2026-09-25", "$3"]], ["r3"], False),
        ]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        logs: list[str] = []
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
            mock.patch.object(fetch_mod, "log", side_effect=lambda msg: logs.append(str(msg))),
        ):
            cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertTrue(any("累计 2" in line for line in logs))
        self.assertTrue(any("累计 3" in line for line in logs))
        spool.close()

    def test_stream_records_without_fields_aborts_cleanly(self):
        # 接口返回了记录却没给字段列表：流式路径要给出明确的 SystemExit，而不是裸 KeyError
        pages = [{"code": 0, "data": {"data": [["x"]], "record_id_list": ["r1"], "has_more": False}}]
        spool = cli_mod.SpoolWriter()
        stats = cli_mod.FetchStats()
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.fetch_records(self.job_feishu, self.mapping, sink=spool, stats=stats)
        self.assertIn("字段列表", str(ctx.exception))
        spool.close()


class TestFreshnessDualMode(OfflineTestCase):
    """freshness_problem 两种输入形态：记录列表（旧）与日期集合（流式）。"""

    def test_records_form(self):
        records = [{"biz_date": "2026-09-26T00:00:00+08:00"}]
        self.assertEqual(dates_mod.freshness_problem(records, "biz_date", "2026-09-27"), ("2026-09-27", "2026-09-26"))

    def test_set_form(self):
        self.assertEqual(dates_mod.freshness_problem({"2026-09-26"}, "x", "2026-09-27"), ("2026-09-27", "2026-09-26"))

    def test_set_form_found(self):
        self.assertIsNone(dates_mod.freshness_problem({"2026-09-27"}, "x", "2026-09-27"))

    def test_set_form_empty(self):
        self.assertEqual(dates_mod.freshness_problem(set(), "x", "2026-09-27"), ("2026-09-27", None))

    def test_none_input(self):
        self.assertEqual(dates_mod.freshness_problem(None, "x", "2026-09-27"), ("2026-09-27", None))


class TestWritePartitionStreaming(OfflineTestCase):
    """write_partition 的流式签名与旧签名兼容分支。"""

    def test_new_signature_writes_from_spool(self):
        table = _FakeTable()
        rows = [{"record_id": "r1", "a": 1}, {"record_id": "r2", "a": 2}]
        spool, stats = make_spool_stats(rows)
        odps = _FakeOdps([{"cnt": 2, "ucnt": 2, "mn": "r1", "mx": "r2"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        self.assertEqual([c[0] for c in table.calls], ["write"])
        spool.close()

    def test_new_signature_row_too_big_checked_before_delete(self):
        table = _FakeTable()
        spool, stats = make_spool_stats([{"record_id": "r1", "big": "x" * 100}])
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        with mock.patch.object(mc_mod, "MAX_ROW_BYTES", 10):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertEqual(table.calls, [])
        spool.close()

    def test_new_signature_missing_record_id_aborts(self):
        table = _FakeTable()
        spool, stats = make_spool_stats([{"a": 1}])  # 无 record_id
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "", "mx": ""}])
        with self.assertRaises(SystemExit) as ctx:
            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertIn("record_id", str(ctx.exception))
        spool.close()

    def test_new_signature_empty_stats_replaces(self):
        table = _FakeTable()
        spool, stats = make_spool_stats([])
        odps = _FakeOdps([{"cnt": 0, "ucnt": 0, "mn": None, "mx": None}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertEqual(table.rows, [])
        self.assertIn("rename to partition", odps.sql)
        spool.close()

    def test_wrong_signature_raises_type_error(self):
        with self.assertRaises(TypeError):
            cli_mod.write_partition(mock.Mock(), _FakeTable(), "p", "t", "json", "20260928", "not-a-spool")


# ===========================================================================
# 加固修复：脱敏 / 临时目录 / --log-file / SQL 超时
# ===========================================================================
class TestRedact(OfflineTestCase):
    """F1：形态级 + 值级双重脱敏。"""

    def test_value_level_leak(self):
        """接口把 app_secret 原文写进 msg（自由文本，形态规则认不出）：值级脱敏兜底。"""
        utils_mod._SECRETS.append("sk-live-abcdef123456")
        msg = "调用失败：Invalid token: sk-live-abcdef123456"
        out = utils_mod.redact(msg)
        self.assertNotIn("sk-live-abcdef123456", out)
        self.assertIn("***", out)

    def test_value_level_url_encoded_leak(self):
        """接口把凭证按 URL 编码形态回显（周围无键名，形态规则认不出）：值级编码形态兜底。"""
        secret = "tok abc/123"  # quote 后 tok%20abc%2F123，quote_plus 后 tok+abc%2F123
        utils_mod._SECRETS.append(secret)
        encoded = utils_mod.quote(secret, safe="")
        self.assertNotEqual(encoded, secret)
        self.assertNotEqual(utils_mod.quote_plus(secret), encoded)
        for text in (
            f"调用失败：raw={secret}",
            f"调用失败：urlencoded={encoded}",
            f"调用失败：plus={utils_mod.quote_plus(secret)}",
        ):
            out = utils_mod.redact(text)
            self.assertNotIn(secret, out)
            self.assertIn("***", out)

    def test_redact_secrets_url_encoded(self):
        secret = "tok abc/123"
        for variant in (secret, utils_mod.quote(secret, safe=""), utils_mod.quote_plus(secret)):
            out = utils_mod.redact_secrets([secret], f"error body {variant} end")
            self.assertNotIn(variant, out)
            self.assertIn("***", out)

    def test_redact_secrets_value_first(self):
        out = utils_mod.redact_secrets(["sk-live-abcdef123456"], "error body sk-live-abcdef123456 end")
        self.assertNotIn("sk-live-abcdef123456", out)
        self.assertIn("***", out)

    def test_shape_bearer(self):
        out = utils_mod.redact("Authorization: Bearer sk-abcdef123456")
        self.assertNotIn("sk-abcdef123456", out)
        self.assertIn("***", out)

    def test_shape_json_fragment(self):
        out = utils_mod.redact('{"app_secret": "abcdef123456", "ok": 1}')
        self.assertNotIn("abcdef123456", out)
        self.assertIn('"app_secret": "***"', out)

    def test_shape_query_access_token(self):
        out = utils_mod.redact("https://x/open?access_token=abcdef123456&page=2")
        self.assertNotIn("abcdef123456", out)
        self.assertIn("access_token=***", out)

    def test_shape_feishu_webhook(self):
        out = utils_mod.redact("https://open.feishu.cn/open-apis/bot/v2/hook/abc-def-123456")
        self.assertNotIn("abc-def-123456", out)
        self.assertIn("/hook/***", out)

    def test_order_bearer_before_query(self):
        """顺序坑：header: 'Authorization=Bearer abc' 的令牌必须被遮住，不能被 query 规则切碎后漏出。

        与 api2ods / sftp2ods 输出逐字对齐：`header: 'Authorization=*** ***'`（令牌已变 ***）。
        若 query 规则先跑，会先把 `Authorization=Bearer` 切成 `Authorization=`、`Bearer` 被吃掉，
        Bearer 规则再也匹配不到，`abc123def` 会原样留在日志里。
        """
        out = utils_mod.redact("header: 'Authorization=Bearer abc123def'")
        self.assertNotIn("abc123def", out)
        self.assertIn("***", out)

    def test_plain_text_untouched(self):
        self.assertEqual(utils_mod.redact("普通文本 abc"), "普通文本 abc")

    def test_empty_and_none(self):
        self.assertEqual(utils_mod.redact(""), "")
        self.assertIsNone(utils_mod.redact(None))


class TestCollectSecretValues(OfflineTestCase):
    """F1：作业密钥收集（app_secret / access_key_secret / webhook）。"""

    def test_collects_feishu_and_mc_and_webhook_id(self):
        job = {
            "feishu": {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx"},
            "maxcompute": {"access_key_secret": "k" * 8},
            "freshness": {"webhook": "https://x/hook/abcdef123456"},
        }
        values = utils_mod.collect_secret_values(job)
        self.assertIn("s" * 8, values)
        self.assertIn("k" * 8, values)
        self.assertIn("abcdef123456", values)  # 裸 hook id（形态脱敏只认 URL）

    def test_non_dict_job_returns_empty(self):
        self.assertEqual(utils_mod.collect_secret_values("nope"), [])


class TestSpoolFailure(OfflineTestCase):
    """F2：临时目录不可写/磁盘满 → 人话 + 退出码 1。"""

    def test_spoolwriter_wraps_oserror(self):
        with mock.patch.object(tempfile, "mkstemp", side_effect=OSError("No space left")):
            with self.assertRaises(OSError) as ctx:
                cli_mod.SpoolWriter()
        self.assertIn("建不了落盘临时文件", str(ctx.exception))

    def test_run_sync_spool_failure_returns_1(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True, sql_timeout=600)
        messages: list[str] = []
        with (
            mock.patch.object(cli_mod, "SpoolWriter", side_effect=OSError("No space left")),
            mock.patch.object(cli_mod, "log", side_effect=lambda m: messages.append(str(m))),
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)
        self.assertTrue(any("临时文件" in m for m in messages))


class TestLogFile(OfflineTestCase):
    """F3：--log-file（追加、UTF-8、父目录自建、指向目录人话）。"""

    def test_absent_returns_none(self):
        self.assertIsNone(cli_mod._open_log_file(""))

    def test_creates_parents_and_appends(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "logs" / "run.log"
            handle = cli_mod._open_log_file(str(path))
            handle.write("x\n")
            handle.close()
            self.assertTrue(path.is_file())
            self.assertEqual(path.read_text(encoding="utf-8"), "x\n")

    def test_pointing_at_directory_reports_clearly(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(SystemExit) as ctx:
                cli_mod._open_log_file(tmp)
            self.assertIn("指向的是目录", str(ctx.exception))

    def test_log_writes_to_file_sink(self):
        handle = io.StringIO()
        utils_mod.add_log_sink(handle)
        try:
            utils_mod.log("写一份到文件")
        finally:
            utils_mod._sinks.remove(handle)  # 直接摘，不关句柄（remove_log_sink 会 close）
        self.assertIn("写一份到文件", handle.getvalue())

    def test_remove_log_sink_detaches_and_closes(self):
        handle = io.StringIO()
        utils_mod.add_log_sink(handle)
        utils_mod.remove_log_sink(handle)
        self.assertNotIn(handle, utils_mod._sinks)
        self.assertTrue(handle.closed)
        utils_mod.remove_log_sink(handle)  # 幂等：第二次不该报错
        utils_mod.remove_log_sink(None)

    def test_broken_sink_does_not_break_logging(self):
        class BrokenSink:
            def write(self, _text):
                raise OSError("disk full")

            def flush(self):
                pass

        sink = BrokenSink()
        utils_mod.add_log_sink(sink)
        try:
            utils_mod.log("照常输出")  # 不该抛
        finally:
            utils_mod.remove_log_sink(sink)

    def test_main_attaches_and_detaches_sink(self):
        with tempfile.TemporaryDirectory() as tmp:
            log_path = pathlib.Path(tmp) / "run.log"
            rc = cli_mod.main(["--log-file", str(log_path)])
            self.assertEqual(rc, 2)  # 没给 --job
            self.assertEqual(len(utils_mod._sinks), 0)  # 句柄已摘掉，重复调用不串
            self.assertTrue(log_path.is_file())
            self.assertIn("--job", log_path.read_text(encoding="utf-8"))


class TestSqlTimeout(OfflineTestCase):
    """F4：MaxCompute SQL 超时保护 + --sql-timeout。"""

    def test_parse_args_default_and_zero(self):
        self.assertEqual(cli_mod.parse_args(["--job", "x"]).sql_timeout, mc_mod.SQL_TIMEOUT_SECONDS)
        self.assertEqual(cli_mod.parse_args(["--job", "x", "--sql-timeout", "0"]).sql_timeout, 0)

    def test_parse_args_negative_is_arg_error(self):
        with self.assertRaises(SystemExit) as ctx:
            cli_mod.parse_args(["--job", "x", "--sql-timeout", "-5"])
        self.assertEqual(ctx.exception.code, 2)

    def test_run_sql_with_timeout_success(self):
        odps = _FakeOdps([{"cnt": 1}])
        inst = mc_mod.run_sql_with_timeout(odps, "select 1", timeout=600, desc="t")
        self.assertIsNotNone(inst)

    def test_run_sql_with_timeout_raises_and_stops(self):
        inst = _FakeInstance([], never_success=True)
        odps = mock.Mock()
        odps.run_sql.return_value = inst
        clock = iter([100.0, 100.0, 1100.0])
        with mock.patch.object(mc_mod.time, "time", side_effect=lambda: next(clock, 10**9)):
            with self.assertRaises(TimeoutError):
                mc_mod.run_sql_with_timeout(odps, "select 1", timeout=5, desc="t")
        self.assertTrue(inst.stopped)

    def test_count_partition_forwards_timeout(self):
        odps = _FakeOdps([{"cnt": 3}])
        with mock.patch.object(mc_mod, "run_sql_with_timeout", wraps=mc_mod.run_sql_with_timeout) as spy:
            self.assertEqual(cli_mod.count_partition(odps, "p", "t", "20260928", timeout=42), 3)
        self.assertEqual(spy.call_args.kwargs["timeout"], 42)

    def test_write_partition_uses_run_sql(self):
        """rename/verify 都走 run_sql（异步实例），不再是阻塞的 execute_sql。"""
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("rename to partition", odps.sql)

    def test_drop_add_partition_forward_timeout(self):
        """分区增删已改用带超时的 DDL（drop if exists / add if not exists），并透传 timeout。"""
        odps = _FakeOdps([])
        with mock.patch.object(mc_mod, "run_sql_with_timeout", wraps=mc_mod.run_sql_with_timeout) as spy:
            mc_mod.drop_partition(odps, "p", "t", "pt=20260928", timeout=7)
            mc_mod.add_partition(odps, "p", "t", "pt=20260928", timeout=7)
        self.assertEqual(spy.call_args.kwargs["timeout"], 7)
        self.assertIn("drop if exists partition (pt='20260928')", odps.sqls[0])
        self.assertIn("add if not exists partition (pt='20260928')", odps.sqls[1])

    def test_partition_ddl_timeout_absorbed_by_retry_and_cleanup(self):
        """分区 DDL 超时按普通失败走既有「重试 + 清临时分区」路径：不 rename、正式分区未动。"""
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        real = mc_mod.run_sql_with_timeout
        drops = {"n": 0}

        def fake(o, sql, timeout=mc_mod.SQL_TIMEOUT_SECONDS, desc="SQL"):
            if "drop if exists" in sql:
                drops["n"] += 1
                raise TimeoutError(f"{desc} 执行超过 {timeout} 秒，已主动停止")
            return real(o, sql, timeout=timeout, desc=desc)

        with mock.patch.object(mc_mod, "run_sql_with_timeout", side_effect=fake):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区未动", str(ctx.exception))
        self.assertNotIn("rename", "\n".join(odps.sqls))
        # 3 次写入重试 + 1 次收尾清理：超时错误全部被吸收，不无限挂起、不留悬挂线程
        self.assertEqual(drops["n"], mc_mod.WRITE_ATTEMPTS + 1)

    def test_old_signature_still_works_with_list(self):
        # 旧调用方（传记录列表、不给 stats）自动装进临时 spool
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}']])


class TestRunSyncSpoolLifecycle(OfflineTestCase):
    """run_sync 流式路径的 spool 生命周期。"""

    def test_dry_run_cleans_spool(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        spool_holder = {}

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            spool_holder["spool"] = sink
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        self.assertFalse(spool_holder["spool"].path.exists())

    def test_zero_rows_cleans_spool(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        spool_holder = {}

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            spool_holder["spool"] = sink
            return stats

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)  # 0 行保护
        self.assertFalse(spool_holder["spool"].path.exists())

    def _sync_args(self):
        return argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=600, force=False)

    def _fetch_capture(self, records, holder):
        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            holder["spool"] = sink
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        return fake_fetch

    def test_connect_failure_closes_spool(self):
        """connect_odps 抛 SystemExit（缺 pyodps/缺凭证）时 spool 也要被收口，不留悬挂句柄。"""
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        holder = {}
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=self._fetch_capture(records, holder)),
            mock.patch.object(cli_mod, "connect_odps", side_effect=SystemExit("缺少 pyodps：pip install pyodps")),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.run_sync(
                    self._sync_args(), make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time()
                )
        spool = holder["spool"]
        self.assertTrue(spool._handle.closed)  # 句柄已关闭：不再有裸泄漏的文件句柄
        self.addCleanup(lambda: spool.path.unlink(missing_ok=True))
        # 失败路径按既有约定 keep=True 保留文件供排查（已在 run_sync 里统一收口）
        self.assertTrue(spool.path.exists())

    def test_ensure_table_failure_closes_spool(self):
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        holder = {}
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=self._fetch_capture(records, holder)),
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", side_effect=SystemExit("建表失败")),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.run_sync(
                    self._sync_args(), make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time()
                )
        spool = holder["spool"]
        self.assertTrue(spool._handle.closed)
        self.addCleanup(lambda: spool.path.unlink(missing_ok=True))

    def test_success_path_removes_spool(self):
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        holder = {}
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=self._fetch_capture(records, holder)),
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition"),
            mock.patch.object(cli_mod, "count_partition", return_value=1),
        ):
            code = cli_mod.run_sync(
                self._sync_args(), make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time()
            )
        self.assertEqual(code, 0)
        self.assertFalse(holder["spool"].path.exists())

    def test_fetch_failure_closes_spool(self):
        """回归：fetch_records 抛 SystemExit（不是 Exception 的子类）时 spool 也要收口，不留悬挂句柄。"""
        holder = {}

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            holder["spool"] = sink
            raise SystemExit("拉取记录失败：模拟")

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            with self.assertRaises(SystemExit):
                cli_mod.run_sync(
                    self._sync_args(), make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time()
                )
        spool = holder["spool"]
        self.assertTrue(spool._handle.closed)  # 句柄已关闭
        self.addCleanup(lambda: spool.path.unlink(missing_ok=True))
        self.assertTrue(spool.path.exists())  # 失败路径按约定 keep=True 保留文件供排查


class TestBizdateMore(OfflineTestCase):
    def test_compact_edge_dates(self):
        self.assertEqual(dates_mod.parse_day_arg("20260930"), date(2026, 9, 30))
        self.assertEqual(dates_mod.parse_day_arg("20260101"), date(2026, 1, 1))
        self.assertEqual(dates_mod.parse_day_arg("20261231"), date(2026, 12, 31))

    def test_iso_edge_dates(self):
        self.assertEqual(dates_mod.parse_day_arg("2026-01-01"), date(2026, 1, 1))
        self.assertEqual(dates_mod.parse_day_arg("2026-12-31"), date(2026, 12, 31))

    def test_leap_year(self):
        self.assertEqual(dates_mod.parse_day_arg("20240229"), date(2024, 2, 29))
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("20230229")

    def test_month_day_bounds(self):
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("20260931")
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("20261301")
        with self.assertRaises(SystemExit):
            dates_mod.parse_day_arg("20260001")

    def test_whitespace_tolerated(self):
        self.assertEqual(dates_mod.parse_day_arg("  20260928  "), date(2026, 9, 28))

    def test_garbage_rejected(self):
        for bad in ("", "2026", "2026-9-28", "2026/09/28", "202609280", "abcdefgh", "2026-09-28T00:00:00"):
            with self.assertRaises(SystemExit):
                dates_mod.parse_day_arg(bad)

    def test_env_bizdate_whitespace(self):
        with mock.patch.dict(os.environ, {"bizdate": " 20260928 "}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 28))

    def test_env_bizdate_empty_string(self):
        with mock.patch.dict(os.environ, {"bizdate": ""}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate())

    def test_env_skynet_fallback(self):
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 28))

    def test_env_bizdate_priority_over_skynet(self):
        with mock.patch.dict(os.environ, {"bizdate": "20260927", "SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 27))

    def test_resolve_bizdate_bad_env_fails(self):
        args = argparse.Namespace(bizdate="")
        with mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True):
            with self.assertRaises(SystemExit):
                dates_mod.resolve_bizdate(args)

    def test_env_bizdate_non_strict_tolerates_dirty(self):
        """--check 用的非严格模式：脏 bizdate 按"未设置"处理，不报错。"""
        with mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate(strict=False))
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "2026-9-7"}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate(strict=False))

    def test_resolve_bizdate_non_strict_falls_back_to_default(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 29, 10, 0, tzinfo=utils_mod.CN_TZ)

        args = argparse.Namespace(bizdate="")
        with (
            mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True),
            mock.patch.object(dates_mod, "datetime", _FixedDatetime),
        ):
            self.assertEqual(dates_mod.resolve_bizdate(args, strict=False), date(2026, 9, 28))


class TestValidateJobMore(OfflineTestCase):
    def test_job_missing_feishu_block(self):
        job = make_job()
        del job["feishu"]
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_feishu_blank_fields(self):
        job = make_job()
        job["feishu"]["app_id"] = "   "
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_missing_maxcompute_block(self):
        job = make_job()
        del job["maxcompute"]
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_maxcompute_blank_ak(self):
        job = make_job()
        job["maxcompute"]["access_key_id"] = ""
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_missing_target_block(self):
        job = make_job()
        del job["target"]
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_target_missing_table(self):
        job = make_job()
        job["target"]["table"] = ""
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_target_bad_table_name(self):
        job = make_job()
        job["target"]["table"] = "ods_t;drop table x"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_target_bad_project_name(self):
        job = make_job()
        job["target"]["project"] = "my project"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_target_column_whitespace(self):
        job = make_job()
        job["target"]["column"] = " json "
        config_mod.validate_job(job)
        self.assertEqual(job["target"]["column"], "json")  # 校验通过的值写回去

    def test_job_allow_empty_bad_type(self):
        job = make_job()
        job["target"]["allow_empty"] = "yes"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_empty_fields(self):
        job = make_job()
        job["fields"] = {}
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_fields_record_id_reserved(self):
        job = make_job()
        job["fields"] = {"日期": "record_id"}
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_fields_duplicate_target(self):
        job = make_job()
        job["fields"] = {"日期": "biz_date", "日期2": "biz_date"}
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_fields_bad_target(self):
        job = make_job()
        job["fields"] = {"日期": "not valid!"}
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_freshness_bad_lag(self):
        job = make_job()
        job["freshness"]["lag_days"] = -1
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_freshness_bool_lag(self):
        job = make_job()
        job["freshness"]["lag_days"] = True
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_freshness_unknown_date_field(self):
        job = make_job()
        job["freshness"]["date_field"] = "not_there"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_freshness_bad_webhook(self):
        job = make_job()
        job["freshness"]["webhook"] = "not-a-url"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_unknown_top_key_warns(self):
        job = make_job()
        job["extra_top"] = 1
        warnings = config_mod.validate_job(job)
        self.assertTrue(any("extra_top" in w for w in warnings))

    def test_job_normalized_values_written_back(self):
        job = make_job()
        job["feishu"]["app_id"] = " cli_x "
        config_mod.validate_job(job)
        self.assertEqual(job["feishu"]["app_id"], "cli_x")


class TestNormalizeDateMore(OfflineTestCase):
    def test_iso_with_millis_and_tz(self):
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27T15:30:00.123+08:00"), "2026-09-27")

    def test_iso_date_only(self):
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27"), "2026-09-27")

    def test_slash_single_digit_month(self):
        self.assertEqual(dates_mod.normalize_date_value("2026/9/7"), "2026-09-07")

    def test_epoch_zero(self):
        self.assertEqual(dates_mod.normalize_date_value(0), "1970-01-01")

    def test_float_value(self):
        self.assertEqual(dates_mod.normalize_date_value(0.0), "1970-01-01")

    def test_bool_rejected(self):
        self.assertIsNone(dates_mod.normalize_date_value(True))
        self.assertIsNone(dates_mod.normalize_date_value(False))

    def test_huge_number_rejected(self):
        self.assertIsNone(dates_mod.normalize_date_value(10**30))

    def test_garbage_text(self):
        for bad in ("", "abc", "2026年9月27日", "27/09/2026", "2026-9-27"):
            self.assertIsNone(dates_mod.normalize_date_value(bad))

    def test_whitespace_trimmed(self):
        self.assertEqual(dates_mod.normalize_date_value(" 2026-09-27 "), "2026-09-27")


class TestBuildRecordsMore(OfflineTestCase):
    def test_missing_mapping_source_aborts(self):
        with self.assertRaises(SystemExit):
            fetch_mod.build_records(["别的列"], ["r1"], [["x"]], {"日期": "biz_date"})

    def test_empty_table_skips_checks(self):
        records = fetch_mod.build_records([], [], [], {"日期": "biz_date"})
        self.assertEqual(records, [])

    def test_empty_table_with_fields_validates_mapping(self):
        with self.assertRaises(SystemExit):
            fetch_mod.build_records(["日期"], [], [], {"别的列": "biz_date"})

    def test_extra_columns_ignored_and_collected(self):
        extra: list[str] = []
        records = fetch_mod.build_records(
            ["日期", "新列"], ["r1"], [["2026-09-27", "x"]], {"日期": "biz_date"}, extra_out=extra
        )
        self.assertEqual(extra, ["新列"])
        self.assertNotIn("新列", records[0])

    def test_row_shorter_than_fields_fills_none(self):
        records = fetch_mod.build_records(
            ["日期", "金额"], ["r1"], [["2026-09-27"]], {"日期": "biz_date", "金额": "amount"}
        )
        self.assertIsNone(records[0]["amount"])

    def test_extra_out_none_ok(self):
        records = fetch_mod.build_records(["日期", "新列"], ["r1"], [["a", "b"]], {"日期": "biz_date"})
        self.assertEqual(records[0]["biz_date"], "a")

    def test_record_id_always_first(self):
        records = fetch_mod.build_records(["日期"], ["r1"], [["a"]], {"日期": "biz_date"})
        self.assertEqual(list(records[0].keys()), ["record_id", "biz_date"])


class TestNotifyMore(OfflineTestCase):
    def test_missing_webhook_skips(self):
        with mock.patch.object(notify_mod, "log") as logger:
            cli_mod.notify("", "t", ["l"])
        self.assertTrue(any("未配置" in str(c) for c in logger.call_args_list))

    def test_requests_missing_skips(self):
        with mock.patch.object(notify_mod, "requests", None), mock.patch.object(notify_mod, "log") as logger:
            notify_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("requests" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_post_exception_swallowed(self):
        with (
            mock.patch.object(notify_mod.requests, "post", side_effect=RuntimeError("down")),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("通知发送失败" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_bad_status_swallowed(self):
        resp = mock.Mock(status_code=500)
        resp.json.return_value = {"code": 1, "msg": "bad"}
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("通知发送失败" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_success_status_code_field(self):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"StatusCode": 0}
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("已发送" in str(c) for c in logger.call_args_list))


class TestRunCheckMore(OfflineTestCase):
    def test_table_exists_and_schema_ok(self):
        o = mock.Mock()
        o.exist_table.return_value = True
        table = _Table([_Col("json", "string"), _Col("pt", "string")], [_Col("pt", "string")])
        o.get_table.return_value = table
        with (
            mock.patch.object(cli_mod, "fetch_records", return_value=[{"a": 1}]),
            mock.patch.object(cli_mod, "connect_odps", return_value=o),
        ):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 0)

    def test_table_schema_mismatch_fails(self):
        o = mock.Mock()
        o.exist_table.return_value = True
        table = _Table([_Col("wrong", "string")], [_Col("pt", "string")])
        o.get_table.return_value = table
        with (
            mock.patch.object(cli_mod, "fetch_records", return_value=[{"a": 1}]),
            mock.patch.object(cli_mod, "connect_odps", return_value=o),
        ):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 1)

    def test_connect_error_fails(self):
        o = mock.Mock()
        o.exist_table.side_effect = RuntimeError("network down")
        with (
            mock.patch.object(cli_mod, "fetch_records", return_value=[{"a": 1}]),
            mock.patch.object(cli_mod, "connect_odps", return_value=o),
        ):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 1)

    def test_api_failure_fails_before_mc(self):
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=SystemExit("boom")),
            mock.patch.object(cli_mod, "connect_odps") as conn,
        ):
            self.assertEqual(cli_mod.run_check(make_job(), "p", "t", "json", "20260928"), 1)
        conn.assert_not_called()


class TestRunLockMore(OfflineTestCase):
    def test_lock_blocks_second(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            first = utils_mod.RunLock(path)
            first.__enter__()
            try:
                second = utils_mod.RunLock(path)
                with self.assertRaises(SystemExit) as ctx:
                    second.__enter__()
                self.assertIn("已有任务", str(ctx.exception))
            finally:
                first.__exit__(None, None, None)

    def test_lock_released_after_exit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            with utils_mod.RunLock(path):
                pass
            with utils_mod.RunLock(path):  # 释放后可再次拿锁
                pass

    def test_lock_file_records_pid(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            with utils_mod.RunLock(path):
                pass
            # 锁释放后再读：Windows 上持锁期间文件被区域锁保护，第二个句柄读不了
            content = path.read_text(encoding="utf-8").strip()
        self.assertIn(str(os.getpid()), content)

    def test_lock_path_hash_distinguishes_jobs(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = pathlib.Path(tmp) / "jobs" / "a" / "demo.json"
            b = pathlib.Path(tmp) / "jobs" / "b" / "demo.json"
            self.assertNotEqual(cli_mod.lock_path(a), cli_mod.lock_path(b))

    def test_lock_path_same_job_same_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            a = pathlib.Path(tmp) / "jobs" / "a" / "demo.json"
            self.assertEqual(cli_mod.lock_path(a), cli_mod.lock_path(a))

    def test_lock_path_custom_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp) / "locks"
            path = cli_mod.lock_path(pathlib.Path("/jobs/demo.json"), root=root)
            self.assertTrue(str(path).startswith(str(root)))


class TestMainMore(OfflineTestCase):
    def test_no_job_returns_2(self):
        self.assertEqual(cli_mod.main(["--bizdate", "20260928"]), 2)

    def test_missing_job_file_returns_1(self):
        self.assertEqual(cli_mod.main(["--job", "/nonexistent/x.json"]), 1)

    def test_bad_bizdate_returns_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = pathlib.Path(tmp) / "demo.json"
            job_path.write_text(json.dumps(make_job()), encoding="utf-8")
            self.assertEqual(cli_mod.main(["--job", str(job_path), "--bizdate", "bad-date"]), 1)

    def test_check_flow_ok(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = pathlib.Path(tmp) / "demo.json"
            job_path.write_text(json.dumps(make_job()), encoding="utf-8")
            with mock.patch.object(cli_mod, "run_check", return_value=0) as check:
                self.assertEqual(cli_mod.main(["--job", str(job_path), "--check"]), 0)
            self.assertTrue(check.called)

    def test_version_output(self):
        with self.assertRaises(SystemExit) as ctx:
            cli_mod.main(["--version"])
        self.assertEqual(ctx.exception.code, 0)

    def test_sync_flow_with_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = pathlib.Path(tmp) / "demo.json"
            job_path.write_text(json.dumps(make_job()), encoding="utf-8")
            with (
                mock.patch.object(cli_mod, "run_sync", return_value=0) as sync,
                mock.patch.object(cli_mod, "lock_path", lambda p: pathlib.Path(tmp) / "x.lock"),
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path)]), 0)
            self.assertTrue(sync.called)

    def test_keyboard_interrupt_before_run_returns_130(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = pathlib.Path(tmp) / "demo.json"
            job_path.write_text(json.dumps(make_job()), encoding="utf-8")
            with (
                mock.patch.object(cli_mod, "run_sync", side_effect=KeyboardInterrupt),
                mock.patch.object(cli_mod, "lock_path", lambda p: pathlib.Path(tmp) / "x.lock"),
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path)]), 130)

    def test_unexpected_error_returns_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = pathlib.Path(tmp) / "demo.json"
            job_path.write_text(json.dumps(make_job()), encoding="utf-8")
            with (
                mock.patch.object(cli_mod, "run_sync", side_effect=ValueError("意外")),
                mock.patch.object(cli_mod, "lock_path", lambda p: pathlib.Path(tmp) / "x.lock"),
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path)]), 1)


class TestSecretsResetAcrossRuns(OfflineTestCase):
    """R2：_SECRETS 是模块级全局，同进程多次 main() 不能跨轮串值。"""

    def test_second_run_does_not_keep_first_secrets(self):
        first = "FIRSTSECRET-abcdef"
        second = "SECONDSECRET-xyzzy"
        with tempfile.TemporaryDirectory() as tmp:
            job_a = pathlib.Path(tmp) / "a.json"
            job_b = pathlib.Path(tmp) / "b.json"
            raw_a = make_job()
            raw_a["feishu"]["app_secret"] = first
            raw_b = make_job()
            raw_b["feishu"]["app_secret"] = second
            job_a.write_text(json.dumps(raw_a), encoding="utf-8")
            job_b.write_text(json.dumps(raw_b), encoding="utf-8")
            # cli 与 utils 共用同一个 _SECRETS 列表（还原生产里两处是同一对象）
            shared: list[str] = []
            with (
                mock.patch.object(cli_mod, "_SECRETS", shared),
                mock.patch.object(utils_mod, "_SECRETS", shared),
                mock.patch.object(cli_mod, "run_sync", return_value=0),
                mock.patch.object(cli_mod, "lock_path", lambda p: pathlib.Path(tmp) / "x.lock"),
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_a), "--bizdate", "20260928"]), 0)
                self.assertIn(first, shared)  # 本轮密钥已登记（用于日志脱敏）
                self.assertEqual(cli_mod.main(["--job", str(job_b), "--bizdate", "20260928"]), 0)
                self.assertIn(second, shared)
                self.assertNotIn(first, shared)  # 上一轮密钥不残留


class TestCheckTolerantBizdate(OfflineTestCase):
    """R4：只读体检 --check 不该被脏 bizdate 环境变量拖垮；正式同步仍严格。"""

    def _job_file(self, tmp) -> pathlib.Path:
        job_path = pathlib.Path(tmp) / "demo.json"
        job_path.write_text(json.dumps(make_job()), encoding="utf-8")
        return job_path

    def test_check_passes_with_dirty_bizdate(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = self._job_file(tmp)
            with (
                mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True),
                mock.patch.object(cli_mod, "run_check", return_value=0) as check,
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path), "--check"]), 0)
            self.assertTrue(check.called)

    def test_sync_still_fails_with_dirty_bizdate(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = self._job_file(tmp)
            with mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True):
                self.assertEqual(cli_mod.main(["--job", str(job_path)]), 1)


class TestEmptyPartitionGuard(OfflineTestCase):
    """R6：0 行 + allow_empty=true 写空分区前先查现有分区行数（非 0 需 --force）。"""

    @staticmethod
    def _args(force=False):
        return argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=600, force=force)

    @staticmethod
    def _job():
        return make_job(target={"project": "test_project", "table": "t", "column": "json", "allow_empty": True})

    def _capture_fetch(self, holder):
        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            holder["spool"] = sink
            return stats

        return fake_fetch

    def _run(self, args, holder):
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=self._capture_fetch(holder)),
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
        ):
            return cli_mod.run_sync(args, self._job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())

    def test_zero_rows_with_existing_data_refuses_without_force(self):
        holder = {}
        with (
            mock.patch.object(cli_mod, "count_partition", return_value=123) as counter,
            mock.patch.object(cli_mod, "write_partition") as writer,
        ):
            code = self._run(self._args(force=False), holder)
        self.assertEqual(code, 1)
        counter.assert_called_once()  # 写前先查了现有分区
        writer.assert_not_called()  # 没有删分区、没有写空分区
        self.addCleanup(lambda: holder["spool"].path.unlink(missing_ok=True))

    def test_zero_rows_with_existing_data_force_writes(self):
        holder = {}
        with (
            mock.patch.object(cli_mod, "count_partition", return_value=0) as counter,
            mock.patch.object(cli_mod, "write_partition") as writer,
        ):
            code = self._run(self._args(force=True), holder)
        self.assertEqual(code, 0)
        writer.assert_called_once()  # --force 明确放行，允许覆盖成空分区
        counter.assert_called_once()  # 只剩写后核对那条（写前保护被跳过）

    def test_zero_rows_with_empty_partition_writes_normally(self):
        holder = {}
        with (
            mock.patch.object(cli_mod, "count_partition", return_value=0) as counter,
            mock.patch.object(cli_mod, "write_partition") as writer,
        ):
            code = self._run(self._args(force=False), holder)
        self.assertEqual(code, 0)
        writer.assert_called_once()  # 分区本来就空：正常写空分区，不拦
        self.assertEqual(counter.call_count, 2)  # 写前保护 + 写后核对各一次


if __name__ == "__main__":
    unittest.main()
