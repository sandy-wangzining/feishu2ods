# -*- coding: utf-8 -*-
"""feishu2ods 离线单元测试：不访问网络、不连 MaxCompute（requests/pyodps 没装也能跑）。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import tempfile
import time
import unittest
from datetime import date, datetime
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import feishu2ods as f2o  # noqa: E402

REQUESTS_AVAILABLE = f2o.requests is not None


class OfflineTestCase(unittest.TestCase):
    """基类：禁止真实 sleep、跳过控制台重配（与 api2ods 测试同款）。"""

    def setUp(self):
        f2o.setup_console()
        patcher = mock.patch.object(f2o, "_console_patched", True)
        patcher.start()
        self.addCleanup(patcher.stop)
        sleep_patcher = mock.patch.object(time, "sleep", lambda *args, **kwargs: None)
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)
        # 脱敏表是模块级状态：每个测试用独立列表，防跨测试污染
        secrets_patcher = mock.patch.object(f2o, "_SECRETS", [])
        secrets_patcher.start()
        self.addCleanup(secrets_patcher.stop)


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
    def __init__(self, rows):
        self._rows = rows

    def open_reader(self):
        return _FakeReader(self._rows)


class _FakeOdps:
    def __init__(self, rows):
        self._rows = rows
        self.sqls = []

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
        self.assertEqual(f2o.parse_day_arg("20260927"), date(2026, 9, 27))

    def test_parse_iso(self):
        self.assertEqual(f2o.parse_day_arg("2026-09-27"), date(2026, 9, 27))

    def test_parse_bad_format(self):
        with self.assertRaises(SystemExit):
            f2o.parse_day_arg("2026/09/27")

    def test_parse_bad_date(self):
        with self.assertRaises(SystemExit):
            f2o.parse_day_arg("20260230")

    def test_env_unset(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(f2o.env_bizdate())

    def test_env_value(self):
        with mock.patch.dict(os.environ, {"bizdate": "20260927"}, clear=True):
            self.assertEqual(f2o.env_bizdate(), date(2026, 9, 27))

    def test_env_bad_value(self):
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "2026-9-7"}, clear=True):
            with self.assertRaises(SystemExit):
                f2o.env_bizdate()

    def test_resolve_cli_wins(self):
        args = argparse.Namespace(bizdate="20260101")
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(f2o.resolve_bizdate(args), date(2026, 1, 1))

    def test_resolve_env(self):
        args = argparse.Namespace(bizdate="")
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(f2o.resolve_bizdate(args), date(2026, 2, 2))

    def test_resolve_default_yesterday(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 29, 10, 0, tzinfo=f2o.CN_TZ).astimezone(tz) if tz else cls(2026, 9, 29, 10, 0)

        args = argparse.Namespace(bizdate="")
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(f2o, "datetime", _FixedDatetime):
            self.assertEqual(f2o.resolve_bizdate(args), date(2026, 9, 28))


# ---------------------------------------------------------------------------
# job 校验
# ---------------------------------------------------------------------------
class TestValidateJob(OfflineTestCase):
    def test_valid_no_warnings(self):
        self.assertEqual(f2o.validate_job(make_job()), [])

    def test_unknown_key_warning(self):
        job = make_job()
        job["extra_key"] = 1
        job["feishu"]["base_ur1"] = "typo"
        warnings = f2o.validate_job(job)
        self.assertTrue(any("作业.extra_key" in w for w in warnings))
        self.assertTrue(any("feishu.base_ur1" in w for w in warnings))

    def test_missing_feishu_block(self):
        job = make_job()
        job.pop("feishu")
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)

    def test_reserved_record_id(self):
        job = make_job(fields={"日期": "record_id"})
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)

    def test_duplicate_english_keys(self):
        job = make_job(fields={"日期": "x", "金额": "x"})
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)

    def test_whitespace_normalized(self):
        job = make_job(maxcompute={"project": " test_project ", "access_key_id": "ak", "access_key_secret": "sk"})
        f2o.validate_job(job)
        self.assertEqual(job["maxcompute"]["project"], "test_project")

    def test_freshness_date_field_must_exist(self):
        job = make_job(freshness={"date_field": "nope"})
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)

    def test_lag_days_bool_rejected(self):
        job = make_job(freshness={"date_field": "biz_date", "lag_days": True})
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)

    def test_allow_empty_must_be_bool(self):
        job = make_job(target={"table": "t", "allow_empty": "false"})
        with self.assertRaises(SystemExit):
            f2o.validate_job(job)


# ---------------------------------------------------------------------------
# 记录组装
# ---------------------------------------------------------------------------
class TestBuildRecords(OfflineTestCase):
    def test_basic_mapping(self):
        records = f2o.build_records(
            ["日期", "金额", "备注"],
            ["r1"],
            [["2026-09-27", "$1", "x"]],
            {"日期": "biz_date", "金额": "amount"},
        )
        self.assertEqual(records, [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}])
        self.assertEqual(list(records[0].keys())[0], "record_id")

    def test_missing_column(self):
        with self.assertRaises(SystemExit):
            f2o.build_records(["日期"], ["r1"], [["x"]], {"不存在的列": "v"})

    def test_empty_table(self):
        self.assertEqual(f2o.build_records([], [], [], {"日期": "biz_date"}), [])

    def test_empty_table_still_checks_mapping(self):
        # 接口给了字段列表但 0 行：映射错了也要报错，不能「空表假通过」
        with self.assertRaises(SystemExit):
            f2o.build_records(["日期"], [], [], {"不存在的列": "v"})

    def test_short_row_fills_none(self):
        records = f2o.build_records(["a", "b"], ["r1"], [["1"]], {"b": "bbb"})
        self.assertIsNone(records[0]["bbb"])

    def test_ids_without_rows_rejected(self):
        # 接口异常：给了记录 ID 却没有行数据，宁可报错也不能当空表静默跳过
        with self.assertRaises(SystemExit):
            f2o.build_records(["a"], ["r1"], [], {"a": "aaa"})

    def test_rows_without_fields_rejected(self):
        with self.assertRaises(SystemExit):
            f2o.build_records([], ["r1"], [["1"]], {"a": "aaa"})

    def test_extra_columns_collected(self):
        extras: list[str] = []
        records = f2o.build_records(
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
        self.assertEqual(f2o.normalize_date_value("2026-09-27T00:00:00.000+08:00"), "2026-09-27")

    def test_normalize_slash(self):
        self.assertEqual(f2o.normalize_date_value("2026/9/7"), "2026-09-07")

    def test_normalize_epoch_ms(self):
        self.assertEqual(f2o.normalize_date_value(0), "1970-01-01")

    def test_normalize_garbage(self):
        self.assertIsNone(f2o.normalize_date_value(True))
        self.assertIsNone(f2o.normalize_date_value("abc"))
        self.assertIsNone(f2o.normalize_date_value(10**30))

    def test_found(self):
        records = [{"biz_date": "2026-09-27T00:00:00.000+08:00"}]
        self.assertIsNone(f2o.freshness_problem(records, "biz_date", "2026-09-27"))

    def test_missing(self):
        records = [{"biz_date": "2026-09-26T00:00:00.000+08:00"}]
        self.assertEqual(f2o.freshness_problem(records, "biz_date", "2026-09-27"), ("2026-09-27", "2026-09-26"))

    def test_empty(self):
        self.assertEqual(f2o.freshness_problem([], "biz_date", "2026-09-27"), ("2026-09-27", None))


# ---------------------------------------------------------------------------
# HTTP 与拉取
# ---------------------------------------------------------------------------
@unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
class TestRequestJson(OfflineTestCase):
    def test_ok(self):
        with mock.patch.object(f2o.requests, "request", return_value=_FakeResponse(200, {"code": 0})):
            self.assertEqual(f2o.request_json("GET", "https://x", "t"), {"code": 0})

    def test_retry_500_then_ok(self):
        responses = [_FakeResponse(500, text="boom"), _FakeResponse(200, {"code": 0})]
        with mock.patch.object(f2o.requests, "request", side_effect=responses) as call:
            self.assertEqual(f2o.request_json("GET", "https://x", "t"), {"code": 0})
        self.assertEqual(call.call_count, 2)

    def test_4xx_raises_api_error(self):
        with mock.patch.object(f2o.requests, "request", return_value=_FakeResponse(403, text="nope")):
            with self.assertRaises(f2o.ApiHttpError) as ctx:
                f2o.request_json("GET", "https://x", "t")
        self.assertEqual(ctx.exception.status, 403)

    def test_exhausted(self):
        with mock.patch.object(f2o.requests, "request", return_value=_FakeResponse(500, text="boom")):
            with self.assertRaises(SystemExit):
                f2o.request_json("GET", "https://x", "t")

    def test_retry_429_then_ok(self):
        responses = [_FakeResponse(429, text="slow down"), _FakeResponse(200, {"code": 0})]
        with mock.patch.object(f2o.requests, "request", side_effect=responses) as call:
            self.assertEqual(f2o.request_json("GET", "https://x", "t"), {"code": 0})
        self.assertEqual(call.call_count, 2)

    def test_non_dict_body_raises_api_error(self):
        with mock.patch.object(f2o.requests, "request", return_value=_FakeResponse(403, text="no")):
            with self.assertRaises(f2o.ApiHttpError):
                f2o.request_json("GET", "https://x", "t")

    def test_redirect_raises_api_error_without_retry(self):
        # 301/302/303 会把 POST 降级成不带 body 的 GET（请求参数全丢），
        # 鉴权头也可能被转发到别的地址——直接失败，不重试
        resp = _FakeResponse(302, text="", headers={"Location": "https://other.example.com/new"})
        with mock.patch.object(f2o.requests, "request", return_value=resp) as call:
            with self.assertRaises(f2o.ApiHttpError) as ctx:
                f2o.request_json("POST", "https://x", "t", body={"app_id": "a", "app_secret": "s"})
        self.assertEqual(ctx.exception.status, 302)
        self.assertEqual(call.call_count, 1)  # 确定性错误不重试

    def test_request_disables_redirects(self):
        with mock.patch.object(f2o.requests, "request", return_value=_FakeResponse(200, {"code": 0})) as call:
            f2o.request_json("GET", "https://x", "t")
        kwargs = call.call_args.kwargs
        self.assertIs(kwargs.get("allow_redirects"), False)


@unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
class TestNotify(OfflineTestCase):
    def test_notify_ok(self):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"code": 0}
        messages: list[str] = []
        with mock.patch.object(f2o.requests, "post", return_value=resp), \
                mock.patch.object(f2o, "log", side_effect=lambda msg: messages.append(str(msg))):
            f2o.notify("https://x/hook/1", "标题", ["第一行"], footer="尾部")
        self.assertIn("飞书通知已发送", "\n".join(messages))

    def test_notify_error_swallowed(self):
        messages: list[str] = []
        with mock.patch.object(f2o.requests, "post", side_effect=RuntimeError("网络坏了")), \
                mock.patch.object(f2o, "log", side_effect=lambda msg: messages.append(str(msg))):
            f2o.notify("https://x/hook/1", "标题", ["第一行"])
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
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            records = f2o.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual([r["record_id"] for r in records], ["r1", "r2"])
        self.assertEqual(records[0]["biz_date"], "2026-09-27")

    def test_max_pages_one(self):
        pages = [self._page([["2026-09-27", "$1"]], ["r1"], True)]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            records = f2o.fetch_records(self.job_feishu, self.mapping, max_pages=1)
        self.assertEqual(len(records), 1)

    def test_token_refresh(self):
        pages = [{"code": 99991663}, self._page([["2026-09-27", "$1"]], ["r1"], False)]
        with mock.patch.object(f2o, "get_tenant_token", side_effect=["tok1", "tok2"]), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            records = f2o.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual(len(records), 1)

    def test_rate_limit_retry(self):
        pages = [{"code": 99991400}, self._page([["2026-09-27", "$1"]], ["r1"], False)]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            records = f2o.fetch_records(self.job_feishu, self.mapping)
        self.assertEqual(len(records), 1)

    def test_fields_changed_mid_pagination(self):
        pages = [
            self._page([["a", "b"]], ["r1"], True),
            self._page([["a", "b"]], ["r2"], False, fields=("日期", "换列")),
        ]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            with self.assertRaises(SystemExit):
                f2o.fetch_records(self.job_feishu, self.mapping)

    def test_duplicate_record_ids(self):
        pages = [
            self._page([["a", "b"]], ["r1"], True),
            self._page([["a", "b"]], ["r1"], False),
        ]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            with self.assertRaises(SystemExit):
                f2o.fetch_records(self.job_feishu, self.mapping)

    def test_row_id_count_mismatch(self):
        pages = [{"code": 0, "data": {"fields": ["日期"], "data": [["x"]], "record_id_list": ["r1", "r2"], "has_more": False}}]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            with self.assertRaises(SystemExit):
                f2o.fetch_records(self.job_feishu, self.mapping)

    def test_rev_changed_mid_pagination(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True, rev=12),
            self._page([["2026-09-26", "$2"]], ["r2"], False, rev=13),
        ]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            with self.assertRaises(SystemExit):
                f2o.fetch_records(self.job_feishu, self.mapping)

    def test_rev_same_mid_pagination_ok(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True, rev=12),
            self._page([["2026-09-26", "$2"]], ["r2"], False, rev=12),
        ]
        with mock.patch.object(f2o, "get_tenant_token", return_value="tok"), \
                mock.patch.object(f2o, "request_json", side_effect=pages):
            self.assertEqual(len(f2o.fetch_records(self.job_feishu, self.mapping)), 2)


# ---------------------------------------------------------------------------
# MaxCompute：DDL / 结构校验 / 写入 / 计数
# ---------------------------------------------------------------------------
class TestMc(OfflineTestCase):
    def test_build_ddl(self):
        ddl = f2o.build_ddl("test_project", "t", "json", "a'b")
        self.assertIn("partitioned by (pt string", ddl)
        self.assertIn("a''b", ddl)

    def test_verify_schema_ok(self):
        table = _Table([_Col("json", "string"), _Col("pt", "string")], [_Col("pt", "string")])
        f2o.verify_schema(table, "t", "json")

    def test_verify_schema_mismatch(self):
        table = _Table([_Col("payload", "string"), _Col("pt", "string")], [_Col("pt", "string")])
        with self.assertRaises(SystemExit):
            f2o.verify_schema(table, "t", "json")

    def test_verify_schema_view(self):
        table = _Table([_Col("json", "string")], [], is_virtual_view=True)
        with self.assertRaises(SystemExit):
            f2o.verify_schema(table, "t", "json")

    @staticmethod
    def _verify_row(cnt, ucnt=None, mn="r1", mx=None):
        """verify_partition 的假返回值（cnt/ucnt/mn/mx）。"""
        return {"cnt": cnt, "ucnt": cnt if ucnt is None else ucnt, "mn": mn, "mx": mx or mn}

    def test_write_partition_calls_and_rows(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, mn="r1", mx="r2")])
        records = [{"record_id": "r1", "a": 1}, {"record_id": "r2", "a": 2}]
        f2o.write_partition(odps, table, "p", "t", "json", "20260928", records)
        # 先写临时分区，再删正式分区、rename 顶上
        self.assertEqual([c[0] for c in table.calls], ["delete", "create", "write", "delete"])
        self.assertEqual(table.calls[0][1], "pt=20260928__tmp")
        self.assertEqual(table.calls[3][1], "pt=20260928")
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        self.assertIn("rename to partition (pt='20260928')", odps.sql)
        self.assertIn("pt='20260928__tmp'", odps.sql)

    def test_write_partition_retry(self):
        table = _FakeTable(fail_first_write=True)
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        f2o.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 2)
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}']])

    def test_write_partition_row_too_big(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(f2o, "MAX_ROW_BYTES", 10):
            with self.assertRaises(SystemExit):
                f2o.write_partition(
                    odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "big": "x" * 100}]
                )
        self.assertEqual(table.calls, [])  # 动分区之前就该报错

    def test_write_partition_count_mismatch_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])  # 计划 2 行，实际 1 行
        with self.assertRaises(SystemExit):
            f2o.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(table.write_attempts, f2o.WRITE_ATTEMPTS)
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_duplicate_ids_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, ucnt=1, mn="r1", mx="r1")])
        with self.assertRaises(SystemExit):
            f2o.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(table.write_attempts, f2o.WRITE_ATTEMPTS)
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_id_range_mismatch_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, mn="rx", mx="ry")])
        with self.assertRaises(SystemExit):
            f2o.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_empty_replaces_with_empty(self):
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 0, "ucnt": 0, "mn": None, "mx": None}])
        f2o.write_partition(odps, table, "p", "t", "json", "20260928", [])
        self.assertEqual([c[0] for c in table.calls], ["delete", "create", "write", "delete"])
        self.assertEqual(table.rows, [])
        self.assertIn("rename to partition (pt='20260928')", odps.sql)

    def test_write_partition_rename_failure_mentions_deleted_final(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(f2o, "rename_partition", side_effect=RuntimeError("ddl boom")):
            with self.assertRaises(SystemExit) as ctx:
                f2o.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区可能已被删掉", str(ctx.exception))
        # 失败后尽力清掉临时分区（最后一步是 delete tmp）
        self.assertEqual(table.calls[-1], ("delete", "pt=20260928__tmp"))

    def test_write_partition_cleanup_failure_keeps_error(self):
        table = mock.Mock()

        def delete(spec, if_exists=False):
            if spec.endswith("__tmp"):
                raise RuntimeError("delete boom")

        table.delete_partition.side_effect = delete
        table.open_writer.side_effect = RuntimeError("never")
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with self.assertRaises(SystemExit) as ctx:
            f2o.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区未动", str(ctx.exception))
        self.assertIn("残留", str(ctx.exception))

    def test_purge_stale_tmp_partitions(self):
        table = _FakeTable()
        table.partitions = [_Part("pt='20260927__tmp'"), _Part("pt='20260928'"), _Part("pt='20260929__tmp'")]
        f2o.purge_stale_tmp_partitions(table, "t")
        self.assertEqual(table.calls, [("delete", "pt='20260927__tmp'"), ("delete", "pt='20260929__tmp'")])

    def test_verify_partition(self):
        odps = _FakeOdps([{"cnt": 3, "ucnt": 3, "mn": "a", "mx": "c"}])
        self.assertEqual(f2o.verify_partition(odps, "p", "t", "json", "20260928"), (3, 3, "a", "c"))
        self.assertIn("get_json_object(json, '$.record_id')", odps.sql)
        self.assertIn("where pt = '20260928'", odps.sql)

    def test_write_partition_rename_failure_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(f2o, "rename_partition", side_effect=RuntimeError("ddl boom")):
            with self.assertRaises(SystemExit):
                f2o.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, f2o.WRITE_ATTEMPTS)

    def test_count_partition(self):
        odps = _FakeOdps([{"cnt": 7}])
        self.assertEqual(f2o.count_partition(odps, "p", "t", "20260928"), 7)
        self.assertIn("where pt = '20260928'", odps.sql)


# ---------------------------------------------------------------------------
# 运行锁
# ---------------------------------------------------------------------------
class TestRunLock(OfflineTestCase):
    def test_lock_path_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = f2o.lock_path(pathlib.Path(tmp) / "x.json", root=pathlib.Path(tmp) / "locks")
            self.assertTrue(str(path).endswith(".lock"))
            self.assertEqual(path.parent, pathlib.Path(tmp) / "locks")
            self.assertTrue(path.parent.is_dir())

    def test_mutual_exclusion(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = f2o.lock_path(pathlib.Path(tmp) / "x.json", root=pathlib.Path(tmp))
            with f2o.RunLock(path):
                with self.assertRaises(SystemExit):
                    with f2o.RunLock(path):
                        pass
            with f2o.RunLock(path):  # 释放之后可以再次拿到
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

        code = f2o.run_init(
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
            "probe_job",                                                       # ① 作业名
            "https://example.feishu.cn/base/IC4TEST?table=tblTEST&view=v",    # ② 链接
            "cli_test123",                                                     # ③ app_id
            "secret-abc-123",                                                  # App Secret
            "biz_date",                                                        # 日期 → 英文键
            "amount",                                                          # 金额 → 英文键
            "",                                                                # 备注 跳过
            "",                                                                # 项目（默认）
            "",                                                                # 表名（默认）
            "",                                                                # 注释（默认）
            "LTAI_TEST",                                                       # AK
            "SK_SECRET_XYZ",                                                   # SK
            "y",                                                               # 新鲜度
            "biz_date",                                                        # date_field
            "",                                                                # webhook
        ]
        code, echoed = self._run(answers, fake_fetch)
        self.assertEqual(code, 0)
        out = pathlib.Path(self._tmp.name) / "jobs" / "probe_job.json"
        self.assertTrue(out.is_file())
        job = json.loads(out.read_text(encoding="utf-8"))
        self.assertEqual(job["fields"], {"日期": "biz_date", "金额": "amount"})
        self.assertEqual(job["target"]["table"], "ods_probe_job_json_df")
        self.assertEqual(job["freshness"], {"date_field": "biz_date", "lag_days": 0})
        f2o.validate_job(json.loads(json.dumps(job)))  # 生成的配置必须能过校验
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
            "",   # 日期 跳过
        ]
        code, _ = self._run(answers, fake_fetch)
        self.assertEqual(code, 1)

    def test_wiki_link_rejected_then_base_ok(self):
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {"日期": "2026-09-27"}

        answers = [
            "probe_job",
            "https://example.feishu.cn/wiki/AbCdEfGh?table=tblTEST",    # wiki 链接 → 提示重问
            "https://example.feishu.cn/base/IC4TEST?table=tblTEST",    # 第二次给 base 链接
            "cli_test123",
            "secret-abc-123",
            "biz_date",
            "",   # 项目（默认）
            "",   # 表名（默认）
            "",   # 注释（默认）
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
            "probe-job",   # 作业名带 '-'（文件名可以，表名不行）
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
        args = f2o.parse_args(["--job", "x", "--no-check"])
        self.assertTrue(args.skip_freshness)
        args = f2o.parse_args(["--job", "x", "--skip-freshness"])
        self.assertTrue(args.skip_freshness)

    def test_main_requires_job(self):
        self.assertEqual(f2o.main([]), 2)

    def test_main_missing_job_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(f2o.main(["--job", str(pathlib.Path(tmp) / "nope.json")]), 1)

    def test_run_check_ok(self):
        o = mock.Mock()
        o.exist_table.return_value = False
        with mock.patch.object(f2o, "fetch_records", return_value=[{"a": 1}]), \
                mock.patch.object(f2o, "connect_odps", return_value=o):
            self.assertEqual(f2o.run_check(make_job(), "p", "t", "json", "20260928"), 0)

    def test_run_check_api_failure(self):
        with mock.patch.object(f2o, "fetch_records", side_effect=SystemExit("boom")):
            self.assertEqual(f2o.run_check(make_job(), "p", "t", "json", "20260928"), 1)

    def test_run_sync_dry_run_pass(self):
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-28", "amount": "$1"}]
        with mock.patch.object(f2o, "fetch_records", return_value=records):
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_freshness_warns_but_continues(self):
        # 缺数据只告警不失败（人填的表，节假日没人填是常态）：rc=0、照常写、飞书告警一次
        args = argparse.Namespace(skip_freshness=False, no_notify=False, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(f2o, "fetch_records", return_value=records), \
                mock.patch.object(f2o, "notify") as notifier:
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_called_once()
        self.assertIn("已照常写入", notifier.call_args.args[1])

    def test_run_sync_freshness_warns_but_no_notify_flag_silences_webhook(self):
        # --no-notify 只关飞书提醒：rc 仍是 0（缺数据不阻塞）
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(f2o, "fetch_records", return_value=records), \
                mock.patch.object(f2o, "notify") as notifier:
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_not_called()

    def test_run_sync_zero_records_still_fails(self):
        # 真异常（表被清空 = 0 行）仍然失败：缺数放行不等于放行空表
        args = argparse.Namespace(skip_freshness=False, no_notify=True, dry_run=True)
        with mock.patch.object(f2o, "fetch_records", return_value=[]), \
                mock.patch.object(f2o, "notify") as notifier:
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)
        notifier.assert_not_called()

    def test_run_sync_new_fields_notifies_once(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=False, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None):
            if extra_out is not None:
                extra_out.extend(["新列A", "新列A", "新列B"])
            return records

        with mock.patch.object(f2o, "fetch_records", side_effect=fake_fetch), \
                mock.patch.object(f2o, "notify") as notifier:
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_called_once()
        joined = "\n".join(notifier.call_args.args[2])
        self.assertIn("新增列", notifier.call_args.args[1])
        self.assertIn("新列A", joined)
        self.assertIn("新列B", joined)

    def test_run_sync_new_fields_no_notify_flag(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None):
            if extra_out is not None:
                extra_out.append("新列A")
            return records

        with mock.patch.object(f2o, "fetch_records", side_effect=fake_fetch), \
                mock.patch.object(f2o, "notify") as notifier:
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_not_called()

    def test_run_sync_skip_freshness(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(f2o, "fetch_records", return_value=records):
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_warns_empty_rows(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": None, "amount": None}]
        messages: list[str] = []
        with mock.patch.object(f2o, "fetch_records", return_value=records), \
                mock.patch.object(f2o, "log", side_effect=lambda msg: messages.append(str(msg))):
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        self.assertTrue(any("全为空" in m for m in messages))

    def test_run_sync_writes_and_counts(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        table = mock.Mock()
        odps = mock.Mock()
        with mock.patch.object(f2o, "fetch_records", return_value=records), \
                mock.patch.object(f2o, "connect_odps", return_value=odps), \
                mock.patch.object(f2o, "ensure_table", return_value=table), \
                mock.patch.object(f2o, "verify_schema"), \
                mock.patch.object(f2o, "purge_stale_tmp_partitions"), \
                mock.patch.object(f2o, "write_partition") as writer, \
                mock.patch.object(f2o, "count_partition", return_value=1):
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        writer.assert_called_once_with(odps, table, "p", "t", "json", "20260928", records)

    def test_run_sync_count_mismatch(self):
        args = argparse.Namespace(skip_freshness=True, no_notify=True, dry_run=False)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(f2o, "fetch_records", return_value=records), \
                mock.patch.object(f2o, "connect_odps", return_value=mock.Mock()), \
                mock.patch.object(f2o, "ensure_table", return_value=mock.Mock()), \
                mock.patch.object(f2o, "verify_schema"), \
                mock.patch.object(f2o, "purge_stale_tmp_partitions"), \
                mock.patch.object(f2o, "write_partition"), \
                mock.patch.object(f2o, "count_partition", return_value=0):
            code = f2o.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)

    def test_main_keyboard_interrupt(self):
        with mock.patch.object(f2o, "_run", side_effect=KeyboardInterrupt):
            self.assertEqual(f2o.main(["--job", "x"]), 130)

    def test_main_unexpected_error(self):
        with mock.patch.object(f2o, "_run", side_effect=RuntimeError("boom")):
            self.assertEqual(f2o.main(["--job", "x"]), 1)


if __name__ == "__main__":
    unittest.main()
