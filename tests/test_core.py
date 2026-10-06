# -*- coding: utf-8 -*-
"""feishu2ods 离线单元测试：不访问网络、不连 MaxCompute（requests/pyodps 没装也能跑）。

运行：python -m unittest discover -s tests -v
"""

from __future__ import annotations

import argparse
import errno
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
from feishu2ods import spool as spool_mod  # noqa: E402
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
        warned_patcher = mock.patch.object(utils_mod, "_sink_write_warned", False)
        warned_patcher.start()
        self.addCleanup(warned_patcher.stop)
        # 调度环境可能带着 bizdate / SKYNET_BIZDATE：不隔离的话默认业务日用例会吃到脏值。
        # 只摘掉这两个键，不动 PATH/TEMP（clear=True 会弄坏临时目录/外部命令）。
        env_patcher = mock.patch.dict(os.environ, clear=False)
        env_patcher.start()
        self.addCleanup(env_patcher.stop)
        os.environ.pop("bizdate", None)
        os.environ.pop("SKYNET_BIZDATE", None)
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

    def json(self):
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
        args = make_args(bizdate="20260101")
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 1, 1))

    def test_resolve_env(self):
        args = make_args()
        with mock.patch.dict(os.environ, {"bizdate": "20260202"}, clear=True):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 2, 2))

    def test_resolve_default_yesterday(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 29, 10, 0, tzinfo=utils_mod.CN_TZ).astimezone(tz) if tz else cls(2026, 9, 29, 10, 0)

        args = make_args()
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch.object(dates_mod, "datetime", _FixedDatetime):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 9, 28))

    def test_explicit_empty_bizdate_errors(self):
        """显式传空 --bizdate（调度脚本 `--bizdate "$pt"` 且 $pt 未定义）必须报错，
        不能按"未指定"静默回退成"昨天"写错分区。"""
        for empty in ("", "   "):
            args = make_args(bizdate=empty)
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit):
                    dates_mod.resolve_bizdate(args)


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

    def test_normalize_zero_epoch_is_not_a_date(self):
        """0（"未填"的常见占位）不能换算成 1970-01-01 的假日期——按认不出返回 None。"""
        self.assertIsNone(dates_mod.normalize_date_value(0))

    def test_normalize_garbage(self):
        self.assertIsNone(dates_mod.normalize_date_value(True))
        self.assertIsNone(dates_mod.normalize_date_value("abc"))
        self.assertIsNone(dates_mod.normalize_date_value(10**30))
        self.assertIsNone(dates_mod.normalize_date_value("2026-02-31"))

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
        self.assertTrue(responses[0].closed, "重试前必须关掉响应，否则连接不归还连接池")

    def test_4xx_closes_response(self):
        resp = _FakeResponse(403, text="nope")
        with mock.patch.object(auth_mod.requests, "request", return_value=resp):
            with self.assertRaises(fetch_mod.ApiHttpError):
                fetch_mod.request_json("GET", "https://x", "t")
        self.assertTrue(resp.closed)

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

    def test_token_can_be_refreshed_more_than_once(self):
        """大表翻页可能跑几个小时：token 第二次失效仍应自动重取，而不是直接中止整轮。"""
        pages = [
            {"code": 99991663},
            {"code": 99991663},
            self._page([["2026-09-27", "$1"]], ["r1"], False),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", side_effect=["tok1", "tok2", "tok3"]),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            records = cli_mod.fetch_records(self.job_feishu, self.mapping)
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

    def test_rev_missing_on_first_page_still_tracked(self):
        # 首屏偶发缺 rev 不能让检查永久失效：用首个带 rev 的页建立基准，后面变了照样中止
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True),
            self._page([["2026-09-26", "$2"]], ["r2"], True, rev=12),
            self._page([["2026-09-25", "$3"]], ["r3"], False, rev=13),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.fetch_records(self.job_feishu, self.mapping)

    def test_rev_baseline_from_later_page_ok(self):
        pages = [
            self._page([["2026-09-27", "$1"]], ["r1"], True),
            self._page([["2026-09-26", "$2"]], ["r2"], True, rev=12),
            self._page([["2026-09-25", "$3"]], ["r3"], False, rev=12),
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            self.assertEqual(len(cli_mod.fetch_records(self.job_feishu, self.mapping)), 3)

    def test_nonstream_extra_out_not_doubled(self):
        pages = [
            {
                "code": 0,
                "data": {
                    "fields": ["日期", "金额", "新列"],
                    "data": [["a", "b", "c"]],
                    "record_id_list": ["r1"],
                    "has_more": False,
                },
            }
        ]
        extra: list[str] = []
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            cli_mod.fetch_records(self.job_feishu, self.mapping, extra_out=extra)
        self.assertEqual(extra, ["新列"])


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
        tmp = f"20260928{mc_mod.TMP_PARTITION_SUFFIX}"
        # 先写临时分区（删残留 tmp + 建 tmp），再删正式分区、rename 顶上；
        # 分区增删走带超时的 DDL（run_sql），表对象只负责 Tunnel 写入
        self.assertEqual([c[0] for c in table.calls], ["write"])
        self.assertEqual(table.calls[0][1], f"pt={tmp}")
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        sqls = "\n".join(odps.sqls)
        self.assertIn(f"drop if exists partition (pt='{tmp}')", sqls)
        self.assertIn(f"add if not exists partition (pt='{tmp}')", sqls)
        self.assertIn("drop if exists partition (pt='20260928')", sqls)
        self.assertIn("rename to partition (pt='20260928')", sqls)
        self.assertIn(f"pt='{tmp}'", odps.sql)

    def test_write_partition_retry(self):
        table = _FakeTable(fail_first_write=True)
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 2)
        self.assertEqual(table.rows, [['{"record_id":"r1","a":1}']])
        # reopen=True 是 Tunnel 断线重连的关键实参：去掉它不该在测试里悄悄通过
        self.assertTrue(table.reopens and all(table.reopens))

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

    def test_rename_failure_preserve_renames_to_keep_marker(self):
        """保留的完整副本要改名成 __keep：否则下一次运行的 purge 会把它当残留清掉。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        calls: list[tuple[str, str]] = []

        def fake_rename(o, project, table_name, old_spec, new_spec, timeout=None):
            calls.append((old_spec, new_spec))
            raise RuntimeError("ddl boom")  # 正式 rename 与保留 rename 都失败

        with mock.patch.object(mc_mod, "rename_partition", side_effect=fake_rename):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(calls[0][1], f"{mc_mod.PARTITION_COLUMN}=20260928")
        keep_calls = [c for c in calls if c[1].endswith(mc_mod.KEEP_PARTITION_SUFFIX)]
        self.assertEqual(len(keep_calls), 1)  # 失败收尾时尝试改名成 __keep

    def test_retry_after_deleted_final_keeps_unique_copy(self):
        """第一次 rename 失败后，tmp 是唯一完整副本：重试只补做 rename、不再清掉重建
        （重建中途再失败就把当天分区彻底弄丢——正式分区已被删）。最终失败时按
        「已核对完整」保留，可直接恢复，不能按"可能已被删掉"含糊描述。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        calls = {"n": 0}
        real_add = mc_mod.add_partition

        def counting_add(*args, **kwargs):
            calls["n"] += 1
            return real_add(*args, **kwargs)

        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("rename boom")),
            mock.patch.object(mc_mod, "add_partition", side_effect=counting_add),
            mock.patch.object(mc_mod, "_partition_exists", return_value=True),  # tmp 还在表里
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 1)  # 只写过一次：重试没有重建 tmp
        self.assertEqual(calls["n"], 1)  # 建分区也只发过一次
        # drop 返回过成功、tmp 完整保留：措辞是确定的；不能退化成"可能已被删掉"的含糊描述
        self.assertIn("正式分区已被删掉", str(ctx.exception))
        self.assertNotIn("可能已被删掉", str(ctx.exception))
        self.assertIn("已保留", str(ctx.exception))

    def test_retry_after_deleted_final_only_retries_rename(self):
        """rename 前两次失败、第三次成功：重试只补做 rename，tmp 只建过一次。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        calls = {"n": 0}
        real_rename = mc_mod.rename_partition

        def flaky_rename(o, project, table_name, old_spec, new_spec, timeout=None):
            calls["n"] += 1
            if calls["n"] <= 2:
                raise RuntimeError("rename boom")
            return real_rename(o, project, table_name, old_spec, new_spec, timeout=timeout)

        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=flaky_rename),
            mock.patch.object(mc_mod, "_partition_exists", return_value=True),  # tmp 还在表里
        ):
            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 1)
        tmp = f"20260928{mc_mod.TMP_PARTITION_SUFFIX}"
        drops = [s for s in odps.sqls if s.startswith("alter table p.t drop if exists partition")]
        self.assertEqual(sum(1 for s in drops if tmp in s), 1)  # tmp 只被清过一次（重试没重建）
        self.assertEqual(calls["n"], 3)

    def test_retry_when_tmp_already_renamed_counts_as_success(self):
        """rename 在服务端已成功、客户端才超时：tmp 不在 = 数据已就位，重试按完成处理——
        决不能补 drop（会把刚顶上去的新分区删掉）。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        calls = {"n": 0}
        real_rename = mc_mod.rename_partition

        def tricky_rename(o, project, table_name, old_spec, new_spec, timeout=None):
            calls["n"] += 1
            real_rename(o, project, table_name, old_spec, new_spec, timeout=timeout)  # 服务端做成了
            raise TimeoutError("客户端超时（实际已成功）")

        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=tricky_rename),
            # tmp 已不在；正式分区在（=上一轮 rename 真的生效了）
            mock.patch.object(
                mc_mod,
                "_partition_exists",
                side_effect=lambda _t, spec: not spec.endswith(mc_mod.TMP_PARTITION_SUFFIX),
            ),
        ):
            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 1)
        self.assertEqual(calls["n"], 1)  # 重试没有再补 rename
        final_drops = [
            s for s in odps.sqls if s.startswith("alter table p.t drop if exists partition") and "20260928'" in s
        ]
        self.assertEqual(len(final_drops), 1)  # 重试没有补 drop 把新分区删掉

    def test_retry_when_both_missing_rebuilds_instead_of_fake_success(self):
        """tmp 探测不到、正式分区也不在：不能直接按成功处理（元数据误报会静默丢数据），
        落回常规路径重建 tmp 收尾。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        calls = {"n": 0}
        messages: list[str] = []
        real_rename = mc_mod.rename_partition

        def flaky_rename(o, project, table_name, old_spec, new_spec, timeout=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise TimeoutError("客户端超时")
            return real_rename(o, project, table_name, old_spec, new_spec, timeout=timeout)

        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=flaky_rename),
            mock.patch.object(mc_mod, "_partition_exists", return_value=False),  # 两个分区都探测不到
            mock.patch.object(mc_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 2)  # 重建过一次（没有假报成功）
        self.assertEqual(calls["n"], 2)
        self.assertIn("状态未知", "\n".join(messages))

    def test_keep_rename_failure_without_existing_keep_warns_at_risk(self):
        """改了 __keep 名失败、又没有现成的 __keep 时，日志不能承诺"不会被误删"：
        那份副本是 __tmp 形态，下一次运行的残留清理就会删掉它，必须让运维立刻手工恢复。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        messages: list[str] = []

        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("rename boom")),
            # 按查询的分区作答：tmp 还在（快速重试路径用），keep 不存在（走"危急"分支）
            mock.patch.object(
                mc_mod,
                "_partition_exists",
                side_effect=lambda _t, spec: not spec.endswith(mc_mod.KEEP_PARTITION_SUFFIX),
            ),
            mock.patch.object(mc_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        joined = "\n".join(messages)
        self.assertIn("当残留清理删掉", joined)
        self.assertNotIn("不会被后续运行的残留清理误删", joined)
        self.assertIn("会被后续残留清理删除", str(ctx.exception))

    def test_keyboard_interrupt_not_replaced_by_finally(self):
        """finally 里不能再抛 SystemExit 覆盖正在传播的 KeyboardInterrupt（退出码要保 130）。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "drop_partition", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])

    def test_write_partition_rename_failure_mentions_deleted_final(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("ddl boom")),
            # tmp 确证还在（快速重试路径）；同 pt 的 __keep 已存在（走"已存在"提示分支）
            mock.patch.object(
                mc_mod,
                "_partition_exists",
                side_effect=lambda _t, spec: (
                    spec.endswith(mc_mod.KEEP_PARTITION_SUFFIX) or spec.endswith(mc_mod.TMP_PARTITION_SUFFIX)
                ),
            ),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区已被删掉", str(ctx.exception))
        # 正式分区已删 + 临时分区是已核对的完整数据：保留不删（可手工 rename 恢复），
        # 最后一步不该再发 drop tmp 的 DDL
        tmp = f"20260928{mc_mod.TMP_PARTITION_SUFFIX}"
        self.assertIn("已保留", str(ctx.exception))
        self.assertNotIn(f"drop if exists partition (pt='{tmp}')", odps.sqls[-1])

    def test_run_sync_rejects_non_bool_allow_empty(self):
        """库调用方绕过 validate_job 时，"allow_empty": "false" 不能被 bool() 吞成 True
        （那会静默跳过 0 行保护、把源表截断成 0 行当成正常写空分区）——与 lag_days 同口径。"""
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
        job = make_job()
        job["target"]["allow_empty"] = "false"
        spool_holder = {}
        # 提前注册清理（放在断言之前）：断言失败时临时 spool 也不能泄漏
        self.addCleanup(lambda: spool_holder.get("spool") and spool_holder["spool"].path.unlink(missing_ok=True))

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            spool_holder["spool"] = sink
            return stats

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.run_sync(args, job, "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertIn("allow_empty", str(ctx.exception))

    def test_probe_failure_falls_back_to_rebuild_not_drop_first(self):
        """tmp 探测失败（不可信）时不能按"还在"处理：快速路径会先 drop 正式分区，
        万一上一轮 rename 已生效，删掉的就是刚写入的新数据。必须落回常规路径重建。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        messages: list[str] = []
        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("rename boom")),
            mock.patch.object(mc_mod, "_partition_exists", return_value=None),  # 探测失败
            mock.patch.object(mc_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 3)  # 每趟都走重建，而不是"先删正式分区"的快速路径
        self.assertIn("状态未知", "\n".join(messages))

    def test_write_partition_cleanup_failure_keeps_error(self):
        table = mock.Mock()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])

        def drop(o, project, table_name, spec, timeout=None):
            if mc_mod.TMP_PARTITION_SUFFIX in spec:
                raise RuntimeError("delete boom")

        with mock.patch.object(mc_mod, "drop_partition", side_effect=drop):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区未动", str(ctx.exception))
        self.assertIn("残留", str(ctx.exception))

    def test_write_partition_verify_systemexit_still_cleans_tmp(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "verify_partition", side_effect=SystemExit("核对失败")):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("核对失败", str(ctx.exception))
        tmp = f"20260928{mc_mod.TMP_PARTITION_SUFFIX}"
        self.assertIn(f"drop if exists partition (pt='{tmp}')", odps.sqls[-1])

    def test_purge_skips_keep_partitions(self):
        """__keep 是"正式分区被删后唯一完整副本"，清理必须跳过它。"""
        table = _FakeTable()
        keep = "pt='20260928__keep'"
        table.partitions = [_Part(keep), _Part("pt='20260927__tmp_otherhost_1'")]
        odps = _FakeOdps([])
        cli_mod.purge_stale_tmp_partitions(odps, table, "p", "t")
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260927__tmp_otherhost_1')", sqls)
        self.assertNotIn("20260928__keep", sqls)

    def test_purge_stale_tmp_partitions(self):
        table = _FakeTable()
        mine = f"pt='20260927{mc_mod.TMP_PARTITION_SUFFIX}'"
        other = "pt='20260929__tmp_otherhost_1'"
        legacy = "pt='20260926__tmp'"
        # 另一进程的 pid 以本进程 pid 为前缀（456 vs 4567）：不能当成"自己的"跳过
        near_miss = f"pt='20260925{mc_mod.TMP_PARTITION_SUFFIX}7'"
        table.partitions = [_Part(mine), _Part("pt='20260928'"), _Part(other), _Part(legacy), _Part(near_miss)]
        odps = _FakeOdps([])
        # pid 探测打桩为"进程已死"：非 POSIX 平台现在保守按"可能还在"返回（见 _pid_alive），
        # 不打桩的话 Windows 上同机残留会被跳过、断言随平台漂移
        with mock.patch.object(mc_mod, "_pid_alive", return_value=False):
            cli_mod.purge_stale_tmp_partitions(odps, table, "p", "t")
        sqls = "\n".join(odps.sqls)
        # 历史残留（含其它机器/其它 pid 与旧版无 run id 的 __tmp）统一清理：
        # 不清理会让 max_pt() 读到 __tmp 半成品；跨机并发不受支持（锁只保证单机互斥）
        self.assertIn("drop if exists partition (pt='20260929__tmp_otherhost_1')", sqls)
        self.assertIn("drop if exists partition (pt='20260926__tmp')", sqls)
        self.assertIn(f"drop if exists partition (pt='20260925{mc_mod.TMP_PARTITION_SUFFIX}7')", sqls)
        self.assertNotIn(f"pt='20260927{mc_mod.TMP_PARTITION_SUFFIX}'", sqls)  # 本进程自己的后缀不动
        self.assertNotIn("20260928", sqls)  # 正式分区不动

    def test_sql_spec_normalizes_quotes(self):
        self.assertEqual(mc_mod._sql_spec("pt=20260928"), "pt='20260928'")
        self.assertEqual(mc_mod._sql_spec("pt='20260928'"), "pt='20260928'")
        self.assertEqual(mc_mod._sql_spec('pt="20260928"'), "pt='20260928'")

    def test_sql_spec_rejects_bad_key_and_empty_value(self):
        with self.assertRaises(SystemExit):
            mc_mod._sql_spec("pt=")
        with self.assertRaises(SystemExit):
            mc_mod._sql_spec("pt;drop=20260928")
        with self.assertRaises(SystemExit):
            mc_mod._sql_spec("=20260928")

    def test_verify_partition(self):
        odps = _FakeOdps([{"cnt": 3, "ucnt": 3, "mn": "a", "mx": "c"}])
        self.assertEqual(mc_mod.verify_partition(odps, "p", "t", "json", "20260928"), (3, 3, "a", "c"))
        self.assertIn("get_json_object(json, '$.record_id')", odps.sql)
        self.assertIn("where pt = '20260928'", odps.sql)

    def test_write_partition_rename_failure_retries(self):
        """rename 失败后的重试只补做 rename，不重新写一遍（tmp 已核对完整、正式分区已删，
        重建中途失败会把当天分区彻底弄丢；重写也不可能让已核对的 tmp 更好）。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with (
            mock.patch.object(mc_mod, "rename_partition", side_effect=RuntimeError("ddl boom")),
            mock.patch.object(mc_mod, "_partition_exists", return_value=True),
        ):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(table.write_attempts, 1)

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

    def test_exit_clears_handle(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            lock = utils_mod.RunLock(path)
            with lock:
                self.assertIsNotNone(lock.fh)
            self.assertIsNone(lock.fh)


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
            "",  # endpoint（默认 us-west-1）
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
        # endpoint 显式问过（不再写死 us-west-1）：回车采纳默认值
        self.assertEqual(job["maxcompute"]["endpoint"], mc_mod.DEFAULT_ENDPOINT)
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

    def test_fetch_expected_error_gives_message(self):
        """拉取阶段的预期错误（网络/接口类）给一条人话 + rc=1。"""

        def boom(feishu):
            raise OSError("connection reset")

        code, echoed = self._run(["probe_job", "IC4TEST", "tblTEST", "cli_test123", "s" * 10], boom)
        self.assertEqual(code, 1)
        self.assertTrue(any("拉取失败" in line for line in echoed), echoed)

    def test_fetch_programming_error_propagates(self):
        """代码缺陷（TypeError 等）不能降级成"拉取失败"：会被掩盖成配置/网络问题。"""

        def boom(feishu):
            raise TypeError("bug")

        with self.assertRaises(TypeError):
            self._run(["probe_job", "IC4TEST", "tblTEST", "cli_test123", "s" * 10], boom)

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
            "",  # endpoint（默认）
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
            "",  # endpoint（默认）
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
        args = make_args(skip_freshness=False, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-28", "amount": "$1"}]
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_freshness_warns_but_continues(self):
        # 缺数据只告警不失败（人填的表，节假日没人填是常态）：rc=0、照常写、飞书告警一次
        args = make_args(skip_freshness=False, no_notify=False, dry_run=True)
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
        args = make_args(skip_freshness=False, no_notify=True, dry_run=True)
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
        args = make_args(skip_freshness=False, no_notify=True, dry_run=True)
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub([])),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)
        notifier.assert_not_called()

    def test_run_sync_new_fields_notifies_once(self):
        args = make_args(skip_freshness=True, no_notify=False, dry_run=True)
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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)

    def test_run_sync_warns_empty_rows(self):
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS)
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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS)
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
        """空白行 = 映射过的字段全为 None；只有 record_id 的记录（没有字段可判空）不算——
        all() 对空序列恒为 True，不显式判空会把这类记录误计成空白行。"""
        stats = cli_mod.FetchStats()
        stats.update(
            [
                {"record_id": "r1", "a": None, "b": None},  # 全空
                {"record_id": "r2", "a": 1},  # 有值
                {"record_id": "r3"},  # 只有 record_id：不算空白行
            ]
        )
        self.assertEqual(stats.empty_rows, 1)

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
    def _spool(self, *args) -> cli_mod.SpoolWriter:
        """创建即登记清理：断言失败时也保证临时文件与句柄被回收。"""
        spool = cli_mod.SpoolWriter(*args)
        self.addCleanup(spool.close)
        return spool

    def test_roundtrip_rows(self):
        spool = self._spool()
        spool.write_records([{"a": 1}, {"b": "中文"}])
        rows = list(spool.iter_rows())
        self.assertEqual(rows, ['{"a":1}', '{"b":"中文"}'])
        self.assertEqual(spool.count, 2)
        spool.close()

    def test_write_returns_count(self):
        spool = self._spool()
        self.assertEqual(spool.write_records([{"a": 1}, {"a": 2}]), 2)
        spool.close()

    def test_explicit_path_is_not_deleted_on_close(self):
        """调用方显式传入的 path 是它的数据文件：close 不能删掉。"""
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "keep.jsonl"
            spool = cli_mod.SpoolWriter(path)
            spool.write_records([{"a": 1}])
            spool.close()
            self.assertTrue(path.exists())

    def test_context_manager_keeps_file_on_exception(self):
        """with 退出：正常路径删除临时文件，异常路径保留（排障用）。"""
        with tempfile.TemporaryDirectory() as tmp:
            ok = pathlib.Path(tmp) / "ok.jsonl"
            with cli_mod.SpoolWriter(ok) as spool:
                spool.write_records([{"a": 1}])
            # 显式 path：无论正常/异常都不删（归属调用方）
            self.assertTrue(ok.exists())

    def test_nan_value_error_mentions_record_id(self):
        """allow_nan=False 的裸 ValueError 要带记录上下文，便于定位是哪条记录。"""
        with self.assertRaises(ValueError) as err:
            spool_mod.dump_record({"record_id": "r9", "v": float("nan")})
        self.assertIn("r9", str(err.exception))

    def test_iter_rows_after_close_raises_readable_error(self):
        """关闭后读回：原来抛没有上下文的 FileNotFoundError（keep=False 已删文件）。"""
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"a": 1}])
        spool.close()
        with self.assertRaises(RuntimeError) as err:
            list(spool.iter_rows())
        self.assertIn("已关闭", str(err.exception))

    def test_close_unlink_failure_logs_warning(self):
        """临时文件删不掉时留一条线索（原来完全静默，残留无从察觉）。"""
        spool = cli_mod.SpoolWriter()
        spool.write_records([{"a": 1}])
        path = spool.path
        logged: list = []
        with (
            mock.patch.object(pathlib.Path, "unlink", side_effect=OSError("busy")),
            mock.patch.object(spool_mod, "log", logged.append),  # spool.py 里 from .utils import log
        ):
            spool.close()
        self.assertTrue(any("删除失败" in str(line) for line in logged), logged)
        path.unlink()  # 补删测试残留（unlink 补丁已退出）

    def test_batches(self):
        spool = self._spool()
        spool.write_records([{"i": i} for i in range(5)])
        batches = list(spool.iter_batches(batch_size=2))
        self.assertEqual([len(b) for b in batches], [2, 2, 1])
        spool.close()

    def test_iter_rows_rerunnable(self):
        spool = self._spool()
        spool.write_records([{"a": 1}])
        self.assertEqual(len(list(spool.iter_rows())), 1)
        self.assertEqual(len(list(spool.iter_rows())), 1)  # 重试场景：重新读一遍
        spool.close()

    def test_empty_spool(self):
        spool = self._spool()
        self.assertEqual(list(spool.iter_rows()), [])
        self.assertEqual(list(spool.iter_batches()), [])
        spool.close()

    def test_close_deletes_file(self):
        spool = self._spool()
        path = spool.path
        spool.write_records([{"a": 1}])
        spool.close()
        self.assertFalse(path.exists())

    def test_close_keep(self):
        spool = self._spool()
        path = spool.path
        spool.write_records([{"a": 1}])
        spool.close(keep=True)
        self.assertTrue(path.exists())
        path.unlink()

    def test_iter_rows_strips_newline(self):
        spool = self._spool()
        spool.write_records([{"a": "x\ny"}])  # 值里含换行会被 JSON 转义
        rows = list(spool.iter_rows())
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].startswith('{"a":"x\\ny"}'))
        spool.close()

    def test_custom_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "spool.jsonl"
            spool = self._spool(path)
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

    def test_json_numeric_secret_is_masked(self):
        """{"password": 12345} 这类不带引号的数字值也要遮（JSON 规则只吃字符串值）。"""
        out = utils_mod.redact('{"password": 12345, "page": 2}')
        self.assertNotIn("12345", out)
        self.assertIn("***", out)

    def test_redact_survives_recursion_error_from_json(self):
        """反转义时 json.loads 抛 RecursionError（超深嵌套）不能打穿脱敏流程。"""
        with mock.patch.object(utils_mod.json, "loads", side_effect=RecursionError("too deep")):
            out = utils_mod.redact('{"k": "v"x"}')
        self.assertIsInstance(out, str)

    def test_deeply_nested_equals_does_not_recursion_error(self):
        """构造性文本（上千个等号连写）不能把脱敏本身打成 RecursionError。

        各回调会把匹配到的值再交给 _redact_shapes 递归；`a=b=c=…` 每层只剥一个等号，
        没有深度上限时第三方响应体里的这种文本会直接打挂日志路径。
        """
        text = "a=" + "b=" * 5000 + "c"
        out = utils_mod.redact(text)
        self.assertIsInstance(out, str)
        self.assertIn("***", out)  # 到上限按「宁可多脱敏」整段遮掉

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

    def test_redact_secrets_aggressively_encoded(self):
        """部分编码器把 "-" 这类字符也编码成 %2D：该形态同样要遮。"""
        out = utils_mod.redact_secrets(["t-abc123"], "url?data=t%2Dabc123 end")
        self.assertNotIn("t%2Dabc123", out)
        self.assertIn("***", out)

    def test_redact_secrets_aggressively_encoded_non_ascii(self):
        """含中文的口令：激进编码变体按字节编码（%E5%AF%86，而不是 Latin-1 的 å…）。"""
        secret = "p@ss-密码"
        encoded = "p%40ss%2D%E5%AF%86%E7%A0%81"
        out = utils_mod.redact_secrets([secret], f"url?data={encoded} end")
        self.assertNotIn(encoded, out)
        self.assertIn("***", out)

    def test_redact_survives_surrogate_secret(self):
        """含孤立代理字符的密钥（surrogateescape 路径名）：脱敏不能抛 UnicodeEncodeError。"""
        secret = "sk-abc" + chr(0xDCE9) + "xyz"
        out = utils_mod.redact_secrets([secret], "err: " + secret + " end")
        self.assertIsInstance(out, str)
        self.assertNotIn(secret, out)

    def test_redact_secrets_accepts_bare_scalar_values(self):
        """values 直接传裸标量（数字/字符串）也不能炸：数字按 ID 类密钥 str 化后遮蔽。"""
        out = utils_mod.redact_secrets(123456, "charge failed id=123456")
        self.assertEqual(out, "charge failed id=***")

    def test_redact_secrets_value_first(self):
        out = utils_mod.redact_secrets(["sk-live-abcdef123456"], "error body sk-live-abcdef123456 end")
        self.assertNotIn("sk-live-abcdef123456", out)
        self.assertIn("***", out)

    def test_log_file_expands_tilde(self):
        """--log-file "~/logs/x.log" 要写到 HOME 下，而不是 CWD 里字面量 "~" 目录。"""
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(os.environ, {"HOME": tmp, "USERPROFILE": tmp}):
                handle = cli_mod._open_log_file("~/logs/run.log")
            try:
                self.assertEqual(
                    pathlib.Path(handle.name).resolve(), (pathlib.Path(tmp) / "logs" / "run.log").resolve()
                )
            finally:
                handle.close()

    def test_reset_secret_values_accepts_scalars(self):
        """标量入参要规范化：字符串不能被按字符拆开（脱敏表为空）、数字不能 TypeError。"""
        utils_mod.reset_secret_values("sk-live-9f3a1b")
        self.assertIn("sk-live-9f3a1b", utils_mod._SECRETS)
        utils_mod.reset_secret_values(1234567890)
        self.assertIn("1234567890", utils_mod._SECRETS)
        out = utils_mod.redact("Invalid token: sk-live-9f3a1b")
        self.assertNotIn("sk-live-9f3a1b", out)  # 这里已换成数字表，只验证不崩
        utils_mod.reset_secret_values(None)
        self.assertEqual(utils_mod._SECRETS, [])

    def test_redact_secrets_masks_numeric_secret(self):
        """数字型密钥（ID 类凭证）str 化后要遮蔽，不能被 isinstance(str) 静默丢弃。"""
        out = utils_mod.redact_secrets(1234567890, "auth failed id=1234567890 end")
        self.assertNotIn("1234567890", out)
        self.assertIn("***", out)

    def test_redact_secrets_skips_short_and_non_str(self):
        out = utils_mod.redact_secrets(["ok", "1", 12345, None], "status=ok code=1")
        self.assertEqual(out, "status=ok code=1")

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

    def test_redact_skips_short_registered_secrets(self):
        utils_mod._SECRETS.append("ok")
        self.assertEqual(utils_mod.redact("status=ok"), "status=ok")


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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True, sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS)
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

    def test_log_sink_write_happens_outside_lock(self):
        """慢 sink（NFS/满盘）只该拖慢这条日志，不该占住全局锁卡死其它线程。"""
        seen = {}

        class Probe:
            def write(self, *_a):
                seen["locked"] = utils_mod._lock.locked()

            def flush(self):
                pass

            def close(self):
                pass

        probe = Probe()
        utils_mod.add_log_sink(probe)
        try:
            utils_mod.log("hello")
        finally:
            utils_mod.remove_log_sink(probe)
        self.assertIs(seen["locked"], False)

    def test_log_writes_to_file_sink(self):
        handle = io.StringIO()
        utils_mod.add_log_sink(handle)
        try:
            utils_mod.log("写一份到文件")
        finally:
            utils_mod._sinks.remove(handle)  # 直接摘，不关句柄（remove_log_sink 会 close）
        self.assertIn("写一份到文件", handle.getvalue())

    def test_add_log_sink_is_idempotent(self):
        """重复登记同一句柄会让日志写两遍、摘除后留下失效句柄；add_log_sink 按身份去重。"""
        handle = io.StringIO()
        utils_mod.add_log_sink(handle)
        try:
            utils_mod.add_log_sink(handle)
            utils_mod.log("只应写一次")
        finally:
            utils_mod._sinks.remove(handle)
        self.assertEqual(handle.getvalue().count("只应写一次"), 1)

    def test_add_log_sink_ignores_none(self):
        """add_log_sink(None) 不能把 None 塞进 _sinks（否则此后每次 log() 都炸在 handle.write）。"""
        utils_mod.add_log_sink(None)
        try:
            utils_mod.log("照常输出")
        finally:
            utils_mod.remove_log_sink(None)
        self.assertNotIn(None, utils_mod._sinks)

    def test_broken_sink_object_does_not_break_logging(self):
        """sink 抛非 I/O 异常（疑似代码缺陷）也不能打挂 log()，且警告要指出这一点。"""

        class Bad:
            def write(self, _text):
                raise AttributeError("boom")

        bad = Bad()
        utils_mod.add_log_sink(bad)
        buf = io.StringIO()
        try:
            with mock.patch.object(sys, "stderr", buf):
                utils_mod.log("业务还在跑")
        finally:
            utils_mod.remove_log_sink(bad)
        self.assertIn("疑似代码缺陷", buf.getvalue())

    def test_broken_stdout_does_not_break_business(self):
        """stdout 断管（BrokenPipeError，如 `| head` 提前退出）时 log() 自身不能抛异常。"""
        with mock.patch("builtins.print", side_effect=BrokenPipeError("closed")):
            utils_mod.log("业务还在跑")  # 不抛即通过

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

            def close(self):
                pass

        sink = BrokenSink()
        utils_mod.add_log_sink(sink)
        try:
            with mock.patch.object(sys, "stderr", io.StringIO()) as err:
                utils_mod.log("照常输出")  # 不该抛
                utils_mod.log("第二次仍不抛")
            warning = err.getvalue()
            self.assertIn("log-file 写入失败", warning)
            self.assertEqual(warning.count("log-file 写入失败"), 1)
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

    def test_main_log_file_directory_returns_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            rc = cli_mod.main(["--job", "x", "--log-file", tmp])
        self.assertEqual(rc, 1)


class TestSqlTimeout(OfflineTestCase):
    """F4：MaxCompute SQL 超时保护 + --sql-timeout。"""

    def test_parse_args_default_and_zero(self):
        self.assertEqual(cli_mod.parse_args(["--job", "x"]).sql_timeout, mc_mod.SQL_TIMEOUT_SECONDS)
        self.assertEqual(cli_mod.parse_args(["--job", "x", "--sql-timeout", "0"]).sql_timeout, 0)

    def test_parse_args_negative_is_arg_error(self):
        with self.assertRaises(SystemExit) as ctx:
            cli_mod.parse_args(["--job", "x", "--sql-timeout", "-5"])
        self.assertEqual(ctx.exception.code, 2)

    def test_main_parse_args_error_returns_code(self):
        logs = []
        with mock.patch.object(cli_mod, "log", logs.append):
            rc = cli_mod.main(["--job", "x", "--sql-timeout", "-5"])
        self.assertEqual(rc, 2)
        self.assertTrue(any("退出码 2" in str(line) for line in logs), logs)

    def test_run_sql_with_timeout_success(self):
        odps = _FakeOdps([{"cnt": 1}])
        inst = mc_mod.run_sql_with_timeout(odps, "select 1", timeout=600, desc="t")
        self.assertIsNotNone(inst)

    def test_run_sql_with_timeout_raises_and_stops(self):
        inst = _FakeInstance([], never_success=True)
        odps = mock.Mock()
        odps.run_sql.return_value = inst
        clock = iter([100.0, 100.0, 1100.0])
        # 生产代码量经过时间用单调时钟（墙钟被校时会误判超时）
        with mock.patch.object(mc_mod.time, "monotonic", side_effect=lambda: next(clock, 10**9)):
            with self.assertRaises(TimeoutError):
                mc_mod.run_sql_with_timeout(odps, "select 1", timeout=5, desc="t")
        self.assertTrue(inst.stopped)

    def test_run_sql_terminated_but_failed_raises(self):
        """已终止但不是成功态：wait_for_success 没抛错时也要显式判成功性，不能当成成功。"""

        class TerminatedFailed(_FakeInstance):
            def is_successful(self):
                return False

            def is_terminated(self):
                return True

            def wait_for_success(self, timeout=None):
                return self  # 模拟"没抛错"的实现差异

        odps = mock.Mock()
        odps.run_sql.return_value = TerminatedFailed([])
        with self.assertRaises(RuntimeError) as ctx:
            mc_mod.run_sql_with_timeout(odps, "select 1", timeout=5, desc="建表 t")
        self.assertIn("已终止但未成功", str(ctx.exception))

    def test_count_partition_no_rows_is_error_not_zero(self):
        """count(*) 读不到行 = SQL 没真正执行，不能按 0 行返回（0 行保护会据此清掉有数据的分区）。"""
        with self.assertRaises(SystemExit) as ctx:
            cli_mod.count_partition(_FakeOdps([]), "p", "t", "20260928")
        self.assertIn("未返回行", str(ctx.exception))

    def test_verify_partition_no_rows_is_error(self):
        """核对 SQL 读不到行同样不能当成"0 行、无 id"（那会让真异常看起来像空分区）。"""
        with self.assertRaises(SystemExit) as ctx:
            mc_mod.verify_partition(_FakeOdps([]), "p", "t", "json", "20260928")
        self.assertIn("未返回行", str(ctx.exception))

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
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        spool_holder = {}

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            spool_holder["spool"] = sink
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        self.assertFalse(spool_holder["spool"].path.exists())

    def test_zero_rows_keeps_spool_for_debugging(self):
        """0 行失败路径保留落盘文件（"失败留证"：哪怕是空的 0 行快照，也要能事后排查）。"""
        args = make_args(skip_freshness=True, no_notify=True, dry_run=True)
        spool_holder = {}

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            spool_holder["spool"] = sink
            return stats

        with mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 1)  # 0 行保护
        path = spool_holder["spool"].path
        self.assertTrue(path.exists())
        self.addCleanup(path.unlink, missing_ok=True)  # 留证文件由用例自己清掉

    def _sync_args(self):
        return make_args(
            skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS, force=False
        )

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
            with self.assertRaises(SystemExit) as ctx:
                dates_mod.env_bizdate()
            self.assertIn("空白", str(ctx.exception))
        with mock.patch.dict(os.environ, {"bizdate": ""}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate(strict=False))

    def test_env_skynet_fallback(self):
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 28))

    def test_env_bizdate_priority_over_skynet(self):
        with mock.patch.dict(os.environ, {"bizdate": "20260927", "SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(), date(2026, 9, 27))

    def test_resolve_bizdate_bad_env_fails(self):
        args = make_args()
        with mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True):
            with self.assertRaises(SystemExit):
                dates_mod.resolve_bizdate(args)

    def test_env_bizdate_non_strict_tolerates_dirty(self):
        """--check 用的非严格模式：脏 bizdate 按"未设置"处理，不报错。"""
        with mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate(strict=False))
        with mock.patch.dict(os.environ, {"SKYNET_BIZDATE": "2026-9-7"}, clear=True):
            self.assertIsNone(dates_mod.env_bizdate(strict=False))

    def test_env_whitespace_bizdate_does_not_shadow_skynet_non_strict(self):
        """空白 bizdate 不得用 or 短路挡住合法的 SKYNET_BIZDATE（非严格可回落到后者）。"""
        with mock.patch.dict(os.environ, {"bizdate": "   ", "SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(strict=False), date(2026, 9, 28))

    def test_env_dirty_bizdate_does_not_shadow_skynet_non_strict(self):
        """脏（非空白）bizdate 同样按"未设置"处理、继续看 SKYNET_BIZDATE（与空白分支同口径）。"""
        with mock.patch.dict(os.environ, {"bizdate": "bad-date", "SKYNET_BIZDATE": "20260928"}, clear=True):
            self.assertEqual(dates_mod.env_bizdate(strict=False), date(2026, 9, 28))

    def test_env_whitespace_bizdate_strict_errors(self):
        with mock.patch.dict(os.environ, {"bizdate": "   ", "SKYNET_BIZDATE": "20260928"}, clear=True):
            with self.assertRaises(SystemExit) as ctx:
                dates_mod.env_bizdate(strict=True)
            self.assertIn("空白", str(ctx.exception))
        args = make_args()
        with mock.patch.dict(os.environ, {"bizdate": "  "}, clear=True):
            with self.assertRaises(SystemExit):
                dates_mod.resolve_bizdate(args, strict=True)

    def test_resolve_bizdate_non_strict_falls_back_to_default(self):
        class _FixedDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return cls(2026, 9, 29, 10, 0, tzinfo=utils_mod.CN_TZ)

        args = make_args()
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

    def test_base_token_rejects_url_metachar(self):
        job = make_job()
        job["feishu"]["base_token"] = "IC/x"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)
        job = make_job()
        job["feishu"]["table_id"] = "tbl x"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)
        job = make_job()
        job["feishu"]["table_id"] = "tbl?x"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)
        job = make_job()
        job["feishu"]["base_token"] = "IC#x"
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)


class TestNormalizeDateMore(OfflineTestCase):
    def test_feishu_id_whitelist_rejects_path_tricks(self):
        """base_token/table_id 会拼进 URL 路径：黑名单挡不住 ".." 与 "%2F"，按白名单校验。"""
        for bad in ("..", "%2E%2E", "a/b", "a b", "tbl?x"):
            with self.assertRaises(SystemExit) as err:
                config_mod._require_feishu_id(bad, "feishu.base_token")
            self.assertIn("只允许", str(err.exception))
        self.assertEqual(config_mod._require_feishu_id("IC4TEST_tbl-1", "feishu.base_token"), "IC4TEST_tbl-1")

    def test_iso_with_millis_and_tz(self):
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27T15:30:00.123+08:00"), "2026-09-27")

    def test_iso_date_only(self):
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27"), "2026-09-27")

    def test_slash_single_digit_month(self):
        self.assertEqual(dates_mod.normalize_date_value("2026/9/7"), "2026-09-07")

    def test_yyyymmdd_sentinels_are_none(self):
        """8 位 yyyymmdd 分支同样受 2000~2100 约束：19700101/99991231 这类哨兵值
        不该冒充真实日期（否则新鲜度告警会显示"当前最新 9999-12-31"）。"""
        self.assertIsNone(dates_mod.normalize_date_value(19700101))
        self.assertIsNone(dates_mod.normalize_date_value(99991231))
        self.assertIsNone(dates_mod.normalize_date_value("19700101"))
        self.assertIsNone(dates_mod.normalize_date_value("99991231"))

    def test_freshness_expected_normalized(self):
        """expected 与 records 两侧同口径归一化：传 date 对象/紧凑串也不能恒判"缺数据"。"""
        records = [{"d": "2026-09-27"}]
        self.assertIsNone(dates_mod.freshness_problem(records, "d", date(2026, 9, 27)))
        self.assertIsNone(dates_mod.freshness_problem(records, "d", "20260927"))

    def test_tmp_writer_alive_non_posix_is_conservative(self):
        """非 POSIX 平台探测不了 pid：按"可能还在"处理（purge 保守跳过，不误删在途分区）。"""
        with mock.patch.object(mc_mod.os, "name", "nt"):
            self.assertTrue(mc_mod._pid_alive(424242))

    def test_freshness_mixed_input_does_not_crash(self):
        """形态判定不能只看首元素：None 占位/str 与 dict 混杂时要么正确提取、
        要么跳过异常元素，不能 TypeError 崩掉新鲜度校验。"""
        mixed = [None, {"d": "2026-09-27"}]
        self.assertIsNone(dates_mod.freshness_problem(mixed, "d", "2026-09-27"))
        # str 与 dict 混杂：两类元素都要参与比较（str 被整体丢弃会天天误报缺数据）
        mixed2 = ["2026-09-27", {"d": "2026-09-20"}]
        self.assertIsNone(dates_mod.freshness_problem(mixed2, "d", "2026-09-27"))

    def test_epoch_out_of_range_is_none(self):
        """换算结果必须落在 2000~2100：0 → 1970、14 位 yyyyMMddHHmmss 当毫秒 → 26xx 年，
        这类假日期会冒充"最新数据"，一律按认不出返回 None。"""
        self.assertIsNone(dates_mod.normalize_date_value(0))
        self.assertIsNone(dates_mod.normalize_date_value(-1))
        self.assertIsNone(dates_mod.normalize_date_value(20260927103000))  # 14 位 yyyymmddHHMMSS
        self.assertIsNone(dates_mod.normalize_date_value(202691))  # 6 位小整数不是 epoch 秒

    def test_yyyymmdd_integer_is_parsed_as_date(self):
        """8 位整数（20260927）按 yyyymmdd 解读，不能当 epoch 秒静默算成 1970 年。"""
        self.assertEqual(dates_mod.normalize_date_value(20260927), "2026-09-27")
        self.assertIsNone(dates_mod.normalize_date_value(20261340))  # 非法日历 → 认不出

    def test_epoch_seconds_vs_millis(self):
        """秒级时间戳不能被当成毫秒解析成 1970 年；毫秒（飞书日期字段）照常。"""
        from datetime import datetime as _dt

        from feishu2ods.utils import CN_TZ

        # 2025-09-26 12:00:00 +08:00
        seconds = int(_dt(2025, 9, 26, 12, 0, tzinfo=CN_TZ).timestamp())
        millis = seconds * 1000
        self.assertEqual(dates_mod.normalize_date_value(seconds), "2025-09-26")
        self.assertEqual(dates_mod.normalize_date_value(millis), "2025-09-26")

    def test_float_value(self):
        self.assertIsNone(dates_mod.normalize_date_value(0.0))  # 与整数 0 同口径：不造 1970 假日期

    def test_bool_rejected(self):
        self.assertIsNone(dates_mod.normalize_date_value(True))
        self.assertIsNone(dates_mod.normalize_date_value(False))

    def test_huge_number_rejected(self):
        self.assertIsNone(dates_mod.normalize_date_value(10**30))

    def test_garbage_text(self):
        for bad in ("", "abc", "2026年9月27日", "27/09/2026", "2026.9.27"):
            self.assertIsNone(dates_mod.normalize_date_value(bad))

    def test_unpadded_dash_date_is_parsed(self):
        """未补零的横杠写法（2026-9-7）与斜杠写法同口径：源端手填的日期列常见，
        不认会让新鲜度校验天天误报缺数据。"""
        self.assertEqual(dates_mod.normalize_date_value("2026-9-7"), "2026-09-07")
        self.assertEqual(dates_mod.normalize_date_value("2026-9-27"), "2026-09-27")

    def test_whitespace_trimmed(self):
        self.assertEqual(dates_mod.normalize_date_value(" 2026-09-27 "), "2026-09-27")

    def test_invalid_calendar_dates_rejected(self):
        self.assertIsNone(dates_mod.normalize_date_value("2026-02-31"))
        self.assertIsNone(dates_mod.normalize_date_value("2026-13-01"))
        self.assertIsNone(dates_mod.normalize_date_value("2026/9/31"))


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
    def test_unsupported_filesystem_fails_closed_by_default(self):
        """文件系统不支持锁（NFS/只读挂载，ENOLCK）时默认拒绝执行（fail-closed）：
        静默无锁继续会让两个实例并发写同一作业/表、数据被静默覆盖；
        显式 FEISHU2ODS_ALLOW_NO_LOCK=1 才接受无互斥风险继续。"""

        class FakeFcntl:
            LOCK_EX, LOCK_NB, LOCK_UN = 2, 4, 8

            @staticmethod
            def flock(fh, flags):
                raise OSError(errno.ENOLCK, "No locks available")

        utils_mod.reset_lock_warning()
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            with (
                mock.patch.object(utils_mod, "fcntl", FakeFcntl),
                mock.patch.dict(os.environ, {}, clear=True),
            ):
                with self.assertRaises(SystemExit) as ctx:
                    utils_mod.RunLock(path).__enter__()
            self.assertIn("ALLOW_NO_LOCK", str(ctx.exception))
            logs: list = []
            utils_mod.reset_lock_warning()
            with (
                mock.patch.object(utils_mod, "fcntl", FakeFcntl),
                mock.patch.dict(os.environ, {"FEISHU2ODS_ALLOW_NO_LOCK": "1"}, clear=True),
                mock.patch.object(utils_mod, "log", side_effect=lambda msg: logs.append(str(msg))),
            ):
                with utils_mod.RunLock(path):
                    pass
            self.assertTrue(any("无互斥风险" in line for line in logs), logs)

    def test_busy_lock_still_reports_running_task(self):
        """加锁失败是"忙"（EAGAIN）时仍旧报"已有任务在运行"：不能把真并发放过去。"""

        class FakeFcntl:
            LOCK_EX, LOCK_NB, LOCK_UN = 2, 4, 8

            @staticmethod
            def flock(fh, flags):
                raise OSError(errno.EAGAIN, "Resource temporarily unavailable")

        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            with mock.patch.object(utils_mod, "fcntl", FakeFcntl):
                with self.assertRaises(SystemExit) as ctx:
                    with utils_mod.RunLock(path):
                        pass
        self.assertIn("已有任务在运行", str(ctx.exception))

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

    def test_table_lock_shared_by_jobs_writing_same_table(self):
        """表级锁：两份配置写同一张表必须撞同一把锁（不同表互不影响），
        且持有期间第二个实例拿不到、释放后可再拿。"""
        with tempfile.TemporaryDirectory() as tmp:
            base = pathlib.Path(tmp)
            a = utils_mod.table_lock_path("p", "ods_x", root=base)
            b = utils_mod.table_lock_path("p", "ods_x", root=base)
            c = utils_mod.table_lock_path("p", "ods_y", root=base)
            self.assertEqual(a, b)
            self.assertNotEqual(a, c)
            self.assertIn("ods_x", a.name)
            with utils_mod.RunLock(a):
                with self.assertRaises(SystemExit):
                    utils_mod.RunLock(a).__enter__()
            with utils_mod.RunLock(a):
                pass  # 释放后可以再拿

    def test_lock_path_probe_unlink_failure_keeps_candidate(self):
        # 探测文件删不掉（少见）不该把整个目录判成不可用而换目录——那会让同一作业的
        # 两个实例锁在不同路径上，互斥失效。这里模拟 unlink 失败，断言仍选第一个候选目录。
        with tempfile.TemporaryDirectory() as tmp:
            job = pathlib.Path(tmp) / "x.json"
            normal = cli_mod.lock_path(job)
            with mock.patch.object(utils_mod.os, "unlink", side_effect=OSError("busy")):
                degraded = cli_mod.lock_path(job)
            self.assertEqual(degraded, normal)
        probe_dir = normal.parent
        for probe in probe_dir.glob(".probe-*"):
            try:
                probe.unlink()
            except OSError:
                pass

    def test_lock_path_resolves_before_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            real = pathlib.Path(tmp) / "demo.json"
            real.write_text("{}", encoding="utf-8")
            via_dot = pathlib.Path(tmp) / "." / "demo.json"
            self.assertEqual(cli_mod.lock_path(via_dot).name, cli_mod.lock_path(real).name)

    def test_lock_open_unicode_error_is_systemexit(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            err = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "bad")
            with mock.patch("builtins.open", side_effect=err):
                with self.assertRaises(SystemExit) as ctx:
                    utils_mod.RunLock(path).__enter__()
            self.assertIn("无法创建运行锁", str(ctx.exception))

    def test_lock_marker_write_failure_still_released(self):
        """拿到锁之后写标记失败（含非 OSError）仍要释放，否则下次运行会误判占用。"""
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            real_open = open

            def wrapping_open(*args, **kwargs):
                handle = real_open(*args, **kwargs)
                if pathlib.Path(args[0]) == path:

                    def boom(_data):
                        raise UnicodeEncodeError("ascii", "主机", 0, 1, "strict")

                    handle.write = boom
                return handle

            with mock.patch("builtins.open", wrapping_open):
                with utils_mod.RunLock(path):
                    pass
            with utils_mod.RunLock(path):
                pass

    def test_lock_post_acquire_error_releases(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "x.lock"
            real_open = open

            def wrapping_open(*args, **kwargs):
                handle = real_open(*args, **kwargs)
                if pathlib.Path(args[0]) == path:

                    def boom(_data):
                        raise RuntimeError("marker boom")

                    handle.write = boom
                return handle

            with mock.patch("builtins.open", wrapping_open):
                with self.assertRaises(RuntimeError):
                    utils_mod.RunLock(path).__enter__()
            with utils_mod.RunLock(path):
                pass

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
            # 脱敏表现在只由 utils 持有（cli 经 reset_secret_values 操作它）
            shared: list[str] = []
            with (
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
        return make_args(
            skip_freshness=True, no_notify=True, dry_run=False, sql_timeout=mc_mod.SQL_TIMEOUT_SECONDS, force=force
        )

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


class TestReviewHardening(OfflineTestCase):
    """本轮线上巡检评审修复的回归用例。"""

    # ------------------------------------------------------------- mc：SQL / 临时 spool

    def test_partition_clause_whitelist(self):
        """拼进 SQL 的分区值必须过白名单（注入形态一律拒绝），且不发起任何查询。"""
        o = mock.Mock()
        for bad in ("2026-09-28", "20260928'; drop table t --", "", "202609281", "２０２６０９２８"):
            with self.assertRaises(SystemExit):
                mc_mod.count_partition(o, "p", "t", bad)
        o.run_sql.assert_not_called()
        # 核对路径同样校验（临时分区带 __tmp 后缀是允许的）
        with self.assertRaises(SystemExit):
            mc_mod.verify_partition(o, "p", "t", "json", "x'")
        o.run_sql.assert_not_called()

    def test_verify_partition_allows_tmp_suffix(self):
        o = mock.Mock()
        instance = mock.Mock()
        instance.open_reader.return_value.__enter__ = mock.Mock(
            return_value=iter([{"cnt": 0, "ucnt": 0, "mn": None, "mx": None}])
        )
        instance.open_reader.return_value.__exit__ = mock.Mock(return_value=False)
        o.run_sql.return_value = instance
        got = mc_mod.verify_partition(o, "p", "t", "json", "20260928__tmp")
        self.assertEqual(got, (0, 0, "", ""))
        self.assertIn("pt = '20260928__tmp'", o.run_sql.call_args.args[0])

    def test_write_partition_rejects_bad_pt(self):
        with self.assertRaises(SystemExit) as ctx:
            mc_mod.write_partition(mock.Mock(), mock.Mock(), "p", "t", "json", "2026-09-28", [])
        self.assertIn("8 位业务日", str(ctx.exception))

    def test_ddl_comment_escapes_backslash_and_quote(self):
        """表注释里反斜杠是转义符：结尾的 "\\" 会吃掉收尾引号、把后续当 SQL 解析。"""
        ddl = mc_mod.build_ddl("p", "t", "json", "尾\\")
        self.assertIn("尾\\\\", ddl)
        ddl2 = mc_mod.build_ddl("p", "t", "json", "it's")
        self.assertIn("it''s", ddl2)

    def test_ddl_and_partition_reject_bad_identifiers(self):
        """库入口自己校验标识符，不依赖 validate_job。"""
        bad = "p;drop"
        with self.assertRaises(SystemExit) as ctx:
            mc_mod.build_ddl(bad, "t", "json", "c")
        self.assertIn("标识符", str(ctx.exception))
        o = mock.Mock()
        for fn in (
            lambda: mc_mod.ensure_table(o, bad, "t", "json", "c"),
            lambda: mc_mod.drop_partition(o, bad, "t", "pt=20260928"),
            lambda: mc_mod.add_partition(o, "p", "t-1", "pt=20260928"),
            lambda: mc_mod.rename_partition(o, "p", "t;x", "pt=20260928__tmp", "pt=20260928"),
        ):
            with self.assertRaises(SystemExit):
                fn()
        o.run_sql.assert_not_called()

    def test_sql_spec_escapes_server_side_value(self):
        """purge 场景的 spec 来自服务端 partition.name：转义不能少。"""
        self.assertEqual(mc_mod._sql_spec("pt=20260928__tmp"), "pt='20260928__tmp'")
        self.assertEqual(mc_mod._sql_spec("pt=a'b"), "pt='a''b'")
        self.assertEqual(mc_mod._sql_spec("pt=a\\b"), "pt='a\\\\b'")

    def test_compat_branch_cleans_temp_spool_on_write_failure(self):
        """旧签名（传记录列表）的临时 spool 在写入失败时也要被清理（句柄+文件）。"""
        made = []
        real_init = spool_mod.SpoolWriter.__init__

        def spy_init(instance, path=None):
            real_init(instance, path)
            made.append(instance.path)

        with (
            mock.patch.object(spool_mod.SpoolWriter, "__init__", spy_init),
            mock.patch.object(spool_mod.SpoolWriter, "write_records", side_effect=OSError("disk full")),
        ):
            with self.assertRaises(OSError):
                mc_mod.write_partition(mock.Mock(), mock.Mock(), "p", "t", "json", "20260928", [{"record_id": "r1"}])
        self.assertEqual(len(made), 1)
        self.assertFalse(made[0].exists(), "兼容分支写入失败后临时 spool 未被清理")

    def test_timeout_stop_failure_is_logged(self):
        """取消失败要留痕：运维看到"已主动停止"会以为云端 SQL 真的停了。"""

        class Hanging:
            def is_successful(self):
                return False

            def is_terminated(self):
                return False

            def stop(self):
                raise RuntimeError("stop failed")

        o = mock.Mock()
        o.run_sql.return_value = Hanging()
        logs = []
        with mock.patch.object(mc_mod, "log", logs.append):
            with self.assertRaises(TimeoutError):
                mc_mod.run_sql_with_timeout(o, "select 1", timeout=0.001)
        self.assertTrue(any("取消失败" in str(line) for line in logs), logs)

    # ------------------------------------------------------------- fetch：流式契约 / 防死循环

    def test_sink_stats_must_be_paired(self):
        """只传 sink 或只传 stats：静默退化成全量模式（不落盘、返回类型也变），必须快速失败。"""
        with self.assertRaises(TypeError):
            cli_mod.fetch_records({}, {}, sink=object())
        with self.assertRaises(TypeError):
            cli_mod.fetch_records({}, {}, stats=object())

    def test_identical_page_repeated_is_detected(self):
        """接口忽略 offset 时会重复返回同一页：页级指纹命中即中止，不能白翻到上限。"""
        feishu = {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx", "table_id": "tblx"}
        page = {
            "code": 0,
            "data": {"fields": ["日期"], "data": [["2026-09-27"]], "record_id_list": ["r1"], "has_more": True},
        }
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=[page, page]),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.fetch_records(feishu, {"日期": "biz_date"})
        self.assertIn("完全相同", str(ctx.exception))

    def test_adjacent_page_overlap_is_detected(self):
        feishu = {"app_id": "cli_x", "app_secret": "s" * 8, "base_token": "ICx", "table_id": "tblx"}
        pages = [
            {
                "code": 0,
                "data": {"fields": ["日期"], "data": [["a"], ["b"]], "record_id_list": ["r1", "r2"], "has_more": True},
            },
            {
                "code": 0,
                "data": {"fields": ["日期"], "data": [["c"], ["d"]], "record_id_list": ["r2", "r3"], "has_more": False},
            },
        ]
        with (
            mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
            mock.patch.object(fetch_mod, "request_json", side_effect=pages),
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.fetch_records(feishu, {"日期": "biz_date"})
        self.assertIn("与上一页", str(ctx.exception))

    # ------------------------------------------------------------- config：写回 / 告警

    def test_fields_source_names_are_stripped_and_written_back(self):
        job = make_job()
        job["fields"] = {" 日期 ": "biz_date", "金额": "amount"}
        config_mod.validate_job(job)
        self.assertEqual(list(job["fields"]), ["日期", "金额"])
        # 去空白后重名要报错（不能静默把两个源列折叠成一个）
        job2 = make_job()
        job2["fields"] = {"日期": "biz_date", " 日期 ": "amount"}
        with self.assertRaises(SystemExit) as ctx:
            config_mod.validate_job(job2)
        self.assertIn("重复", str(ctx.exception))

    def test_comment_is_stripped_and_newline_rejected(self):
        job = make_job()
        job["target"]["comment"] = "  注释  "
        config_mod.validate_job(job)
        self.assertEqual(job["target"]["comment"], "注释")
        job2 = make_job()
        job2["target"]["comment"] = "两行\n注释"
        with self.assertRaises(SystemExit) as ctx:
            config_mod.validate_job(job2)
        self.assertIn("换行", str(ctx.exception))

    def test_plaintext_http_warns(self):
        """http:// 不阻断（本地调试可用），但必须留一条明文传输告警。"""
        job = make_job()
        job["feishu"]["base_url"] = "http://127.0.0.1:8000/base/ICx"
        job["maxcompute"]["endpoint"] = "http://service.example/api"
        job["freshness"]["webhook"] = "http://open.feishu.cn/hook/1"
        warnings = config_mod.validate_job(job)
        text = "\n".join(warnings)
        self.assertIn("base_url", text)
        self.assertIn("endpoint", text)
        self.assertIn("webhook", text)

    # ------------------------------------------------------------- dates / cli / spool

    def test_freshness_problem_accepts_generator(self):
        """迭代器入参不能被"看一眼元素类型"消费掉第一条（它的日期漏比较会误报缺数据）。"""
        gen = ({"d": "2026-09-27"} for _ in range(1))
        self.assertIsNone(dates_mod.freshness_problem(gen, "d", "2026-09-27"))
        self.assertEqual(
            dates_mod.freshness_problem(iter([{"d": "2026-09-26"}]), "d", "2026-09-27"),
            ("2026-09-27", "2026-09-26"),
        )

    def test_freshness_missing_date_field_is_clean_error(self):
        job = make_job()
        job["freshness"] = {"lag_days": 0}
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub([])):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.run_sync(make_args(), job, "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertIn("date_field", str(ctx.exception))

    def test_freshness_non_object_is_clean_error(self):
        job = make_job()
        job["freshness"] = "yes"
        with mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub([])):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.run_sync(make_args(), job, "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertIn("freshness 必须是对象", str(ctx.exception))

    def test_lag_days_zero_is_not_treated_as_missing(self):
        job = make_job()
        job["freshness"] = {"date_field": "biz_date", "lag_days": 0}
        records = [{"record_id": "r1", "biz_date": "2026-09-26"}]
        messages: list[str] = []
        args = make_args(skip_freshness=False, no_notify=True, dry_run=True)
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "log", side_effect=lambda m: messages.append(str(m))),
        ):
            code = cli_mod.run_sync(args, job, "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        self.assertTrue(any("晚一天出数" in m for m in messages), messages)
        self.assertFalse(any("业务日 - 0 天" in m for m in messages), messages)

    def test_lag_days_must_be_non_negative_int(self):
        job = make_job()
        job["freshness"] = {"date_field": "biz_date", "lag_days": "1"}
        with mock.patch.object(
            cli_mod, "fetch_records", side_effect=fetch_stub([{"record_id": "r1", "biz_date": "2026-09-28"}])
        ):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.run_sync(make_args(), job, "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertIn("lag_days", str(ctx.exception))

    def test_int_exit_code_is_logged(self):
        """运行期抛出的 int SystemExit 必须留痕（原来既不打印也不写日志，--log-file 里一字没有）。"""
        logs = []
        with (
            mock.patch.object(cli_mod, "_run", side_effect=SystemExit(3)),
            mock.patch.object(cli_mod, "log", logs.append),
        ):
            rc = cli_mod.main([])
        self.assertEqual(rc, 3)
        self.assertTrue(any("以退出码 3 结束" in str(line) for line in logs), logs)

    def test_temp_file_cleaned_when_open_fails(self):
        """mkstemp 建了文件、随后 open 失败（句柄用尽/磁盘满）时不能把临时文件留在 temp。"""
        made = []
        real_mkstemp = spool_mod.tempfile.mkstemp

        def spy_mkstemp(*args, **kwargs):
            handle, name = real_mkstemp(*args, **kwargs)
            made.append(pathlib.Path(name))
            return handle, name

        with (
            mock.patch.object(spool_mod.tempfile, "mkstemp", side_effect=spy_mkstemp),
            mock.patch.object(spool_mod, "open", side_effect=OSError("too many open files"), create=True),
        ):
            with self.assertRaises(OSError):
                spool_mod.SpoolWriter()
        self.assertEqual(len(made), 1)
        self.assertFalse(made[0].exists())

    # ------------------------------------------------------------- utils / wizard

    def test_url_auth_regex_stays_linear(self):
        """_URL_AUTH_RE 的 scheme 部分必须限长：无上限时在长小写字母数字串上会 O(n²)。"""

        def elapsed(kb: int) -> float:
            n = kb * 1024
            text = "a" * n + "://" + "b" * n + "@"
            started = time.perf_counter()
            out = utils_mod.redact(text)
            self.assertEqual(out, text)
            return time.perf_counter() - started

        small, big = elapsed(4), elapsed(16)
        self.assertLess(big, max(small * 8, 0.5), f"脱敏耗时 {small:.3f}s → {big:.3f}s，疑似 O(n²)")

    def test_wizard_endpoint_is_asked_and_used(self):
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {}

        answers = iter(
            [
                "ep_job",  # 作业名
                "IC4TEST",  # base_token
                "tblTEST",  # table_id
                "cli_test123",  # app_id
                "secret-abc-123",  # app_secret
                "biz_date",  # 日期 → 英文键
                "",  # 项目（默认）
                "",  # 表名（默认）
                "",  # 注释（默认）
                "LTAI_TEST",  # AK
                "SK_SECRET_XYZ",  # SK
                "http://mc.example/api",  # endpoint（非默认值，必须写进配置）
                "n",  # 不要新鲜度
            ]
        )
        with tempfile.TemporaryDirectory() as tmp:
            code = wizard_mod.run_init(
                out_path="",
                ask=lambda prompt="": next(answers, ""),
                echo=lambda *a: None,
                workdir=pathlib.Path(tmp),
                fetch_fields=fake_fetch,
                ask_secret=lambda prompt="": next(answers, ""),
            )
            self.assertEqual(code, 0)
            job = json.loads((pathlib.Path(tmp) / "jobs" / "ep_job.json").read_text(encoding="utf-8"))
        self.assertEqual(job["maxcompute"]["endpoint"], "http://mc.example/api")

    def test_wizard_file_is_created_with_600(self):
        """生成的文件含明文密钥：写临时文件再 replace，不 O_TRUNC 截断正在用的作业文件。"""
        fields = ["日期"]

        def fake_fetch(feishu):
            return fields, {}

        answers = iter(
            ["w6_job", "IC4TEST", "tblTEST", "cli_test123", "s" * 10, "biz_date", "", "", "", "LTAI", "SK", "", "n"]
        )
        fake_os = mock.Mock(wraps=wizard_mod.os)
        fake_os.name = "posix"
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "w6.json"
            with mock.patch.object(wizard_mod, "os", fake_os):
                code = wizard_mod.run_init(
                    out_path=str(out),
                    ask=lambda prompt="": next(answers, ""),
                    echo=lambda *a: None,
                    workdir=pathlib.Path(tmp),
                    fetch_fields=fake_fetch,
                    ask_secret=lambda prompt="": next(answers, ""),
                )
            self.assertEqual(code, 0)
            self.assertTrue(out.is_file())
        live_opens = [call for call in fake_os.open.call_args_list if call.args and os.fspath(call.args[0]) == str(out)]
        self.assertEqual(live_opens, [])  # 不得 O_TRUNC 打开正在用的作业文件
        self.assertTrue(fake_os.replace.called)
        chmod_calls = [call for call in fake_os.chmod.call_args_list if os.fspath(call.args[0]) == str(out)]
        self.assertTrue(chmod_calls)
        self.assertEqual(chmod_calls[-1].args[1], 0o600)

    def test_wizard_interrupt_cleans_tmp_file(self):
        """Ctrl+C 落在写盘途中（fsync）：含明文密钥的临时文件必须清掉，
        向导的"已取消，未生成任何文件"才属实（原来只接 OSError，中断会留下 .tmp）。"""

        def fake_fetch(feishu):
            return ["日期"], {}

        answers = iter(
            ["w7_job", "IC4TEST", "tblTEST", "cli_test123", "s" * 10, "biz_date", "", "", "", "LTAI", "SK", "", "n"]
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = pathlib.Path(tmp) / "w7.json"
            with mock.patch.object(wizard_mod.os, "fsync", side_effect=KeyboardInterrupt):
                # run_init 不接 KeyboardInterrupt（由 cli.main 统一收口报 130）：
                # 这里断言的是"临时文件在异常上抛前已被清掉"
                with self.assertRaises(KeyboardInterrupt):
                    wizard_mod.run_init(
                        out_path=str(out),
                        ask=lambda prompt="": next(answers, ""),
                        echo=lambda *a: None,
                        workdir=pathlib.Path(tmp),
                        fetch_fields=fake_fetch,
                        ask_secret=lambda prompt="": next(answers, ""),
                    )
            self.assertFalse(out.exists())
            self.assertEqual(list(pathlib.Path(tmp).glob(".w7.json.*.tmp")), [])

    # ------------------------------------------------- 复核第二轮：裁判确认的缺陷

    def test_spaced_sensitive_value_masked_to_eol(self):
        """口令短语含空格（password=my secret）不能被第一个词截断：敏感键的值遮到行尾。"""
        out = utils_mod.redact("login failed: password=my secret and more")
        self.assertNotIn("secret", out)
        self.assertNotIn("more", out)
        self.assertEqual(utils_mod.redact("Invalid token: *** / ***"), "Invalid token: *** / ***")
        # 未闭合引号（日志截断）：KV/JSON 要收尾引号、常规 QUERY 不吃引号，必须走
        # 这条兜底，否则三套规则全绕过、明文泄露
        out = utils_mod.redact('password="abc123456')
        self.assertNotIn("abc123456", out)
        out = utils_mod.redact("password='abc123456")
        self.assertNotIn("abc123456", out)
        # 「带引号的键」+ 不带引号的值：KV/JSON 都不收，必须走 SPACE 兜底
        out = utils_mod.redact('"password": my secret')
        self.assertNotIn("secret", out)

    def test_url_userinfo_password_with_at_sign(self):
        """userinfo 口令含 @（proxy 场景）要按最后一个 @ 切分：余段不能明文留下。"""
        out = utils_mod.redact("HTTPS_PROXY=https://user:p@ss@proxy:8080")
        self.assertNotIn("p@ss", out)
        self.assertNotIn("ss@proxy", out)
        self.assertIn("user:***@", out)

    def test_api_err_is_redacted(self):
        """_api_err 的返回值会被调用方拼进 SystemExit（可能直出 stderr）：必须过 redact，
        网关错误页/msg 里回显的 user:pass@host、access_token=xxx 不能明文外泄。"""
        out = utils_mod._api_err("err at https://user:pw123456@proxy/x?access_token=tok123456")
        self.assertNotIn("pw123456", out)
        self.assertNotIn("tok123456", out)
        out = utils_mod._api_err({"code": 1, "msg": "bad access_token=tok123456"})
        self.assertNotIn("tok123456", out)
        # 先脱敏再截断：长文本在 200 字符处截断会把成对引号截坏，规则失配导致泄露
        long_text = "x" * 180 + ' password="SECRETVALUE123456"'
        out = utils_mod._api_err(long_text)
        self.assertNotIn("SECRETVALUE123456", out)

    def test_redact_quoted_value_after_key(self):
        """!r 插值/repr 形态（access_token='t-xxx'，行中）必须遮：query 规则的值部分不吃引号。"""
        out = utils_mod.redact("拉取失败 access_token='t-g1045abc123456' url=https://x")
        self.assertNotIn("t-g1045abc123456", out)
        self.assertIn("access_token='***'", out)
        out = utils_mod.redact('fail: code=1, token: "t-g1045abc123456"')
        self.assertNotIn("t-g1045abc123456", out)
        # 键名不敏感、值里再嵌 k=v 的也要递归兜住
        out = utils_mod.redact("note: 'access_token=abc123456'")
        self.assertNotIn("abc123456", out)

    def test_lock_dir_env_override_pins_path(self):
        """FEISHU2ODS_LOCK_DIR 把锁钉在固定目录：跨身份/跨 TMPDIR 也拿同一把锁。"""
        with tempfile.TemporaryDirectory() as tmp:
            job = pathlib.Path(tmp) / "x.json"
            custom = pathlib.Path(tmp) / "locks"
            with mock.patch.dict(os.environ, {"FEISHU2ODS_LOCK_DIR": str(custom)}):
                got = cli_mod.lock_path(job)
            self.assertEqual(got.parent, custom)
            self.assertTrue(custom.is_dir())
            self.assertNotEqual(got.parent, cli_mod.lock_path(job).parent)

    def test_lock_dir_env_override_unusable_fails_loudly(self):
        """显式指定的锁目录不可用要立刻失败，不能静默换目录（那正是互斥失效的来源）。"""
        with tempfile.TemporaryDirectory() as tmp:
            job = pathlib.Path(tmp) / "x.json"
            with (
                mock.patch.dict(os.environ, {"FEISHU2ODS_LOCK_DIR": str(pathlib.Path(tmp) / "ro")}),
                mock.patch.object(pathlib.Path, "mkdir", side_effect=OSError("read-only")),
            ):
                with self.assertRaises(SystemExit) as ctx:
                    cli_mod.lock_path(job)
            self.assertIn("FEISHU2ODS_LOCK_DIR", str(ctx.exception))

    def test_tmp_writer_alive_parses_run_id(self):
        """同机 + pid 存活 = 在途；别机/旧命名/自己的都按残留处理。"""
        self.assertFalse(mc_mod._tmp_writer_alive("20260927__tmp"))
        self.assertFalse(mc_mod._tmp_writer_alive("20260927__tmp_otherhost_1"))
        self.assertFalse(mc_mod._tmp_writer_alive("20260927__tmp_hostx"))
        self.assertFalse(mc_mod._tmp_writer_alive(f"20260927__tmp_{mc_mod._TMP_HOST}_{os.getpid()}"))
        alive = f"20260927__tmp_{mc_mod._TMP_HOST}_424242"
        with mock.patch.object(mc_mod, "_pid_alive", return_value=True) as probe:
            self.assertTrue(mc_mod._tmp_writer_alive(alive))
            probe.assert_called_once_with(424242)
        with mock.patch.object(mc_mod, "_pid_alive", return_value=False):
            self.assertFalse(mc_mod._tmp_writer_alive(alive))

    def test_purge_cleans_redundant_keep_after_recovery(self):
        """正式分区已恢复（同 pt 存在）后 __keep 是冗余的：字符串序大于正式分区，
        不清理会让下游 max_pt() 永远读到旧快照；正式分区还没回来的才必须保留。"""
        table = _FakeTable()
        table.partitions = [_Part("pt='20260927'"), _Part("pt='20260927__keep'"), _Part("pt='20260928__keep'")]
        odps = _FakeOdps([])
        cli_mod.purge_stale_tmp_partitions(odps, table, "p", "t")
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260927__keep')", sqls)
        self.assertNotIn("20260928__keep", sqls)

    def test_purge_skips_in_flight_tmp(self):
        """同机另一 job 进程还活着时，它的在途 tmp 分区不能被当残留清掉。"""
        table = _FakeTable()
        inflight = f"pt='20260927__tmp_{mc_mod._TMP_HOST}_424242'"
        stale = "pt='20260926__tmp_otherhost_1'"
        table.partitions = [_Part(inflight), _Part(stale)]
        odps = _FakeOdps([])
        messages: list[str] = []
        with (
            mock.patch.object(mc_mod, "_tmp_writer_alive", side_effect=lambda v: v.startswith("20260927")),
            mock.patch.object(mc_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.purge_stale_tmp_partitions(odps, table, "p", "t")
        sqls = "\n".join(odps.sqls)
        self.assertNotIn("20260927__tmp_", sqls)
        self.assertIn("20260926__tmp_otherhost_1", sqls)
        self.assertTrue(any("在途" in message for message in messages))

    @unittest.skipUnless(os.name == "posix", "os.kill(pid, 0) 探测只在 POSIX 有效")
    def test_pid_alive_probe_mapping(self):
        self.assertTrue(mc_mod._pid_alive(os.getpid()))
        with mock.patch.object(mc_mod.os, "kill", side_effect=ProcessLookupError):
            self.assertFalse(mc_mod._pid_alive(1))
        with mock.patch.object(mc_mod.os, "kill", side_effect=PermissionError):
            self.assertTrue(mc_mod._pid_alive(1))

    def test_split_base_ref_keeps_dash_and_underscore(self):
        """token 可含 - _（config._FEISHU_ID_RE 的口径）：解析不能截断出「合法但错误」的值。"""
        base, table = wizard_mod._split_base_ref("https://x.feishu.cn/base/IC4abc-def_X?table=tblAbc-Def_123")
        self.assertEqual(base, "IC4abc-def_X")
        self.assertEqual(table, "tblAbc-Def_123")

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_notify_requires_explicit_success_code(self):
        """缺 code/StatusCode 的 200 响应不能算「已发送」（误填地址会静默失效）。"""

        def run(payload):
            resp = mock.Mock(status_code=200)
            resp.json.return_value = payload
            messages: list[str] = []
            with (
                mock.patch.object(notify_mod.requests, "post", return_value=resp),
                mock.patch.object(notify_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
            ):
                cli_mod.notify("https://x/hook/1", "t", ["l"])
            return "\n".join(messages)

        self.assertIn("已发送", run({"code": 0}))
        self.assertIn("已发送", run({"StatusCode": 0}))
        self.assertIn("响应为空", run({}))  # 少数网关只回 {}：保留 200 宽容但说明依据
        failed = run({"msg": "ok"})
        self.assertIn("通知发送失败", failed)
        self.assertNotIn("已发送", failed)

    def test_yyyymmdd_text_is_parsed_as_date(self):
        """文本列的 20260927 与数字同口径；非法日历仍认不出。"""
        self.assertEqual(dates_mod.normalize_date_value("20260927"), "2026-09-27")
        self.assertEqual(dates_mod.normalize_date_value(" 20260927 "), "2026-09-27")
        self.assertIsNone(dates_mod.normalize_date_value("20261340"))

    def test_dotted_date_text_is_parsed(self):
        """点分写法（2026.09.27）仍按"认不出"处理（形态不明确不猜；横杠/斜杠才认）。

        真要支持的话得放开 DATE_RE 的分隔符——那是口径决定，先在这里把现状钉住。
        """
        self.assertIsNone(dates_mod.normalize_date_value("2026.09.27"))

    def test_close_failure_keeps_spool_file(self):
        """close() 失败（磁盘满刷盘失败）时保留落盘文件：那是唯一副本，不能在 finally 里删掉。"""
        spool = spool_mod.SpoolWriter()
        spool.write_records([{"record_id": "r1"}])
        path = pathlib.Path(spool.path)
        real_close = spool._handle.close

        def failing_close():
            real_close()
            raise OSError("No space left")

        messages: list[str] = []
        with (
            mock.patch.object(spool._handle, "close", side_effect=failing_close),
            mock.patch.object(spool_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            with self.assertRaises(OSError):
                spool.close()
        try:
            self.assertTrue(path.exists())
            self.assertTrue(any("已保留" in message for message in messages))
        finally:
            path.unlink()


if __name__ == "__main__":
    unittest.main()
