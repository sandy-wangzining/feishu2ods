# -*- coding: utf-8 -*-
"""飞书拉取、分页、统计与落盘。"""

from __future__ import annotations

import pathlib
import sys
import tempfile
import time
import unittest
from collections import Counter
from datetime import date
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import fetch as fetch_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402
from feishu2ods import spool as spool_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
    _FakeResponse,
    make_args,
    make_job,
)


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


class TestFetchRecords(OfflineTestCase):
    def test_null_or_empty_record_id_rejected_before_string_conversion(self):
        for record_id in (None, ""):
            for stream in (False, True):
                with self.subTest(record_id=record_id, stream=stream):
                    page = self._page([["2026-09-27", "$1"]], [record_id], False)
                    with (
                        mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
                        mock.patch.object(fetch_mod, "request_json", return_value=page),
                    ):
                        sink = mock.Mock()
                        kwargs = {"sink": sink, "stats": cli_mod.FetchStats()} if stream else {}
                        with self.assertRaisesRegex(SystemExit, "record_id"):
                            cli_mod.fetch_records(self.job_feishu, self.mapping, **kwargs)
                        sink.write_records.assert_not_called()

    def test_duplicate_ids_within_or_across_nonadjacent_pages(self):
        for ids_by_page in ((["r1", "r1"],), (["r1", "r2"], ["r3"], ["r2", "r4"])):
            for stream in (False, True):
                with self.subTest(ids_by_page=ids_by_page, stream=stream):
                    pages = [
                        self._page([["a", "b"] for _ in ids], ids, i < len(ids_by_page) - 1)
                        for i, ids in enumerate(ids_by_page)
                    ]
                    with (
                        cli_mod.SpoolWriter() as sink,
                        mock.patch.object(fetch_mod, "get_tenant_token", return_value="tok"),
                        mock.patch.object(fetch_mod, "request_json", side_effect=pages),
                    ):
                        kwargs = {"sink": sink, "stats": cli_mod.FetchStats()} if stream else {}
                        with self.assertRaisesRegex(SystemExit, "重复"):
                            cli_mod.fetch_records(self.job_feishu, self.mapping, **kwargs)

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
