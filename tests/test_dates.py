# -*- coding: utf-8 -*-
"""业务日、日期规范化与体检容错。"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
from datetime import date
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import config as config_mod  # noqa: E402
from feishu2ods import dates as dates_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
    make_args,
    make_job,
)


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
        args = make_args()
        with (
            mock.patch.dict(os.environ, {}, clear=True),
            mock.patch.object(dates_mod, "default_bizdate", return_value=date(2026, 9, 28)),
        ):
            self.assertEqual(dates_mod.resolve_bizdate(args), date(2026, 9, 28))

    def test_explicit_empty_bizdate_errors(self):
        """显式传空 --bizdate（调度脚本 `--bizdate "$pt"` 且 $pt 未定义）必须报错，
        不能按"未指定"静默回退成"昨天"写错分区。"""
        for empty in ("", "   "):
            args = make_args(bizdate=empty)
            with mock.patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(SystemExit):
                    dates_mod.resolve_bizdate(args)


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
        args = make_args()
        with (
            mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True),
            mock.patch.object(dates_mod, "default_bizdate", return_value=date(2026, 9, 28)),
        ):
            self.assertEqual(dates_mod.resolve_bizdate(args, strict=False), date(2026, 9, 28))


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
                mock.patch.object(dates_mod, "default_bizdate", return_value=date(2026, 9, 18)),
                mock.patch.object(cli_mod, "run_check", return_value=0) as check,
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path), "--check"]), 0)
            # --check 容错后应真的回落到默认业务日，而不是别的原因让 main 返回 0
            self.assertEqual(check.call_args.args[4], "20260918")

    def test_sync_still_fails_with_dirty_bizdate(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_path = self._job_file(tmp)
            logs = []
            with (
                mock.patch.dict(os.environ, {"bizdate": "bad-date"}, clear=True),
                mock.patch.object(cli_mod, "log", logs.append),
            ):
                self.assertEqual(cli_mod.main(["--job", str(job_path)]), 1)
            self.assertTrue(any("bizdate" in str(line) for line in logs), logs)


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

    def test_invalid_time_cannot_satisfy_freshness(self):
        for value in (
            "2026-09-27T25:61:00",
            "2026-09-27 99:99:99",
            "2026-09-27T12:60:00",
            "2026-09-27T12:30:60",
            "2026/9/27 25:00:00",
            "2026-09-27\tgarbage",
        ):
            with self.subTest(value=value):
                self.assertIsNone(dates_mod.normalize_date_value(value))
                self.assertEqual(
                    dates_mod.freshness_problem([{"d": value}], "d", "2026-09-27"),
                    ("2026-09-27", None),
                )

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

    def test_freshness_accepts_date_objects(self):
        """库调用方直接传 date 对象也要认（与 expected 侧支持 date 对称）。"""
        self.assertIsNone(dates_mod.freshness_problem([date(2026, 9, 27)], "d", date(2026, 9, 27)))

    def test_freshness_mixed_input_does_not_crash(self):
        """形态判定不能只看首元素：None 占位/str 与 dict 混杂时要么正确提取、
        要么跳过异常元素，不能 TypeError 崩掉新鲜度校验。"""
        mixed = [None, {"d": "2026-09-27"}]
        self.assertIsNone(dates_mod.freshness_problem(mixed, "d", "2026-09-27"))
        # str 与 dict 混杂：两类元素都要参与比较（str 被整体丢弃会天天误报缺数据）
        mixed2 = ["2026-09-27", {"d": "2026-09-20"}]
        self.assertIsNone(dates_mod.freshness_problem(mixed2, "d", "2026-09-27"))

    def test_trailing_garbage_not_parsed(self):
        """日期后还有多余字符不能当成该日期：2026-09-270 / 2026-09-27abc 不是 9 月 27 日。"""
        for bad in ("2026-09-270", "2026-09-27abc", "2026/9/270", "2026/9/27abc"):
            self.assertIsNone(dates_mod.normalize_date_value(bad))
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27 15:30:00"), "2026-09-27")
        self.assertEqual(dates_mod.normalize_date_value("2026-09-27T15:30:00"), "2026-09-27")

    def test_fullwidth_iso_day_arg_rejected(self):
        """--bizdate 的 ISO 分支也要只认 ASCII 数字（全角与紧凑写法同口径）。"""
        for bad in ("２０２６-０９-２７", "2026-０９-27"):
            with self.assertRaises(SystemExit):
                dates_mod.parse_day_arg(bad)

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
