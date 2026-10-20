# -*- coding: utf-8 -*-
"""配置校验、字段映射与新鲜度。"""

from __future__ import annotations

import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import config as config_mod  # noqa: E402
from feishu2ods import dates as dates_mod  # noqa: E402
from feishu2ods import fetch as fetch_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
    make_job,
)


class TestValidateJob(OfflineTestCase):
    def test_valid_no_warnings(self):
        self.assertEqual(config_mod.validate_job(make_job()), [])

    def test_lag_days_upper_bound(self):
        """lag_days 过大时 date - timedelta 会 OverflowError：配置阶段就要拒绝。"""
        job = make_job()
        job["freshness"] = {"date_field": "biz_date", "lag_days": 100_000_000}
        with self.assertRaises(SystemExit) as ctx:
            config_mod.validate_job(job)
        self.assertIn("lag_days", str(ctx.exception))

    def test_require_text_does_not_echo_secret(self):
        """配置错误不能把密钥原值带进日志/告警。"""
        with self.assertRaises(SystemExit) as ctx:
            config_mod._require_text(["sk-SECRET-123"], "feishu.app_secret")
        self.assertNotIn("SECRET-123", str(ctx.exception))
        self.assertIn("list", str(ctx.exception))
        with self.assertRaises(SystemExit) as ctx:
            config_mod._require_text("   ", "feishu.app_secret")
        self.assertIn("空白", str(ctx.exception))

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

    def test_job_target_column_cannot_be_partition_column(self):
        for column in ("pt", "PT"):
            job = make_job()
            job["target"]["column"] = column
            with self.subTest(column=column), self.assertRaises(SystemExit) as ctx:
                config_mod.validate_job(job)
            self.assertIn("分区列", str(ctx.exception))

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

    def test_job_freshness_webhook_list(self):
        job = make_job()
        job["freshness"]["webhook"] = [" https://x/hook/1 ", "https://x/hook/2", "https://x/hook/1"]
        config_mod.validate_job(job)
        self.assertEqual(job["freshness"]["webhook"], ["https://x/hook/1", "https://x/hook/2"])

    def test_job_freshness_webhook_list_bad_item(self):
        job = make_job()
        job["freshness"]["webhook"] = ["https://x/hook/1", "not-a-url"]
        with self.assertRaises(SystemExit):
            config_mod.validate_job(job)

    def test_job_freshness_webhook_list_http_warns(self):
        job = make_job()
        job["freshness"]["webhook"] = ["http://x/hook/1"]
        warnings = config_mod.validate_job(job)
        self.assertTrue(any("http://" in w for w in warnings))

    def test_job_freshness_webhook_list_http_warns_once(self):
        """多个 http 地址只提示一次（去重靠标志位，不靠其它告警文本里是否含 http://）。"""
        job = make_job()
        job["freshness"]["webhook"] = ["http://x/hook/1", "http://y/hook/2"]
        warnings = config_mod.validate_job(job)
        self.assertEqual(sum(1 for w in warnings if "http://" in w), 1)

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
