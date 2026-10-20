# -*- coding: utf-8 -*-
"""历次代码审查回归用例。"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import tempfile
import time
import unittest
from datetime import date
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

from _helpers import (
    OfflineTestCase,
    _FakeOdps,
    _FakeTable,
    _Part,
    fetch_stub,
    make_args,
    make_job,
)


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

    def test_secret_prompt_eof_is_user_interrupt(self):
        """密钥提示处 Ctrl+D：按用户中断（130）处理，不能退回 input 再报成未预期错误。"""
        with mock.patch.object(wizard_mod.getpass, "getpass", side_effect=EOFError):
            with self.assertRaises(KeyboardInterrupt):
                wizard_mod._default_ask_secret("App Secret：")

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

    def test_tenant_access_token_is_registered_for_redaction(self):
        """运行时拿到的 token 不在配置里，必须补进值级脱敏表，裸回显时也要遮住。"""
        token = "t-abc123456789"
        with mock.patch.object(
            auth_mod, "request_json", return_value={"code": 0, "tenant_access_token": token, "expire": 7200}
        ):
            self.assertEqual(auth_mod.get_tenant_token("id", "secret"), token)
        self.assertNotIn(token, utils_mod.redact(f"gateway echoed {token}"))

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
        self.assertFalse(mc_mod._pid_alive(10**100))
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
