# -*- coding: utf-8 -*-
"""命令行主流程、运行锁、空分区保护与退出码。"""

from __future__ import annotations

import errno
import json
import os
import pathlib
import sys
import tempfile
import time
from datetime import date
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import config as config_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402
from feishu2ods import utils as utils_mod  # noqa: E402
from feishu2ods import wizard as wizard_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
    _Col,
    _Table,
    fetch_stub,
    make_args,
    make_job,
)


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
            self.assertEqual(a, utils_mod.table_lock_path("P", "ODS_X", root=base))  # 大小写不敏感
            self.assertNotEqual(a, c)
            self.assertLess(len(a.name), 80)
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

    def test_lock_path_surrogate_bytes_do_not_crash(self):
        """路径含非 UTF-8 原始字节（surrogateescape）时锁名也要能算出来，不能启动即崩。"""
        path = cli_mod.lock_path(pathlib.Path("jobs") / "\udcff.json")
        self.assertTrue(path.name.endswith(".lock"))


class TestRunInit(OfflineTestCase):
    def test_fetch_failure_does_not_echo_secret_value(self):
        secret = "wizard-secret-abcdef"

        def failed(_feishu):
            raise RuntimeError(f"credential rejected: {secret}")

        code, echoed = self._run(["job", "https://x/base/ICx?table=tblx", "cli_x", secret], failed)
        self.assertEqual(code, 1)
        self.assertNotIn(secret, "\n".join(echoed))

    def test_http_4xx_gets_friendly_wizard_message(self):
        """4xx（应用没加协作者等）要走向导的友好提示，不能落到未预期错误+traceback。"""

        def forbidden(_feishu):
            raise auth_mod.ApiHttpError(403, "app not added as collaborator")

        code, echoed = self._run(["job", "https://x/base/ICx?table=tblx", "cli_x", "secret"], forbidden)
        self.assertEqual(code, 1)
        joined = "\n".join(echoed)
        self.assertIn("HTTP 403", joined)
        self.assertIn("请确认", joined)

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

    def test_run_sync_freshness_dry_run_does_not_notify(self):
        # dry-run 不写库，不能发"已照常写入"的卡片
        args = make_args(skip_freshness=False, no_notify=False, dry_run=True)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "notify") as notifier,
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_not_called()

    def test_run_sync_freshness_notifies_after_write(self):
        # 缺数据只告警不失败：写库并核对成功后才发"已照常写入"卡片
        args = make_args(skip_freshness=False, no_notify=False, dry_run=False)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]
        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fetch_stub(records)),
            mock.patch.object(cli_mod, "notify") as notifier,
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition") as writer,
            mock.patch.object(cli_mod, "count_partition", return_value=1),
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        writer.assert_called_once()
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
        args = make_args(skip_freshness=True, no_notify=False, dry_run=False)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            if extra_out is not None:
                extra_out.extend(["新列A", "新列A", "新列B"])
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch),
            mock.patch.object(cli_mod, "notify") as notifier,
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition"),
            mock.patch.object(cli_mod, "count_partition", return_value=1),
        ):
            code = cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        self.assertEqual(code, 0)
        notifier.assert_called_once()
        joined = "\n".join(notifier.call_args.args[2])
        self.assertIn("新增列", notifier.call_args.args[1])
        self.assertIn("新列A", joined)
        self.assertIn("新列B", joined)

    def test_run_sync_new_fields_dry_run_does_not_notify(self):
        """dry-run 不写库：新增列只记日志，不发"已同步"卡片。"""
        args = make_args(skip_freshness=True, no_notify=False, dry_run=True)
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

    def test_run_sync_write_failure_does_not_notify(self):
        """写库失败时不能发“已照常写入/已同步”卡片（新增列与缺数据都一样）。"""
        args = make_args(skip_freshness=False, no_notify=False, dry_run=False)
        records = [{"record_id": "r1", "biz_date": "2026-09-27", "amount": "$1"}]

        def fake_fetch(feishu, mapping, max_pages=None, extra_out=None, sink=None, stats=None):
            if extra_out is not None:
                extra_out.append("新列A")
            return fetch_stub(records)(feishu, mapping, max_pages, extra_out, sink, stats)

        with (
            mock.patch.object(cli_mod, "fetch_records", side_effect=fake_fetch),
            mock.patch.object(cli_mod, "notify") as notifier,
            mock.patch.object(cli_mod, "connect_odps", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "ensure_table", return_value=mock.Mock()),
            mock.patch.object(cli_mod, "verify_schema"),
            mock.patch.object(cli_mod, "purge_stale_tmp_partitions"),
            mock.patch.object(cli_mod, "write_partition", side_effect=RuntimeError("tunnel down")),
        ):
            with self.assertRaises(RuntimeError):
                cli_mod.run_sync(args, make_job(), "p", "t", "json", "20260928", date(2026, 9, 28), time.time())
        notifier.assert_not_called()

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


class TestMainMore(OfflineTestCase):
    def test_no_job_returns_2(self):
        self.assertEqual(cli_mod.main(["--bizdate", "20260928"]), 2)

    def test_bad_cli_identifier_returns_2(self):
        """纯命令行覆盖值写错按参数问题退出码 2（不是运行失败 1）。"""
        self.assertEqual(cli_mod.main(["--job", "x", "--table", "2024_sales"]), 2)
        self.assertEqual(cli_mod.main(["--job", "x", "--project", "bad-name"]), 2)
        self.assertEqual(cli_mod.main(["--job", "x", "--project", "   "]), 2)

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
