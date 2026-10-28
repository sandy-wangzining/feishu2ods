# -*- coding: utf-8 -*-
"""MaxCompute 写入、临时分区与 SQL 超时。"""

from __future__ import annotations

import pathlib
import sys
import time
from datetime import date
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import mc as mc_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
    _Col,
    _FakeInstance,
    _FakeOdps,
    _FakeTable,
    _Part,
    _Table,
    make_args,
    make_job,
    make_spool_stats,
)


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
        # 先写独立暂存表的分区，核对后用单条 OVERWRITE 发布；
        # 分区增删走带超时的 DDL（run_sql），表对象只负责 Tunnel 写入
        self.assertEqual([c[0] for c in odps.stage.calls], ["write"])
        self.assertEqual(odps.stage.calls[0][1], f"pt={tmp}")
        self.assertEqual(odps.stage.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        sqls = "\n".join(odps.sqls)
        self.assertIn(f"drop if exists partition (pt='{tmp}')", sqls)
        self.assertIn(f"add if not exists partition (pt='{tmp}')", sqls)
        self.assertNotIn("alter table p.t drop", sqls)
        self.assertIn("insert overwrite table p.t", sqls)
        self.assertIn("drop table if exists p.tmp_feishu2ods_", odps.sql)

    def test_successful_write_clears_stale_keep(self):
        """恢复成功后要删掉同 pt 的 __keep，否则 max_pt() 会读到旧快照。"""
        table = _FakeTable()
        table.partitions.append("pt='20260928__keep'")
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260928__keep')", sqls)
        self.assertIn("insert overwrite table p.t", sqls)

    def test_keep_cleanup_failure_does_not_fail_write(self):
        """正式分区已写入成功：清旧 __keep 失败只告警，不能把成功任务报成失败。"""
        table = _FakeTable()
        table.partitions.append("pt='20260928__keep'")

        class KeepDropFails(_FakeOdps):
            def run_sql(self, sql):
                if "drop if exists partition" in sql and "__keep" in sql:
                    raise RuntimeError("keep drop flake")
                return super().run_sql(sql)

        odps = KeepDropFails([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "log") as logger:
            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertTrue(any("清理旧保留分区" in str(call) for call in logger.call_args_list))

    def test_write_partition_retry(self):
        table = _FakeTable(fail_first_write=True)
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        odps.stage.fail_first_write = True
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(odps.stage.write_attempts, 2)
        self.assertEqual(odps.stage.rows, [['{"record_id":"r1","a":1}']])
        # reopen=True 是 Tunnel 断线重连的关键实参：去掉它不该在测试里悄悄通过
        self.assertTrue(odps.stage.reopens and all(odps.stage.reopens))

    def test_write_partition_row_too_big(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "MAX_ROW_BYTES", 10):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(
                    odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "big": "x" * 100}]
                )
        self.assertEqual(odps.stage.calls, [])  # 动分区之前就该报错

    def test_write_partition_count_mismatch_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])  # 计划 2 行，实际 1 行
        with self.assertRaises(SystemExit):
            cli_mod.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(odps.stage.write_attempts, mc_mod.WRITE_ATTEMPTS)
        self.assertNotIn("rename to partition", "\n".join(odps.sqls))

    def test_write_partition_duplicate_ids_retries(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(2, ucnt=1, mn="r1", mx="r1")])
        with self.assertRaises(SystemExit):
            cli_mod.write_partition(
                odps, table, "p", "t", "json", "20260928", [{"record_id": "r1"}, {"record_id": "r2"}]
            )
        self.assertEqual(odps.stage.write_attempts, mc_mod.WRITE_ATTEMPTS)
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
        self.assertEqual([c[0] for c in odps.stage.calls], ["write"])
        self.assertEqual(odps.stage.rows, [])
        self.assertIn("partition (pt='20260928')", "\n".join(odps.sqls))

    def test_keyboard_interrupt_not_replaced_by_finally(self):
        """finally 里不能再抛 SystemExit 覆盖正在传播的 KeyboardInterrupt（退出码要保 130）。"""
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "drop_partition", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])

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

    def test_write_partition_cleanup_failure_keeps_error(self):
        table = mock.Mock()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])

        def drop(o, project, table_name, spec, timeout=None):
            if mc_mod.TMP_PARTITION_SUFFIX in spec:
                raise RuntimeError("delete boom")

        with mock.patch.object(mc_mod, "drop_partition", side_effect=drop):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("正式分区未执行删除", str(ctx.exception))
        self.assertIn("暂存表", str(ctx.exception))

    def test_verify_systemexit_keeps_stage_and_target(self):
        table = _FakeTable()
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        with mock.patch.object(mc_mod, "verify_partition", side_effect=SystemExit("核对失败")):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("核对失败", str(ctx.exception))
        self.assertNotIn("insert overwrite", "\n".join(odps.sqls))
        self.assertNotIn("drop table", "\n".join(odps.sqls))

    def test_purge_skips_keep_partitions(self):
        """__keep 是"正式分区被删后唯一完整副本"，清理必须跳过它。"""
        table = _FakeTable()
        keep = "pt='20260928__keep'"
        table.partitions = [_Part(keep), _Part("pt='20260927'"), _Part("pt='20260927__tmp_otherhost_1'")]
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
        table.partitions = [
            _Part(mine),
            _Part("pt='20260928'"),
            _Part(other),
            _Part(legacy),
            _Part(near_miss),
            _Part("pt='20260929'"),
            _Part("pt='20260926'"),
            _Part("pt='20260925'"),
        ]
        table.partitions.append(_Part("pt='20260927'"))
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
        self.assertIn(
            f"pt='20260927{mc_mod.TMP_PARTITION_SUFFIX}'", sqls
        )  # 新流程不在正式表创建 tmp，旧 PID 复用残留也应清理
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

    def test_count_partition(self):
        odps = _FakeOdps([{"cnt": 7}])
        self.assertEqual(cli_mod.count_partition(odps, "p", "t", "20260928"), 7)
        self.assertIn("where pt = '20260928'", odps.sql)


class TestWritePartitionStreaming(OfflineTestCase):
    """write_partition 的流式签名与旧签名兼容分支。"""

    def test_ensure_table_uses_explicit_project(self):
        odps = mock.Mock()
        with mock.patch.object(mc_mod, "run_sql_with_timeout"):
            result = mc_mod.ensure_table(odps, "destination", "t", "json", "comment")
        odps.get_table.assert_called_once_with("destination.t")
        self.assertIs(result, odps.get_table.return_value)

    def test_exhausted_write_inside_outer_except_still_raises(self):
        with mock.patch.object(mc_mod, "drop_partition", side_effect=RuntimeError("DDL failed")):
            try:
                raise ValueError("caller is handling another exception")
            except ValueError:
                with self.assertRaises(SystemExit):
                    cli_mod.write_partition(
                        mock.Mock(), _FakeTable(), "p", "t", "json", "20260928", [{"record_id": "r1"}]
                    )

    def test_duplicate_or_partly_missing_ids_rejected_before_ddl(self):
        for ids in (("r1", "r1"), ("r1", "")):
            for stream in (False, True):
                with self.subTest(ids=ids, stream=stream):
                    rows = [{"record_id": rid} for rid in ids]
                    with cli_mod.SpoolWriter() as spool:
                        spool.write_records(rows)
                        stats = cli_mod.FetchStats()
                        stats.update(rows)
                        odps = mock.Mock()
                        table = mock.Mock()
                        args = (spool, stats) if stream else (rows,)
                        with self.assertRaisesRegex(SystemExit, "record_id"):
                            cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", *args)
                        odps.run_sql.assert_not_called()
                        table.open_writer.assert_not_called()

    def test_new_signature_writes_from_spool(self):
        table = _FakeTable()
        rows = [{"record_id": "r1", "a": 1}, {"record_id": "r2", "a": 2}]
        spool, stats = make_spool_stats(rows)
        odps = _FakeOdps([{"cnt": 2, "ucnt": 2, "mn": "r1", "mx": "r2"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertEqual(odps.stage.rows, [['{"record_id":"r1","a":1}'], ['{"record_id":"r2","a":2}']])
        self.assertEqual([c[0] for c in odps.stage.calls], ["write"])
        spool.close()

    def test_new_signature_row_too_big_checked_before_delete(self):
        table = _FakeTable()
        spool, stats = make_spool_stats([{"record_id": "r1", "big": "x" * 100}])
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        with mock.patch.object(mc_mod, "MAX_ROW_BYTES", 10):
            with self.assertRaises(SystemExit):
                cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", spool, stats)
        self.assertEqual(odps.stage.calls, [])
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
        self.assertEqual(odps.stage.rows, [])
        self.assertIn("insert overwrite", "\n".join(odps.sqls))
        spool.close()

    def test_wrong_signature_raises_type_error(self):
        with self.assertRaises(TypeError):
            cli_mod.write_partition(mock.Mock(), _FakeTable(), "p", "t", "json", "20260928", "not-a-spool")


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
        """发布/verify 都走 run_sql（异步实例），不再是阻塞的 execute_sql。"""
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertIn("insert overwrite", "\n".join(odps.sqls))

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
        """暂存分区 DDL 超时会重试；不发布，也不删除正式分区。"""
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
        self.assertIn("正式分区未执行删除", str(ctx.exception))
        self.assertNotIn("rename", "\n".join(odps.sqls))
        # 3 次暂存写入重试；失败保留暂存表：超时错误全部被吸收，不无限挂起、不留悬挂线程
        self.assertEqual(drops["n"], mc_mod.WRITE_ATTEMPTS)

    def test_old_signature_still_works_with_list(self):
        # 旧调用方（传记录列表、不给 stats）自动装进临时 spool
        table = _FakeTable()
        odps = _FakeOdps([{"cnt": 1, "ucnt": 1, "mn": "r1", "mx": "r1"}])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        self.assertEqual(odps.stage.rows, [['{"record_id":"r1","a":1}']])
