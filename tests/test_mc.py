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

    def test_successful_write_clears_stale_keep(self):
        """恢复成功后要删掉同 pt 的 __keep，否则 max_pt() 会读到旧快照。"""
        table = _FakeTable()
        table.partitions.append("pt='20260928__keep'")
        odps = _FakeOdps([self._verify_row(1, mn="r1")])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        sqls = "\n".join(odps.sqls)
        self.assertIn("drop if exists partition (pt='20260928__keep')", sqls)
        self.assertIn("rename to partition (pt='20260928')", sqls)

    def test_recovery_rename_path_clears_keep(self):
        """上一轮留下 __keep + 首次 rename 失败：重试走"tmp 还在"恢复分支，也要清 keep。"""
        table = _FakeTable()
        tmp = f"20260928{mc_mod.TMP_PARTITION_SUFFIX}"
        table.partitions = [f"pt='{tmp}'", "pt='20260928__keep'"]

        class RenameOnceFails(_FakeOdps):
            def __init__(self, rows):
                super().__init__(rows)
                self.failed = False

            def run_sql(self, sql):
                if "rename to partition" in sql and not self.failed:
                    self.failed = True
                    raise RuntimeError("rename flake")
                return super().run_sql(sql)

        odps = RenameOnceFails([self._verify_row(1, mn="r1")])
        cli_mod.write_partition(odps, table, "p", "t", "json", "20260928", [{"record_id": "r1", "a": 1}])
        sqls = "\n".join(odps.sqls)
        self.assertTrue(odps.failed)
        self.assertIn("drop if exists partition (pt='20260928__keep')", sqls)

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

    def test_probe_failure_preserves_verified_copy(self):
        """探测失败时既不能删正式分区，也不能销毁可能是唯一完整副本的临时分区。"""
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
        self.assertEqual(table.write_attempts, 1)
        tmp_drops = [sql for sql in odps.sqls if "drop if exists" in sql and mc_mod.TMP_PARTITION_SUFFIX in sql]
        self.assertEqual(len(tmp_drops), 1)  # 仅初次写入前清理，重试不再删完整副本
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


class TestWritePartitionStreaming(OfflineTestCase):
    """write_partition 的流式签名与旧签名兼容分支。"""

    def test_ensure_table_uses_explicit_project(self):
        odps = mock.Mock()
        with mock.patch.object(mc_mod, "run_sql_with_timeout"):
            result = mc_mod.ensure_table(odps, "destination", "t", "json", "comment")
        odps.get_table.assert_called_once_with("destination.t")
        self.assertIs(result, odps.get_table.return_value)

    def test_unknown_tmp_probe_can_recover_without_rewriting(self):
        table = _FakeTable()
        with (
            mock.patch.object(mc_mod, "verify_partition", return_value=(1, 1, "r1", "r1")),
            mock.patch.object(mc_mod, "drop_partition") as drop,
            mock.patch.object(mc_mod, "add_partition"),
            mock.patch.object(mc_mod, "rename_partition", side_effect=[TimeoutError("uncertain"), None]) as rename,
            mock.patch.object(mc_mod, "_partition_exists", side_effect=[None, True, False]),
        ):
            cli_mod.write_partition(mock.Mock(), table, "p", "t", "json", "20260928", [{"record_id": "r1"}])
        self.assertEqual(table.write_attempts, 1)
        self.assertEqual(rename.call_count, 2)
        tmp_drops = [call for call in drop.call_args_list if call.args[3].endswith(mc_mod.TMP_PARTITION_SUFFIX)]
        self.assertEqual(len(tmp_drops), 1)

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
