"""数值精度、时区、大字段与隔离发布的回归测试。"""

import json
from datetime import datetime
from decimal import Decimal
from unittest import mock
from zoneinfo import ZoneInfo

from _helpers import OfflineTestCase, _FakeInstance, _FakeOdps, _FakeTable, _Part, make_job

from feishu2ods import auth, config, dates, mc, spool


class PublishingOdps(_FakeOdps):
    """分别保存正式表和暂存表；发布成功时一次性替换正式数据。"""

    def __init__(self, target, *, publish_error=None, fail_after_commit=False, cleanup_error=False):
        super().__init__([])
        self.target = target
        self.publish_error = publish_error
        self.fail_after_commit = fail_after_commit
        self.cleanup_error = cleanup_error
        self.publish_attempts = 0

    def run_sql(self, sql):
        if sql.startswith("select count(*) as cnt, count(distinct rid)"):
            self.sqls.append(sql)
            ids = [json.loads(row[0])["record_id"] for row in self.stage.rows]
            return _FakeInstance(
                [{"cnt": len(ids), "ucnt": len(set(ids)), "mn": min(ids, default=None), "mx": max(ids, default=None)}]
            )
        if sql.startswith("insert overwrite"):
            self.sqls.append(sql)
            self.publish_attempts += 1
            if self.publish_error:
                if isinstance(self.publish_error, Exception):
                    result = mock.Mock()
                    result.id = "rejected-publish"
                    result.is_successful.return_value = False
                    result.is_terminated.return_value = True
                    result.wait_for_success.side_effect = self.publish_error
                    return result
                raise self.publish_error
            self.target.rows = list(self.stage.rows)
            self.target.partitions = [_Part("pt='20261026'")]
            if self.fail_after_commit and self.publish_attempts == 1:
                result = _FakeInstance([])
                result.id = "completed-publish"
                result.is_successful = mock.Mock(side_effect=TimeoutError("response lost after commit"))
                result.is_terminated = mock.Mock(return_value=True)
                result.stop = mock.Mock()
                return result
            return _FakeInstance([])
        if sql.startswith("drop table") and self.cleanup_error:
            raise RuntimeError("cleanup unavailable")
        return super().run_sql(sql)


class TestPublicIntegrity(OfflineTestCase):
    def test_http_and_spool_preserve_decimal_numbers(self):
        response = auth.requests.Response()
        response.status_code = 200
        response._content = b'{"record_id":"r1","amount":9007199254740993.01,"tiny":1e-400}'
        with mock.patch.object(auth.requests, "request", return_value=response):
            record = auth.request_json("GET", "https://example.invalid", "offline")
        self.assertEqual(record["amount"], Decimal("9007199254740993.01"))
        text = spool.dump_record(record)
        self.assertIn('"amount":9007199254740993.01', text)
        self.assertIn('"tiny":1E-400', text)

    def test_non_finite_decimal_is_rejected(self):
        with self.assertRaises(ValueError):
            spool.dump_record({"record_id": "r1", "amount": Decimal("NaN")})
        for value in ("NaN", "sNaN", "Infinity"):
            self.assertIsNone(dates.normalize_date_value(Decimal(value)))

    def test_freshness_epoch_and_iso_agree_across_midnight_and_dst(self):
        for text in ("2026-10-26T20:00:00-04:00", "2026-11-01T01:30:00-04:00", "2026-11-01T01:30:00-05:00"):
            epoch = int(datetime.fromisoformat(text).timestamp() * 1000)
            for zone in ("America/New_York", "Asia/Shanghai", "UTC"):
                with self.subTest(text=text, zone=zone):
                    tz = ZoneInfo(zone)
                    self.assertEqual(dates.normalize_date_value(text, tz), dates.normalize_date_value(epoch, tz))

    def test_freshness_placeholder_dates_cannot_overflow_timezone_conversion(self):
        for text, zone in (
            ("9999-12-31T23:59:59Z", "Asia/Shanghai"),
            ("9999-12-31T23:59:59-12:00", "UTC"),
            ("0001-01-01T00:00:00+14:00", "America/New_York"),
        ):
            with self.subTest(text=text, zone=zone):
                tz = ZoneInfo(zone)
                self.assertIsNone(dates.normalize_date_value(text, tz))
                stats = spool.FetchStats(date_field="day", date_tz=tz)
                stats.update([{"record_id": "r1", "day": text}, {"record_id": "r2", "day": "2026-10-26"}])
                self.assertEqual(stats.date_values, {"2026-10-26"})

    def test_configured_freshness_zone_applies_to_stats(self):
        tz = dates.freshness_zone({"timezone": "America/New_York"})
        stats = spool.FetchStats(date_field="day", date_tz=tz)
        stats.update([{"record_id": "r1", "day": 1793059200000}])
        self.assertEqual(stats.date_values, {"2026-10-26"})
        job = make_job()
        job["freshness"]["timezone"] = "not/a/zone"
        with self.assertRaisesRegex(SystemExit, "freshness.timezone"):
            config.validate_job(job)

    def test_byte_batches_preserve_order_and_allow_one_large_row(self):
        with spool.SpoolWriter() as data:
            records = [{"record_id": f"r{i}", "text": "汉" * 20} for i in range(5)]
            data.write_records(records)
            batches = list(data.iter_batches(batch_size=500, max_bytes=200))
            self.assertEqual([len(batch) for batch in batches], [2, 2, 1])
            self.assertEqual([json.loads(row) for batch in batches for row in batch], records)
            self.assertEqual([len(batch) for batch in data.iter_batches(max_bytes=10)], [1] * 5)

    def target(self):
        table = _FakeTable()
        table.rows = [['{"record_id":"old"}']]
        table.partitions = [_Part("pt='20261026'")]
        return table

    def write(self, odps, records=None):
        mc.write_partition(
            odps, odps.target, "p", "t", "json", "20261026", records if records is not None else [{"record_id": "r1"}]
        )

    def test_staged_records_are_invisible_to_target_until_verified(self):
        target = self.target()
        odps = PublishingOdps(target)
        real_verify = mc.verify_partition

        def verify(*args, **kwargs):
            self.assertEqual(target.rows, [['{"record_id":"old"}']])
            self.assertEqual([part.name for part in target.partitions], ["pt='20261026'"])
            self.assertNotEqual(args[2], "t")
            return real_verify(*args, **kwargs)

        with mock.patch.object(mc, "verify_partition", side_effect=verify):
            self.write(odps)
        self.assertEqual(target.rows, [['{"record_id":"r1"}']])
        self.assertEqual(odps.publish_attempts, 1)
        self.assertTrue(
            any(sql.startswith("insert overwrite table p.t partition (pt='20261026')") for sql in odps.sqls)
        )
        self.assertFalse(any("alter table p.t " in sql for sql in odps.sqls))
        self.assertTrue(any("lifecycle 1" in sql for sql in odps.sqls))
        ddl = next(sql for sql in odps.sqls if sql.startswith("create table"))
        self.assertIn("'lifecycle.deletemeta'='true'", ddl)
        self.assertNotIn("lifecycle", mc.build_ddl("p", "t", "json", "正式数据"))
        self.assertTrue(any("drop table if exists p.tmp_feishu2ods_" in sql for sql in odps.sqls))

    def test_verification_failure_preserves_old_target(self):
        odps = PublishingOdps(self.target())
        with mock.patch.object(mc, "verify_partition", return_value=(2, 2, "r1", "r2")):
            with self.assertRaises(SystemExit):
                self.write(odps)
        self.assertEqual(odps.publish_attempts, 0)
        self.assertEqual(odps.target.rows, [['{"record_id":"old"}']])

    def test_failed_publication_preserves_target_and_verified_stage(self):
        odps = PublishingOdps(self.target(), publish_error=RuntimeError("SQL unavailable"))
        with self.assertRaisesRegex(SystemExit, "暂存表"):
            self.write(odps)
        self.assertEqual(odps.target.rows, [['{"record_id":"old"}']])
        self.assertEqual(odps.stage.write_attempts, 1)
        self.assertEqual(odps.publish_attempts, mc.WRITE_ATTEMPTS)
        self.assertFalse(any(sql.startswith("drop table") for sql in odps.sqls))

    def test_pending_publish_blocks_retries_and_later_writers(self):
        from feishu2ods import _common

        odps = PublishingOdps(self.target())
        original = odps.run_sql
        pending = mock.Mock()
        pending.id = "pending-publish"
        pending.is_successful.side_effect = ConnectionError("poll failed")
        pending.is_terminated.return_value = False
        pending.stop.side_effect = ConnectionError("cancel failed")
        publications = []

        def run_sql(sql):
            if sql.startswith("insert overwrite"):
                publications.append(sql)
                return pending
            return original(sql)

        odps.get_instance = mock.Mock(return_value=pending)
        with mock.patch.object(odps, "run_sql", side_effect=run_sql):
            for _ in range(2):
                with self.assertRaises(_common.UnresolvedSqlError):
                    self.write(odps)
        self.assertEqual(len(publications), 1)
        self.assertEqual(odps.target.rows, [['{"record_id":"old"}']])
        pending.is_terminated.return_value = True
        self.write(odps)
        self.assertEqual(odps.target.rows, [['{"record_id":"r1"}']])

    def test_lost_publish_response_is_retried_idempotently(self):
        odps = PublishingOdps(self.target(), fail_after_commit=True)
        self.write(odps)
        self.assertEqual(odps.stage.write_attempts, 1)
        self.assertEqual(odps.publish_attempts, 2)
        self.assertEqual(odps.target.rows, [['{"record_id":"r1"}']])

    def test_cleanup_failure_does_not_fail_published_snapshot(self):
        odps = PublishingOdps(self.target(), cleanup_error=True)
        self.write(odps)
        self.assertEqual(odps.target.rows, [['{"record_id":"r1"}']])

    def test_interrupt_preserves_old_target_and_propagates(self):
        odps = PublishingOdps(self.target(), publish_error=KeyboardInterrupt())
        with self.assertRaises(KeyboardInterrupt):
            self.write(odps)
        self.assertEqual(odps.target.rows, [['{"record_id":"old"}']])

    def test_allowed_empty_snapshot_publishes_empty_partition(self):
        odps = PublishingOdps(self.target())
        self.write(odps, records=[])
        self.assertEqual(odps.target.rows, [])
        self.assertEqual(odps.publish_attempts, 1)

    def test_upgrade_preserves_only_legacy_temporary_copy(self):
        for suffix in ("__tmp_otherhost_123", mc.TMP_PARTITION_SUFFIX):
            target = _FakeTable()
            target.partitions = [_Part(f"pt='20261025{suffix}'")]
            odps = _FakeOdps([])
            with self.assertRaisesRegex(SystemExit, "唯一副本"):
                mc.purge_stale_tmp_partitions(odps, target, "p", "t")
            self.assertEqual(odps.sqls, [])

    def test_publish_identifiers_are_checked_before_any_sql(self):
        odps = mock.Mock()
        with self.assertRaises(SystemExit):
            mc.write_partition(odps, _FakeTable(), "p", "t;drop", "json", "20261026", [])
        odps.run_sql.assert_not_called()
