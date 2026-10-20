# -*- coding: utf-8 -*-
"""公共运行契约；从 api2ods/shared/test_runtime.py 同步，三个项目独立执行。"""

from __future__ import annotations

import importlib
import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = next(name for name in ("sftp2ods", "api2ods", "feishu2ods") if (ROOT / name / "__init__.py").is_file())
common = importlib.import_module(f"{PACKAGE}._common")
mc = importlib.import_module(f"{PACKAGE}.mc")


class TestSharedRuntime(unittest.TestCase):
    @unittest.skipUnless(hasattr(common.os, "getuid"), "POSIX 目录权限检查")
    def test_private_lock_dir_rejects_shared_writable_directory(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(common.tempfile, "gettempdir", return_value=tmp):
            path = common.private_lock_dir(PACKAGE)
            self.assertEqual(path.name, f"{PACKAGE}-locks-{common.os.getuid()}")
            path.chmod(0o777)
            with self.assertRaises(PermissionError):
                common.private_lock_dir(PACKAGE)

    @unittest.skipUnless(hasattr(common.os, "getuid"), "POSIX 软链检查")
    def test_private_lock_dir_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(common.tempfile, "gettempdir", return_value=tmp):
            target = Path(tmp) / "other"
            target.mkdir()
            link = Path(tmp) / f"{PACKAGE}-locks-{common.os.getuid()}"
            link.symlink_to(target, target_is_directory=True)
            with self.assertRaises(PermissionError):
                common.private_lock_dir(PACKAGE)

    @unittest.skipUnless(hasattr(common.os, "getuid"), "POSIX 默认回退目录集成检查")
    def test_default_lock_fallback_rejects_unsafe_directory(self):
        cli = importlib.import_module(f"{PACKAGE}.cli")
        utils = importlib.import_module(f"{PACKAGE}.utils")
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "readonly-install"
            root.mkdir()
            (root / ".run-locks").write_text("file", encoding="utf-8")
            with mock.patch.object(common.tempfile, "gettempdir", return_value=tmp):
                unsafe = common.private_lock_dir(PACKAGE)
                unsafe.chmod(0o777)
                with mock.patch.dict(common.os.environ, {}, clear=False):
                    common.os.environ.pop(f"{PACKAGE.upper()}_LOCK_DIR", None)
                    with self.assertRaisesRegex(SystemExit, "LOCK_DIR"):
                        if PACKAGE == "feishu2ods":
                            with mock.patch.object(utils, "__file__", str(root / "utils.py")):
                                utils._lock_base_dir()
                        else:
                            with mock.patch.object(cli, "ROOT", root):
                                cli._lock_path(Path("job.json"))

    def test_default_bizdate_is_us_eastern_yesterday(self):
        now = datetime(2026, 9, 24, 10, 0, tzinfo=timezone.utc)  # 06:00 EDT
        self.assertEqual(common.default_bizdate(now=now).isoformat(), "2026-09-23")
        boundary = datetime(2026, 9, 24, 3, 0, tzinfo=timezone.utc)  # 23:00 EDT 前一天
        self.assertEqual(common.default_bizdate(now=boundary).isoformat(), "2026-09-22")

    def test_nested_secret_values_keep_numeric_ids_and_auth_token(self):
        self.assertEqual(
            common.leaf_strings({"a": ["token", 1234, True, None], "b": ("secret",)}), ["token", "1234", "secret"]
        )
        self.assertEqual(common.with_scheme_bare("Bearer abc"), ["Bearer abc", "abc"])

    def test_webhook_targets_deduplicate_and_skip_invalid_items(self):
        self.assertEqual(common.webhook_targets([" a ", "", "a", None, 1, "b"]), ["a", "b"])
        self.assertEqual(common.webhook_targets(" a "), ["a"])

    def test_label_and_card_keep_data_separate_from_display(self):
        self.assertEqual(common.safe_label("a\n`b`\x00"), "a 'b' ")
        self.assertEqual(common.safe_label("abcdef", 3), "abc…")
        card = common.feishu_card("标题", ["第一行", 2], "尾部")
        self.assertEqual(card["card"]["elements"][0]["text"]["content"], "第一行\n2")
        self.assertEqual(card["card"]["elements"][-1]["elements"][0]["content"], "尾部")

    def test_sql_polling_error_or_interrupt_cancels_instance(self):
        for error in (RuntimeError("network flake"), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                instance = mock.Mock()
                instance.is_successful.side_effect = error
                odps = mock.Mock()
                odps.run_sql.return_value = instance
                with self.assertRaises(type(error)) as raised:
                    mc.run_sql_with_timeout(odps, "select 1", timeout=1)
                self.assertIs(raised.exception, error)
                instance.stop.assert_called_once()

    def test_sql_submission_time_is_part_of_timeout(self):
        clock = SimpleNamespace(now=0, monotonic=lambda: clock.now, sleep=mock.Mock())
        instance = mock.Mock()
        instance.is_successful.return_value = False
        instance.is_terminated.return_value = False

        def submit(_sql):
            clock.now = 10
            return instance

        with mock.patch.object(mc, "time", clock):
            with self.assertRaises(TimeoutError):
                mc.run_sql_with_timeout(SimpleNamespace(run_sql=submit), "select 1", timeout=5)
        instance.stop.assert_called_once()
        clock.sleep.assert_not_called()

    def test_sql_success_does_not_cancel(self):
        instance = mock.Mock()
        instance.is_successful.return_value = True
        odps = mock.Mock()
        odps.run_sql.return_value = instance
        self.assertIs(mc.run_sql_with_timeout(odps, "select 1"), instance)
        instance.stop.assert_not_called()

    def test_cancel_error_preserves_original_failure(self):
        instance = mock.Mock()
        original = RuntimeError("poll failed")
        instance.is_successful.side_effect = original
        instance.stop.side_effect = RuntimeError("stop failed")
        odps = mock.Mock()
        odps.run_sql.return_value = instance
        with mock.patch.object(mc, "log") as log:
            with self.assertRaises(RuntimeError) as raised:
                mc.run_sql_with_timeout(odps, "select 1")
        self.assertIs(raised.exception, original)
        self.assertIn("取消失败", str(log.call_args))


class TestSharedSync(unittest.TestCase):
    def test_interrupted_replace_keeps_old_file_and_cleans_temp(self):
        tool = self._tool()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runtime.py"
            path.write_bytes(b"old\n")
            for error in (OSError("replace failed"), KeyboardInterrupt()):
                with self.subTest(error=type(error).__name__):
                    with mock.patch.object(tool.os, "replace", side_effect=error):
                        with self.assertRaises(type(error)):
                            tool.write_atomic(path, b"new\n")
                    self.assertEqual(path.read_bytes(), b"old\n")
                    self.assertEqual(list(Path(tmp).glob(".*.tmp")), [])
            tool.write_atomic(path, b"new\n")
            self.assertEqual(path.read_bytes(), b"new\n")

    @staticmethod
    def _tool():
        spec = importlib.util.spec_from_file_location("ods_sync_test", ROOT / "tools/sync_common.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_checkout_passes_offline_consistency_check(self):
        with redirect_stdout(io.StringIO()):
            self.assertEqual(self._tool().main(["--check", str(ROOT)]), 0)

    def test_sync_then_detect_drift_without_overwriting(self):
        tool = self._tool()
        with tempfile.TemporaryDirectory() as tmp, redirect_stdout(io.StringIO()):
            base = Path(tmp)
            source = base / "shared"
            source.mkdir()
            files = tool.file_map(PACKAGE)
            for name, target in files.items():
                (source / name).write_bytes((ROOT / target).read_bytes())
            repo = base / "renamed-checkout"
            (repo / PACKAGE).mkdir(parents=True)
            (repo / PACKAGE / "__init__.py").write_text("", encoding="utf-8")
            self.assertEqual(tool.main(["--source", str(source), str(repo)]), 0)
            self.assertEqual(tool.main(["--check", str(repo)]), 0)
            target = repo / PACKAGE / "_common.py"
            target.write_text("# unexpected local change\n", encoding="utf-8")
            self.assertEqual(tool.main(["--check", str(repo)]), 1)
            self.assertEqual(target.read_text(encoding="utf-8"), "# unexpected local change\n")
            manifest = json.loads((repo / "shared-runtime.json").read_text(encoding="utf-8"))
            self.assertEqual(set(manifest["files"]), set(files.values()))


if __name__ == "__main__":
    unittest.main()
