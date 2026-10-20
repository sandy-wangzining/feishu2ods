# -*- coding: utf-8 -*-
"""脱敏、密钥收集与日志文件。"""

from __future__ import annotations

import io
import os
import pathlib
import sys
import tempfile
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import utils as utils_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
)


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

    def test_collects_every_webhook_in_array(self):
        job = {
            "freshness": {
                "webhook": [
                    "https://x/hook/hook-one-1111",
                    "https://x/hook/hook-two-2222",
                ]
            }
        }
        values = utils_mod.collect_secret_values(job)
        self.assertIn("hook-one-1111", values)
        self.assertIn("hook-two-2222", values)

    def test_secret_containers_dict_and_generator(self):
        """dict/生成器等容器也要登记（此前被当成标量包一层，脱敏表实际为空）。"""
        utils_mod.reset_secret_values({"a": "secret-one-111", "b": "secret-two-222"})
        self.assertNotIn("secret-one-111", utils_mod.redact("x secret-one-111 y"))
        self.assertNotIn("secret-two-222", utils_mod.redact("x secret-two-222 y"))
        utils_mod.reset_secret_values(item for item in ["secret-three-333"])
        self.assertNotIn("secret-three-333", utils_mod.redact("x secret-three-333 y"))
        self.assertNotIn("secret-four-444", utils_mod.redact_secrets({"k": "secret-four-444"}, "x secret-four-444 y"))
        self.assertNotIn(
            "secret-five-555", utils_mod.redact_secrets((item for item in ["secret-five-555"]), "x secret-five-555 y")
        )

    def test_safe_label_escapes_control_and_backticks(self):
        """外部列名进日志/飞书卡片前转义：换行/控制字符/反引号不能原样拼入。"""
        self.assertEqual(utils_mod.safe_label("a`b\nc\x00d"), "a'b c d")
        shown = utils_mod.safe_label("x" * 100)
        self.assertEqual(len(shown), 81)
        self.assertTrue(shown.endswith("…"))

    def test_non_dict_job_returns_empty(self):
        self.assertEqual(utils_mod.collect_secret_values("nope"), [])


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

    def test_removed_sink_race_is_not_reported_as_broken(self):
        """快照取完后句柄被正常摘除并关闭：不能误报"写入失败"（并发收尾场景）。"""

        class ClosingSink:
            def write(self, _text):
                utils_mod._sinks.remove(self)  # 模拟 remove_log_sink 在快照之后摘掉
                raise ValueError("I/O operation on closed file")

            def flush(self):
                pass

            def close(self):
                pass

        handle = ClosingSink()
        utils_mod.add_log_sink(handle)
        with mock.patch.object(sys, "stderr", io.StringIO()) as err:
            utils_mod.log("并发收尾")
        self.assertNotIn(handle, utils_mod._sinks)
        self.assertFalse(utils_mod._sink_write_warned)
        self.assertNotIn("写入失败", err.getvalue())

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
