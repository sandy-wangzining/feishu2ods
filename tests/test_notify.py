# -*- coding: utf-8 -*-
"""飞书通知。"""

from __future__ import annotations

import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from feishu2ods import auth as auth_mod  # noqa: E402
from feishu2ods import cli as cli_mod  # noqa: E402
from feishu2ods import notify as notify_mod  # noqa: E402

REQUESTS_AVAILABLE = auth_mod.requests is not None

from _helpers import (
    OfflineTestCase,
)


class TestNotify(OfflineTestCase):
    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_notify_ok(self):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"code": 0}
        messages: list[str] = []
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.notify("https://x/hook/1", "标题", ["第一行"], footer="尾部")
            # 不跟随重定向：307 会把卡片 JSON 原样重发到别的地址，302/303 还会丢 body
            self.assertFalse(notify_mod.requests.post.call_args.kwargs["allow_redirects"])
        self.assertIn("飞书通知已发送", "\n".join(messages))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_notify_error_swallowed(self):
        messages: list[str] = []
        with (
            mock.patch.object(notify_mod.requests, "post", side_effect=RuntimeError("网络坏了")),
            mock.patch.object(notify_mod, "log", side_effect=lambda msg: messages.append(str(msg))),
        ):
            cli_mod.notify("https://x/hook/1", "标题", ["第一行"])
        self.assertIn("飞书通知发送失败", "\n".join(messages))


class TestNotifyMore(OfflineTestCase):
    def test_missing_webhook_skips(self):
        with mock.patch.object(notify_mod, "log") as logger:
            cli_mod.notify("", "t", ["l"])
        self.assertTrue(any("未配置" in str(c) for c in logger.call_args_list))

    def test_invalid_webhook_type_warns_and_skips(self):
        """webhook 写成对象等非法类型时不能静默跳过告警（配置错要能看见）。"""
        with mock.patch.object(notify_mod, "log") as logger:
            cli_mod.notify({"url": "https://x/hook/1"}, "t", ["l"])
        self.assertTrue(any("没有可用地址" in str(c) for c in logger.call_args_list))

    def test_requests_missing_skips(self):
        with mock.patch.object(notify_mod, "requests", None), mock.patch.object(notify_mod, "log") as logger:
            notify_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("requests" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_post_exception_swallowed(self):
        with (
            mock.patch.object(notify_mod.requests, "post", side_effect=RuntimeError("down")),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("通知发送失败" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_multiple_webhooks(self):
        """webhook 写成数组时逐个群发送（多个群同时告警）。"""
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"code": 0}
        hooks = ["https://x/hook/1", "https://x/hook/2"]
        with mock.patch.object(notify_mod.requests, "post", return_value=resp) as post:
            cli_mod.notify(hooks, "t", ["l"])
        self.assertEqual([call.args[0] for call in post.call_args_list], hooks)

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_multiple_webhooks_partial_failure(self):
        """单个群失败不影响其它群，失败只记日志（不改主流程退出码）。"""
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"code": 0}

        def post(url, **kwargs):
            if url.endswith("/bad"):
                raise RuntimeError("down")
            return resp

        with (
            mock.patch.object(notify_mod.requests, "post", side_effect=post) as fake_post,
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify(["https://x/hook/bad", "https://x/hook/ok"], "t", ["l"])
        self.assertEqual(fake_post.call_count, 2)
        self.assertTrue(any("通知发送失败" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_bad_status_swallowed(self):
        resp = mock.Mock(status_code=500)
        resp.json.return_value = {"code": 1, "msg": "bad"}
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("通知发送失败" in str(c) for c in logger.call_args_list))

    @unittest.skipUnless(REQUESTS_AVAILABLE, "没装 requests")
    def test_success_status_code_field(self):
        resp = mock.Mock(status_code=200)
        resp.json.return_value = {"StatusCode": 0}
        with (
            mock.patch.object(notify_mod.requests, "post", return_value=resp),
            mock.patch.object(notify_mod, "log") as logger,
        ):
            cli_mod.notify("https://x/hook/1", "t", ["l"])
        self.assertTrue(any("已发送" in str(c) for c in logger.call_args_list))
