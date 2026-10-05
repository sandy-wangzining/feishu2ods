# -*- coding: utf-8 -*-
"""飞书告警：缺数据 / 新增列等场景发群卡片（失败只记日志，不影响主流程退出码）。"""

from __future__ import annotations

from .utils import log, redact

try:
    import requests
except ImportError:  # pragma: no cover - 未安装时告警降级为一条日志
    requests = None


def notify(webhook: str, title: str, lines: list[str], footer: str = "") -> None:
    """发飞书群卡片消息；未配 webhook 或发送失败只记日志（不影响主流程退出码）。"""
    if not webhook:
        log("  警告：未配置 freshness.webhook，跳过飞书告警")
        return
    if requests is None:
        log("  警告：缺少 requests，跳过飞书告警（pip install requests）")
        return
    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {"template": "red", "title": {"tag": "plain_text", "content": title}},
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": "\n".join(lines)}}],
        },
    }
    if footer:
        card["card"]["elements"].append({"tag": "hr"})
        card["card"]["elements"].append({"tag": "note", "elements": [{"tag": "plain_text", "content": footer}]})
    try:
        resp = requests.post(webhook, json=card, timeout=15)
        data = resp.json()
    except Exception as exc:  # noqa: BLE001 - 告警失败不该改变主流程的退出码
        # 显式 redact：requests 的异常消息里带完整 URL（末段 hook id 即凭据）。log() 出口
        # 本身也会脱敏，这里再遮一道，不依赖"下游一定会脱敏"这个假设
        log(f"  警告：飞书通知发送失败：{type(exc).__name__}: {redact(str(exc))}")
        return
    code = data.get("code", data.get("StatusCode", None)) if isinstance(data, dict) else None
    # `False == 0`、`0.0 == 0` 都是真：布尔 false / 浮点 0 的"失败"响应不能当成成功码
    if resp.status_code == 200 and type(code) is int and code == 0:
        log("飞书通知已发送")
    elif resp.status_code == 200 and isinstance(data, dict) and not data:
        # 少数转发网关成功时只回 {}：保留按 HTTP 200 判定的宽容，但把依据说清楚
        log("飞书通知已发送（响应为空，按 HTTP 200 判定）")
    else:
        # 有 JSON 但没有 code/StatusCode（如误填成其它接口的地址）：不能当成功——
        # 告警通道静默失效比报错更危险
        log(f"  警告：飞书通知发送失败：HTTP {resp.status_code} {str(data)[:200]}")
