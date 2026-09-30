# -*- coding: utf-8 -*-
"""飞书告警：缺数据 / 新增列等场景发群卡片（失败只记日志，不影响主流程退出码）。"""

from __future__ import annotations

from .utils import log

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
        log(f"  警告：飞书通知发送失败：{type(exc).__name__}: {exc}")
        return
    if resp.status_code == 200 and isinstance(data, dict) and data.get("code", data.get("StatusCode", 0)) == 0:
        log("飞书通知已发送")
    else:
        log(f"  警告：飞书通知发送失败：HTTP {resp.status_code} {str(data)[:200]}")


