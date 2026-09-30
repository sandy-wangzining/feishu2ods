# -*- coding: utf-8 -*-
"""飞书 HTTP：带重试的请求（限流/5xx 退避、4xx 快速失败）与 tenant_access_token。"""

from __future__ import annotations

import time

from .utils import _api_err, log, redact

try:
    import requests
except ImportError:  # pragma: no cover - 离线测试环境可以不带 requests
    requests = None

FEISHU_HOST = "https://open.feishu.cn"
TOKEN_URL = f"{FEISHU_HOST}/open-apis/auth/v3/tenant_access_token/internal"
HTTP_ATTEMPTS = 3                               # 单次请求最大尝试次数（429/5xx/网络抖动才重试）

class ApiHttpError(Exception):
    """HTTP 4xx（登录/权限/参数类确定性错误）：不再重试，由调用方决定处置。"""

    def __init__(self, status: int, body: str):
        super().__init__(f"HTTP {status}")
        self.status = status
        self.body = body



# =============================================================================
# 飞书：tenant_access_token + 全量拉记录
# =============================================================================
def request_json(method: str, url: str, desc: str, *, params=None, body=None, headers=None, tries: int = HTTP_ATTEMPTS):
    """带重试的 HTTP 请求 → JSON。

    只对「可能自己好起来」的失败重试：429 / 5xx / 连接超时 / 响应不是 JSON。
    其余 4xx 抛 ApiHttpError（密钥错、权限错等确定性错误，重试无意义）；报错文本过脱敏。
    """
    if requests is None:
        raise SystemExit("缺少 requests，请先 pip install requests")
    last: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            # allow_redirects=False：requests 默认跟随重定向，而 301/302/303 会把 POST 降级成
            # 不带 body 的 GET（请求参数全丢），且自定义鉴权头（Authorization）会被转发到重定向
            # 目标。这两种后果都比"直接失败"危险得多（与 api2ods 2.1.8 的修复同款）
            resp = requests.request(
                method, url, params=params, json=body, headers=headers, timeout=(10, 30), allow_redirects=False
            )
        except requests.RequestException as exc:
            last = exc
        else:
            if 300 <= resp.status_code < 400:
                # 不跟随重定向：把 Location 报出来让用户直接改成最终地址。
                # 抛 ApiHttpError（确定性错误、不重试），别让它掉进重试的退避里
                location = redact(str(resp.headers.get("Location") or ""))
                raise ApiHttpError(
                    resp.status_code,
                    f"接口返回重定向 HTTP {resp.status_code}（Location: {location}）：本工具不跟随重定向"
                    f"——301/302/303 会把 POST 降级成不带 body 的 GET（请求参数全丢），"
                    f"鉴权头也可能被转发到别的地址。请把地址改成最终地址",
                )
            if resp.status_code >= 500 or resp.status_code == 429:
                last = requests.RequestException(f"HTTP {resp.status_code}：{resp.text[:200]}")
            elif 400 <= resp.status_code < 500:
                raise ApiHttpError(resp.status_code, resp.text[:300])
            else:
                try:
                    return resp.json()
                except ValueError as exc:
                    last = exc
        if attempt < tries:
            wait = 5 * attempt
            log(f"  [{desc}] 第 {attempt} 次失败：{redact(last)}；{wait}s 后重试")
            time.sleep(wait)
    raise SystemExit(f"{desc} 失败（重试 {tries} 次）：{redact(last)}")


def get_tenant_token(app_id: str, app_secret: str) -> str:
    """app_id/app_secret → tenant_access_token（有效期 2 小时；失败时给明确原因）。"""
    try:
        data = request_json(
            "POST", TOKEN_URL, "获取 tenant_access_token", body={"app_id": app_id, "app_secret": app_secret}
        )
    except ApiHttpError as exc:
        raise SystemExit(f"获取 tenant_access_token 失败：HTTP {exc.status}：{redact(exc.body)}") from None
    if not isinstance(data, dict) or data.get("code") != 0:
        raise SystemExit(f"获取 tenant_access_token 失败：{_api_err(data)}（检查 app_id/app_secret）")
    token = str(data.get("tenant_access_token") or "")
    if not token:
        raise SystemExit("tenant_access_token 为空（接口返回异常）")
    log(f"已获取 tenant_access_token（有效期 {data.get('expire')} 秒）")
    return token


