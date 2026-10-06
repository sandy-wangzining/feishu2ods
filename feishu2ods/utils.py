# -*- coding: utf-8 -*-
"""通用工具：日志（带时间戳+脱敏+可选写文件）、运行锁（flock/msvcrt）、_api_err、脱敏。"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import pathlib
import re
import socket
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, quote_plus, unquote

try:
    import fcntl  # Linux / macOS：进程级运行锁
except ImportError:  # pragma: no cover - Windows 没有 fcntl
    fcntl = None

try:
    import msvcrt  # Windows：用首字节锁实现同样的效果
except ImportError:  # pragma: no cover - Linux / macOS 没有 msvcrt
    msvcrt = None

CN_TZ = timezone(timedelta(hours=8))  # 运行日历日基准（固定 +08:00，无夏令时）
_SECRETS: list[str] = []  # 日志脱敏用（job 里读到的密钥值）


def reset_secret_values(values) -> None:
    """重设值级脱敏表（每次 main() 开头调用）。

    锁内完成 clear+登记：`_SECRETS` 是模块级状态，并发调用 main() 时"先清空"会把
    另一轮已登记的密钥抹掉（该轮日志漏遮）；redact() 读它也要走同一把锁的快照语义。
    """
    if values is None:
        values = []
    elif not isinstance(values, (list, tuple, set, frozenset)):
        # 与 redact_secrets 同口径的入参规范化：标量直接传进来时，字符串会被 for
        # 按字符拆开（全部短于阈值被跳过、脱敏表实际为空）、数字会 TypeError
        values = [values]
    with _lock:
        _SECRETS.clear()
        _SECRETS.extend(str(v) for v in values if isinstance(v, (str, int, float)) and not isinstance(v, bool))


_console_patched = False
_lock = threading.Lock()
_sinks: list = []  # 日志文件副本（--log-file）；句柄由调用方负责关闭
_sink_write_warned = False  # --log-file 写入失败只向 stderr 提示一次，避免刷屏


def setup_console() -> None:
    """stdout/stderr 切 UTF-8，避免 Windows 控制台中文乱码/报错（切不了就跳过）。"""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, OSError, ValueError):
            # 只吞"这个流不支持 reconfigure"（含 io.UnsupportedOperation，它是 OSError/ValueError
            # 的子类）；吞掉别的异常会把本函数自身的编程错误也一起静默
            pass


def add_log_sink(handle) -> None:
    """把日志再写一份到文件（--log-file），句柄由调用方负责关闭。

    重复登记同一句柄会让每条日志写两遍，且摘除/关闭后列表里还留着失效句柄（再写就抛）；
    这里按身份去重，None 直接忽略（留在列表里会让每次 log() 都炸在 handle.write 上）。
    """
    if handle is None:
        return
    with _lock:
        if handle not in _sinks:
            _sinks.append(handle)


def remove_log_sink(handle) -> None:
    """摘掉日志文件并关闭句柄（同一进程里多次调用 main 时，残留句柄会继续写已关闭的文件）。"""
    if handle is None:
        return
    with _lock:
        if handle in _sinks:
            _sinks.remove(handle)
    closer = getattr(handle, "close", None)
    if not callable(closer):
        return
    try:
        closer()
    except (OSError, ValueError):
        # 句柄已经被关过（同一进程里 main 多次调用时 _detach 会跑两遍）
        pass


def log(msg: str) -> None:
    """带时间戳（北京时间）的日志；所有输出过一道密钥脱敏，编码异常时降级不中断。

    --log-file 挂上后同时写一份到文件（追加、UTF-8）；写文件失败只跳过文件那一路，
    绝不影响控制台输出与主流程（磁盘满/句柄被关都不该让同步任务挂掉）。
    """
    global _console_patched
    global _sink_write_warned
    if not _console_patched:
        # check-then-set 放进锁里：多线程首次调用时不会重复执行 setup_console
        # （TextIOWrapper.reconfigure 不是线程安全的）
        with _lock:
            if not _console_patched:
                setup_console()
                _console_patched = True
    try:
        safe_msg = redact(msg)
    except Exception:  # noqa: BLE001 - 脱敏失败绝不能把日志/业务打挂
        # 但不能退化为原文：异常消息里很可能就是凭证——只留占位，宁可丢日志内容
        safe_msg = "[脱敏失败，原文已省略]"
    line = f"[{datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}] {safe_msg}"
    # print 与 sink 写入都放在锁外：慢速目标（管道压满、NFS/满盘上的 --log-file）只会
    # 拖慢这条日志本身，不该把全局 _lock 占住——否则其它线程的 log/add_log_sink/
    # remove_log_sink 会一起卡死，整个进程表现为停滞
    try:
        try:
            print(line, flush=True)
        except UnicodeEncodeError:
            encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
            print(line.encode(encoding, "replace").decode(encoding, "replace"), flush=True)
    except (OSError, ValueError, RuntimeError, AttributeError):
        # stdout 管道已关闭/断管（BrokenPipeError、`| head` 提前退出、句柄被关；
        # sys.stdout 属性缺失/解释器收尾等极端形态会抛 RuntimeError/AttributeError）：
        # 日志函数不能反过来把业务打挂（写文件的那一路下面还有自己的兜底）
        pass
    with _lock:
        sinks = list(_sinks)  # 快照：写的时候不持锁
    for handle in sinks:
        try:
            handle.write(line + "\n")
            handle.flush()
        except Exception as exc:  # noqa: BLE001 - 日志失败绝不能打挂业务（见下）
            # 这里必须兜住**所有**异常：sink 是外部句柄，写失败的花样不受控
            # （磁盘满/句柄已关/被塞了 None 之类），而"日志不得打挂业务"是硬约定。
            # 但必须可见一次，且区分"预期内的 I/O 失败"与"看起来是编程错误"
            if not _sink_write_warned:
                _sink_write_warned = True
                kind = "" if isinstance(exc, (OSError, ValueError)) else "（疑似代码缺陷）"
                msg = redact(str(exc))
                try:
                    print(
                        f"警告：--log-file 写入失败{kind}（后续同类错误不再提示）：{type(exc).__name__}: {msg}",
                        file=sys.stderr,
                    )
                except (OSError, ValueError, RuntimeError, AttributeError):
                    # stderr 也断管/被关（含属性缺失的极端形态）：放弃提示，但绝不让 log() 抛出去
                    pass


# =============================================================================
# 脱敏：日志/异常里不出现密钥、签名、token
# =============================================================================
# 密钥字段名按「词」判断：先按下划线/中划线/驼峰切开再看每个词，这样
# accessToken / client_secret / X-Api-Key 都能认出来，而 task=? 不会因为含 "sk" 被误伤
_SENSITIVE_WORDS = {
    "sign",
    "signature",
    "sig",
    "token",
    "secret",
    "password",
    "passwd",
    "authorization",
    "auth",
    "apikey",
    "key",
    "accesskey",
    "sk",
    "ak",
    # 常见简写与 scheme 名：?pwd= / ?pw= / ?pass= / bearer: <token>
    "pwd",
    "pw",
    "pass",
    "bearer",
    # 连写形态：?appkey= / ?appsecret=（无下划线时词切分切不出 "key"，切不出就漏遮）
    "appkey",
    "appsecret",
}
_WORD_RE = re.compile(r"[A-Z]+(?![a-z])|[A-Z][a-z0-9]*|[a-z0-9]+")
# 参数名做左边界限制（不用 \b：下划线在正则里算词字符，client_secret 会被漏掉）
_QUERY_RE = re.compile(r"(?i)(?<![A-Za-z0-9_])([A-Za-z0-9_.\-]{1,64})([\"']?)([:=])([ \t]*)([^&\s\"']+)")
# 敏感键的值吃到行尾/`&`为止：`password=my secret` 原来只遮 "my"、"secret" 明文留下
# （口令短语很常见）。负向先行断言只挡「引号后紧跟 ***」的已遮罩文本（避免把
# `"secret_key": "***", "page": 2` 整行再吞一遍）；未闭合引号（password="abc 被日志
# 截断）或「带引号的键」+ 不带引号的值（\"password\": my secret）必须走这条兜底——
# 否则 KV/JSON 要收尾引号、常规 QUERY 的值类不吃引号，三套规则全绕过
_QUERY_SPACE_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9_])(?P<key>[A-Za-z0-9_.\-]{1,64})(?P<q>[\"']?)(?P<sep>\s*[:=]\s*)(?!\s*[\"']?\s*\*\*\*)(?P<val>[^&\n]+)"
)


def _mask_spaced_values(text: str) -> str:
    """敏感键 + 无引号值遮到行尾/`&`（口令短语含空格不被第一个词截断）。

    用扫描器而不是一次 sub：非敏感键的贪婪值会吞掉其后的 k=v（sub 不重叠），
    这里非敏感只前移到"值起点"继续扫，后续键照常处理。
    """
    parts: list[str] = []
    pos = 0
    while True:
        match = _QUERY_SPACE_RE.search(text, pos)
        if not match:
            parts.append(text[pos:])
            break
        parts.append(text[pos : match.start()])
        head = f"{match.group('key')}{match.group('q')}{match.group('sep')}"
        if _is_sensitive_key(match.group("key")):
            parts.append(head + "***")
            pos = match.end()
        else:
            parts.append(head)
            pos = match.start("val")
    return "".join(parts)


# 同时认单引号：异常里直接插值的 dict（f"{cfg}"）和 repr（{exc!r}）都是单引号形态。
# 值用「回引号」收尾而不是 [^"']*：repr 对「值里含单引号」的串会改用双引号包裹。
# 值体里 (?!\\.) 让两个分支互斥：否则一串反斜杠会让回溯指数爆炸（同 api2ods/sftp2ods 的修复）。
_JSON_RE = re.compile(r"""(?i)(["']([^"']{1,64})["']\s*:\s*)(?P<q>["'])((?:\\.|(?!\\.)(?!(?P=q))[\s\S])*)(?P=q)""")
# 键不带引号、值带引号（access_token='t-xxx' / app_secret: "xx"）：f-string 的 !r 插值与
# repr 的输出正好是这种形态，而 _QUERY_RE 的值部分 [^&\s"']+ 不吃引号——行中出现的这类
# 取值会整段漏遮（行首的由 _HEADER_RE 兜底，行中不会）。值体与 _JSON_RE 同款互斥分支，
# 转义引号与「另一种引号出现在值里」（repr 会改用另一种引号包裹）都能认。
_KV_QUOTED_RE = re.compile(
    r"(?i)(?<![A-Za-z0-9_])([A-Za-z0-9_.\-]{1,64})(\s*[:=]\s*)(?P<q>[\"'])((?:\\.|(?!\\.)(?!(?P=q))[^\n])*)(?P=q)"
)
_BEARER_RE = re.compile(r"(?i)(\b(?:bearer)\s+)[A-Za-z0-9._~+/=-]{6,}")
_BASIC_RE = re.compile(r"(?i)(authorization:\s*basic\s+)\S{8,}")
# URL 里的 userinfo（https://user:pass@host）：代理/接口地址常把账号密码写在地址里，
# requests 的连接类异常消息会原样回显整条地址。scheme 部分限长（{0,63}）：无上限时
# 在长小写字母数字串上会在每个起始位置贪婪回扫（实测 20KB 要 10 秒、40KB 要 50 秒）
_URL_AUTH_RE = re.compile(r"(?i)([a-z][a-z0-9+.\-]{0,63}://[^/\s:@]+):([^\s/]+)@")
# 请求头行：'X-Api-Key: xxx' / 'X-Api-Key=xxx'（requests 抛错时带的 headers 是这种形态）。
# 值要吃到行尾：只吃第一个词的话 "Authorization: Token abc…" 会变成 "*** abc…"（凭证明文留下）
_HEADER_RE = re.compile(r"(?im)^(\s*([A-Za-z0-9_.\-]{1,64})\s*[:=]\s*)(.+)$")
# 飞书 webhook 形态：open.feishu.cn/open-apis/bot/v2/hook/<id>；scheme 部分可选——
# requests 的异常消息里只带 URL 的路径（"Max retries exceeded with url: /open-apis/..."）
# 可选前缀限长（{0,1024}）：无上限的惰性展开在"超长且无空白、又没有 /hook/"的
# 文本上会二次回溯（每个 https:// 起点都要扫到 token 末尾）；限长后保持线性。
# 真实 webhook 的 URL 前缀远短于 1024 字符。
_WEBHOOK_RE = re.compile(r"(?i)((?:https?://[^\s\"']{0,256}?)?/hook/)[A-Za-z0-9\-_]{4,}")
# 飞书 webhook 里的 hook id（/hook/<id>）：报错/日志里常只出现后半截
_WEBHOOK_ID_RE = re.compile(r"/hook/([A-Za-z0-9\-_]{4,})")


def _is_sensitive_key(name) -> bool:
    """字段名是否含密钥语义（按词切分，避免 "task=1" 这类含 sk 的普通参数被误伤）。"""
    original = str(name)
    # 切词保留原大小写：驼峰分支（[A-Z][a-z0-9]*）在已经 lower 的串上永远匹配不到
    words = _WORD_RE.findall(original)
    if any(word.lower() in _SENSITIVE_WORDS for word in words):
        return True
    lowered = original.lower()
    # 不用分隔符的写法：accesstoken / secretkey / accesskeyid
    return any(
        word in lowered
        for word in (
            "token",
            "secret",
            "password",
            "passwd",
            "signature",
            "apikey",
            "accesskey",
            "secretkey",
            "privatekey",
            "signkey",
            "keyid",
        )
    )


# 脱敏递归的深度上限：正常文本（嵌套 JSON、值里再嵌 k=v）深度 ≤3；构造性文本
# （如 "a=b=c=…" 上千个等号、第三方响应体里的任意内容）能把递归喂到 Python 上限，
# 把"脱敏"本身打成 RecursionError。到上限按"宁可多脱敏"整段遮掉。
_MAX_REDACT_DEPTH = 10


def _redact_shapes(text: str, _depth: int = 0) -> str:
    """形态级脱敏：把「认得出的凭证写法」里的值换成 ***。

    覆盖：Bearer/Basic、JSON/配置片段（"secret_key": "…"）、带引号值（token='…'）、
    URL userinfo、飞书 webhook、URL query（?token=…）、请求头行（X-Api-Key: …）。
    规则顺序按"认得出的形态"从严到宽：Bearer/Basic 与配置片段先处理——query 规则会按
    `=` / `:` 把值截断，先跑它的话 `header: 'Authorization=Bearer abc'` 会被切成
    `Authorization=`，后面的 Bearer 规则就再也匹配不到了（令牌原样留在日志里）。

    _depth：内部递归深度，调用方不要传（见 _MAX_REDACT_DEPTH）。
    """
    if _depth >= _MAX_REDACT_DEPTH:
        return "***"

    def _bearer(match: re.Match) -> str:
        """Bearer / Basic 形态：scheme 保留，值换掉。"""
        return match.group(1) + "***"

    def _url_auth(match: re.Match) -> str:
        """URL 里的 userinfo：只留账号，密码换掉（scheme://user:***@host）。"""
        return f"{match.group(1)}:***@"

    def _webhook(match: re.Match) -> str:
        """飞书 webhook：保留 /hook/ 路径，hook id（凭证）换掉。"""
        return f"{match.group(1)}***"

    def _json(match: re.Match) -> str:
        """JSON/配置片段里的 "key": "value"：只吃字符串值，保留引号结构。"""
        qchar = match.group("q")
        prefix, value = match.group(1), match.group(4)
        if _is_sensitive_key(match.group(2)):
            return f"{prefix}{qchar}***{qchar}"
        # 值本身可能是"被 JSON 编码成字符串的一整段 JSON"：内层引号是 \"，任何按引号认
        # 边界的规则都匹配不到。反转义 → 脱敏 → 再转义回去
        if '\\"' in value:
            try:
                decoded = json.loads(f'"{value}"')
            except (ValueError, RecursionError):
                # RecursionError：值里含超深嵌套（构造性 payload）时 json.loads 会递归爆栈；
                # 脱敏流程不能反过来把进程打崩，按"反转义失败"处理
                decoded = None
            if decoded is not None:
                redacted = _redact_shapes(decoded, _depth + 1)
                if redacted != decoded:
                    return f"{prefix}{qchar}{json.dumps(redacted, ensure_ascii=False)[1:-1]}{qchar}"
        # 键名不敏感时值里也可能藏着密钥（'X-Api-Key: xxx' 头行、查询串、嵌套结构），递归一次
        return f"{prefix}{qchar}{_redact_shapes(value, _depth + 1)}{qchar}"

    def _kv_quoted(match: re.Match) -> str:
        """`key='value'` / `key: "value"`（键无引号、值有引号）：命中密钥词才遮值。"""
        key, gap, qchar, value = (match.group(1), match.group(2), match.group(3), match.group(4))
        head = f"{key}{gap}{qchar}"
        if _is_sensitive_key(key):
            return f"{head}***{qchar}"
        # 键名不敏感时值里也可能藏着密钥（'note=access_token=abc'）：递归一次兜底
        redacted = _redact_shapes(value, _depth + 1)
        if redacted != value:
            return f"{head}{redacted}{qchar}"
        return match.group(0)

    def _query(match: re.Match) -> str:
        """URL 查询串 / `"key": 12345`：命中密钥词才替换，其余递归兜底。"""
        key, quote, sep, gap, value = (match.group(1), match.group(2), match.group(3), match.group(4), match.group(5))
        head = f"{key}{quote}{sep}{gap}"  # 原样保留引号/分隔符/空白，只换值
        if _is_sensitive_key(key):
            return f"{head}***"
        if "%" in value:
            try:
                decoded = unquote(value)
            except Exception:  # noqa: BLE001 - 解码失败按原文处理
                decoded = value
            if decoded != value and _redact_shapes(decoded, _depth + 1) != decoded:
                return f"{head}***"
        return f"{head}{_redact_shapes(value, _depth + 1)}"

    def _header(match: re.Match) -> str:
        """多行文本里的一行 "Header: value"：只吃头名命中密钥词的行。"""
        if _is_sensitive_key(match.group(2)):
            return f"{match.group(1)}***"
        return f"{match.group(1)}{_redact_shapes(match.group(3), _depth + 1)}"

    out = str(text)
    out = _BEARER_RE.sub(_bearer, out)
    out = _BASIC_RE.sub(_bearer, out)
    # URL userinfo 规则必须同时出现 "://" 与 "@" 才可能匹配，先做一次 O(n) 预判省掉
    # 一次无谓的全量扫描；scheme 部分已限长（见 _URL_AUTH_RE），配合预判保持线性
    if "://" in out and "@" in out:
        out = _URL_AUTH_RE.sub(_url_auth, out)
    out = _WEBHOOK_RE.sub(_webhook, out)
    # JSON 片段规则至少要出现引号才可能匹配：没引号的长文本直接跳过，省一遍全量扫描
    if '"' in out or "'" in out:
        out = _JSON_RE.sub(_json, out)
        out = _KV_QUOTED_RE.sub(_kv_quoted, out)
    # 敏感键 + 无引号值先整体遮到行尾（password=my secret），再走常规 query 扫描
    out = _mask_spaced_values(out)
    out = _QUERY_RE.sub(_query, out)
    # 头行规则放最后：它最宽松（只要求行首是 name: value），前面几条先处理过更精确的形态
    return _HEADER_RE.sub(_header, out)


_SECRET_MIN_LEN = 6
"""短于该长度的密钥值不做值级替换：`1` / `ok` 这种在普通文本里出现概率太高，
替换只会把报错信息搅乱，而真实凭证不会这么短。"""


def redact(text) -> str:
    """形态级 + 值级双重脱敏：日志/异常里不出现密钥、签名、token。

    值级：把 job 里登记过的密钥值（`_SECRETS`）原样出现的部分换成 ***——
    接口把凭证写进自由文本（`Invalid token: sk-xxx`）时，只有按值精确替换才挡得住。
    明文之外还替换其 URL 编码形态（quote / quote_plus）：接口若把凭证以 `%2D` 这类编码
    形态回显（周围又没有 `access_token=` 之类键名、形态规则认不出），只替换明文会漏遮。
    形态级：Bearer/Basic、JSON 片段、query、头行、URL userinfo、飞书 webhook 这些
    「认证文书的写法」统一遮掉——接口回显的是变形形态（没回显原值）时靠它兜底。
    两条路互不替代：值级先遮、再走形态规则。密钥一旦进日志就等于泄露，宁可多脱敏。
    """
    if not text:
        return text
    out = str(text)  # 宽容度：调用方直接传异常对象/数字也不会炸
    with _lock:
        # 快照必须在锁内取：reset_secret_values 的 clear+extend 与这里并发时，
        # 直接迭代模块级列表会读到清空/半替换的中间态、漏遮本轮密钥
        secrets_snapshot = [s for s in _SECRETS if isinstance(s, str)]
    for secret in sorted(secrets_snapshot, key=len, reverse=True):
        # 长值先替：短值先替会把长密钥切成半截、留下可辨认的碎片
        # 短于 _SECRET_MIN_LEN 的值（`1` / `ok`）出现在普通文本里太常见，值级替换会把报错搅乱
        if not secret or len(secret) < _SECRET_MIN_LEN:
            continue
        # 编码形态的说法：`+`/`/`/`=` 会被 quote 编码；部分编码器更激进，连 `-` 也编码
        # 成 %2D——明文、quote、quote_plus 与"非字母数字全编码"四种形态一起替换
        # 按字节（不是 chr(b) 的 Latin-1 字符）判断：>=0x80 的字节在 Latin-1 里常恰好是
        # "字母"（0xE5='å'），原样保留会让含中文的密钥生成错误的编码变体、漏遮
        aggressive = "".join(
            f"%{b:02X}" if not (b < 128 and chr(b).isalnum()) else chr(b)
            for b in secret.encode("utf-8", "surrogatepass")
        )
        try:
            encoded_forms = (quote(secret, safe=""), quote_plus(secret))
        except UnicodeError:
            # 含孤立代理字符的密钥（surrogateescape 解出的路径名被登记为敏感值）：
            # quote 内部 strict 编码会抛——跳过编码变体，绝不让脱敏反过来打崩业务
            encoded_forms = ()
        for variant in (secret, *encoded_forms, aggressive):
            if variant:
                out = out.replace(variant, "***")
    return _redact_shapes(out)


def redact_secrets(values, text) -> str:
    """值级 + 形态级双重脱敏：配置里的密钥值原样出现时也遮掉。

    形态规则认的是 `token=…` / `Bearer …` / `"key": "value"` 这类写法；接口若把凭证
    写进自由文本（`Invalid token: sk-xxx`），只有按配置值精确替换才挡得住。
    明文之外还替换其 URL 编码形态（quote / quote_plus）：接口回显编码过的凭证时也能遮住。
    两条路互不替代，值级先遮、再走 redact()（它还带 _SECRETS 值级 + 形态规则兜底）。
    """
    if not text:
        return text
    out = str(text)
    if not isinstance(values, (list, tuple, set, frozenset)):
        # 标量（单个字符串/数字）会让下面的 for 抛 TypeError 或把字符串拆成单字符：
        # 值与 api2ods/sftp2ods 同口径，按"只有一个密钥"包一层
        values = [values]
    # 与 reset_secret_values / _leaf_strings 同口径：数字型密钥（ID 类凭证写起来就是数字）
    # str 化后参与遮蔽，不能按 isinstance(str) 静默丢弃（否则该凭证在报错文本里保持明文）
    secrets = {str(s) for s in (values or ()) if isinstance(s, (str, int, float)) and not isinstance(s, bool)}
    secrets = {s for s in secrets if len(s) >= _SECRET_MIN_LEN}
    for secret in sorted(secrets, key=len, reverse=True):
        # 按字节（不是 chr(b) 的 Latin-1 字符）判断：>=0x80 的字节在 Latin-1 里常恰好是
        # "字母"（0xE5='å'），原样保留会让含中文的密钥生成错误的编码变体、漏遮
        aggressive = "".join(
            f"%{b:02X}" if not (b < 128 and chr(b).isalnum()) else chr(b)
            for b in secret.encode("utf-8", "surrogatepass")
        )
        try:
            encoded_forms = (quote(secret, safe=""), quote_plus(secret))
        except UnicodeError:
            # 含孤立代理字符的密钥（surrogateescape 解出的路径名被登记为敏感值）：
            # quote 内部 strict 编码会抛——跳过编码变体，绝不让脱敏反过来打崩业务
            encoded_forms = ()
        for variant in (secret, *encoded_forms, aggressive):
            if variant:
                out = out.replace(variant, "***")
    return redact(out)


def _leaf_strings(value) -> list[str]:
    """递归收集 dict/list/tuple/set 里的字符串叶子（数字也按 str 收：ID 类密钥写起来就是数字）。"""
    if isinstance(value, dict):
        return [item for sub in value.values() for item in _leaf_strings(sub)]
    if isinstance(value, (list, tuple, set, frozenset)):
        return [item for sub in value for item in _leaf_strings(sub)]
    if isinstance(value, str):
        return [value]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return [str(value)]
    return []


def _webhook_values(value) -> list[str]:
    """webhook 除整条 URL 外，再收集裸 hook id（形态脱敏认 URL，裸 id 要靠值级遮）。"""
    result: list[str] = []
    for part in _leaf_strings(value):
        result.append(part)
        match = _WEBHOOK_ID_RE.search(part)
        if match:
            result.append(match.group(1))
    return result


def collect_secret_values(job: dict) -> list[str]:
    """收集作业配置里"可能被接口回显"的密钥字面量，作为值级脱敏的输入。

    形态识别（redact）盖不住接口把凭证写进**自由文本**的报错（如
    `Invalid token: sk-xxx`），但这类回显的内容必然是请求里用过的凭证，而凭证就
    躺在这几处配置里：feishu.app_secret、maxcompute.access_key_*、
    freshness.webhook（hook id 即凭证）。返回值已去重、过滤短值并按从长到短排序。
    """
    if not isinstance(job, dict):
        return []
    values: list[str] = []
    feishu = job.get("feishu")
    if isinstance(feishu, dict):
        for key, val in feishu.items():
            if _is_sensitive_key(str(key)):
                values += _leaf_strings(val)
    maxcompute = job.get("maxcompute")
    if isinstance(maxcompute, dict):
        for key, val in maxcompute.items():
            if _is_sensitive_key(str(key)):
                values += _leaf_strings(val)
    freshness = job.get("freshness")
    if isinstance(freshness, dict):
        values += _webhook_values(freshness.get("webhook"))
    cleaned = (value.strip() for value in values)
    return sorted({value for value in cleaned if len(value) >= _SECRET_MIN_LEN}, key=len, reverse=True)


def _api_err(data) -> str:
    """接口返回里的 code/msg → 报错片段（data 不是对象时也能安全展示）。

    返回值统一过 redact：调用方把它拼进 SystemExit/异常，而 SystemExit 文本可能被
    调度器直接打到 stderr，不经过 log() 的脱敏——接口回显的网关错误页里可能带
    `https://user:pass@host` 或 `?access_token=xxx`，msg 里也常回显参数。
    """
    if isinstance(data, dict):
        return redact(f"code={data.get('code')} msg={data.get('msg')}")
    # 先脱敏再截断：先截 200 字符会把成对的引号截断（值只剩开引号），所有按「成对引号」
    # 认边界的形态规则失配，凭证原样漏出
    return f"接口返回不是 JSON 对象：{redact(str(data))[:200]}"


# =============================================================================
# 运行锁（与 api2ods 同款）：避免定时任务与手动重跑同时写同一个分区
# =============================================================================
class RunLock:
    """进程级运行锁：避免同一台机器上定时任务与手动执行（或两个实例）同时跑。

    - Linux / macOS：flock 排它锁；
    - Windows：msvcrt 首字节锁（同样是排它、非阻塞）；
    - 两种锁都没有的平台：退化为"不阻塞"，不挡运行；
    - 锁随进程退出自动释放，进程被 kill 也由内核释放，不会残留死锁；
    - 锁文件在各自机器的本地磁盘上：只在单机内互斥，本地与服务器同时跑同一作业没有保护，
      正式调度请固定跑在一台机器上。
    """

    def __init__(self, path: pathlib.Path):
        self.path = path
        self.fh = None

    def __enter__(self):
        """拿锁；已被别人持有就抛 SystemExit（不等待），拿不到直接让本次运行退出。"""
        if fcntl is None and msvcrt is None:
            return self
        try:
            # "a+" 而不是 "w"：w 会在打开时把文件截断，持锁进程刚写进去的 pid 就被抹掉了
            # encoding + errors=replace：locale 非 UTF-8 时读写中文主机名不会抛 UnicodeError
            # POSIX 上 O_NOFOLLOW：锁路径若是符号链接，拒绝跟随（避免锁到别人的文件上）
            open_kwargs = {"encoding": "utf-8", "errors": "replace"}
            nofollow = getattr(os, "O_NOFOLLOW", 0)
            if nofollow:
                # 显式传 mode：io.open 的 opener 约定只有 (path, flags)，os.open 缺省 mode=0o777
                # （umask 决定最终权限，umask=0 时锁文件 0777）；锁文件按 0600 创建
                open_kwargs["opener"] = lambda path, flags, _nf=nofollow: os.open(path, flags | _nf, 0o600)
            self.fh = open(self.path, "a+", **open_kwargs)
        except (OSError, UnicodeError) as exc:
            raise SystemExit(
                f"无法创建运行锁文件 {self.path}（{exc}）；请检查该路径所在目录是否存在/可写，或用 --job 指定别处的作业"
            ) from exc
        acquired = False
        try:
            if not _try_lock(self.fh):
                holder = ""
                try:
                    self.fh.seek(0)
                    holder = (self.fh.read(200) or "").strip()
                except (OSError, UnicodeError):
                    pass
                try:
                    self.fh.close()
                except (OSError, ValueError):
                    pass
                self.fh = None
                detail = f"（持有者：{holder}）" if holder else ""
                raise SystemExit(
                    f"已有任务在运行{detail}（锁文件 {self.path}），本次退出。"
                    f"该锁只在同一台机器上生效，跨机并发（如本地与服务器同时跑）请自行避免。"
                )
            acquired = True
            try:
                # 拿到锁之后才截断+写标记：拿不到锁时绝不能动内容
                self.fh.seek(0)
                self.fh.truncate()
                self.fh.write(
                    f"{os.getpid()} {socket.gethostname()} {datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}"
                )
                self.fh.flush()
            except (OSError, UnicodeError):  # 写标记只是给人看，失败不影响加锁
                pass
            return self
        except BaseException:
            # 锁已拿到但随后失败（含 UnicodeEncodeError）：__enter__ 没返回则 __exit__ 不会跑，必须在这里释放
            if self.fh is not None:
                try:
                    if acquired:
                        _unlock(self.fh)
                finally:
                    try:
                        self.fh.close()
                    except (OSError, ValueError):
                        pass
                    self.fh = None
            raise

    def __exit__(self, *exc_info):
        """解锁并关句柄；锁文件本身保留（不删文件，避免削掉别人的锁）。"""
        if self.fh is None:
            return
        try:
            _unlock(self.fh)
        finally:
            try:
                self.fh.close()
            except (OSError, ValueError):
                pass
            self.fh = None


def _try_lock(fh) -> bool:
    """对已打开的文件加排它锁；别人拿着锁时返回 False（不阻塞等待）。

    三类 OSError 必须分开：忙 = False（真有人持锁）；"文件系统不支持锁"（ENOLCK/ENOTSUP，
    如 NFS/只读挂载）= 告警一次后按无锁继续；其余上抛，别让它冒充"已有任务在运行"。
    """
    if fcntl is not None:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError as exc:
            return _lock_oserror_result(exc)
    if msvcrt is not None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError as exc:
            return _lock_oserror_result(exc)
    return True  # 两种锁都没有：不阻塞（退回"无锁"行为）


# flock / msvcrt 忙（别人正持锁）。EDEADLK/EDEADLOCK 会出现于 Windows 的 LK_NBLCK。
_LOCK_BUSY = {errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK}
for _name in ("EDEADLK", "EDEADLOCK"):
    if hasattr(errno, _name):
        _LOCK_BUSY.add(getattr(errno, _name))
# 文件系统不支持锁 / 锁资源耗尽：不是"别人在跑"，不能伪装成"已有任务在运行"
_LOCK_UNSUPPORTED = {
    value for value in (getattr(errno, name, None) for name in ("ENOLCK", "ENOTSUP", "EOPNOTSUPP")) if value
}
_lock_warned = False


def reset_lock_warning() -> None:
    """清掉"文件系统不支持锁"的告警去重标记（每次运行开始时调，与 _SECRETS 同口径）。"""
    global _lock_warned
    _lock_warned = False


def _lock_oserror_result(exc: OSError) -> bool:
    """加锁失败的 OSError：忙 → False；文件系统不支持锁 → 默认 fail-closed 拒绝执行，
    除非显式设置 FEISHU2ODS_ALLOW_NO_LOCK=1 接受无互斥风险。

    拿不到 errno（部分 Windows msvcrt 失败形态）时保守按"忙"处理：宁可提示得含糊一点，
    也不能把"确实有别的实例在跑"误判成"没有锁"而放过去（并发写同一分区才是真事故）。
    """
    global _lock_warned
    code = exc.errno
    if code is None or code == 0 or code in _LOCK_BUSY:
        return False
    if code in _LOCK_UNSUPPORTED:
        if os.environ.get("FEISHU2ODS_ALLOW_NO_LOCK", "").strip() == "1":
            if not _lock_warned:
                _lock_warned = True
                log(
                    f"  警告：文件系统不支持运行锁（{exc}）；FEISHU2ODS_ALLOW_NO_LOCK=1 "
                    f"已显式接受无互斥风险，本次不加锁继续"
                )
            return True
        # fail-closed：无锁继续会让两个实例并发写同一作业/表（purge/rename 互拆、数据被
        # 静默覆盖），宁可拒绝执行——把锁目录指到支持锁的本地磁盘再跑
        raise SystemExit(
            f"运行锁所在文件系统不支持加锁（{exc}）：拒绝无锁执行（并发实例会互相写坏数据）。"
            f"请用 FEISHU2ODS_LOCK_DIR 把锁目录指到本地磁盘，"
            f"或确认无人并发时设置 FEISHU2ODS_ALLOW_NO_LOCK=1"
        )
    raise


def _unlock(fh) -> None:
    """释放锁；释放失败也没关系——进程退出时内核会兜底释放，不该因此让任务报错。"""
    if fcntl is not None:
        try:
            fcntl.flock(fh, fcntl.LOCK_UN)
        except OSError:
            pass
    elif msvcrt is not None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass


def _lock_base_dir(root: pathlib.Path | None = None) -> pathlib.Path:
    """运行锁目录解析（lock_path 与 table_lock_path 共用）。

    优先级：root 参数（单元测试用）> 环境变量 FEISHU2ODS_LOCK_DIR > 工具目录下
    .run-locks/ > 系统临时目录。`FEISHU2ODS_LOCK_DIR` 用于把锁钉在与运行者身份/环境
    无关的同一目录上——否则"root 能写工具目录、普通用户退回 TMPDIR"这类差异会让同一
    作业的两个实例锁在不同文件上，互斥静默失效。
    """
    if root is not None:
        base = pathlib.Path(root)
        base.mkdir(parents=True, exist_ok=True)
        return base
    override = os.environ.get("FEISHU2ODS_LOCK_DIR", "").strip()
    if override:
        base = pathlib.Path(override).expanduser()
        try:
            base.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            # 显式指定的目录不可用要立刻失败：静默换目录等于互斥失效，正是这个开关要防的事
            raise SystemExit(f"FEISHU2ODS_LOCK_DIR 指定的锁目录不可用（{exc}）：{base}") from exc
        return base
    candidates = [
        pathlib.Path(__file__).resolve().parent / ".run-locks",
        pathlib.Path(tempfile.gettempdir()) / "feishu2ods-locks",
    ]
    for index, base in enumerate(candidates):
        try:
            base.mkdir(parents=True, exist_ok=True)
            # 探测文件名必须唯一（mkstemp）：并发启动时共享的探测文件会被别的进程删掉，
            # 导致"静默"落到下一个候选目录——同一作业的两个实例锁在不同路径上，互斥失效
            handle, probe = tempfile.mkstemp(prefix=".probe-", dir=str(base))
            os.close(handle)
        except OSError:
            continue
        try:
            os.unlink(probe)
        except OSError:
            # 探测文件删不掉（少见：只读挂载/文件被占用）不该把整个目录判成不可用——
            # 否则会静默换目录，同一作业的两个实例锁在不同路径上，互斥失效。
            pass
        if index > 0:
            # 退回目录是按用户/环境解析的（TMPDIR、macOS 的 /var/folders、systemd PrivateTmp）：
            # 不同身份/环境跑同一作业可能拿到不同目录，互斥静默失效。至少把事实说出来
            log(
                f"  提示：工具目录不可写，运行锁放在 {base}；"
                f"若存在多用户/多环境混跑，请用 FEISHU2ODS_LOCK_DIR 固定同一锁目录"
            )
        return base
    # 两个候选目录都探测失败（极罕见：工具目录与系统临时目录都不可写）：最后退回系统
    # 临时目录。路径可能与其它实例的锁目录不一致（互斥可能失效），必须显式提示
    fallback = pathlib.Path(tempfile.gettempdir())
    log(
        f"  警告：工具目录与系统临时目录都不可写，运行锁临时退回 {fallback}；"
        f"请用 FEISHU2ODS_LOCK_DIR 指定一个可写的固定锁目录，否则并发保护可能失效"
    )
    return fallback


def lock_path(job_path: pathlib.Path, root: pathlib.Path | None = None) -> pathlib.Path:
    """每个作业一把运行锁（不同作业可并行，同一作业不会重复跑）。

    锁名带路径哈希：jobs/a/api.json 与 jobs/b/api.json 同名不同作业，只按文件名会互相阻塞。
    注意：锁文件在各机器本地磁盘上，只保证单机互斥（跨机并发仍会互相写坏，正式跑固定一台）。
    """
    resolved = pathlib.Path(job_path).expanduser().resolve()
    stem = resolved.stem or "job"
    # sha256 截 16 位十六进制：sha1 只取 8 位（32 位）时不同作业有可观的碰撞概率，
    # 撞了会互相阻塞（解锁时还可能删错对方的锁）；与 sftp2ods / api2ods 的锁同口径
    digest = hashlib.sha256(os.fsencode(str(resolved).encode("utf-8"))).hexdigest()[:16]
    return _lock_base_dir(root) / f"{stem}-{digest}.lock"


def table_lock_path(project: str, table: str, root: pathlib.Path | None = None) -> pathlib.Path:
    """目标表级运行锁：不同作业（两份配置）指向同一张表时也要串行。

    作业锁只管"同一个作业不重复跑"：jobs/a.json 与 jobs/b.json 写同一张表时互不阻塞，
    一边的 purge 会清掉另一边正在写的临时分区、rename 交叉执行，最终一方数据被静默覆盖
    （各自的写后核对只校验自己这批行数，发现不了）。锁名直接用「项目.表名」：两者都已过
    标识符白名单（字母/数字/下划线），做文件名安全；同一张表的任何写法都会撞到同一把锁。
    """
    return _lock_base_dir(root) / f"table-{project}.{table}.lock"
