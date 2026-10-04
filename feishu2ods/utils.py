# -*- coding: utf-8 -*-
"""通用工具：日志（带时间戳+脱敏+可选写文件）、运行锁（flock/msvcrt）、_api_err、脱敏。"""

from __future__ import annotations

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
    """把日志再写一份到文件（--log-file），句柄由调用方负责关闭。"""
    with _lock:
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
    line = f"[{datetime.now(CN_TZ).strftime('%Y-%m-%d %H:%M:%S')}] {redact(msg)}"
    with _lock:
        try:
            print(line, flush=True)
        except UnicodeEncodeError:
            encoding = getattr(sys.stdout, "encoding", None) or "utf-8"
            print(line.encode(encoding, "replace").decode(encoding, "replace"), flush=True)
        for handle in _sinks:
            try:
                handle.write(line + "\n")
                handle.flush()
            except Exception as exc:  # noqa: BLE001 - 日志文件问题不影响主流程
                if not _sink_write_warned:
                    _sink_write_warned = True
                    print(
                        f"警告：--log-file 写入失败（后续同类错误不再重复提示）：{type(exc).__name__}: {exc}",
                        file=sys.stderr,
                    )


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
_QUERY_RE = re.compile(r"(?i)(?<![A-Za-z0-9_])([A-Za-z0-9_.\-]{1,64})=([^&\s\"']+)")
# 同时认单引号：异常里直接插值的 dict（f"{cfg}"）和 repr（{exc!r}）都是单引号形态。
# 值用「回引号」收尾而不是 [^"']*：repr 对「值里含单引号」的串会改用双引号包裹。
# 值体里 (?!\\.) 让两个分支互斥：否则一串反斜杠会让回溯指数爆炸（同 api2ods/sftp2ods 的修复）。
_JSON_RE = re.compile(r"""(?i)(["']([^"']{1,64})["']\s*:\s*)(?P<q>["'])((?:\\.|(?!\\.)(?!(?P=q))[\s\S])*)(?P=q)""")
_BEARER_RE = re.compile(r"(?i)(\b(?:bearer)\s+)[A-Za-z0-9._~+/=-]{6,}")
_BASIC_RE = re.compile(r"(?i)(authorization:\s*basic\s+)\S{8,}")
# URL 里的 userinfo（https://user:pass@host）：代理/接口地址常把账号密码写在地址里，
# requests 的连接类异常消息会原样回显整条地址。scheme 部分限长（{0,63}）：无上限时
# 在长小写字母数字串上会在每个起始位置贪婪回扫（实测 20KB 要 10 秒、40KB 要 50 秒）
_URL_AUTH_RE = re.compile(r"(?i)([a-z][a-z0-9+.\-]{0,63}://[^/\s:@]+):([^/\s@]+)@")
# 请求头行：'X-Api-Key: xxx' / 'X-Api-Key=xxx'（requests 抛错时带的 headers 是这种形态）。
# 值要吃到行尾：只吃第一个词的话 "Authorization: Token abc…" 会变成 "*** abc…"（凭证明文留下）
_HEADER_RE = re.compile(r"(?im)^(\s*([A-Za-z0-9_.\-]{1,64})\s*[:=]\s*)(.+)$")
# 飞书 webhook 形态：open.feishu.cn/open-apis/bot/v2/hook/<id>；scheme 部分可选——
# requests 的异常消息里只带 URL 的路径（"Max retries exceeded with url: /open-apis/..."）
_WEBHOOK_RE = re.compile(r"(?i)((?:https?://[^\s\"']*?)?/hook/)[A-Za-z0-9\-_]{4,}")
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


def _redact_shapes(text: str) -> str:
    """形态级脱敏：把「认得出的凭证写法」里的值换成 ***。

    覆盖：Bearer/Basic、JSON/配置片段（"secret_key": "…"）、URL userinfo、飞书 webhook、
    URL query（?token=…）、请求头行（X-Api-Key: …）。
    规则顺序按"认得出的形态"从严到宽：Bearer/Basic 与配置片段先处理——query 规则会按
    `=` / `:` 把值截断，先跑它的话 `header: 'Authorization=Bearer abc'` 会被切成
    `Authorization=`，后面的 Bearer 规则就再也匹配不到了（令牌原样留在日志里）。
    """

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
            except ValueError:
                decoded = None
            if decoded is not None:
                redacted = _redact_shapes(decoded)
                if redacted != decoded:
                    return f"{prefix}{qchar}{json.dumps(redacted, ensure_ascii=False)[1:-1]}{qchar}"
        # 键名不敏感时值里也可能藏着密钥（'X-Api-Key: xxx' 头行、查询串、嵌套结构），递归一次
        return f"{prefix}{qchar}{_redact_shapes(value)}{qchar}"

    def _query(match: re.Match) -> str:
        """URL 查询串 / `key=value`：命中密钥词才替换，其余递归兜底。"""
        if _is_sensitive_key(match.group(1)):
            return f"{match.group(1)}=***"
        value = match.group(2)
        if "%" in value:
            try:
                decoded = unquote(value)
            except Exception:  # noqa: BLE001 - 解码失败按原文处理
                decoded = value
            if decoded != value and _redact_shapes(decoded) != decoded:
                return f"{match.group(1)}=***"
        return f"{match.group(1)}={_redact_shapes(value)}"

    def _header(match: re.Match) -> str:
        """多行文本里的一行 "Header: value"：只吃头名命中密钥词的行。"""
        if _is_sensitive_key(match.group(2)):
            return f"{match.group(1)}***"
        return f"{match.group(1)}{_redact_shapes(match.group(3))}"

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
    for secret in sorted(set(_SECRETS), key=len, reverse=True):
        # 长值先替：短值先替会把长密钥切成半截、留下可辨认的碎片
        # 短于 _SECRET_MIN_LEN 的值（`1` / `ok`）出现在普通文本里太常见，值级替换会把报错搅乱
        if not secret or len(secret) < _SECRET_MIN_LEN:
            continue
        for variant in (secret, quote(secret, safe=""), quote_plus(secret)):
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
    secrets = {secret for secret in (values or ()) if isinstance(secret, str) and len(secret) >= _SECRET_MIN_LEN}
    for secret in sorted(secrets, key=len, reverse=True):
        for variant in (secret, quote(secret, safe=""), quote_plus(secret)):
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
    """接口返回里的 code/msg → 报错片段（data 不是对象时也能安全展示）。"""
    if isinstance(data, dict):
        return f"code={data.get('code')} msg={data.get('msg')}"
    return f"接口返回不是 JSON 对象：{str(data)[:200]}"


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
                open_kwargs["opener"] = lambda path, flags, _nf=nofollow: os.open(path, flags | _nf)
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
    """对已打开的文件加排它锁；别人拿着锁时返回 False（不阻塞等待）。"""
    if fcntl is not None:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False
    if msvcrt is not None:
        try:
            fh.seek(0)
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False
    return True  # 两种锁都没有：不阻塞（退回"无锁"行为）


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


def lock_path(job_path: pathlib.Path, root: pathlib.Path | None = None) -> pathlib.Path:
    """每个作业一把运行锁（不同作业可并行，同一作业不会重复跑）。

    优先放工具目录下 .run-locks/；工具目录不可写（如 pip 装在只读位置）时退回系统临时目录；
    root 显式指定锁目录（单元测试用，避免往工具目录写测试残留）。
    锁名带路径哈希：jobs/a/api.json 与 jobs/b/api.json 同名不同作业，只按文件名会互相阻塞。
    注意：锁文件在各机器本地磁盘上，只保证单机互斥（跨机并发仍会互相写坏，正式跑固定一台）。
    """
    resolved = pathlib.Path(job_path).expanduser().resolve()
    stem = resolved.stem or "job"
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:8]
    name = f"{stem}-{digest}"
    if root is not None:
        base = pathlib.Path(root)
        base.mkdir(parents=True, exist_ok=True)
        return base / f"{name}.lock"
    candidates = [
        pathlib.Path(__file__).resolve().parent / ".run-locks",
        pathlib.Path(tempfile.gettempdir()) / "feishu2ods-locks",
    ]
    for base in candidates:
        try:
            base.mkdir(parents=True, exist_ok=True)
            # 探测文件名必须唯一（mkstemp）：并发启动时共享的探测文件会被别的进程删掉，
            # 导致"静默"落到下一个候选目录——同一作业的两个实例锁在不同路径上，互斥失效
            handle, probe = tempfile.mkstemp(prefix=".probe-", dir=str(base))
            os.close(handle)
            os.unlink(probe)
            return base / f"{name}.lock"
        except OSError:
            continue
    return pathlib.Path(tempfile.gettempdir()) / f"feishu2ods-{name}.lock"
