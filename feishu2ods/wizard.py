# -*- coding: utf-8 -*-
"""交互式建配置向导（--init）。"""

from __future__ import annotations

import getpass
import json
import os
import pathlib
import re
import sys
import tempfile

from .config import (
    DEFAULT_COLUMN,
    DEFAULT_FRESHNESS_LAG_DAYS,
    IDENT_RE,
    _require_feishu_id,
    validate_job,
)
from .fetch import fetch_field_sample
from .mc import DEFAULT_ENDPOINT


# =============================================================================
# 交互式建配置（--init）：生成新的 jobs/*.json
# =============================================================================
def _ask(ask, prompt: str, default: str = "") -> str:
    """问一个问题；空输入用默认值（与 api2ods 向导同款）。"""
    hint = f"（默认 {default}）" if default else ""
    return str(ask(f"{prompt}{hint}：") or "").strip() or default


def _split_base_ref(raw: str) -> tuple[str, str]:
    """把用户粘贴的 Base 链接拆成 (base_token, table_id)；不是链接就当裸 token。

    字符集与 config._FEISHU_ID_RE 一致（允许 - _）：只用 [A-Za-z0-9] 会在 token 里
    第一个 - / _ 处截断，截断值照样通过白名单校验，向导不报错却把错的 token 落进配置。
    """
    text = raw.strip()
    match = re.search(r"/base/([A-Za-z0-9_-]+)", text)
    base_token = match.group(1) if match else text
    table_match = re.search(r"[?&]table=(tbl[A-Za-z0-9_-]+)", text)
    return base_token, (table_match.group(1) if table_match else "")


def _default_ask_secret(prompt: str = "") -> str:
    """密钥输入：不回显；环境不支持隐藏输入时退回普通 input（照常可用）。"""
    try:
        return getpass.getpass(prompt)
    except Exception:  # noqa: BLE001 - 没有 tty 等场景退回普通输入
        # 退回 input() 时输入会明文回显，而提示语里写着"输入不回显"——必须显式纠正预期
        print("（警告：当前环境无法隐藏输入，接下来输入的密钥会明文回显）", file=sys.stderr)
        return input(prompt)


def run_init(
    out_path: str = "",
    ask=input,
    echo=print,
    workdir: pathlib.Path | None = None,
    fetch_fields=None,
    ask_secret=None,
) -> int:
    """交互式生成作业配置，返回退出码（0 成功 / 1 取消）。

    fetch_fields(feishu) -> (fields, samples)：默认走真实接口拉第一页做列名/样例展示；
    ask_secret(prompt) -> str：密钥类输入默认用 getpass（不回显）；单元测试都可注入假实现（离线跑）。
    """
    root = pathlib.Path(workdir) if workdir else pathlib.Path.cwd()
    fetch_fields = fetch_fields or fetch_field_sample
    ask_secret = ask_secret or _default_ask_secret

    echo("=== feishu2ods 配置向导（直接回车用默认值；随时 Ctrl+C 取消）===")

    # ① 作业名
    job_name = _ask(ask, "① 作业名（英文/数字/下划线，用于文件名和日志）", "feishu_table")
    safe_name = re.sub(r"[^0-9A-Za-z_\-]", "_", job_name).strip("_") or "feishu_table"
    if safe_name != job_name:
        echo(f"   提示：特殊字符已替换为下划线，文件名用 {safe_name}")
    job_name = safe_name
    out = pathlib.Path(out_path) if out_path else root / "jobs" / f"{job_name}.json"
    if out.exists():
        # 防手滑覆盖生产作业（里面有密钥）：默认不覆盖，想覆盖要显式答 y
        answer = _ask(ask, f"⚠️ {out} 已存在，覆盖吗？（y/n）", "n").lower()
        if answer not in ("y", "yes", "1"):
            echo("已取消（原文件未动）。")
            return 1

    # ② Base 链接 / token
    base_token = table_id = ""
    base_url = ""
    for _ in range(3):
        raw = _ask(ask, "② 多维表格链接（直接粘贴整条 URL，或只填 base_token）")
        if not raw:
            echo("   不能为空。")
            continue
        if "/wiki/" in raw:
            echo(
                "   wiki 知识库链接里是 wiki 节点 token、不是 base_token；请在浏览器里打开该表格后复制 /base/ 开头的链接，或直接填 base_token。"
            )
            continue
        base_token, table_id = _split_base_ref(raw)
        # 与 config 的校验共用一个白名单（黑名单挡不住 ".."、"%2F" 这类会改变 URL 路径的值，
        # 而这里拿到 token 后就会拼请求 URL——必须先过校验再发请求）
        try:
            base_token = _require_feishu_id(base_token, "base_token")
        except SystemExit:
            echo("   链接/token 看起来不对，示例：https://xxx.feishu.cn/base/IC4xxxxxxxx")
            continue
        url_match = re.search(r"(https?://[^/]+/base/[A-Za-z0-9_-]+)", raw)
        base_url = url_match.group(1) if url_match else ""
        if not table_id:
            table_id = _ask(ask, "   链接里没带 table 参数，请粘贴数据表 ID（tbl 开头）")
        if not re.match(r"\Atbl[A-Za-z0-9_-]+\Z", table_id or ""):
            echo("   数据表 ID 应以 tbl 开头，示例：tbleAbCdEfGh123456")
            continue
        break
    else:
        echo("❌ 连续三次无效，已取消。")
        return 1

    # ③ 应用凭证
    echo("   提示：应用需要先被加为这张表格的「可阅读」协作者。")
    app_id = ""
    for _ in range(3):
        app_id = _ask(ask, "③ 飞书自建应用 app_id（cli_ 开头）")
        if re.match(r"\Acli_[A-Za-z0-9]+\Z", app_id):
            break
        echo("   app_id 应以 cli_ 开头。")
    else:
        echo("❌ app_id 连续三次无效，已取消。")
        return 1
    app_secret = _ask(ask_secret, "   App Secret（输入不回显）")
    if not app_secret:
        echo("❌ App Secret 不能为空，已取消。")
        return 1

    # ④ 拉列名和样例
    feishu = {"app_id": app_id, "app_secret": app_secret, "base_token": base_token, "table_id": table_id}
    if base_url:
        feishu["base_url"] = base_url  # 告警卡片里带表格链接
    echo("   正在拉取表格列名 ...")
    try:
        fields, samples = fetch_fields(feishu)
    except SystemExit as exc:
        echo(f"❌ 拉取失败：{exc}")
        echo("   请确认：应用已加为协作者、app_id/app_secret 正确、table_id 正确；然后重新运行 --init")
        return 1
    except (OSError, RuntimeError) as exc:
        # 只接预期的连接/接口类错误（与 sftp2ods 向导同口径）：TypeError/AttributeError
        # 这类代码缺陷继续上抛，不能被"给一条人话"掩盖成"拉取失败"
        echo(f"❌ 拉取失败（{type(exc).__name__}）：{exc}")
        echo("   请检查网络与凭证后重新运行 --init")
        return 1
    if not fields:
        echo("❌ 表格没有列（或接口没返回字段列表），请检查后重试。")
        return 1
    echo(f"   共 {len(fields)} 列：" + "、".join(fields[:10]) + ("……" if len(fields) > 10 else ""))

    # ⑤ 列名 → 英文键
    echo("")
    echo("⑤ 给要同步的列起英文键（JSON 键）；不需要的列直接回车跳过；至少映射一列。")
    mapping: dict[str, str] = {}
    used: set[str] = set()
    for field in fields:
        sample = samples.get(field, "")
        if sample:
            echo(f"   「{field}」样例：{sample}")
        for _ in range(3):
            key = _ask(ask, f"   「{field}」的英文键（回车跳过）")
            if not key:
                break
            if not IDENT_RE.match(key):
                echo("   英文键须为 字母/数字/下划线，且不能以数字开头。")
                continue
            if key == "record_id":
                echo("   record_id 是保留键（记录 ID 自动输出），换一个。")
                continue
            if key in used:
                echo(f"   英文键 {key!r} 已用过，换一个。")
                continue
            mapping[field] = key
            used.add(key)
            break
        else:
            echo("   连续三次没填对，跳过该列。")
    if not mapping:
        echo("❌ 一列都没映射，已取消。")
        return 1

    # ⑥ 目标表（项目和表名会拼进 DDL，就地校验、错三次才取消，不用等最后一步）
    for _ in range(3):
        project = _ask(ask, "⑥ MaxCompute 项目", "my_project")
        if IDENT_RE.match(project):
            break
        echo("   项目名只能是 字母/数字/下划线，且不能以数字开头。")
    else:
        echo("❌ 项目名连续三次无效，已取消。")
        return 1
    default_table = f"ods_{job_name.replace('-', '_')}_json_df"  # 表名不允许 '-'，默认值先换掉
    for _ in range(3):
        table_name = _ask(ask, "   目标表名", default_table)
        if IDENT_RE.match(table_name):
            break
        echo("   表名只能是 字母/数字/下划线，且不能以数字开头。")
    else:
        echo("❌ 表名连续三次无效，已取消。")
        return 1
    comment = _ask(ask, "   表注释", "飞书多维表格同步（JSON 原样，写临时分区后原子替换 pt=业务日）")

    # ⑦ 阿里云凭证
    echo("⑦ MaxCompute 凭证（与其它作业相同即可）")
    ak = sk = ""
    for _ in range(3):
        ak = _ask(ask, "   access_key_id（LTAI 开头）")
        sk = _ask(ask_secret, "   access_key_secret（输入不回显）")
        if ak and sk:
            break
        echo("   两个都不能为空。")
    else:
        echo("❌ access_key_id / access_key_secret 连续三次为空，已取消。")
        return 1
    # endpoint 不再写死 us-west-1：非该地域的项目生成出来会一跑就连不上，
    # 而报错要到真正执行时才出现——这里显式问一次（默认值仍是 us-west-1）
    endpoint = ""
    for _ in range(3):
        endpoint = _ask(ask, "   endpoint（非 us-west-1 地域的项目请改）", DEFAULT_ENDPOINT)
        if endpoint.startswith(("http://", "https://")):
            break
        echo("   endpoint 必须以 http(s):// 开头。")
    else:
        echo("❌ endpoint 连续三次无效，已取消。")
        return 1

    # ⑧ 新鲜度
    freshness = None
    need = _ask(ask, "⑧ 要不要「缺少业务日数据」飞书告警？（y/n）", "y").lower()
    if need in ("y", "yes", "1"):
        keys = list(mapping.values())
        date_field = ""
        for _ in range(3):
            date_field = _ask(ask, f"   用哪个键判日期（可选：{', '.join(keys)}）", keys[0])
            if date_field in keys:
                break
            echo("   必须是刚映射过的英文键之一。")
        else:
            date_field = keys[0]  # 三次没填对就用第一个键，不让整个向导白跑
            echo(f"   连续三次没填对，改用 {date_field}")
        webhook = ""
        for _ in range(3):
            webhook = _ask(ask, "   告警 webhook（飞书群机器人地址，可留空后补）")
            if not webhook or webhook.startswith(("http://", "https://")):
                break
            echo("   webhook 必须以 http(s):// 开头（或直接回车留空）。")
        else:
            webhook = ""
            echo("   连续三次格式不对，先留空（之后可在 job 文件里补 freshness.webhook）。")
        freshness = {"date_field": date_field, "lag_days": DEFAULT_FRESHNESS_LAG_DAYS}
        if webhook:
            freshness["webhook"] = webhook

    job = {
        "job": job_name,
        "description": f"飞书多维表格 {base_token}/{table_id} → {project}.{table_name}（json + pt 分区，临时分区 + 原子替换）",
        "feishu": feishu,
        "maxcompute": {"project": project, "endpoint": endpoint, "access_key_id": ak, "access_key_secret": sk},
        "fields": mapping,
        "target": {
            "project": project,
            "table": table_name,
            "column": DEFAULT_COLUMN,
            "comment": comment,
            "allow_empty": False,
        },
    }
    if freshness:
        job["freshness"] = freshness

    # 先按校验器过一遍（就地规范化：去空白等），避免生成一份跑不起来的配置；
    # 未知键/明文 http 端点等告警也要展示——静默写进生成的配置会让人以为一切正常
    try:
        for warning in validate_job(job):
            echo(f"⚠️ {warning}")
    except SystemExit as exc:
        echo(f"❌ 生成的配置没过校验（{exc}）；请重新运行 --init")
        return 1

    out = pathlib.Path(out_path) if out_path else root / "jobs" / f"{job_name}.json"
    tmp_name = None
    try:
        out.parent.mkdir(parents=True, exist_ok=True)
        # 文件含 app_secret/AK/SK 明文：先写同目录临时文件再 os.replace，避免 O_TRUNC
        # 先截断正在用的作业文件——写到一半崩溃会把密钥配置抹成空文件。
        # mkstemp 默认 0600；newline="\n"：生成的文件固定 LF，Windows 上跑出来的也能给服务器用
        fd, tmp_name = tempfile.mkstemp(prefix=f".{out.name}.", suffix=".tmp", dir=str(out.parent))
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(job, ensure_ascii=False, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, out)
        tmp_name = None
        if os.name != "nt":
            try:
                os.chmod(out, 0o600)  # 已存在文件的旧权限一并收紧（Windows 忽略）
            except OSError:
                # 文件已经写好：最后一步收紧失败（不支持 chmod 的文件系统）只跳过，
                # 不能把"已生成成功"误报成"写文件失败"
                pass
    except OSError as exc:
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except OSError as cleanup_exc:
                # 临时文件里是含密钥的完整配置：清理失败要说一声（用户可手工删）
                echo(f"⚠️ 临时文件未清理（{cleanup_exc}），请手工删除：{tmp_name}")
        echo(f"❌ 写文件失败：{exc}")
        return 1
    except BaseException:
        # Ctrl+C / SystemExit 等中断：临时文件里是含密钥的完整配置，必须清掉——
        # 原来只有 OSError 分支做清理，中断后会留下 .xxx.json.XXXX.tmp，
        # 而向导还提示"已取消，未生成任何文件"
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except OSError as cleanup_exc:
                echo(f"⚠️ 临时文件未清理（{cleanup_exc}），请手工删除：{tmp_name}")
        raise
    try:
        echo("")
        echo(f"✅ 已生成：{out}")
        echo("   ⚠️ 文件含明文密钥（app_secret / AccessKeySecret），请勿提交到版本库；只保留在使用机器上。")
        echo(f"   下一步：python feishu2ods.py --job {out} --check      # 体检（不写库）")
        echo(f"           python feishu2ods.py --job {out} --dry-run    # 试跑（不写库）")
    except KeyboardInterrupt:
        # 文件已在 os.replace 时写全：中断只打断收尾提示，不能按"已取消"上报
        echo("")
        echo(f"✅ 已生成：{out}（Ctrl+C 落在收尾阶段，文件已写全）")
    return 0
