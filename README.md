# feishu2ods

[![tests](https://github.com/sandy-wangzining/feishu2ods/actions/workflows/tests.yml/badge.svg)](https://github.com/sandy-wangzining/feishu2ods/actions/workflows/tests.yml)

飞书多维表格（Base）→ MaxCompute ODS。把整表记录按字段映射写成单行 JSON，存入 `pt=<业务日>` 分区；每次运行生成一份全量快照。

Feishu Base → MaxCompute snapshot ingestion. Standalone CLI for Windows, macOS and Linux. Requires Python 3.10+, a Feishu app with read access, and MaxCompute credentials. No other ODS project or company service is required.

同系列：[api2ods](https://github.com/sandy-wangzining/api2ods)（REST API）· [sftp2ods](https://github.com/sandy-wangzining/sftp2ods)（SFTP 文件）。本工具适合定时快照，不提供增量、实时同步。

## 安装与准备

需要 Python 3.10+，飞书自建应用的 App ID / App Secret，以及目标 MaxCompute 项目的凭证。应用应已发布，并被加入目标 Base 的可阅读协作者。

```bash
git clone https://github.com/sandy-wangzining/feishu2ods.git
cd feishu2ods
python -m venv .venv
# macOS / Linux
.venv/bin/python -m pip install .
# Windows PowerShell
.\.venv\Scripts\python.exe -m pip install .
```

也可用 `pipx install "git+https://github.com/sandy-wangzining/feishu2ods.git"` 独立安装命令行工具。
运行依赖为 requests、pyodps、simplejson（无损十进制 JSON），Windows 会自动安装 tzdata。
源码运行请先 `python -m pip install -r requirements.txt`；`python feishu2ods.py`、`python -m feishu2ods` 与安装后的 `feishu2ods` 命令等价。

地域和时区都可配置。请把 `maxcompute.endpoint` 设置为项目实际地域的 HTTPS 地址；默认 us-west-1 仅适用于该地域。不要直接沿用示例的项目、凭证或数据表 token。

## 第一次接表

```bash
feishu2ods --init
feishu2ods --job jobs/my_table.json --check
feishu2ods --job jobs/my_table.json --bizdate 20261026 --dry-run
feishu2ods --job jobs/my_table.json --bizdate 20261026
```

`--init` 读取列名和样例，交互生成字段映射与作业配置，密钥输入不回显；`--init-out` 可指定输出路径。
也可复制 `jobs/feishu_example.example.json` 后手工填写。真实作业含密钥，已通过 `.gitignore` 排除，勿提交到 Git。

`--check` 只检查配置、API 连通、字段映射与目标表结构，不写库、不发通知。`--dry-run` 拉取并校验全部数据，不连接 MaxCompute。

## 数据如何发布

1. 用 App ID / App Secret 获取 tenant_access_token。
2. 按 `limit=500 + offset` 拉取，记录逐页落到本地临时 JSONL。
3. 将 Base 列名按 `fields` 映射成 JSON 键，保留 `record_id` 与字段原始值。
4. 在目标项目创建独立的 `tmp_feishu2ods_<随机 ID>` 暂存表（生命周期 1 天），通过 Tunnel 分批上传。
5. 核对暂存的行数、record_id 去重数及 ID 范围；通过后以单条 `INSERT OVERWRITE` 发布正式业务日分区。
6. 成功后尽力删除暂存表，再核对正式分区行数；失败残留由生命周期回收，日志给出暂存表名。

正式表不会创建新版本的 `__tmp` / `__keep` 分区。暂存和校验期间旧快照保持可读；单分区 OVERWRITE 按 MaxCompute 的原子提交语义完成，不再执行「先删旧分区 + rename」。接口返回失败或响应丢失时，命令会报告失败；以云端实例及正式分区核对结果确认是否提交，重跑为幂等覆盖。

该流程需要目标项目的建表、暂存表读写和删除、目标分区写入及查询权限，并会产生暂存存储和发布 SQL 的费用。请按项目权限与计费规则授权、预算。

## 配置

完整示例见 `jobs/feishu_example.example.json`，主要字段如下：

- `feishu.app_id`、`app_secret`：飞书自建应用凭证。
- `feishu.base_token`、`table_id`：Base 和数据表标识；`base_url` 可选，用于通知链接。
- `maxcompute.project`、`endpoint`、`access_key_id`、`access_key_secret`：目标连接配置。
- `fields`：Base 列名 → JSON 英文键，至少一项；英文键唯一，不能使用保留键 `record_id`。
- `target.project`：可选，覆盖 maxcompute.project；`table` 必填，`column` 默认 json；`comment` 可选。
- `target.allow_empty`：默认 false。显式允许空快照时，若正式分区已有数据，仍需 `--force` 才会清空。
- `freshness.date_field`：用于判日期的英文键，必须包含在映射中；不配置 freshness 就不做新鲜度检查。
- `freshness.lag_days`：预期日期 = 业务日减 N 天，默认 0。
- `freshness.timezone`：IANA 时区名，例如 Asia/Shanghai、America/New_York、UTC。未配置时使用 ODS_BIZDATE_TZ，后者默认 America/New_York。
- `freshness.webhook`：可选，飞书群机器人地址或地址数组；某个群通知失败不影响其它群和同步退出码。

### 业务日与新鲜度

业务日优先级：`--bizdate` > 环境变量 `bizdate` / `SKYNET_BIZDATE` > `ODS_BIZDATE_TZ` 时区的昨天。接受 YYYYMMDD 或 YYYY-MM-DD，正式运行不会把非法值静默回退。

新鲜度支持日期文本（2026-10-26、2026/10/26、20261026）、紧凑数字日期、epoch 秒/毫秒及 ISO 时间。带偏移的 ISO 与 epoch 都转换到 `freshness.timezone` 后取日；无时区的日期文本保留源侧日期。时间戳结果需落在 2000～2100 年，避免把普通数字误认作日期。

例如 `2026-10-26T20:00:00-04:00` 与 `1793059200000` 是同一时刻；在 America/New_York 下都归 26 日，在 Asia/Shanghai 下都归 27 日。按源业务口径选择时区，IANA 时区可自动处理夏令时。

缺少预期日期只告警，快照仍发布；零行默认失败。缺数据和新增列的通知在发布并核对成功后才发送，试跑或写失败不会声称已同步。通知失败只记录警告，调度应依靠进程退出码监控同步结果。

## 完整性与内存边界

- 翻页期间字段列表或表格版本 rev 变化、record_id 重复、映射列缺失，一律中止。
- 新增未映射列不阻塞，同步其余字段并提醒；空白行原样保留，下游按业务字段过滤。
- JSON 小数按 Decimal 解析，simplejson 将其保留为 JSON 数字，避免金额先转 float 丢失精度；非有限数 NaN / Infinity 拒绝写入。
- 正文逐页落盘，写入批次同时限制 500 行与约 8MB；单条约 7MB 上限在远端写入前检查。去重 ID 和日期统计仍随数据量增长，整个进程并非恒定 8MB 内存。
- 失败时本地 spool 保留并打印路径，排查后可删除；成功及正常试跑会清理。
- 目标表必须为单列 string + 单级 pt string 分区的普通表；宽表、视图、事务表会拒绝写入。
- SQL 默认超时 600 秒，超时或中断会请求取消云端实例；网络响应丢失时不能仅凭本地退出码断言云端已取消。
- 日志与告警对凭证做形态和值级脱敏；密钥仍应由操作者妥善保管。

下游按明确的八位业务日分区读取。`MAX_PT()` 只返回有数据的最大分区；允许写空分区时，它可能返回上一天，请勿用它判断某个业务日是否成功发布。

## 并发与部署

每个作业有运行锁，另有按 `project.table` 归一化的目标表锁。不同作业、安装目录、虚拟环境和三个 ODS 工具写同一目标表时互斥；不同表可并行。争用锁时后启动的任务以失败退出，调度可稍后重试。

目标锁默认在系统临时目录的 `ods-target-locks[-UID]`。多用户或不同 TMPDIR / 容器运行时，用 `ODS_TARGET_LOCK_DIR` 指向同一固定目录。`FEISHU2ODS_LOCK_DIR` 继续控制作业锁。操作系统退出会释放锁，不需要手工删除锁文件。

跨机器只有共享文件系统确实支持跨主机文件锁时才可能互斥；仅指定共享路径不提供分布式保证，推荐固定调度机器。不支持文件锁时默认拒绝运行；仅确认无人并发时，才用 FEISHU2ODS_ALLOW_NO_LOCK=1 显式接受无锁风险。

## 从旧版本升级

先停掉所有旧进程，再安装更新并一起更新会写同一目标的工具。目标表结构和原有作业 JSON 保持可用，无需修改下游八位业务日分区口径。

- 新流程需要独立暂存表及发布 SQL 的权限和费用预算。
- 旧数据按北京时间核新鲜度的作业，请显式增加 `freshness.timezone: "Asia/Shanghai"`；未配置时现在与业务日时区一致。
- 旧版本遗留 `__keep`：正式分区已存在时清理冗余副本，否则保留并提示恢复。
- 旧版本遗留 `__tmp`：若对应正式分区不存在，程序停止并保留这份可能的唯一副本。先核对内容，恢复到正式分区或确认无用后删除，再重跑；不会自动毁掉唯一副本。
- 新版本失败的暂存表可按日志定位；推荐直接重跑，未经核对不要手工把半成品发布。

## 命令行与退出码

常用选项：`--job`、`--bizdate`、`--check`、`--dry-run`、`--init` / `--init-out`、`--project` / `--table`（测试覆盖目标）、`--force`（空分区覆盖确认）、`--skip-freshness`（兼容 `--no-check`）、`--no-notify`、`--sql-timeout`（0 表示不限时）、`--log-file`（追加 UTF-8 日志）、`--version`。完整参数见 `feishu2ods --help`。

退出码：0 成功（含体检/试跑）；1 配置、源端、校验或写入失败；2 命令行参数错误；130 用户中断。`--check` 无远端修改，正式运行以非零码向调度报告失败。

## 开发与验证

```bash
python -m pip install -e ".[dev]"
python -m unittest discover -s tests
python tools/sync_common.py --check
ruff check .
ruff format --check .
```

测试离线执行，不使用真实凭证、飞书或数仓。CI 覆盖 Windows / macOS / Linux 和项目支持的 Python 版本，并检查 wheel 独立安装。公共源码随包携带，维护说明见 [docs/shared-runtime.md](docs/shared-runtime.md)；贡献约束见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## License

MIT，见 [LICENSE](LICENSE)。
