# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 修复

- **脱敏补齐为「形态级 + 值级」双重（安全）**：原来 `utils.redact()` 只做一件事——把 job 配置里
  的已知密钥值做 `str.replace`（且要求长度 ≥6）。接口/鉴权若不回显原值、而是回显**变形形态**
  （`Authorization: Bearer sk-xxxx`、`?access_token=xxxx`、`{"app_secret": "xxxx"}`、飞书
  webhook `/hook/<id>`、URL 里的 `user:pass@host`、`X-Api-Key: …`），这些凭证会**原样进日志**。
  api2ods / sftp2ods 都有形态级规则，feishu2ods 缺失，属安全回归。现在补齐为与 api2ods
  `utils.redact()` 能力对齐的形态级规则（Bearer/Basic、JSON/配置片段、URL query、请求头行、
  URL userinfo、飞书 webhook），并保留值级兜底；规则顺序为「Bearer/Basic 与 JSON 片段先跑、
  query 规则后跑」，避免 `header: 'Authorization=Bearer abc'` 被 query 规则按 `=` 切成
  `Authorization=` 后令牌漏出。新增 `redact_secrets(values, text)`（值级先遮、形态级兜底）与
  `collect_secret_values(job)`，`cli` 的作业上下文错误出口统一走 `redact_secrets`，确保
  `app_secret` / `access_key_secret` / webhook 出现在任何异常文本里都被遮蔽。

- **临时目录不可写/磁盘满不再裸 traceback**：原来 `SpoolWriter.__init__` 直接
  `tempfile.mkstemp()` + `open()`，失败时抛裸 `OSError`/`FileNotFoundError`。现在转换/包装成
  带原因的人话（「建不了落盘临时文件（…）：原因」），`cli.run_sync` 捕获后记一条
  「❌ 无法创建落盘临时文件（检查系统临时目录是否可写/磁盘是否已满）：…」并按**运行失败
  （退出码 1）**结束（与 api2ods 同款处理），错误信息经脱敏。

- **SQL 超时补齐到分区增删（原覆盖不全）**：上一轮的超时只盖住了建表 / 校验 / rename，
  `write_partition` 里的 `table.delete_partition` / `table.create_partition`（清残留 `__tmp`、
  建 `__tmp`、删旧正式分区）以及 `purge_stale_tmp_partitions` 的清理仍是 pyodps 同步不限时调用。
  MaxCompute 侧在这几处卡住时任务依旧会**无限挂起并一直占着运行锁**，把后续调度全部顶掉。
  现在新增 `drop_partition` / `add_partition`（`ALTER TABLE … DROP IF EXISTS PARTITION` /
  `ADD IF NOT EXISTS PARTITION`）并统一走 `run_sql_with_timeout`：`IF EXISTS` / `IF NOT EXISTS`
  保持原有的幂等语义（等价 `if_exists=True` / `if_not_exists=True`），分区 spec 仍走 `_sql_spec`
  转义（并兼容 pyodps `partitions` 已带引号的形态）。**原子替换流程不变**：超时按普通失败走
  既有的「重试 3 次 + 清临时分区」，正式分区不会丢；也不再依赖任何"悬挂线程"（DDL 走异步实例
  轮询，超时即 `stop()` 取消）。

- **脱敏补齐 URL 编码形态（原只遮明文）**：`redact()` / `redact_secrets()` 的值级替换原来只做
  明文 `str.replace`。若接口把凭证以 **URL 编码**形态回显（如 `tok%20abc%2F123` 对应
  `tok abc/123`），且周围没有可识别的键名（`access_token=` 这种形态规则能挡），凭证会原样进日志。
  现在值级替换同时替换其 `quote(secret, safe="")` 与 `quote_plus(secret)` 形态（`urllib.parse`
  标准库，无新依赖），明文 / URL 编码 / `+` 编码三种形态都能被遮蔽。

- **`_SECRETS` 不再跨轮残留**：脱敏表是模块级全局，`cli._run` 原来只做追加、从不复位——
  同一进程里多次调用 `main()`（调度框架常复用进程）时会累积上一轮的密钥值。现在每次运行
  入口先 `_SECRETS.clear()` 再登记，保证不跨轮串值；登记时机仍在 `load_job` 之后、任何校验
  日志与请求之前（密钥写错形态时的报错也不会泄进日志）。

- **`--check` 不再被脏 `bizdate` 环境变量拖垮**：`dates.env_bizdate()` 原为严格模式，而
  `cli._run` 里 `resolve_bizdate` 又跑在 `args.check` 分支之前——环境里只要有个格式不对的
  `bizdate`，连只读体检都会直接失败。现在给日期解析补 `strict` 形参（对齐 api2ods /
  sftp2ods），`--check` 走非严格（拿不到合法业务日时按默认 T-1 并打警告），**正式同步路径仍
  严格**（非法业务日必须报错，绝不静默回退成"昨天"写错分区）。

- **写库异常分支不再泄漏临时 JSONL**：`run_sync` 里 `connect_odps` / `ensure_table` 抛
  `SystemExit`（缺 pyodps、缺凭证）时异常直接冒泡到 `main`，`SpoolWriter` 未 `close()`，
  临时文件既没关句柄也没删。现在写库段用 `try/finally` 统一收口（对齐 api2ods）：失败路径
  `keep=True` 保留文件供排查（日志打印路径），正常路径删除。

- **写 0 行前的空分区保护（原缺）**：`target.allow_empty=true` 且本次拉到 0 行时，原来会直接
  「删旧分区 + rename」把已有分区清空（不可逆）。现在与 sftp2ods 对齐：写空分区前先
  `count_partition` 查目标分区现有行数，**非 0 则拒绝写库并以退出码 1 结束**（提示
  "若确认就是要写空分区，请加 `--force`"）；现有行数本来就是 0 则正常写空分区。

### 功能

- **新增 `--log-file`（运维）**：日志同时写一份到指定文件——追加、UTF-8、父目录自动创建；
  指向的是目录时给一句人话（而非 `IsADirectoryError` 裸 traceback）。Linux 上跑 cron/调度时
  stdout 会被截断，落盘是事后翻查的唯一途径。`utils.log()` 新增文件 sink 支持，`main` 在
  退出时摘掉并关闭句柄（同一进程内重复调用不串）。行为与 api2ods 对齐。

- **新增 MaxCompute SQL 超时保护与 `--sql-timeout`**：原来 `mc.py` 的建表（`execute_sql`）、
  分区核对（`verify_partition`）、行数核对（`count_partition`）、`rename_partition` 都走
  pyodps 默认（不限时），云端卡住时调度任务会无限挂起、一直占着运行锁把后续调度全挡掉。
  现在统一经 `run_sql_with_timeout`（`run_sql` 异步提交 + 轮询，超时主动 `stop()` 取消并抛
  `TimeoutError`），默认 600 秒，命令行 `--sql-timeout` 可调，`0`=不限制，口径与 api2ods /
  sftp2ods 完全一致（负数在 argparse 阶段报错，退出码 2）。

- **新增 `--force`（数据保护开关）**：`--force` 供「0 行写空分区」的保护使用——源端确实把这天
  清空时，显式加 `--force` 才允许把已有分区覆盖成空分区（语义与 sftp2ods 的 `--force` 一致）。

- **补齐企业级基础文件**：新增 `LICENSE`（MIT，与 api2ods / sftp2ods 同协议同署名）、
  `CONTRIBUTING.md`（写全设计红线：全量快照 `pt=<业务日>`、临时分区 + 原子替换、宁可失败不可
  静默丢数、密钥不进日志、峰值内存与行数解耦、跨平台）、`MANIFEST.in`、
  `.github/ISSUE_TEMPLATE/bug_report.yml`、`.github/ISSUE_TEMPLATE/feature_request.yml`、
  `.github/pull_request_template.md`。

### 测试

- **单测不再往仓库目录写锁文件**：单测用临时作业路径、锁名哈希每次不同，原来会把运行锁写到
  仓库的 `.run-locks/` 下并无限累积。现在测试基类把锁根路径 `monkeypatch` 到临时目录，并在
  收尾时清理——只改测试侧，生产行为不变。

### 工程

- **补齐 CI 格式门禁并锁定 ruff 版本**：工作流原来只跑 `ruff check .`，缺 `ruff format --check .`
  （api2ods / sftp2ods 都有）；dev 依赖是浮动 `ruff>=0.5`，CI 装的版本会随时间变化，"本地干净、
  CI 失败"这类问题很难查。现在补上格式门禁并把 ruff 锁到 `==0.16.9`，与另两个仓库统一。

## [1.5.0] - 2026-09-30

- 流式落盘（`SpoolWriter` + `FetchStats`）：记录边拉边写本地临时 JSONL，写库时逐批读回，
  峰值内存与总行数无关；临时分区 + 原子替换；新鲜度校验、新增列提醒等（历史能力，此处补记）。
