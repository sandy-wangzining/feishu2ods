# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 修复

- **流式拉取的翻页日志「累计」数不再虚高（显示修复）**：`fetch_records` 流式模式下 `raw_rows`
  不再累积，原日志用 `len(raw_rows) + len(page_rows)` 算累计数，导致每页都显示「累计 = 本页行数」
  （如第 2 页明明累计 1000 条仍打印 `累计 500`）。现在流式取 `FetchStats.count`、非流式取
  `len(raw_rows)`，两种模式都反映真实累计条数。只影响日志显示，不影响数据与退出码。

- **拉取阶段失败不再泄漏临时 JSONL**：`run_sync` 的拉取段原来用 `except Exception` 收口临时
  spool，但 `fetch_records` 的失败大多抛 `SystemExit`（鉴权失败、翻页一致性中止、映射列缺失等），
  而 `SystemExit` 不是 `Exception` 的子类——漏掉了它，失败时既不关文件句柄、也打不出「已保留数据
  文件」的日志。现在改为 `except BaseException`（与写库段 `try/finally` 的收口口径一致），失败
  路径统一 `keep=True` 保留文件供排查。退出码不变（仍是 1 / 130）。

- **流式路径「有记录却没给字段列表」的报错对齐非流式**：`_page_records` 在字段列表为空但有记录时
  会裸抛 `KeyError`（`index[source]` 取不到键），用户只看到一条「未预期错误」加堆栈；非流式
  `build_records` 此时会给出明确的 `SystemExit` 提示。现在流式也对齐成同一明确报错（退出码仍是 1）。

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

- **拼进 SQL 的分区值加白名单、标识符补校验（安全）**：MaxCompute 没有绑定参数，`pt` 与
  project/table/column 只能拼进语句；现在 `count_partition` / `verify_partition` 统一走
  `_partition_clause`（白名单：8 位业务日，写入期临时分区带 `__tmp` 后缀）并对标识符做
  `_require_identifier` 校验，引号/反斜杠/空格等注入面归零（原实现只对 pt 做引号转义）。
  同时 `_sql_spec`（DDL 用）对来自服务端 `partition.name` 的值补转义。
- **表注释转义反斜杠**：`target.comment` 来自作业配置（用户可任意编辑），原来只把单引号
  双写；MaxCompute 字符串字面量里反斜杠本身是转义符，结尾的 `\` 会吃掉收尾引号、把后续
  内容当 SQL 解析。现在先转义反斜杠再转义引号；config 侧同时把注释去掉首尾空白并拒绝换行。
- **默认 MaxCompute endpoint 改 https**：作业未写 endpoint 时（向导生成的配置、示例模板）
  走明文 HTTP 会暴露 AK/SK 签名与查询结果；`mc.DEFAULT_ENDPOINT` 与 `jobs/*.example.json`
  统一改 https。
- **向导写文件先按 0600 创建再写入（安全）**：生成的文件含 `app_secret` / `access_key_secret`
  明文，原来 `write_text` 先按默认 umask（通常 0644）创建、再 chmod，存在同机其他用户可读
  的窗口期；现在创建即 0600，并在输出里提醒"请勿提交到版本库"。
- **向导显式询问 endpoint（不再写死 us-west-1）**：非 us-west-1 地域的项目原来生成的配置
  一跑就连不上，且报错要到真正执行时才出现；现在像其它字段一样交互询问（默认值仍为
  us-west-1，格式不对会重问）。
- **`freshness` 缺 `date_field` / 写成非对象给干净的配置错**：原来直接 `freshness["date_field"]`
  下标取值，配置漏字段时抛裸 `KeyError`，被 `main` 的兜底 except 变成"未预期错误"+traceback，
  用户看不出是配置问题。现在给出明确报错（且校验放在建落盘临时文件之前，失败退出时没有
  悬挂句柄）。
- **0 行失败路径保留落盘文件**：`target.allow_empty=false` 且拉到 0 行时原来 `spool.close()`
  删掉落盘文件，偏离本文件"失败留证（哪怕是空的 0 行快照）"的约定；现在与其它失败路径
  统一 `keep=True` 并打印文件路径。
- **int 退出码的 `SystemExit` 留痕**：运行期抛出的 int 退出码（如库内部 `sys.exit(1)`）原来
  既不打印也不写日志，`--log-file` 里完全查不到这次为什么失败；现在按统一格式记一笔
  （"以退出码 N 结束"）。
- **`fetch_records` 的 sink/stats 必须成对传入**：只传一个会静默退化成"全量累积并返回 list"
  （既不落盘、返回类型也变，大表可能直接爆内存），现在在函数入口快速失败（TypeError）。
- **防死循环的 seen 集合改为有界（内存契约）**：原来把全量 `record_id` 存进 set，500 万行
  要几百 MB，与"峰值内存只与单页/页数有关"的承诺矛盾；现在改为页级指纹（sha1）+ 上一页
  ID 集合，忽略 offset 的重复页与相邻页重叠仍会被立刻中止。
- **兼容分支（旧签名传记录列表）的临时 spool 在写入失败时被清理**：原来赋值发生在写入
  之后，写入抛错（磁盘满等）时临时文件与句柄永远不被 close；现在先赋值再写、失败即清理。
- **SQL 超时取消失败留日志**：`instance.stop()` 失败原来静默吞掉，运维看到"已主动停止"会
  以为云端 SQL 真的停了（可能仍在跑、占着运行锁）；现在记一条"取消失败（云端可能仍在执行）"。
- **空记录判定修正**：`all()` 对空序列恒为 True——只有 `record_id`、没有任何映射字段的记录
  原来被算成"空白行"，`empty_rows` 的数据质量提示失真；现在显式判空。
- **`SpoolWriter` 建文件失败的临时文件清理**：`mkstemp` 已创建文件、随后 `open` 失败
  （句柄用尽/磁盘满）时原来把文件留在系统 temp；现在 best-effort 清掉再抛人话报错。
- **`fields` 列名去空白后写回配置**：带首尾空白的 Base 列名原来能通过校验但拉取时匹配不上
  列、整列静默为 null（与"校验通过的值会写回"的文档承诺不符）；现在写回去空白后的列名，
  去空白后重名会直接报错。
- **http:// 地址给出明文传输告警**：`feishu.base_url` / `maxcompute.endpoint` /
  `freshness.webhook` 允许 http（本地调试）但必须留痕——token、AK/SK 签名与告警内容会明文
  经网络传输；现在在未知键告警之外各补一条明文提示。
- **`freshness_problem` 不再消费迭代器入参的第一条记录**：原来"先看一条判类型、再整体遍历"
  对生成器会把它前进一格，第一条记录的日期不参与比较、可能误报"缺数据"；现在先实体化再判。
- **`_URL_AUTH_RE` 的 scheme 部分限长（安全）**：无上限时在长小写字母数字串上会在每个起始
  位置贪婪回扫（实测 20KB 要 10 秒、40KB 要 50 秒），限长后配合 `://`/`@` 预判保持线性。
- **控制台补丁与编码兜底加固**：`_console_patched` 的 check-then-set 放进锁里（多线程首次
  调用不再重复 reconfigure）；`setup_console` 的异常捕获收窄到"流不支持 reconfigure"
  （含 `io.UnsupportedOperation`，它是 OSError/ValueError 的子类），不再吞掉编程错误。
- **临时分区名带"本机+进程"标记，purge 统一清理所有 `__tmp` 残留**：写入期临时分区从
  `pt=20260928__tmp` 改为 `pt=20260928__tmp_<主机>_<pid>`；写库前的清理会清掉**所有**
  `__tmp*` 历史残留（含旧版无标记的），避免残留分区字符串序大于正式分区、被下游 max_pt()
  读到半成品——运行锁只保证单机互斥、正式调度固定一台机器（见 README），跨机并发不受支持。
- **写分区失败（含校验 SystemExit / Ctrl+C）也保证清理本轮临时分区**：原来重试循环只 catch
  Exception，SystemExit/KeyboardInterrupt 会跳过 tmp 清理、失败说明也不带"重跑可修复"结论；
  现在 finally 统一清理，并 `from last` 保留原始异常链。
- **拉数之后的整段流程统一收口落盘文件**：notify / 统计 / 新鲜度校验抛异常时不再泄漏 spool
  句柄（原来 try/finally 只包写库段）；dry-run 走同一个删除路径。
- **`main` 的参数与日志文件错误也进统一日志出口**：parse_args / --log-file 的 SystemExit
  原来在 try 之外（注释声称的分支不可达），现在参数错也有带时间戳的日志；`--help/--version`
  的退出码 0 原样放行。
- **运行锁加固**：锁名先 `resolve()` 再哈希（同一作业的不同写法命中同一把锁）；POSIX 上
  `O_NOFOLLOW`（锁路径是符号链接时拒绝跟随，避免 truncate 破坏任意文件）；拿到锁后任何异常
  都会释放并置空句柄；`__exit__` 幂等（重复调用不再对已关闭句柄操作）。
- **环境变量 bizdate/SKYNET_BIZDATE 逐个尝试**：第一个非法不再跳过第二个；空白值在严格模式
  （正式同步）下直接报错（避免静默写错 pt），只读体检（--check）按未设置继续并告警。
- **日期值规范化校验真实日历日期**（2026-13-99、2026/2/30 不再当成合法日期）；
  `freshness.lag_days` 在 run_sync 也做非负整数校验。
- **`base_token`/`table_id` 拒绝 URL 元字符**（空白、`/ ? #`——它们会直接拼进接口 URL 路径）。
- **`_sql_spec` 与分区 DDL 补校验**：分区字段名必须是合法标识符、值不能为空；DDL 与
  drop/add/rename 入口统一补 `_require_identifier`（不再依赖调用方先校验）。
- **向导写配置改为原子写**：mkstemp（0600）+ fsync + `os.replace`，写到一半崩溃不会把已有
  作业文件（含明文密钥）截断。
- **日志与脱敏**：`--log-file` 写入失败向 stderr 提示一次；短于 `_SECRET_MIN_LEN` 的登记
  密钥值不再参与值级替换（`1`/`ok` 这类会把正常文本搅乱）；`remove_log_sink` 收窄异常。
- **fetch：非流式模式的未映射列不再重复收集**（原来告警打两遍、extra_out 出现重复列名）。
- **spool.close 幂等**（重复调用是空操作）。

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
- **离线用例补齐本轮修复的回归覆盖**：分区值白名单（含全角数字）、表注释转义、`_sql_spec`
  转义、兼容分支临时 spool 清理、取消失败日志、sink/stats 成对、重复页/相邻页重叠检测、
  `fields` 去空白写回与重名、注释去空白/拒换行、http 明文告警、生成器入参、`freshness`
  缺字段的干净报错、int 退出码留痕、mkstemp 失败清理、向导 endpoint 询问与 0600、脱敏
  线性判据等（用例 245 → 266）。
- **测试夹具与生命周期修正**：`argparse.Namespace(...)` 字面量统一收敛到 `make_args()`
  构造（字段集合与 `parse_args` 对齐，避免实现新增 `args.xxx` 时用例以 AttributeError 而非
  真实行为失败）；`sql_timeout=600` 改用 `mc_mod.SQL_TIMEOUT_SECONDS`（常量改动后不再
  悄悄不一致）；`TestSpoolWriter` 的 spool 创建即登记 `addCleanup`（断言失败也保证临时文件
  与句柄回收）。另核实：报告中"_SECRETS 追加无清理会跨用例污染"不成立——测试基类已按用例把
  `utils._SECRETS` patch 成独立列表并还原，无需修改。
- **第二次复审批次的回归用例**：purge 全量清理（含其它主机/旧版 `__tmp` 残留、本进程后缀除外）、
  写分区遇 SystemExit 仍清临时分区、锁名先 resolve 再哈希、非流式 `extra_out` 不重复、
  utf-16 JSON 错误体、`code=null`/"0" 视为成功、`base_token` 元字符拒绝、真实日历日期校验、
  空白 bizdate 语义（严格报错 / 体检容忍）、原子写（不 O_TRUNC 在用文件）等（离线用例 266 → 285）。

### 工程

- **补齐 CI 格式门禁并锁定 ruff 版本**：工作流原来只跑 `ruff check .`，缺 `ruff format --check .`
  （api2ods / sftp2ods 都有）；dev 依赖是浮动 `ruff>=0.5`，CI 装的版本会随时间变化，"本地干净、
  CI 失败"这类问题很难查。现在补上格式门禁并把 ruff 锁到 `==0.16.9`，与另两个仓库统一。

### 文档

- **README 补齐退出码表并修正过期说明**：README 长期缺「退出码（调度侧判断成败）」章节
  （api2ods / sftp2ods 都有），现补 0 / 1 / 2 / 130 对照表；并修正 v1.5.0 拆包后过期的部署说明
  ——原文只写"把 `feishu2ods.py`、`requirements.txt`、作业文件放好即可"，实际还必须带上
  `feishu2ods/` 包目录，否则入口壳 `feishu2ods.py` 会 ImportError。另补充 `--init-out` 的说明、
  把两处 `--sql-timeout` 覆盖范围统一为「建表 / 分区增删与清理 / 分区核对 / 写后行数核对 / rename」。
- **标识符校验提示与实际规则对齐**：`IDENT_RE` 允许以下划线开头，但报错信息与向导文案写成
  "字母开头"，前后矛盾。现统一为"字母/数字/下划线，且不能以数字开头"，与实际接受的输入一致
  （只改文案，校验行为不变）。
- **示例作业泛化为中性命名（信息卫生）**：示例文件改名为 `jobs/feishu_example.example.json`
  （原文件名与文件内的 `job` 名都带生产业务口径，一并去掉），`"job"` 同步改为 `feishu_example`，
  README / CONTRIBUTING / MANIFEST / `.github` 的引用一处不留地跟着改。文件内容里，目标表名
  （原为某张生产业务表）与 `fields` 的 Base 列名（原为一组生产业务指标名）一并换成中性示例
  （表名 `ods_example_json_df`，列名「日期 / 名称 / 数量 / 金额」），description / comment 同步
  去掉业务口径；保留"中文列名 → 英文键"的示例价值，凭证仍为占位符。同时把 README 里"用户自己的
  作业文件"示例命令统一为 `jobs/my_table.json`（与 api2ods 的 `jobs/my_api.json`、sftp2ods 的
  `jobs/my_sftp.json` 同一命名习惯；`--init` 生成的文件名规则不受影响）。
- **补充新鲜度支持日期形态的说明**：README 明确 `freshness.date_field` 的值会规范成 `yyyy-MM-dd`
  再比对，只认 ISO 串（取前 10 位）/ `yyyy/MM/dd` / epoch 毫秒数字，**紧凑数字串（如 `20260927`）
  不被识别**（会误判缺数据），见「行为说明」第 6 条与「常见问题」。
- README：`maxcompute.endpoint` 默认值标注为 https。

## [1.5.0] - 2026-09-30

- 流式落盘（`SpoolWriter` + `FetchStats`）：记录边拉边写本地临时 JSONL，写库时逐批读回，
  峰值内存与总行数无关；临时分区 + 原子替换；新鲜度校验、新增列提醒等（历史能力，此处补记）。
