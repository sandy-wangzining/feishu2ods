# feishu2ods

飞书多维表格（Base）→ MaxCompute ODS 通用小工具：每条记录一行 JSON，写进「单列 string +
`pt` 分区」表：每次运行把全量快照写进 `pt=<业务日>`——先写 `pt=<业务日>__tmp` 临时分区，
核对行数后再「删旧分区 + rename」原子替换（重复跑幂等；写入期间旧快照完整可读，下游不会
读到空/半截分区）。JSON 键用 job 里配置的英文映射（Base 中文列名 → 英文键），值原样保留
（如 `"$6,079.07"`、日期 ISO 串）；清洗与落表口径由下游 DWD 用 `get_json_object` 自行解析。

`pt`（业务日）取值优先级：`--bizdate` > 环境变量 `bizdate` / `SKYNET_BIZDATE`（DataWorks）>
当天-1（CN）；格式 `YYYYMMDD` 或 `YYYY-MM-DD`。

环境要求：Python 3.10+（代码用到 `X | None` 类型标注），依赖见 `requirements.txt`（requests + pyodps）。

## 安装

```bash
# 方式一：pip（建议放虚拟环境；在源码目录执行）
python3 -m venv venv && ./venv/bin/pip install .    # macOS / Linux
py -3 -m venv venv; .\venv\Scripts\pip install .    # Windows（PowerShell）
feishu2ods --version

# 方式二：pipx（全局命令行工具，隔离环境；Windows / macOS / Linux 通用）
pipx install .

# 方式三：直接从 GitHub 安装
pipx install "git+https://github.com/sandy-wangzining/feishu2ods.git"

# 方式四：源码直接跑（不安装；Windows 把 python 换成 py -3 即可）
python feishu2ods.py --job jobs/xxx.json --check
```

> `python feishu2ods.py` 与 `python -m feishu2ods` 等价（前者是保留给既有调度命令的入口壳）。
> 运行时要求 Python 3.10+（代码用到 `X | None` 类型标注），Windows 上会自动带上 `tzdata` 依赖。

## 目录

- `feishu2ods/` 代码包（v1.5.0 起从单文件拆出，按职责分模块）：
  - `cli.py` 命令行入口（--check / 正式同步 / --init 分发）
  - `auth.py` 飞书 HTTP 请求（重试/限流退避）与 tenant_access_token
  - `fetch.py` 多维表格拉取（offset 翻页 + 一致性保护 + 流式落盘）
  - `mc.py` MaxCompute：建表 / 结构校验 / 写分区（原子替换）/ 写后核对
  - `spool.py` 流式落盘（SpoolWriter）与拉取统计（FetchStats）
  - `config.py` job 配置读取与校验；`dates.py` 业务日与日期规范化
  - `notify.py` 飞书群告警；`utils.py` 日志 / 脱敏 / 运行锁；`wizard.py` --init 向导
- `feishu2ods.py` 兼容入口（等价 `python -m feishu2ods`，保留给既有调度命令）
- `jobs/*.json` 作业配置（含密钥，已 gitignore；格式参考 `jobs/feishu_example.example.json`）
- `tests/` 离线单测：`python -m unittest discover -s tests -v`（349 条，不访问网络、不连 MaxCompute）
- `requirements.txt` 依赖（requests + pyodps）

## 快速开始（已有作业）

本地：

```bash
python feishu2ods.py --job jobs/my_table.json --check       # 体检（不写库、不发告警）
python feishu2ods.py --job jobs/my_table.json --dry-run     # 试跑：拉数校验，不写库
python feishu2ods.py --job jobs/my_table.json --bizdate 20260928   # 正式：写 pt=20260928（临时分区 + 原子替换）
python feishu2ods.py --job jobs/my_table.json               # 不传 --bizdate：取环境变量，再默认当天-1
```

部署机（把工具目录同步上去即可）：

```bash
cd ~/feishu2ods
./venv/bin/python feishu2ods.py --job jobs/my_table.json --bizdate "${bizdate}"
```

常用参数：`--check`（体检）、`--init`（交互式生成作业，接新表用；密钥输入不回显，`--init-out` 可指定
输出路径）、`--bizdate`（业务日 pt）、
`--dry-run`（不写库；校验与告警仍按正式逻辑执行，不想发告警加 `--no-notify`）、
`--skip-freshness`（跳过新鲜度校验，兼容旧写法 `--no-check`）、`--no-notify`（不发告警）、
`--force`（0 行写空分区前的保护放行开关，见下）、`--project` / `--table`（覆盖目标，测试用）、
`--sql-timeout`（单条 MaxCompute SQL 超时秒数，默认 600，0=不限制；建表 / 分区增删与清理 /
分区核对 / 写后行数核对 / rename 都受保护）、`--log-file`（日志同时写一份到该文件：追加、UTF-8、
父目录自动创建——Linux 上跑 cron/调度时 stdout 会被截断，落盘是事后翻查的唯一途径）。

## 接入一张新表（复用流程）

1. 把飞书自建应用加为该表格的「可阅读」协作者（表格 → 分享/协作者 → 添加应用）；
2. `python feishu2ods.py --init`：按提示生成 `jobs/<作业名>.json`——自动拉列名和样例，
   逐列起英文键（不需要的列回车跳过）；项目、表名、注释、新鲜度告警都可回车用默认值，
   填错会当场提示重填（wiki 知识库链接里不是 base_token，会提示换 /base/ 链接）；
3. `python feishu2ods.py --job jobs/<作业名>.json --check` 体检（配置 + API 连通 + 映射 + 目标表）；
4. `--dry-run` 试跑，条数/日期没问题后正式跑；调度命令同 my_table（换 --job 即可）。

## job 配置说明

一般不用手写（`--init` 生成后核对即可）：

| 字段 | 说明 |
| --- | --- |
| `feishu.app_id` / `feishu.app_secret` | 飞书自建应用（需先被加为目标表格的「可阅读」协作者） |
| `feishu.base_token` / `feishu.table_id` | 多维表格 token / 数据表 ID |
| `feishu.base_url` | 可选，告警卡片里带表格链接 |
| `maxcompute.project` | 默认项目（`target.project` 未给时用它） |
| `maxcompute.endpoint` | 可选，默认美国硅谷接入地址（https） |
| `maxcompute.access_key_id` / `access_key_secret` | 阿里云凭证 |
| `fields` | Base 列名 → JSON 英文键（必填；英文键须是字母/数字/下划线，唯一） |
| `target.project` / `target.table` / `target.column` | 目标表（column 默认 `json`；表会自动建） |
| `target.comment` | 可选，表注释 |
| `target.allow_empty` | 可选，默认 `false`：拉到 0 行时拒绝写库；设为 `true` 时允许写空分区，但写前会先查目标分区现有行数，非 0 时仍需加 `--force` 才放行 |
| `freshness.date_field` | 用哪个英文键判日期（必须是 `fields` 的英文值）；值规范化成 `yyyy-MM-dd` 后比对，支持的形态见「行为说明」第 6 条 |
| `freshness.lag_days` | 预期日期 = bizdate - N 天，默认 0（即必须有业务日当天的数据） |
| `freshness.webhook` | 缺失时发飞书群告警的 webhook |

## 行为说明

1. 鉴权：`app_secret` 换 `tenant_access_token`（有效期 2 小时，翻页中途失效会自动重取一次）；
2. 拉取：v3 records 接口 `limit=500 + offset` 翻页（接口上限 2000）；接口限流（99991400）自动退避重试；
   翻页中途字段列表变化、或表格版本号（rev）变化都会中止（防 offset 翻页期间被编辑导致静默漏行）；
   记录 ID 重复会立刻中止（防接口忽略 offset 重复写）；**流式落盘**：记录边拉边写本地临时
   JSONL，写库时逐批读回——峰值内存只与单页数据量有关，与总行数无关（十几万行的大表
   也不再全量驻留内存）；新鲜度/空行/去重 ID 等统计在拉取过程中逐页累积；
3. 映射：映射的 Base 列缺失（被改名/删除）→ 直接报错（空表也校验）；未映射的列 → 忽略、
   照常同步其余字段，并发一条飞书提醒（列名 + 处理步骤：要不要加映射由人工决定；`--no-notify` 可关）；
   Base 里「除 record_id 外全为空」的空白行会**原样同步**（日志会提示条数），下游记得按日期字段过滤；
   金额等值原样带格式（如 `"$23.32 "` 尾随空格），下游解析前先 `trim`；
4. 写入：写库前先校验目标表结构（单列 string + pt 分区），不符直接报错、**绝不先清表**；
   单行超过约 7MB 会在动分区前报错；结构正确才写 `pt=<业务日>__tmp` 临时分区 + Tunnel 分批写入，
   临时分区核对「行数 + record_id 去重数 + 最小/最大 id」（不一致不替换），再「删旧分区 + rename」原子替换；
   全程失败自动重试 3 次（重跑幂等；失败后尽力清掉临时分区，若有残留下次运行也会先清理）；
   建表 / 分区增删与清理 / 分区核对 / 写后行数核对 / rename 这些 SQL 都有超时保护（默认 600 秒，`--sql-timeout` 可调，
   0=不限制；超时主动取消，避免云端卡住时调度任务无限挂起、一直占着运行锁把后续调度全挡掉）；
   **0 行保护**：`target.allow_empty=true` 且本次拉到 0 行时，写空分区前会先查目标分区现有行数——
   非 0 则拒绝写库（退出码 1）并提示加 `--force`，防止源端被截断/清空时把好数据抹掉；现有行数本来就是
   0 则正常写空分区；
5. 并发保护：同一作业有运行锁（Linux flock / Windows msvcrt，进程退出自动释放，不会残留死锁）；
   定时任务与手动重跑重叠时，后启动的一边主动退出（报错里带持有者 pid/机器/时间）；不同作业互不影响；
   写库阶段还有按「项目.表名」的表级锁：两份配置指向同一张表（迁移期新旧作业并存）时同机串行，
   不会互删临时分区、互相覆盖；
   文件系统不支持锁（NFS / 只读挂载等）时告警一次后**无锁继续**——这种环境确实没有互斥能力，
   但不该把作业判成"已有任务在运行"而永远跑不起来；
   **锁只在同一台机器内生效**：本地手跑与服务器调度同时跑同一作业没有保护，正式跑请固定一台机器；
   锁目录默认在工具目录 `.run-locks/`（不可写时退回系统临时目录，退回时日志会提示）；若同一作业
   会以不同身份或不同 TMPDIR 跑（root 手动补数与普通用户调度混用），用环境变量
   `FEISHU2ODS_LOCK_DIR` 把锁目录钉在固定位置，避免两个实例锁在不同文件上、互斥静默失效；
6. 新鲜度校验（配了 `freshness` 才做）：`date_field` 里必须出现「业务日 - `lag_days`」
   （业务日 = `--bizdate` / 环境变量 / 当天-1；默认 lag_days=0，即少 bizdate 当天就告警）。
   `date_field` 的值会先规范成 `yyyy-MM-dd` 再比对，支持的形态：ISO 串
   （`2026-09-27T00:00:00+08:00`，取前 10 位；未补零的 `2026-9-7` 也认）、`yyyy/MM/dd`、
   epoch 毫秒数字（换算结果须落在 2000~2100，0/14 位 `yyyyMMddHHmmss` 这类会按"认不出"处理）、
   8 位紧凑日期（数字 `20260927` 或文本 `"20260927"` 都认）。
   **缺数据只告警不失败**：很多表是人填的（节假日/休假没人填是常态），快照照常写入——
   缺的那天只是没有数据行（DWD 按源数据日期字段重新分区，下游任务不受影响），并发一条
   飞书提醒；**表被清空（0 行）仍按失败处理**（那才是真异常）。日志会提示调 `lag_days`
   或用 `--skip-freshness` 跳过校验；同一天出现多行会打警告（DWD 会落多行）；
7. `--check` 只体检：配置 + API 连通 + 字段映射 + 目标表结构，不写库、不发告警、不校验新鲜度。
   它是只读的，环境变量 `bizdate` 格式不对时不会把体检拖垮（按默认当天-1 继续并打警告）；
   正式同步路径仍严格要求业务日合法（非法的环境变量 `bizdate` 直接报错，不会静默回退）。

8. **脱敏**：日志与异常里的 app_secret、tenant_access_token、AK/SK、webhook 一律脱敏——
   形态规则（`Bearer …`、`"key": "value"`、`app_secret=…`）之外，还按配置里的密钥值精确遮蔽，
   包括其 URL 编码形态（`quote` / `quote_plus`，以及把 `-` 也编码的激进编码器形态；含中文的
   密钥同样覆盖）：接口把凭证写进自由文本报错时也不会漏；飞书告警卡片走同一套脱敏。

### 退出码（调度侧判断成败）

| 码 | 含义 |
|---|---|
| 0 | 成功（含 `--dry-run`；`--check` 体检通过；`--init` 生成配置成功；0 行且 `allow_empty=true` 时，目标分区本来就空、或显式加了 `--force`，也会写空分区并返回 0） |
| 1 | 运行失败：鉴权/拉取失败、映射的列缺失、翻页中途字段列表或表格版本号（rev）变化、写库失败、写后行数对不上、0 行保护触发、`--check` 未通过、`--init` 取消；作业文件不存在、业务日格式不对等配置类问题也归这里 |
| 2 | 命令行参数问题（缺 `--job`、`--sql-timeout` 为负等 argparse 层），**没有发起任何远端操作** |
| 130 | 用户中断（Ctrl+C），正式同步与 `--init` 一致 |

配置类错误统一以 `SystemExit` 抛出，退出码同为 1：凡是"重跑结果一样"的问题都归到 1，
调度直接告警即可，不必按码分流重试。

## 部署到服务器（一次性）

```bash
# 本机打包上传到部署机（scp/rsync 均可）
# 服务器上：
mkdir -p ~/feishu2ods/jobs
cd ~/feishu2ods
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
# 把 feishu2ods.py、feishu2ods/ 包目录、requirements.txt、jobs/my_table.json 放好即可
# （v1.5.0 起代码拆成 feishu2ods/ 包，feishu2ods.py 只是入口壳，缺了包目录会 ImportError）
```

## 常见问题

- `获取 tenant_access_token 失败`：`app_id`/`app_secret` 不对，或应用未发布版本；
- `拉取记录失败：code=91403`：应用没被加为表格协作者（在表格「分享/协作者」里把应用加为可阅读）；
- `已有任务在运行（锁文件 ...）`：同一台机器上同一作业还有一个实例在跑（报错里会显示持有者 pid/机器/启动时间）；
  锁随进程退出自动释放，确认没有实例在跑时**不需要**手动删锁文件；跨机并发（本地与服务器同时跑）不受此锁保护；
- `表结构与工具要求不一致，拒绝写入`：目标表已有别的结构，人工核对后再处理（工具不会动不符合结构的表）；
- `本次拉到 0 行，但该分区现有 N 行`：`target.allow_empty=true` 下的空分区保护触发——源端可能被
  截断/清空，为保住已有数据工具拒绝写库。确认表格确实变成 0 行后，重跑时加 `--force` 即可覆盖成空分区；
- 日期校验失败但表里其实有数据：查 `freshness.date_field` 是否指对字段、`lag_days` 是否要调；
  也看日期列的形态——只认 ISO（`2026-09-27T…` 取前 10 位）、`yyyy/MM/dd`、epoch 毫秒数字，
  **紧凑数字串 `20260927` 不被识别**（会被误判缺数），需在 Base 侧改成可识别格式；
- 下游解析报错/行数为 0：表格里可能有空白行（日志会提示「全为空」）、或值带尾随空格（如 `"$23.32 "`），
  用 `get_json_object(...) is not null` 过滤空白行、`trim` 后再转数值；
- 看到 `pt=...__tmp` 分区：某次写入中断留下的临时分区，重跑该作业会自动清掉并重建，不影响正式分区；
- 日志/告警里会不会出现密钥：不会——统一过脱敏（形态规则 + 按配置里的密钥值精确遮蔽，
  含 URL 编码形态），告警卡片同样脱敏后才发出；
- 接新表拿不准配置：`--init` 生成后先 `--check`，报错信息都会指到具体字段。
