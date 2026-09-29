# feishu2ods

飞书多维表格（Base）→ MaxCompute ODS 通用小工具：每条记录一行 JSON，写进「单列 string +
`pt` 分区」表：每次运行把全量快照写进 `pt=<业务日>`——先写 `pt=<业务日>__tmp` 临时分区，
核对行数后再「删旧分区 + rename」原子替换（重复跑幂等；写入期间旧快照完整可读，下游不会
读到空/半截分区）。JSON 键用 job 里配置的英文映射（Base 中文列名 → 英文键），值原样保留
（如 `"$6,079.07"`、日期 ISO 串）；清洗与落表口径由下游 DWD 用 `get_json_object` 自行解析。

`pt`（业务日）取值优先级：`--bizdate` > 环境变量 `bizdate` / `SKYNET_BIZDATE`（DataWorks）>
当天-1（CN）；格式 `YYYYMMDD` 或 `YYYY-MM-DD`。

环境要求：Python 3.10+（代码用到 `X | None` 类型标注），依赖见 `requirements.txt`（requests + pyodps）。

## 目录

- `feishu2ods.py` 主脚本（单文件）
- `jobs/*.json` 作业配置（含密钥，已 gitignore；格式参考 `jobs/feishu_ai_cost.example.json`）
- `tests/` 离线单测：`python -m unittest discover -s tests -v`（不访问网络、不连 MaxCompute）
- `requirements.txt` 依赖（requests + pyodps）

## 快速开始（已有作业）

本地：

```bash
python feishu2ods.py --job jobs/feishu_ai_cost.json --check       # 体检（不写库、不发告警）
python feishu2ods.py --job jobs/feishu_ai_cost.json --dry-run     # 试跑：拉数校验，不写库
python feishu2ods.py --job jobs/feishu_ai_cost.json --bizdate 20260928   # 正式：写 pt=20260928（临时分区 + 原子替换）
python feishu2ods.py --job jobs/feishu_ai_cost.json               # 不传 --bizdate：取环境变量，再默认当天-1
```

部署机（把工具目录同步上去即可）：

```bash
cd ~/feishu2ods
./venv/bin/python feishu2ods.py --job jobs/feishu_ai_cost.json --bizdate "${bizdate}"
```

常用参数：`--check`（体检）、`--init`（交互式生成作业，接新表用；密钥输入不回显）、`--bizdate`（业务日 pt）、
`--dry-run`（不写库；校验与告警仍按正式逻辑执行，不想发告警加 `--no-notify`）、
`--skip-freshness`（跳过新鲜度校验，兼容旧写法 `--no-check`）、`--no-notify`（不发告警）、
`--project` / `--table`（覆盖目标，测试用）。

## 接入一张新表（复用流程）

1. 把飞书自建应用加为该表格的「可阅读」协作者（表格 → 分享/协作者 → 添加应用）；
2. `python feishu2ods.py --init`：按提示生成 `jobs/<作业名>.json`——自动拉列名和样例，
   逐列起英文键（不需要的列回车跳过）；项目、表名、注释、新鲜度告警都可回车用默认值，
   填错会当场提示重填（wiki 知识库链接里不是 base_token，会提示换 /base/ 链接）；
3. `python feishu2ods.py --job jobs/<作业名>.json --check` 体检（配置 + API 连通 + 映射 + 目标表）；
4. `--dry-run` 试跑，条数/日期没问题后正式跑；调度命令同 feishu_ai_cost（换 --job 即可）。

## job 配置说明

一般不用手写（`--init` 生成后核对即可）：

| 字段 | 说明 |
| --- | --- |
| `feishu.app_id` / `feishu.app_secret` | 飞书自建应用（需先被加为目标表格的「可阅读」协作者） |
| `feishu.base_token` / `feishu.table_id` | 多维表格 token / 数据表 ID |
| `feishu.base_url` | 可选，告警卡片里带表格链接 |
| `maxcompute.project` | 默认项目（`target.project` 未给时用它） |
| `maxcompute.endpoint` | 可选，默认美国硅谷接入地址 |
| `maxcompute.access_key_id` / `access_key_secret` | 阿里云凭证 |
| `fields` | Base 列名 → JSON 英文键（必填；英文键须是字母/数字/下划线，唯一） |
| `target.project` / `target.table` / `target.column` | 目标表（column 默认 `json`；表会自动建） |
| `target.comment` | 可选，表注释 |
| `target.allow_empty` | 可选，默认 `false`：拉到 0 行时拒绝写库 |
| `freshness.date_field` | 用哪个英文键判日期（必须是 `fields` 的英文值） |
| `freshness.lag_days` | 预期日期 = bizdate - N 天，默认 0（即必须有业务日当天的数据） |
| `freshness.webhook` | 缺失时发飞书群告警的 webhook |

## 行为说明

1. 鉴权：`app_secret` 换 `tenant_access_token`（有效期 2 小时，翻页中途失效会自动重取一次）；
2. 拉取：v3 records 接口 `limit=500 + offset` 翻页（接口上限 2000）；接口限流（99991400）自动退避重试；
   翻页中途字段列表变化、或表格版本号（rev）变化都会中止（防 offset 翻页期间被编辑导致静默漏行）；
   记录 ID 重复会立刻中止（防接口忽略 offset 重复写）；
3. 映射：映射的 Base 列缺失（被改名/删除）→ 直接报错（空表也校验）；未映射的列 → 忽略并打一条告警；
   Base 里「除 record_id 外全为空」的空白行会**原样同步**（日志会提示条数），下游记得按日期字段过滤；
   金额等值原样带格式（如 `"$23.32 "` 尾随空格），下游解析前先 `trim`；
4. 写入：写库前先校验目标表结构（单列 string + pt 分区），不符直接报错、**绝不先清表**；
   单行超过约 7MB 会在动分区前报错；结构正确才写 `pt=<业务日>__tmp` 临时分区 + Tunnel 分批写入，
   临时分区核对「行数 + record_id 去重数 + 最小/最大 id」（不一致不替换），再「删旧分区 + rename」原子替换；
   全程失败自动重试 3 次（重跑幂等；失败后尽力清掉临时分区，若有残留下次运行也会先清理）；
5. 并发保护：同一作业有运行锁（Linux flock / Windows msvcrt，进程退出自动释放，不会残留死锁）；
   定时任务与手动重跑重叠时，后启动的一边主动退出（报错里带持有者 pid/机器/时间）；不同作业互不影响；
   **锁只在同一台机器内生效**：本地手跑与服务器调度同时跑同一作业没有保护，正式跑请固定一台机器；
6. 新鲜度校验（配了 `freshness` 才做）：`date_field` 里必须出现「业务日 - `lag_days`」
   （业务日 = `--bizdate` / 环境变量 / 当天-1；默认 lag_days=0，即少 bizdate 当天就告警），
   否则发飞书告警并以非 0 退出、不写库（日志会提示调 lag_days 或用 --skip-freshness 补数）；
   同一天出现多行会打警告（DWD 会落多行）；
7. `--check` 只体检：配置 + API 连通 + 字段映射 + 目标表结构，不写库、不发告警、不校验新鲜度。

## 部署到服务器（一次性）

```bash
# 本机打包上传到部署机（scp/rsync 均可）
# 服务器上：
mkdir -p ~/feishu2ods/jobs
cd ~/feishu2ods
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
# 把 feishu2ods.py、requirements.txt、jobs/feishu_ai_cost.json 放好即可
```

## 常见问题

- `获取 tenant_access_token 失败`：`app_id`/`app_secret` 不对，或应用未发布版本；
- `拉取记录失败：code=91403`：应用没被加为表格协作者（在表格「分享/协作者」里把应用加为可阅读）；
- `已有任务在运行（锁文件 ...）`：同一台机器上同一作业还有一个实例在跑（报错里会显示持有者 pid/机器/启动时间）；
  锁随进程退出自动释放，确认没有实例在跑时**不需要**手动删锁文件；跨机并发（本地与服务器同时跑）不受此锁保护；
- `表结构与工具要求不一致，拒绝写入`：目标表已有别的结构，人工核对后再处理（工具不会动不符合结构的表）；
- 日期校验失败但表里其实有数据：查 `freshness.date_field` 是否指对字段、`lag_days` 是否要调；
- 下游解析报错/行数为 0：表格里可能有空白行（日志会提示「全为空」）、或值带尾随空格（如 `"$23.32 "`），
  用 `get_json_object(...) is not null` 过滤空白行、`trim` 后再转数值；
- 看到 `pt=...__tmp` 分区：某次写入中断留下的临时分区，重跑该作业会自动清掉并重建，不影响正式分区；
- 接新表拿不准配置：`--init` 生成后先 `--check`，报错信息都会指到具体字段。
