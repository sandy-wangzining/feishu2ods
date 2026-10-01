# 贡献指南

欢迎提 Issue 和 PR。这个工具的目标很明确：**配置驱动、写入幂等、宁可失败也不静默写坏数据**。
提改动前请先读一下下面的"设计红线"，很多看似"顺手优化"的改法会踩到它们。

## 环境准备

```bash
git clone https://github.com/sandy-wangzining/feishu2ods.git
cd feishu2ods
python3 -m venv venv
./venv/bin/pip install -e ".[dev]"        # Windows: .\venv\Scripts\pip install -e ".[dev]"
```

只需要 Python 3.10+，核心依赖只有 `requests` + `pyodps`，没有系统级依赖。

## 跑测试与代码检查

```bash
python -m unittest discover -s tests -v   # 全部用例：不访问网络、不连 MaxCompute
ruff check .                              # 代码检查（配置在 ruff.toml，当前 0 告警）
ruff format --check .                     # 格式检查
```

- 测试**必须离线可跑**：不许依赖真实飞书接口、真实 MaxCompute、本机特定的文件（CI 会在
  ubuntu / windows / macos × 多版本 Python 上跑同一套用例）。
- 需要外部资源时用 `unittest.mock` 打桩；需要作业文件时在 `tempfile.TemporaryDirectory()`
  里现写一份，**不要引用 `jobs/*.json`**（真实作业文件含密钥，不入库）。
- 单测写运行锁时把锁目录指到临时目录（基类已 `monkeypatch` 掉工具目录下的 `.run-locks/`），
  别让测试往仓库里丢锁文件。
- 改动涉及数据完整性判定（空分区保护、写后行数/去重核对、原子替换）时，请补上"改之前会失败"
  的回归用例——这类 bug 的共同点是**静默**：跑完显示成功，数据却少了或错了。

## 设计红线（改代码前必读）

1. **全量快照写 `pt=<业务日>`**：每次运行把整张表的数据按 `fields` 映射后写进当天的分区，
   不写增量、不写多分区。`pt`（业务日）取值优先级 `--bizdate` > 环境变量
   `bizdate` / `SKYNET_BIZDATE` > 当天-1（CN）。
2. **临时分区 + 原子替换**：写入必须先落到 `pt=<业务日>__tmp`，核对通过后再「删旧分区 + rename」
   换成正式分区——写入期间旧快照始终完整可读，下游不会读到空/半截分区。任何失败路径都不能让
   正式分区停在一半；写前校验（如单行大小）必须在动分区之前完成。
3. **宁可失败，不可静默丢数**：翻页中途字段列表/版本号（rev）变化、record_id 重复、映射的列
   缺失、写后行数或去重 id 对不上——一律抛错并以非 0 退出码结束（调度系统靠它告警）。
   需要放宽的情况（如 `target.allow_empty=true`、`--skip-freshness`）必须**打警告日志留痕**，
   并且写空分区前先查目标分区现有行数（非 0 时需 `--force` 才放行）。
4. **密钥不进日志**：任何进日志/异常的文本都要过 `utils.redact()`（形态级 + 值级双重脱敏）；
   新增的日志出口也要走 `log()`。测试里写假密钥；真实密钥只写在 `jobs/*.json`（已 `.gitignore`）。
5. **峰值内存与行数解耦**：记录边拉边落盘（`spool.py` 的 `SpoolWriter`），写库时逐批读回；
   新增功能时保持"峰值内存只与单页/单批数据量有关，与总行数无关"。
6. **每个错误都要归位**：配置/鉴权错 → `SystemExit`（配置校验阶段立即失败）；
   MaxCompute 的 SQL 一律经 `run_sql_with_timeout`（默认 600 秒、超时主动取消），
   避免云端卡住时任务无限挂起、一直占着运行锁。
7. **跨平台**：路径用 `pathlib`，编码一律显式 `utf-8`，控制台输出走 `utils.log()`
   （它会自动处理 Windows 老控制台的编码问题）。Windows / macOS / Linux 三种行为都要能跑。

## 代码风格

- 中文注释与日志（目标用户是国内数仓同学），注释解释**为什么**，不复述代码在做什么。
- 每个模块头部写清职责；对外函数写 docstring，说明参数含义与失败行为。
- 行宽 120；`ruff` 配置只选"能发现真问题"的规则（`E4/E7/E9/F/W/I/UP`），
  风格类规则故意没开——不用为了对齐风格改代码。
- 不引入新依赖：核心只用 `requests` + `pyodps`。确有必要的依赖请先在 Issue 里讨论。

## 提交 PR

1. 从 `main` 切分支，一个 PR 做一件事；
2. 本地跑通 `python -m unittest discover -s tests` 与 `ruff check .`、`ruff format --check .`；
3. PR 描述里写清：**为什么改**（复现步骤 / 影响的作业）、**怎么验证的**；
4. 涉及行为变化或修 bug 的，同步更新 `CHANGELOG.md`（修 bug 说明"原来会怎样、现在怎样"）；
5. 涉及配置项/命令行参数的，同步更新 `README.md` 与 `jobs/feishu_ai_cost.example.json`
   （三者是同一份文档的三个入口，容易漏）。

提交信息用 Conventional Commits 风格（`fix:` / `feat:` / `docs:` / `test:` / `refactor:`），
第一行说清做了什么，需要时在正文里补"为什么"。

## 想接入一张新表？

多数情况写一份 job 配置就够了，不用改代码——照 README 的「接入一张新表」走，
`feishu2ods --init` 可以交互式生成配置（自动拉列名和样例、逐列起英文键）。
配置里表达不了的表格形态，欢迎提 Issue 说明字段结构。

## 想加一种鉴权/翻页方式？

- 飞书的 `tenant_access_token` 鉴权与 `limit + offset` 翻页在 `auth.py` / `fetch.py` 里；
  限流退避、rev/字段变化中止、record_id 去重等一致性保护都在 `fetch.py`，改动要格外小心；
- 加完请补单测：翻页与一致性保护一定要覆盖"中途变化就中止"的回归用例。

## License

贡献的代码按 MIT 许可（见 `LICENSE`）。
