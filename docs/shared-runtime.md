# 三个 ODS 项目的公共源码

唯一维护源位于 **api2ods 仓库的 `shared/`**。sftp2ods、api2ods、feishu2ods 都随包携带
`_common.py`，无需安装另一个 ODS 项目、设置 PYTHONPATH、连接源码仓库或下载额外公共包。
现有命令行、作业 JSON、日志入口与各自写库策略保持兼容。

## 已共用的部分

- SQL 提交计时、轮询、心跳、超时和异常退出时取消云端实例。
- 告警地址归一化和飞书卡片结构。
- 外部标签展示、嵌套凭证值收集、认证方案与裸 token 拆分。
- 公共契约测试和离线一致性检查工具。

分页、SFTP 解析、凭证加载和分区写入策略由各自项目维护。特别是 feishu2ods 的临时分区替换
与另外两个项目的直接覆盖并不等价，共用工具不改变这些业务行为。

## 修改与同步

不要直接修改包内 `_common.py`、`tests/test_shared_runtime.py`、`tools/sync_common.py`
或本说明的副本。修改 api2ods 的 `shared/` 对应源文件，然后一次同步三个仓库：

```bash
# 在 api2ods 仓库根目录执行；路径可按实际 checkout 位置调整
python shared/sync_common.py . ../sftp2ods ../feishu2ods
python tools/sync_common.py --check . ../sftp2ods ../feishu2ods
```

也可从其它项目执行：

```bash
python tools/sync_common.py --source ../api2ods/shared .
```

`shared-runtime.json` 记录本次同步各文件的内容摘要。单独 clone 任一仓库后，执行
`python tools/sync_common.py --check` 即可离线检查副本与已记录版本是否一致；在 api2ods
内运行时还会与 `shared/` 原始文件比对。CI 会执行同一检查。校验会忽略 Windows checkout 的
CRLF 差异，不会自动下载或升级公共源码。

每次公共改动必须一起更新三份副本、测试、摘要与变更记录，并分别运行全量单测、ruff 和
独立安装检查，再分别提交三个仓库。单独项目的修复不需要同步其它项目。

同步采用逐文件原子替换，并保留已有文件权限；多文件、跨仓库同步不具备事务性。
这是源码维护命令，部署前应检查完整差异并运行 `--check`；中途失败时重新同步可补全。
