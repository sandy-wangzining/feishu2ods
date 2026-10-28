# 三个 ODS 项目的公共源码

唯一维护源位于 **api2ods 仓库的 `shared/`**。sftp2ods、api2ods、feishu2ods 都随包携带
`_common.py`，无需安装另一个 ODS 项目、设置 PYTHONPATH、连接源码仓库或下载额外公共包。
项目保持独立安装；公共运行层不依赖任何团队内部服务。行为变化与迁移步骤见各项目 README 和 CHANGELOG。

## 已共用的部分

- 默认业务日：没传日期时统一取业务日时区的昨天；时区可用 `ODS_BIZDATE_TZ` 覆盖，默认 `America/New_York`。
- 默认临时运行锁目录的隔离与安全校验（POSIX 按 UID 隔离、拒绝软链/他人属主/公共可写目录）。
- 目标表锁：按不区分大小写的 `project.table` 生成统一键，三个工具和不同安装目录共用。
  默认目录为系统临时目录下的 `ods-target-locks[-UID]`；多用户或不同 `TMPDIR` 运行时，用
  `ODS_TARGET_LOCK_DIR` 指向同一固定目录。它不代替各工具原有的作业锁。
- SQL 提交计时、轮询、心跳、超时和异常退出时取消云端实例。
- 告警地址归一化和飞书卡片结构。
- 外部标签展示、嵌套凭证值收集、认证方案与裸 token 拆分。
- 公共契约测试和离线一致性检查工具。

默认业务日使用标准库 `zoneinfo`；Windows 下需要 `tzdata`，三个项目的依赖都已按
`platform_system == "Windows"` 声明，无需手工安装。Linux 读取系统 `/usr/share/zoneinfo`；
精简镜像若没有该目录，需安装系统 `tzdata` 包或 `pip install tzdata`。

分页、SFTP 解析、凭证加载和分区发布由各自项目维护。三个工具均先写独立生命周期暂存表，
核对后以单条 INSERT OVERWRITE 发布正式分区，不提前删除正式分区；feishu2ods 额外核对 record_id。

锁通过操作系统文件锁实现。同一台机器、同一锁目录可以互斥；跨主机只有在共享文件系统确实
支持跨主机文件锁时才有效，不能仅凭配置了共享路径就承诺分布式互斥。正式运行应固定调度机器。
不支持文件锁时默认失败；各工具已有的 `*_ALLOW_NO_LOCK=1` 是显式接受并发风险的逃生开关。
升级前先停止旧进程，再更新所有会写同一目标的工具，避免旧版本仍使用旧锁位置。

## 多用户共享锁目录

POSIX 默认锁文件仍为 `0600`。跨用户运行时，管理员应先建立共用组、将所有调度账号加入组，
并为目标锁和需要共享的作业锁准备固定的 **setgid 目录**（`2770`）。例如 Linux 上，`ods-sync`
组已存在且当前登录已取得该组权限时：

```bash
sudo install -d -o root -g ods-sync -m 2770 /var/lock/ods-target-locks
export ODS_TARGET_LOCK_DIR=/var/lock/ods-target-locks
```

所有写同一目标的进程都必须使用该目录。作业锁需要跨用户共用时，为相应工具另建同样权限的
固定目录并设置 `API2ODS_LOCK_DIR` / `SFTP2ODS_LOCK_DIR` / `FEISHU2ODS_LOCK_DIR`。
setgid 让新文件继承目录组；运行锁在该目录中以 `0660` 打开，不受调用者严格 `umask` 影响，
其余目录新建锁仍为 `0600`。锁文件不含凭证，只保存进程标记；共享组应仅包含可信的调度账号。

迁移既有目录时，先停止所有相关进程，由属主或管理员将目录及已有 `.lock` 文件的组改为共用组，
目录设为 `2770`、锁文件设为 `0660`。属主再次运行也会修正 setgid 目录中遗留锁文件的读写权限；
不要删除锁文件来迁移权限，否则可能破坏仍在使用的 inode 锁。Windows 使用目录继承的 ACL，
需给所有运行账号授予该目录及锁文件的读写权限。

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
