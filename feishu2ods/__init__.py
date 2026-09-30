# -*- coding: utf-8 -*-
"""feishu2ods 包：飞书多维表格 → MaxCompute ODS（JSON 原样落库，json + pt 分区，临时分区原子替换）。

模块划分：
    cli     命令行入口（--check / 正式同步 / --init 分发）
    auth    飞书 HTTP 请求（重试/限流退避）与 tenant_access_token
    fetch   多维表格拉取（offset 翻页 + 一致性保护 + 流式落盘）
    mc      MaxCompute：建表 / 结构校验 / 写分区（原子替换）/ 写后核对
    spool   流式落盘（SpoolWriter）与拉取统计（FetchStats）
    config  job 配置读取与校验
    dates   业务日解析与日期值规范化
    notify  飞书群告警
    utils   日志 / 脱敏 / 运行锁
    wizard  --init 交互式配置向导
"""

VERSION = "1.5.0"
"""版本号。必须是字面量（pyproject 的 dynamic version 用 attr 静态读取；
`from .config import VERSION` 这种间接引用静态解析读不到，会 fallback 到执行导入，
此时根目录的兼容薄壳 feishu2ods.py 会遮蔽包导致构建失败）。"""
