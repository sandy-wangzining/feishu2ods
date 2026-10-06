# Changelog

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 修复

- **redact 的密钥表快照移入锁内（安全）**：与 `reset_secret_values` 的 clear+extend
  并发时，直接迭代模块级列表会读到中间态、漏遮本轮密钥；现在锁内取快照、锁外替换。
- **`--log-file` 展开 `~`（可用性）**：同上。

- **低危分歧项处置（安全/可靠性）**：脱敏表改由 `reset_secret_values` 在锁内重设
  （并发调用 main() 不再抹掉别的运行的密钥）；日志出口脱敏失败不再退化为原文（改为
  占位提示，防凭证原样进日志）；值级循环过滤非字符串元素。

- **锁名计算对任意文件名都成立（可靠性）**：作业路径含非 UTF-8 字节（surrogateescape 的
  代理字符，如从旧系统解包出来的文件名）时，锁名哈希原来 `encode("utf-8")` 直接抛
  UnicodeEncodeError、进程在加锁前就崩溃；改用 `os.fsencode`（与文件系统同口径）。
  值级脱敏的"激进编码"变体对同款代理字符补 `surrogatepass`，`quote`/`quote_plus`
  编码变体遇到代理字符时跳过该形态，日志出口的脱敏再包一层兜底——脱敏本身绝不抛。

- **日志写入不再持有全局锁（可靠性）**：log() 原来在模块级锁内执行 stdout 与 --log-file
  的 write/flush——慢速目标（管道被压满、NFS/满盘上的日志盘）会把其它线程的 log_once /
  add_log_sink / remove_log_sink 一起卡死，整个进程表现为停滞；现在锁内只做 sink 快照，
  写入全部在锁外（写失败摘除仍在锁内，且不覆盖并发新加进来的 sink）。

- **redact_secrets 接受裸标量 values（健壮性）**：`values` 直接传数字/字符串时，原来会把
  字符串拆成单字符或抛 TypeError；现在按"只有一个密钥"包一层（非字符串值仍跳过、不误伤
  文本；与 sftp2ods / api2ods 同款）。

- **值级脱敏补一个口子（安全）**：密钥值的 URL 编码形态原来只替 quote/quote_plus 两种
  （`+`/`/`/`=` 被覆盖），部分编码器把 `-` 也编码成 `%2D` 时仍会漏——补"非字母数字全编码"
  变体（`redact` 的值级替换与 `redact_secrets` 两处，与 sftp2ods / api2ods 同口径）。

- **运行锁不再把"文件系统不支持锁"误报成"已有任务在运行"（可用性）**：`_try_lock` 原来把所有
  `OSError` 一律当"忙"，在 NFS/只读挂载（`ENOLCK`/`ENOTSUP`）上作业会直接以"已有任务在运行
  （锁文件 …）"退出，删锁文件也没用——等于永远跑不起来。现在区分"忙"（报已有任务在运行）、
  "文件系统不支持锁"（告警一次后无锁继续，与 api2ods / sftp2ods 同口径）、其余上抛；
  拿不到 errno 的形态仍保守按"忙"处理，不放过真并发。

- **`count_partition` / `verify_partition` 读不到行不再静默按 0 处理（数据安全）**：校验 SQL
  必然返回一行，"读不到结果"说明 SQL 没真正执行 / reader 异常。返回 0 会把"没读到结果"伪装成
  "分区确实 0 行"——写前的 0 行保护会据此以为"分区本来就是空的"，接着把有数据的分区清空，
  写后核对还会 `0 == 0`、rc=0 静默丢数。现在直接报"未返回行"失败（与 sftp2ods 同口径）。

- **SQL 超时改用单调时钟 + 已终止未成功时显式失败（正确性）**：`run_sql_with_timeout` 原来用
  墙钟计时（NTP 校时/夏令时回拨会误判超时并 `stop()` 掉正在跑的 SQL），且在 `is_terminated()`
  后只调用一次 `wait_for_success` 就返回实例——它万一没抛（超时语义/实现差异），失败的 SQL 会被
  当成成功、后续按"其实没执行"继续删/rename 分区。现在与 sftp2ods / api2ods 同口径：
  单调时钟 + wait 之后再判一次成功性。

- **失败的响应显式关闭（可靠性）**：`request_json` 的重定向 / 4xx / 5xx / 非 JSON 分支原来
  不关响应对象，连接不会归还连接池（重试次数多时会占着连接）；现在与 api2ods 的 http 层同口径
  先关再报错/重试。

- **`target.allow_empty` 在 run_sync 里显式校验类型（数据安全）**：`bool("false")` 是 True——
  库调用方绕过 validate_job 时，字符串形态的 "false" 会静默跳过 0 行保护（源表被截断成
  0 行也照样写空分区、退出码 0）。现在与 lag_days 同口径：非布尔直接报配置错。
  分区探测改为三态：只有**确证** tmp 还在才走"补 drop + 补 rename"的快速路径，
  探测失败/状态未知一律落回常规路径重建，绝不先删正式分区（万一上一轮 rename 已生效，
  删掉的就是刚写入的新数据）。

- **重试收尾先确认分区现状再动 DDL（数据安全）**：「重试只补 rename」的快速路径有两个边角——
  (1) `final_deleted` 是在 drop 之前置位的，删正式分区失败后正式分区还在，rename 因「目标已存在」
  必然失败、还谎称「已被删掉」；现在先探测 tmp 还在不在：在就先补一次幂等的 drop 再 rename；
  (2) tmp 已不在时先确认正式分区真的就位才按完成处理（元数据误报不能变成假成功），确认后不再补
  drop、避免把刚顶上去的新分区删掉；两者都探测不到（状态未知）则落回常规路径重建收尾，不假报成功。
  报错文案按「确认让位/未知」区分；purge 的全量分区列表改为取一次复用（原来会发两遍元数据请求）。

- **__keep 改名失败时不再作虚假承诺（数据安全）**：改名失败留下的其实是 `__tmp` 形态，下一次运行的
  残留清理会删掉它，但日志原来仍写「__keep 分区不会被误删」。现在分两种情况：同 pt 的
  `__keep` 已存在（上一轮的完整副本仍在保护中）就说明本轮 tmp 会被回收、恢复请用 `__keep`；
  没有现成 `__keep` 时改为紧急提示「请立即手工 rename 恢复」，异常文案也如实描述。

- **重试不再清掉已核对完整的临时分区（数据安全）**：第一次 rename 失败后（正式分区已删），
  那份 tmp 是唯一完整副本；原来的重试会先清掉它再重建，重建中途再失败（限流/断连）就把
  当天分区彻底弄丢。现在这种状态下的重试只补做 rename；最终仍失败时完整副本按 __keep 保留。

- **冗余 `__keep` 在正式分区恢复后就地清理（数据安全）**：`pt__keep` 的字符串序大于同日期
  正式分区，重跑成功恢复正式分区后若不清掉它，下游 `where pt = max_pt()` 会永远读到上一轮
  失败的旧快照，且 `__keep` 不是 8 位业务日、按 yyyyMMdd 解析的下游会取错日期。现在 purge
  检测到同 pt 的正式分区已就位即清理该冗余分区；正式分区还没回来的照旧保留并提示。

- **未补零的横杠日期（如 2026-9-7）与斜杠写法同口径（可用性）**：`DATE_RE` 原来强制两位
  月/日，源端手填的未补零日期解析为 None，新鲜度校验每天误报缺数据。

- **`key='value'` 形态的密钥不再漏进日志（安全）**：query 规则的值部分不吃引号，
  `access_token='t-xxx'`（f-string 的 `!r` 插值 / repr 输出就是这种形态）在行中会整段
  漏遮；新增「键无引号 + 值带引号」规则，命中密钥词即遮值，键不敏感时递归兜底
  （值里嵌的 `token=…` 也认）。与 sftp2ods / api2ods 同款修复。

- **残留清理不再误删在途临时分区（数据安全）**：同机另一个作业（两份配置指向同一张表）
  正在写时，它的 `pt__tmp_<host>_<pid>` 分区名里带着还活着的本机 pid，purge 现在识别
  出来并跳过（清掉会把在途写入连同「正式分区已删」的窗口一起打死）；pid 已死的同机
  残留与跨机残留照旧清理。

- **运行锁目录可用 `FEISHU2ODS_LOCK_DIR` 固定（可靠性）**：工具目录不可写时会退回按
  用户/TMPDIR 解析的临时目录，同一作业的 root 实例与普通用户实例会锁在不同文件上、
  互斥静默失效；现在可用环境变量钉在固定目录（指定的目录不可用直接报错、不静默换），
  退回临时目录时也有明确提示。

- **向导解析 Base 链接不再截断 token（正确性）**：`/base/` 与 `?table=` 的字符集原来
  只认字母数字，token 含 `-`/`_` 时会被截断，且截断值能骗过白名单校验写进 job 配置；
  现在与 `config._FEISHU_ID_RE` 同口径（`-`/`_` 都认）。

- **飞书通知不再把「没有 code 的 200 响应」当成功（可靠性）**：webhook 误填成其它接口
  （回 `{"msg": "ok"}` 这类）时原来会打印「已发送」、告警通道静默失效；现在要求显式
  `code`/`StatusCode` 为 0（仅空 `{}` 保留按 HTTP 200 判定的宽容，措辞注明依据）；
  `False == 0`、`0.0 == 0` 的响应同样不算成功码（布尔 false / 浮点 0 的"失败"不能当成功）。

- **向导中断时清理含密钥的临时文件（安全）**：写盘途中 Ctrl+C（fsync 慢盘时的高发窗口）
  原来不会被清理逻辑接住（`except Exception` 漏掉 BaseException），在 jobs/ 里留下
  含明文密钥的 `.<作业名>.json.XXXX.tmp`，而向导还提示"未生成任何文件"；现在清理覆盖
  BaseException，中断前先删临时文件。

- **文件系统不支持运行锁时改为 fail-closed（数据安全）**：原来在 NFS/只读挂载
  （ENOLCK/ENOTSUP）上"告警一次后无锁继续"——两个实例会并发写同一作业/表
  （purge/rename 互拆、数据被静默覆盖）。现在默认直接拒绝执行并给出指引；确认无人并发
  时可用 `FEISHU2ODS_ALLOW_NO_LOCK=1` 显式接受无互斥风险（此时保留告警后继续）。

- **带分隔符的文本日期也纳入哨兵区间、新鲜度输入统一收集（正确性）**："9999-12-31"/
  "1970-01-01" 这类文本占位原来绕过 2000~2100 约束（四个日期分支抽成同一处口径）；
  新鲜度校验对 dict/str 混合输入改为逐元素统一收集（原来 str 元素会被整体丢弃、
  天天误报缺数据），并给 notify 的异常日志补显式 redact（不依赖 log() 出口必然脱敏）。

- **新鲜度校验两侧同口径归一化（正确性）**：`expected` 原来原样比较——调用方传 date
  对象或紧凑串（库调用方）时与规范化后的日期集合恒不相等、天天误报「缺数据」；现在与
  records 侧同样过一遍 normalize_date_value。

- **非 POSIX 平台的 pid 探测改为保守「可能还在」（数据安全）**：Windows 上
  `os.kill(pid, 0)` 会真的杀进程不能用，原来直接按"不在"返回、purge 会把同机在途写入的
  临时分区当残留删掉；现在返回"可能还在"，保守跳过。

- **log() 的 stdout/stderr 兜底扩展（可用性）**：除断管（OSError/ValueError）外，
  再兜 RuntimeError/AttributeError（sys.stdout 属性缺失等极端形态），"日志函数不打挂
  业务"的约定不留缝。

- **两处脱敏加固（安全）**：敏感键 + 无引号的值改为遮到行尾/`&`——`password=my secret`
  原来只遮第一个词、`secret` 明文留下（口令短语很常见）；URL userinfo 的口令按"最后一个
  `@`"切分——`https://user:p@ss@proxy:8080` 原来在第一个 @ 截断、口令余段明文留下。
  已是 `***` 形态的文本不重复吞（负向先行断言），非敏感键不吞后续键（扫描器实现）；
  未闭合引号（`password="abc` 被日志截断）与「带引号的键」+ 不带引号的值
  （`"password": my secret`）不再绕过三套规则。

- **两处边角（安全/可用性）**：`_api_err` 改为先脱敏再截断 200 字符（先截断会把成对引号
  截坏、形态规则失配，凭证原样漏出）；锁目录两个候选都不可用时退回系统临时目录会打警告
  （路径可能与其它实例不一致、并发保护可能失效，提示用 FEISHU2ODS_LOCK_DIR 固定）。

- **`_api_err` 的返回值统一过脱敏（安全）**：它拼进 SystemExit 文本（可能被调度器直接
  打到 stderr、不经过 log() 的脱敏），网关错误页/接口 msg 里回显的 `user:pass@host`、
  `access_token=xxx` 会明文外泄；现在与 log() 同口径先 redact。

- **向导写盘成功后的中断不再谎称「未生成任何文件」（可用性）**：os.replace 已完成、收尾阶段
  （chmod/echo）被 Ctrl+C 时，含明文密钥的文件其实已在磁盘上，原来会按取消上报；现在如实
  打印已生成的路径并返回 0。—— 与 api2ods / feishu2ods 同款修复。

- **8 位 yyyymmdd 哨兵值不再冒充真实日期（正确性）**：19700101/99991231 这类
  "未填/永久有效"写法原来绕过 epoch 合理区间直接返回（数字与文本两个分支），会冒充
  "最新数据"写进新鲜度告警；现在与 epoch 分支同口径约束在 2000~2100。

- **新鲜度校验的输入形态按元素判定（可用性）**：原来只看首元素——None 占位或
  str/dict 混杂输入会以 TypeError（dict 不可哈希）崩掉整个新鲜度校验；现在带 dict 的
  输入按记录列表口径提取、纯字符串集合跳过非字符串元素。

- **同机同表再加一道表级运行锁（数据安全）**：作业锁只管"同一个作业不重复跑"——
  jobs/a.json 与 jobs/b.json 写同一张表时互不阻塞，一边的 purge 会清掉另一边正在写的
  临时分区、rename 交叉执行，最终一方数据被静默覆盖（各自的行数核对发现不了）。现在
  run_sync 外面再套一把按「项目.表名」命名的锁，同机写同一张表的所有作业串行。

- **显式传空 --bizdate 改为报错（正确性）**：`--bizdate` 的 argparse 默认值原来是空串，
  与"显式传了空值"无法区分——调度脚本 `--bizdate "$pt"` 且 `$pt` 未定义时会被当成
  "未指定"、静默回退成"昨天"写错分区。现在默认值是 None，显式空值走格式校验直接报错。

- **日期换算加合理区间（正确性）**：epoch 换算结果必须落在 2000~2100——0/小整数
  （"未填"的占位）会得到 1970、14 位 `yyyyMMddHHmmss` 会被当毫秒算出 26xx 年的假日期，
  这类假日期会冒充"最新数据"写进告警、掩盖"日期列格式不支持"这个真正的问题，现在按
  认不出返回 None。

- **业务日白名单按 ASCII 匹配（正确性）**：Python 的 `\d` 默认匹配 Unicode 数字，全角数字
  （中文输入法常见）会被当成合法业务日；`re.ASCII` 后只认 ASCII 数字。

- **文本形态的业务日期（"20260927"）与数字同口径（可用性）**：新鲜度校验原来只认
  int/float 的 8 位日期，源表把业务日期存成文本列时解析为 None，每次调度都误报缺数据。

- **spool 关闭失败时保留落盘文件（数据安全）**：磁盘满导致 `close()` 刷盘失败时，原来
  的 `finally` 仍会删掉临时文件——那是本次拉取唯一的落盘副本；现在关闭失败即保留文件
  并把路径写进日志，再由异常上抛。

- **保留分区改用 `__keep` 标记，不再被下一次运行的残留清理误删（数据安全）**：
  「正式分区已删、临时分区是唯一完整副本」时保留它，但下一次任何作业运行的
  `purge_stale_tmp_partitions` 会把它当残留清掉——恢复路径形同虚设。现在保留时先
  改名成 `pt__keep`（排在正式分区之后，max_pt 读到的正是已核对的完整数据），
  purge 显式跳过并提示存在，由人工恢复或删除。

- **webhook 脱敏正则的可选前缀限长 256（性能/可用性）**：`(?i)((?:https?://[^\s"']*?)?/hook/)`
  在"超长、无空白、又不含 /hook/"的文本上二次回溯（50KB 实测 19.7s，且 redact 在
  log() 的锁内执行，会拖住所有线程）；限长后同一输入 0.29s，正常 webhook 照常遮蔽。

- **临时分区"保留"判定改用本轮状态（正确性）**：`final_deleted` 是跨重试的累计标志，
  重试会先清掉上一轮保留的 tmp 再重建——重试若没走到 verify，finally 仍会谎称
  "临时分区已保留"。现在用 `tmp_complete`（每轮重试开头复位、verify 通过后置位）判断，
  状态确定时措辞"已被删掉 + 已保留"，不确定时保守描述"可能已被删掉"。

- **finally 里不再抛 SystemExit 覆盖在传播的异常（可用性）**：重试被 KeyboardInterrupt
  打断时，原来的"失败收尾"会把 130 覆盖成 1、原始异常也丢了；现在只在没有其它异常
  传播时才抛 SystemExit。

- **日志函数对任意 sink 失败都不得打挂业务（可用性）**：`add_log_sink(None)` 直接忽略
  （None 留在 _sinks 里会让此后每次 log() 炸在 handle.write）；sink 抛非 I/O 异常时也
  兜住并提示"疑似代码缺陷"；stderr 断管时告警 print 自身也加了保护。

- **向导的 base_token 用与 config 相同的白名单校验（安全）**：原来只挡 "/" 和空格，
  ".."/"%2F" 这类值会先被拼进带 Bearer token 的请求 URL（早于 config 校验）；
  现在拿到 token 先过白名单再发请求。

- **正式分区被删后的失败路径不再把临时分区也删掉（数据安全）**：`write_partition` 的
  drop(正式)→rename(临时) 之间若 rename 失败，临时分区是**已经核对过**的完整数据，继续
  按"清理残留"删掉才是真的丢这一分区；现在该路径保留它并提示可手工 rename 恢复。

- **锁文件权限显式 0600（安全）**：`RunLock` 的 O_NOFOLLOW opener 没给 `os.open` 传 mode，
  锁文件按 `0o777 & ~umask` 创建（umask=0 时就是 0777，同机其他用户可打开并 flock 它来
  阻塞任务）；现在显式 0600。

- **feishu id 改为正向白名单校验（安全）**：`base_token`/`table_id` 会拼进 URL 路径，
  原黑名单挡不住 `..` 与 `%2F`（可把带 token 的请求打到非预期接口）；现在只允许
  字母/数字/下划线/中划线。

- **token 失效可多次重取（可用性）**：大表翻页可能跑几个小时、token（2h）会多次过期，
  原来是一次性标志——第二次失效直接中止整轮；现在允许最多 3 次重取（防死循环）。

- **8 位整数按 yyyymmdd 解读（正确性）**：源表把业务日期存成数字时（20260927），
  原来按 epoch 秒静默算成 1970 年，新鲜度校验会稳定误报；现在按 yyyymmdd 解析
  （非法日历返回 None），秒/毫秒时间戳按量级照旧。

- **`SpoolWriter` 只删自己创建的临时文件（正确性）**：调用方显式传入的 path 是它的数据
  文件，原来 close() 一律 unlink；现在只有 mkstemp 自建的才删除，并补上上下文管理器
  （异常路径保留文件排障）。

- **NaN/Infinity 的报错带记录上下文（可用性）**：`allow_nan=False` 抛的裸 ValueError
  现在附带 record_id，能直接定位是哪条记录。

- **日志 sink 的错误类型收窄（可用性）**：只接 (OSError, ValueError) 这类"文件写不进去"
  的预期错误；其它异常是编程错误，静默吞掉会让日志永久失效却查不出原因。

- **`redact` 返回 str 契约 + 叶子收集深度上限（可用性）**：falsy 输入（0/None/{}）不再原样
  返回非字符串；`_leaf_strings` 加深度上限（与 `_redact_shapes` 同口径），构造性深层嵌套
  不会把脱敏打成 RecursionError。

- **向导三处（可用性）**：最后一次 chmod 失败不再把"已生成成功"误报成"写文件失败"；
  `validate_job` 的告警（未知键/http 端点等）会展示而不是静默写进配置；写失败路径的
  临时文件清理失败会提示手工删除（临时文件含明文密钥）。

- **`request_json` 的 tries 兜底（可用性）**：极端的 tries<=0 不再打印 "失败（重试 0 次）：None"。

- **记录缺 record_id 的判空改显式判断（正确性）**：`not stats.min_id` 会把合法的 falsy 值
  （如 0）误判成缺失；改为 `in (None, "")`。

- **示例作业的 endpoint 改 https（安全）**：示例会被直接复制使用，明文 HTTP 会暴露
  AccessKey 签名与数据。

- **非严格模式下脏 bizdate 不再打断环境变量兜底链（可用性）**：`--check` 的
  `env_bizdate(strict=False)` 遇到格式不对的 `bizdate` 时原来直接返回 None（不再看
  `SKYNET_BIZDATE`）；现在按"未设置"处理继续往后看，与"空白值"分支同口径。

- **向导拉取阶段只兜预期的错误（可用性）**：`--init` 拉列名原来 `except Exception` 全接，
  `TypeError`/`AttributeError` 这类代码缺陷会被掩盖成"拉取失败（未预期错误）"；现在只接
  `OSError`/`RuntimeError`（与 sftp2ods 向导同口径），编程错误继续上抛。

- **脱敏递归加上深度上限（安全）**：`_redact_shapes` 的 query/JSON/头行回调会把匹配值再
  交给自身递归；形如 `a=b=c=…`（上千个等号）的构造性文本（第三方响应体不可控）每层只剥
  一个等号，能把递归喂到 Python 上限、把日志脱敏本身打成 `RecursionError`。现在深度超过
  10 层按"宁可多脱敏"整段遮成 `***`（与 api2ods 同口径）。

- **epoch 时间戳按量级识别秒/毫秒（正确性）**：`normalize_date_value` 原来一律按毫秒解析，
  秒级时间戳（10 位）会被静默解析成 1970 年——现在按 1e11 阈值自动识别（毫秒要到 1973 年、
  秒要到 5138 年才越过，两段区间互不重叠），算不出返回 None 的契约不变。

- **`iter_rows` 在关闭后给出明确报错（可用性）**：`close(keep=False)` 会删掉落盘文件，
  之后读回原来抛没有上下文的 `FileNotFoundError`；现在与 `write_records` 同口径抛
  `RuntimeError`。

- **临时文件删除失败留一条日志（可用性）**：`close()` 的 `unlink` 失败原来完全静默，
  残留文件无从察觉。

- **`log()` 兜住 stdout 断管（可用性）**：stdout 管道被提前关闭（`| head` 退出、
  BrokenPipeError）时，原来日志函数自身会抛异常把业务打挂；现在控制台输出失败静默跳过。

- **`add_log_sink` 按身份去重（可用性）**：重复登记同一句柄会让每条日志写两遍，摘除/关闭
  后列表里还留着失效句柄（再写就抛）。

- **缺 `target.table` 时给明确报错（可用性）**：`_run` 里取表名原来用直接下标；现在用
  `.get` + 明确报错，防御未来调用顺序变化时变成 `KeyError`（"未预期错误"）。

- **运行锁名哈希从 `sha1[:8]` 换成 `sha256[:16]`（可用性）**：32 位哈希下不同作业有可观的
  碰撞概率，撞了会互相阻塞（解锁时还可能删错对方的锁）；与 sftp2ods / api2ods 同口径。

- **临时分区残留清理的「归属判断」改用后缀结尾匹配（正确性）**：`purge_stale_tmp_partitions`
  原先用子串包含（`TMP_PARTITION_SUFFIX in spec`）判断分区是不是本进程的——pid 前缀相同时
  （本机 456 与 4567）会把另一进程的残留分区误认成自己的而跳过，残留永远清不掉，下游
  `max_pt()` 继续读到半成品。现在取分区值本体（去掉 `pt='…'` 外壳）用 `endswith` 比对，
  只放行与本进程后缀完全一致的分区；其余残留（含旧版无 run id 的 `__tmp`）照常清理。

- **翻页 rev 一致性检查不再因首屏缺 rev 而静默失效（正确性）**：原来用「第一页」建立 rev
  基准；首屏响应若缺 `rev` 字段，基准恒为 None，后续所有页的比对全部跳过——offset 翻页期间
  表格被编辑会静默漏行且无人察觉。现在用「首个带 rev 的页」建立基准，检查在其余页之间照常
  生效（rev 一直缺失时才无从比对，行为同前）。

- **运行锁探测失败不再静默换目录（并发安全）**：`lock_path` 原来把「探测文件 unlink 失败」
  与「目录不可用」混在同一个 `except OSError` 里；unlink 偶发失败会让本进程改用系统临时
  目录——同一作业的两个实例锁在不同路径上，互斥失效。现在只把 mkdir/mkstemp 失败视为目录
  不可用，unlink 失败忽略（最多留一个隐藏探测文件，不影响锁位置）。

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
