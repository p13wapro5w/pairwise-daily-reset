# pairwise-daily-reset

## 领域

游戏、图形与多媒体

## 主题

日常任务刷新

## 说明

服务器时间、按玩家时区刷新与领取幂等。

修复后运行：

```bash
python -m pytest -q
```

- `tests/test_daily_reset.py`：15 个契约测试（题目要求，全部通过）
- `tests/test_daily_reset_behavior.py`：23 个行为测试，锁定 10 类线上事故不回归

## 事故与修复对应

| # | 线上事故 | 修复入口 |
|---|---------|---------|
| 1 | 读客户端本地时钟，改系统时间刷两次 | `server_time` / `clock_skew`：只信服务端时钟，超差客户端时钟仅可用于展示 |
| 2 | 时区写死 UTC+8，海外玩家刷新时间错位 | `player_timezone` / `next_reset_at`：时区取玩家资料，按玩家本地凌晨 4 点算 |
| 3 | 跨天在线玩家不刷新，必须重登 | `online_refresh` / `DailyResetService.heartbeat`：心跳路径按 day_key 热滚服，日键回退显式拦截 |
| 4 | 刷新与领取并发，一份奖励领两遍 | `claim_idempotent` + `IdempotencyLedger`：原子 check-and-insert，20 并发只成功 1 次；reward_id 按天作用域隔离 |
| 5 | 断线重连恢复缓存，任务还是昨天的 | `reconnect_sync` / `reconnect`：日键与配置版本任一不一致即重新校准，缓存只作展示兜底 |
| 6 | 日常周常同秒刷新，全服打爆登录队列 | `stagger_window`：player_id 稳定哈希错峰，日常/周常不同 salt，偏移对玩家固定可预告 |
| 7 | 补偿邮件按刷新次数重复发三封 | `compensation_dedup` / `mail_once`：命名空间化幂等键，重放只发一次 |
| 8 | 任务池无种子，全服一样或纯随机无法攻略 | `seeded_pool` / `task_roll`：种子由「配置版本+日期」派生，全服当天一致可攻略、跨天自动换池、序列可复现 |
| 9 | 刷新清零已接任务进度，玩家白打 | `keep_progress`：同 ID 任务继承进度，仅重置阶段领取标记；`carry_over` 标记支持逐任务控制 |
| 10 | 刷新规则改配置不带版本号，旧客户端按老时间算 | `config_version` / `ResetConfig.config_version`：版本不一致门控升级，重连与心跳检测到版本变化强制滚服 |

## 生产化注意点

- `IdempotencyLedger` 为线程安全内存实现，多实例部署需替换为带唯一索引的 DB 表（`namespace:player_id:event_id`）。
- `DailyResetService` 的时钟、配置、账本均可注入；单实例内所有玩家操作由 RLock 保护。
- 周常使用 ISO-8601 周年/周号（`%G-W%V`），避免跨年周归属错误。
