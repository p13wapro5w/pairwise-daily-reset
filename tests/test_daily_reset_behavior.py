"""行为测试：对应线上的 10 类刷新事故，防止回归。"""

import threading
from datetime import datetime, timedelta

import gameplay.daily_reset as dr
from gameplay.daily_reset import (
    DailyResetService,
    FixedOffset,
    IdempotencyLedger,
    ResetConfig,
    TaskState,
    claim_idempotent,
    clock_skew,
    compensation_dedup,
    config_version,
    keep_progress,
    mail_once,
    next_reset_at,
    online_refresh,
    player_timezone,
    reconnect_sync,
    seeded_pool,
    stagger_window,
    task_roll,
    week_boundary,
)


def local_ts(year, month, day, hour=0, minute=0, tz_offset=8):
    return datetime(year, month, day, hour, minute,
                    tzinfo=FixedOffset(tz_offset)).timestamp()


# 1. 服务器时间：改客户端本地时钟不能影响刷新判定 ------------------------------------


def test_server_time_is_authoritative_and_correctable():
    assert dr.server_time(1000, 5) == 1005
    before = dr.server_time()
    assert before > 1_700_000_000  # 走真实服务端时钟，而非任何客户端输入


def test_clock_skew_flags_untrusted_client_clock():
    ok = clock_skew(client_now=100, server_now=120, tolerance=30)
    assert ok["client_trusted"] is True
    bad = clock_skew(client_now=100, server_now=200, tolerance=30)
    assert bad["client_trusted"] is False
    assert bad["skew_seconds"] == 100


# 2. 按玩家时区：不再写死 UTC+8 ------------------------------------------------------


def test_player_timezone_offset_and_fallback():
    assert player_timezone(8).utcoffset(None) == timedelta(hours=8)
    assert player_timezone(-5).utcoffset(None) == timedelta(hours=-5)
    # 非法偏移回落到默认时区，绝不抛异常或静默当成 UTC+8
    assert player_timezone(99, fallback=0).utcoffset(None) == timedelta(0)


def test_reset_follows_player_local_time_not_utc8():
    tz_minus5 = player_timezone(-5)
    # UTC 20:00 对 UTC-5 玩家是当天 15:00；对 UTC+8 玩家已是次日 04:00
    utc_ts = datetime(2026, 11, 22, 20, 0, tzinfo=dr.timezone.utc).timestamp()
    svc = DailyResetService()
    assert svc.day_key(-5, utc_ts) == "2026-11-22"
    assert svc.day_key(8, utc_ts) == "2026-11-23"


def test_next_reset_at_local_4am_and_exact_boundary():
    tz = player_timezone(8)
    before = local_ts(2026, 11, 23, 3, 0, 8)
    assert next_reset_at(before, 4, tz) == local_ts(2026, 11, 23, 4, 0, 8)
    exact = local_ts(2026, 11, 23, 4, 0, 8)
    # 恰在刷新点：本轮已属新一天，返回下一天
    assert next_reset_at(exact, 4, tz) == local_ts(2026, 11, 24, 4, 0, 8)


# 3. 在线热刷新：跨天不重登也能看到新任务 --------------------------------------------


def test_online_refresh_hot_reload_and_rollback_guard():
    assert online_refresh("2026-11-22", "2026-11-22")["action"] == "unchanged"
    state = {"day_key": "2026-11-22"}
    result = online_refresh("2026-11-22", "2026-11-23", state)
    assert result["refreshed"] is True
    assert state["day_key"] == "2026-11-23"
    # 日键回退（时钟异常/脏数据）必须被拦截
    back = online_refresh("2026-11-23", "2026-11-22")
    assert back["action"] == "clock_rollback_blocked"
    assert back["refreshed"] is False


def test_heartbeat_rolls_over_without_relogin():
    clock = {"now": local_ts(2026, 11, 22, 10, 0, 8)}
    svc = DailyResetService(
        ResetConfig(task_pool=("a", "b", "c"), pool_size=3),
        clock=lambda: clock["now"],
    )
    player = svc.register_player(1001, 8)
    assert player.day_key == "2026-11-22"

    clock["now"] = local_ts(2026, 11, 23, 5, 0, 8)  # 已过本地凌晨 4 点
    result = svc.heartbeat(1001)
    assert "daily_rollover" in result["events"]
    assert result["day_key"] == "2026-11-23"
    assert svc._players[1001].day_key == "2026-11-23"


# 4. 领取幂等：刷新与领取并发，同一份奖励只发一次 -------------------------------------


def test_claim_is_idempotent():
    ledger = IdempotencyLedger()
    first = claim_idempotent(1, "reward_x", ledger, now=100.0)
    second = claim_idempotent(1, "reward_x", ledger, now=200.0)
    assert first["accepted"] is True and second["accepted"] is False
    assert second["duplicate"] is True
    # 不同奖励互不影响
    assert claim_idempotent(1, "reward_y", ledger)["accepted"] is True


def test_concurrent_claims_win_exactly_once():
    ledger = IdempotencyLedger()
    outcomes = []
    barrier = threading.Barrier(20)

    def worker():
        barrier.wait()
        outcomes.append(claim_idempotent(7, "raid_reward", ledger)["accepted"])

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert outcomes.count(True) == 1


def test_claim_scope_resets_each_new_day():
    clock = {"now": local_ts(2026, 11, 22, 10, 0, 8)}
    svc = DailyResetService(clock=lambda: clock["now"])
    svc.register_player(1, 8)
    assert svc.claim(1, "login_gift")["accepted"] is True
    assert svc.claim(1, "login_gift")["duplicate"] is True

    clock["now"] = local_ts(2026, 11, 23, 5, 0, 8)
    svc.heartbeat(1)  # 热刷新
    again = svc.claim(1, "login_gift")
    assert again["accepted"] is True  # 新一天的同名奖励重新开放


# 5. 重连校准：缓存只作展示，重连以服务器为准 -----------------------------------------


def test_reconnect_recalibrates_stale_cache():
    state = {"day_key": "2026-11-22"}
    ok = reconnect_sync("2026-11-23", "2026-11-23")
    assert ok["action"] == "cached_ok"

    result = reconnect_sync("2026-11-22", "2026-11-23", 1, 2, state)
    assert result["action"] == "recalibrate"
    assert "day_key_mismatch" in result["reasons"]
    assert "config_version_mismatch" in result["reasons"]
    assert state["day_key"] == "2026-11-23"
    assert state["config_version"] == 2


def test_reconnect_service_pulls_yesterdays_player_forward():
    clock = {"now": local_ts(2026, 11, 23, 9, 0, 8)}
    svc = DailyResetService(ResetConfig(config_version=2), clock=lambda: clock["now"])
    result = svc.reconnect(
        55, {"day_key": "2026-11-22", "tz_offset_hours": 8}, cached_config_version=1
    )
    assert result["sync"]["action"] == "recalibrate"
    assert result["state"].day_key == "2026-11-23"
    assert result["state"].config_version == 2


# 6. 错峰刷新：玩家被稳定打散，日常/周常不同 salt -------------------------------------


def test_stagger_window_spread_stable_and_salt_separated():
    window = 300
    offsets = [stagger_window(pid, window) for pid in range(3000)]
    assert all(0 <= o < window for o in offsets)
    # 稳定性：同玩家多次计算结果一致（可预告自己的刷新时间）
    assert stagger_window(42, window) == stagger_window(42, window)
    # 打散效果：桶利用率足够高，不会全服挤在同一秒
    assert len(set(offsets)) >= window * 0.9
    # 日常/周常不同 salt，两类刷新不再同秒撞车
    assert any(
        stagger_window(pid, window, "daily") != stagger_window(pid, window, "weekly")
        for pid in range(50)
    )
    assert stagger_window(1, 0) == 0


# 7. 补偿/邮件去重：同一事件重放只发一封 ----------------------------------------------


def test_compensation_and_mail_dedup():
    ledger = IdempotencyLedger()
    first = compensation_dedup(9, "incident_2026_11_22", ledger)
    for _ in range(3):  # 重试/重放三封的事故场景
        again = compensation_dedup(9, "incident_2026_11_22", ledger)
        assert again["duplicate"] is True
    assert first["accepted"] is True
    assert mail_once(9, "mail_1", ledger)["accepted"] is True
    assert mail_once(9, "mail_1", ledger)["duplicate"] is True
    # 不同命名空间互不干扰：同名 ID 的邮件与补偿是两条记录
    assert mail_once(9, "x", ledger)["accepted"] is True
    assert compensation_dedup(9, "x", ledger)["accepted"] is True


# 8. 种子可控：全服当天一致可做攻略，跨天自动换池 -------------------------------------


def test_seeded_pool_is_deterministic():
    pool_a = seeded_pool(42, 10)
    pool_b = seeded_pool(42, 10)
    assert pool_a == pool_b and len(pool_a) == 10
    assert seeded_pool(43, 10) != pool_a  # 换种子换池
    assert len(set(pool_a)) == len(pool_a)  # 无重复任务
    assert task_roll("task_01", 42) == task_roll("task_01", 42)


def test_server_wide_pool_shared_per_day_rotates_next_day():
    svc = DailyResetService()
    day1, day2 = "2026-11-22", "2026-11-23"
    p1 = svc.roll_tasks(day1)
    p2 = svc.roll_tasks(day1)
    p3 = svc.roll_tasks(day1, player_id=999)
    # 无 player 维度时全服同池；带 player 维度仍是可复现的确定序列
    assert p1 == p2
    assert svc.roll_tasks(day1, player_id=999) == p3
    assert svc.roll_tasks(day2) != p1  # 次日自动换池


# 9. 进度保留：刷池不清进度，阶段奖励按天重置 -----------------------------------------


def test_keep_progress_merges_and_resets_claimed_stages():
    old = [
        TaskState("a", progress=5, target=10, claimed_stages=["stage1"]),
        TaskState("b", progress=3, target=3, claimed_stages=["stage1"]),
    ]
    merged = {t.task_id: t for t in keep_progress(old, ["a", "c"])}
    assert merged["a"].progress == 5          # 进度继承，玩家不白打
    assert merged["a"].claimed_stages == []   # 阶段奖励标记按天重置
    assert merged["c"].progress == 0          # 新任务从零开始
    assert "b" not in merged                  # 不在新池的任务移除


def test_keep_progress_respects_carry_flag():
    old = [
        TaskState("a", progress=5, carry_over=False),
        TaskState("b", progress=5, carry_over=True),
    ]
    merged = {t.task_id: t for t in keep_progress(old, ["a", "b"], carry_all=False)}
    assert merged["a"].progress == 0  # 显式不继承的任务清零重来
    assert merged["b"].progress == 5


def test_rollover_keeps_progress_end_to_end():
    clock = {"now": local_ts(2026, 11, 22, 10, 0, 8)}
    svc = DailyResetService(
        ResetConfig(task_pool=("a", "b", "c"), pool_size=3),
        clock=lambda: clock["now"],
    )
    svc.register_player(1, 8)
    svc._players[1].tasks["a"].progress = 7
    clock["now"] = local_ts(2026, 11, 23, 5, 0, 8)
    svc.heartbeat(1)
    # 小池子保证任务 a 仍在新池中：进度必须保留
    assert svc._players[1].tasks["a"].progress == 7


# 10. 配置版本：规则改动必须升版本，旧客户端被门控 -------------------------------------


def test_config_version_gating():
    assert config_version(3, 3)["action"] == "proceed"
    old = config_version(2, 3)
    assert old["action"] == "upgrade_client"
    assert old["needs_migration"] is True
    newer = config_version(4, 3)
    assert newer["compatible"] is False
    assert newer["action"] == "reject_forward_unsupported"


def test_config_bump_forces_recalibration_on_reconnect():
    v1 = DailyResetService(ResetConfig(config_version=1))
    player = v1.register_player(8, 8)
    assert player.config_version == 1
    v2 = DailyResetService(ResetConfig(config_version=2))
    result = v2.reconnect(8, {"day_key": player.day_key, "tz_offset_hours": 8},
                          cached_config_version=1)
    assert "config_version_mismatch" in result["sync"]["reasons"]
    assert result["state"].config_version == 2


# 周常边界与错峰调度 -------------------------------------------------------------------


def test_week_boundary_and_weekly_rollover():
    tz = player_timezone(8)
    # 找一个周一 00:00（+8）
    monday = datetime(2026, 1, 5, 0, 0, tzinfo=FixedOffset(8))
    while monday.weekday() != 0:
        monday += timedelta(days=1)
    sunday = monday + timedelta(days=6)  # 周日 00:00
    result = week_boundary(sunday.timestamp(), 7, tz)
    expected = monday + timedelta(days=7)  # 次日即新周一 00:00
    assert result["boundary_ts"] == expected.timestamp()
    assert result["next_boundary_ts"] == expected.timestamp() + 7 * 86400

    clock = {"now": monday.timestamp() - 60}  # 周一前一分钟，仍是旧周
    svc = DailyResetService(clock=lambda: clock["now"])
    svc.register_player(3, 8)
    old_week = svc._players[3].week_key
    clock["now"] = monday.timestamp() + 60    # 跨入新周
    events = svc.heartbeat(3)["events"]
    assert "weekly_rollover" in events
    assert svc._players[3].week_key != old_week


def test_schedule_uses_player_stagger_offsets():
    svc = DailyResetService(ResetConfig(stagger_window_seconds=300))
    plan = svc.schedule_for(123, 8, now=local_ts(2026, 11, 23, 10, 0, 8))
    base = next_reset_at(local_ts(2026, 11, 23, 10, 0, 8), 4, player_timezone(8))
    assert plan["daily_reset_ts"] == base + plan["daily_stagger_offset"]
    assert 0 <= plan["daily_stagger_offset"] < 300
    assert 0 <= plan["weekly_stagger_offset"] < 300
