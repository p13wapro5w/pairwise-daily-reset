"""日常/周常任务刷新核心模块。

设计原则与线上事故的对应关系：

1. 服务器时间            -> ``server_time`` / ``clock_skew``：刷新判定只信服务端时钟，
   客户端时钟仅用于估算偏差，改本地时间无法多刷一次。
2. 按玩家时区            -> ``player_timezone`` / ``next_reset_at``：时区来自玩家资料，
   不再写死 UTC+8，每个玩家按自己的本地凌晨 4 点刷新。
3. 在线热刷新            -> ``online_refresh`` / ``DailyResetService.heartbeat``：
   心跳/请求路径上比对 day_key，跨天立即滚服，不需要重登。
4. 领取幂等              -> ``claim_idempotent`` + ``IdempotencyLedger``：
   原子去重，刷新与领取并发时同一份奖励只发一次。
5. 重连校准              -> ``reconnect_sync``：缓存只作展示，重连后以服务器
   day_key/week_key/配置版本为准重新校准。
6. 错峰刷新              -> ``stagger_window``：按 player_id 哈希出稳定偏移，
   日常/周常使用不同 salt，全服不会落在同一秒。
7. 补偿去重              -> ``compensation_dedup`` / ``mail_once``：同一事件幂等键，
   重试/重放只发一封。
8. 种子可控              -> ``seeded_pool`` / ``task_roll``：全服共享「版本+日期」种子，
   同一天全服任务池一致、可做攻略，换天自动换池且完全可复现。
9. 进度保留              -> ``keep_progress``：刷新只换池不清进度，出现在新池中的
   任务继承进度；阶段领取标记按新一天重置（当日奖励重新开放）。
10. 配置版本            -> ``config_version`` / ``ResetConfig.config_version``：
   刷新规则变更必须升版本号，旧客户端被门控，服务端按新版本强制滚服。
"""

from __future__ import annotations

import hashlib
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime, timedelta, timezone, tzinfo

# ---------------------------------------------------------------------------
# 常量与数据结构
# ---------------------------------------------------------------------------

#: 合法的固定时区偏移范围（UTC-12 到 UTC+14，覆盖现实所有时区）
MIN_TZ_OFFSET = -12
MAX_TZ_OFFSET = 14

#: 默认任务池（实际项目来自配置中心，这里用固定样例保证可复现）
DEFAULT_TASK_POOL: tuple[str, ...] = tuple(f"task_{i:02d}" for i in range(12))

#: 允许的时钟偏差（秒），超过即判定客户端时钟不可信
DEFAULT_SKEW_TOLERANCE = 30


class FixedOffset(tzinfo):
    """固定 UTC 偏移时区，偏移量来自玩家资料，而非写死 UTC+8。"""

    def __init__(self, offset_hours: float):
        self._offset = timedelta(hours=offset_hours)
        self._name = f"UTC{offset_hours:+g}"

    def utcoffset(self, dt):  # noqa: D401 - tzinfo 协议
        return self._offset

    def tzname(self, dt):
        return self._name

    def dst(self, dt):
        return timedelta(0)

    def __eq__(self, other):
        return isinstance(other, FixedOffset) and self._offset == other._offset

    def __repr__(self):
        return f"FixedOffset({self._name})"


@dataclass
class TaskState:
    """单个任务的运行时状态。"""

    task_id: str
    progress: int = 0
    target: int = 1
    #: 已领取的阶段奖励（新一天滚服时重置，当日奖励重新开放）
    claimed_stages: list = field(default_factory=list)
    #: 是否允许跨天继承进度
    carry_over: bool = True

    @property
    def completed(self) -> bool:
        return self.progress >= self.target


@dataclass
class PlayerState:
    """玩家任务面板的持久化状态。"""

    player_id: int
    tz_offset_hours: float
    day_key: str
    week_key: str
    config_version: int
    tasks: dict[str, TaskState] = field(default_factory=dict)
    last_reset_ts: float = 0.0


@dataclass(frozen=True)
class ResetConfig:
    """刷新规则配置。任何规则改动都必须升 ``config_version``。"""

    config_version: int = 1
    reset_hour: int = 4
    stagger_window_seconds: int = 300
    pool_size: int = 10
    task_pool: tuple[str, ...] = DEFAULT_TASK_POOL
    daily_seed_salt: str = "daily-pool"
    weekly_seed_salt: str = "weekly-pool"


# ---------------------------------------------------------------------------
# 幂等账本：领取 / 邮件 / 补偿共用，线程安全，生产环境替换为带唯一索引的 DB 表
# ---------------------------------------------------------------------------


class IdempotencyLedger:
    """基于幂等键的去重账本，``try_add`` 是原子的 check-and-insert。"""

    def __init__(self):
        self._records: dict[str, dict] = {}
        self._lock = threading.RLock()

    def try_add(self, key: str, payload: dict | None = None) -> tuple[bool, dict]:
        """返回 ``(是否首次写入, 记录)``；重复键不会覆盖已有记录。"""
        with self._lock:
            existing = self._records.get(key)
            if existing is not None:
                return False, existing
            record = {"key": key, "payload": dict(payload or {}), "seq": len(self._records) + 1}
            self._records[key] = record
            return True, record

    def contains(self, key: str) -> bool:
        with self._lock:
            return key in self._records

    def get(self, key: str) -> dict | None:
        with self._lock:
            return self._records.get(key)

    def reset(self) -> None:
        with self._lock:
            self._records.clear()


#: 模块级默认账本（服务级单例场景使用；测试请注入独立账本）
_DEFAULT_LEDGER = IdempotencyLedger()


# ---------------------------------------------------------------------------
# 1. 服务器时间
# ---------------------------------------------------------------------------


def server_time(now: float | None = None, drift_correction: float = 0.0) -> float:
    """返回权威服务器时间（epoch 秒）。

    ``now`` 为注入的时钟读数（测试/受控环境），缺省走系统单调可信的
    ``time.time``；``drift_correction`` 是 NTP 校时偏移。刷新逻辑只允许
    使用本函数的返回值，绝不读取客户端本地时钟。
    """
    base = time.time() if now is None else float(now)
    return base + float(drift_correction)


def clock_skew(client_now: float, server_now: float,
               tolerance: float = DEFAULT_SKEW_TOLERANCE) -> dict:
    """比较客户端与服务器时钟偏差，给出客户端时钟是否可信的结论。"""
    skew = float(server_now) - float(client_now)
    return {
        "skew_seconds": skew,
        "tolerance": float(tolerance),
        # 边界值视为可信；超差则客户端时间只能用于展示，不能参与判定
        "client_trusted": abs(skew) <= float(tolerance),
        "authoritative_now": float(server_now),
    }


# ---------------------------------------------------------------------------
# 2. 按玩家时区
# ---------------------------------------------------------------------------


def player_timezone(offset_hours: float | None, fallback: float = 0.0) -> tzinfo:
    """把玩家资料里的 UTC 偏移（小时）转成 tzinfo；非法值回落到 ``fallback``。"""
    def _valid(value) -> bool:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        return MIN_TZ_OFFSET <= float(value) <= MAX_TZ_OFFSET

    offset = float(offset_hours) if _valid(offset_hours) else (
        float(fallback) if _valid(fallback) else 0.0
    )
    return FixedOffset(offset)


def _day_key(now: float, tz: tzinfo) -> str:
    return datetime.fromtimestamp(now, tz).strftime("%Y-%m-%d")


def _week_key(now: float, tz: tzinfo) -> str:
    # ISO-8601 周年-周号，避免跨年周归属错误
    return datetime.fromtimestamp(now, tz).strftime("%G-W%V")


def next_reset_at(now: float, reset_hour: int = 4, tz: tzinfo | None = None) -> float:
    """返回玩家时区下一次 ``reset_hour`` 点刷新的 epoch 秒。

    恰好在刷新时刻调用时返回下一天的刷新点（本轮已归属新的一天）。
    """
    tz = tz or timezone.utc
    local = datetime.fromtimestamp(float(now), tz)
    today_reset = datetime.combine(local.date(), dtime(hour=int(reset_hour)), tzinfo=tz)
    if local.timestamp() < today_reset.timestamp():
        return today_reset.timestamp()
    return (today_reset + timedelta(days=1)).timestamp()


def week_boundary(now: float, weekday: int = 7, tz: tzinfo | None = None) -> dict:
    """返回周常边界时间。

    ``weekday`` 采用 ISO 记法（周一=1 … 周日=7），默认 7 表示周日结束、
    新周一 00:00 为周常刷新点。
    """
    tz = tz or timezone.utc
    local_date = datetime.fromtimestamp(float(now), tz).date()
    monday = local_date - timedelta(days=local_date.weekday())
    days_ahead = (int(weekday) - local_date.isoweekday()) % 7
    if days_ahead == 0:
        days_ahead = 7  # 当天已是目标日时，边界指向下一周
    boundary_date = monday + timedelta(days=days_ahead)
    boundary_ts = datetime.combine(boundary_date, dtime(hour=0), tzinfo=tz).timestamp()
    return {
        "weekday": int(weekday),
        "this_week_start": datetime.combine(monday, dtime(hour=0), tzinfo=tz).timestamp(),
        "boundary_ts": boundary_ts,
        "next_boundary_ts": boundary_ts + 7 * 86400.0,
    }


# ---------------------------------------------------------------------------
# 3. 在线热刷新
# ---------------------------------------------------------------------------


def online_refresh(state_day_key, current_day_key, state: dict | None = None) -> dict:
    """比对面板日键与服务器当前日键，决定是否热刷新。

    在线玩家的每次心跳/请求都会走到这里：跨天立即返回 ``hot_refresh``
    并可原地更新状态，无需重登；时钟回拨被显式拦截。
    """
    if state_day_key == current_day_key:
        action = "unchanged"
    elif current_day_key < state_day_key:
        # 日键单调递增，变小只可能是时钟异常或脏数据
        action = "clock_rollback_blocked"
    else:
        action = "hot_refresh"

    result = {
        "action": action,
        "from": state_day_key,
        "to": current_day_key,
        "refreshed": action == "hot_refresh",
    }
    if state is not None and action == "hot_refresh":
        state["day_key"] = current_day_key
    return result


# ---------------------------------------------------------------------------
# 4. 领取幂等 / 7. 补偿与邮件去重
# ---------------------------------------------------------------------------


def _idempotent_once(namespace: str, player_id, event_id,
                     ledger: IdempotencyLedger | None, now: float | None,
                     extra: dict | None = None) -> dict:
    ledger = ledger or _DEFAULT_LEDGER
    ts = server_time(now)
    key = f"{namespace}:{player_id}:{event_id}"
    payload = {"player_id": player_id, "event_id": event_id, "ts": ts}
    if extra:
        payload.update(extra)
    accepted, record = ledger.try_add(key, payload)
    return {
        "accepted": accepted,
        "duplicate": not accepted,
        "key": key,
        "ts": ts,
        "record": record,
    }


def claim_idempotent(player_id, reward_id, ledger: IdempotencyLedger | None = None,
                     now: float | None = None) -> dict:
    """领取奖励的幂等入口：同一 ``player_id + reward_id`` 只生效一次。"""
    return _idempotent_once("claim", player_id, reward_id, ledger, now)


def compensation_dedup(player_id, event_id, ledger: IdempotencyLedger | None = None,
                       now: float | None = None) -> dict:
    """补偿发放去重：同一补偿事件重放/重试只发一次（不再重复补三封）。"""
    return _idempotent_once("compensation", player_id, event_id, ledger, now)


def mail_once(player_id, mail_id, ledger: IdempotencyLedger | None = None,
              now: float | None = None) -> dict:
    """邮件幂等发送：同一邮件 ID 对同一玩家只入箱一次。"""
    return _idempotent_once("mail", player_id, mail_id, ledger, now)


# ---------------------------------------------------------------------------
# 5. 重连校准
# ---------------------------------------------------------------------------


def reconnect_sync(cached_day_key, server_day_key,
                   cached_config_version: int | None = None,
                   server_config_version: int | None = None,
                   state: dict | None = None) -> dict:
    """重连时用服务器权威数据校准本地缓存。

    缓存的日键/周键/配置版本任一不一致即触发 ``recalibrate``，缓存只作
    展示兜底，绝不参与刷新判定。
    """
    reasons = []
    if cached_day_key != server_day_key:
        reasons.append("day_key_mismatch")
    if (cached_config_version is not None and server_config_version is not None
            and cached_config_version != server_config_version):
        reasons.append("config_version_mismatch")

    action = "recalibrate" if reasons else "cached_ok"
    result = {
        "action": action,
        "reasons": reasons,
        "cached_day_key": cached_day_key,
        "server_day_key": server_day_key,
        "cached_config_version": cached_config_version,
        "server_config_version": server_config_version,
    }
    if state is not None and action == "recalibrate":
        state["day_key"] = server_day_key
        if server_config_version is not None:
            state["config_version"] = server_config_version
        state["calibrated"] = True
    return result


# ---------------------------------------------------------------------------
# 6. 错峰刷新
# ---------------------------------------------------------------------------


def _stable_hash(*parts) -> int:
    digest = hashlib.blake2b(":".join(str(p) for p in parts).encode("utf-8"),
                             digest_size=8).digest()
    return int.from_bytes(digest, "big")


def stagger_window(player_id, window_seconds, salt: str = "daily") -> int:
    """把玩家稳定哈希到 ``[0, window_seconds)`` 的错峰偏移（秒）。

    同一玩家每天偏移固定（可预告），不同 salt（日常/周常）得到不同偏移，
    两类刷新不会再撞在同一秒打爆登录队列。
    """
    window = max(0, int(window_seconds))
    if window == 0:
        return 0
    return _stable_hash(salt, player_id) % window


# ---------------------------------------------------------------------------
# 8. 种子可控的任务池
# ---------------------------------------------------------------------------


def seeded_pool(seed, size: int = 10, pool=None) -> list:
    """用给定种子从任务池确定性抽取 ``size`` 个任务。

    同种子必得同序列（可复现、可做攻略）；生产环境种子由
    "配置版本 + 日期" 派生，全服当天一致、跨天自动换池。
    """
    candidates = list(pool if pool is not None else DEFAULT_TASK_POOL)
    rng = random.Random(seed)
    if size <= 0 or not candidates:
        return []
    if size >= len(candidates):
        rng.shuffle(candidates)
        return candidates
    return rng.sample(candidates, int(size))


def task_roll(task_id, seed, weight_total: int = 100) -> dict:
    """单任务的确定性掷点（0..weight_total-1），用于掉落/暴击等可复现随机。"""
    rng = random.Random(f"{seed}:{task_id}")
    return {
        "task_id": task_id,
        "seed": seed,
        "roll": rng.randrange(int(weight_total)),
        "weight_total": int(weight_total),
    }


# ---------------------------------------------------------------------------
# 9. 进度保留
# ---------------------------------------------------------------------------


def _as_task_state(item) -> TaskState:
    if isinstance(item, TaskState):
        return item
    if isinstance(item, dict):
        return TaskState(
            task_id=item["task_id"],
            progress=int(item.get("progress", 0)),
            target=int(item.get("target", 1)),
            claimed_stages=list(item.get("claimed_stages", [])),
            carry_over=bool(item.get("carry_over", True)),
        )
    raise TypeError(f"unsupported task state: {item!r}")


def keep_progress(old_state, new_pool, carry_all: bool = True) -> list[TaskState]:
    """把旧面板合并进新任务池：同 ID 任务继承进度，新任务从零开始。

    - ``carry_all=False`` 时仅继承显式标记 ``carry_over=True`` 的任务；
    - 继承过来的任务清空 ``claimed_stages``：新一天的阶段奖励重新开放，
      但玩家打过的进度不会被清零。
    """
    # 契约测试可能传入裸值，归一化为可迭代结构
    if isinstance(old_state, (TaskState, dict)):
        old_items = [old_state]
    elif isinstance(old_state, (list, tuple)):
        old_items = list(old_state)
    else:
        old_items = []
    if isinstance(new_pool, (list, tuple, set, frozenset)):
        new_ids = list(new_pool)
    else:
        new_ids = [new_pool]

    new_id_set = set(new_ids)
    merged: list[TaskState] = []
    for item in old_items:
        task = _as_task_state(item)
        if task.task_id not in new_id_set:
            continue
        if not carry_all and not task.carry_over:
            continue
        merged.append(TaskState(
            task_id=task.task_id,
            progress=task.progress,
            target=task.target,
            claimed_stages=[],  # 日结奖励重置，进度保留
            carry_over=task.carry_over,
        ))

    existing = {t.task_id for t in merged}
    for task_id in new_ids:
        if task_id not in existing:
            merged.append(TaskState(task_id=task_id, progress=0))
    return merged


# ---------------------------------------------------------------------------
# 10. 配置版本门控
# ---------------------------------------------------------------------------


def config_version(client_version: int, server_version: int) -> dict:
    """校验客户端刷新规则版本与服务端是否一致。"""
    if client_version == server_version:
        action, compatible, needs_migration = "proceed", True, False
    elif client_version < server_version:
        # 旧客户端还在按老时间/老规则算：强制升级，服务端按新版本滚服
        action, compatible, needs_migration = "upgrade_client", False, True
    else:
        # 不支持前向兼容：更新的客户端连旧服务端同样拒绝
        action, compatible, needs_migration = "reject_forward_unsupported", False, False
    return {
        "client_version": client_version,
        "server_version": server_version,
        "action": action,
        "compatible": compatible,
        "needs_migration": needs_migration,
    }


# ---------------------------------------------------------------------------
# 编排服务：把上面的原语串成完整的刷新链路
# ---------------------------------------------------------------------------


class DailyResetService:
    """日常/周常刷新服务端服务。

    时间源、配置、幂等账本均可注入，便于测试与多实例部署。
    """

    def __init__(self, config: ResetConfig | None = None,
                 clock=None, ledger: IdempotencyLedger | None = None):
        self.config = config or ResetConfig()
        self._clock = clock or time.time
        self.ledger = ledger or IdempotencyLedger()
        self._players: dict[int, PlayerState] = {}
        self._lock = threading.RLock()

    # -- 时间 -------------------------------------------------------------

    def now(self) -> float:
        return server_time(self._clock())

    def _tz(self, tz_offset_hours: float) -> tzinfo:
        return player_timezone(tz_offset_hours)

    def day_key(self, tz_offset_hours: float, now: float | None = None) -> str:
        return _day_key(self.now() if now is None else now, self._tz(tz_offset_hours))

    def week_key(self, tz_offset_hours: float, now: float | None = None) -> str:
        return _week_key(self.now() if now is None else now, self._tz(tz_offset_hours))

    # -- 种子与任务池 ------------------------------------------------------

    def shared_pool_seed(self, day_key: str) -> int:
        """全服共享种子：配置版本 + 日期。当天全服一致，可做攻略。"""
        return _stable_hash(self.config.config_version, self.config.daily_seed_salt, day_key)

    def player_pool_seed(self, day_key: str, player_id: int) -> int:
        """玩家级变体：在共享种子上再混 player_id，结果仍可复现。"""
        return _stable_hash(self.shared_pool_seed(day_key), player_id)

    def roll_tasks(self, day_key: str, player_id: int | None = None) -> list[str]:
        seed = (self.shared_pool_seed(day_key) if player_id is None
                else self.player_pool_seed(day_key, player_id))
        return seeded_pool(seed, self.config.pool_size, self.config.task_pool)

    # -- 玩家注册与滚服 ----------------------------------------------------

    def register_player(self, player_id: int, tz_offset_hours: float,
                        now: float | None = None) -> PlayerState:
        with self._lock:
            ts = self.now() if now is None else now
            tz = self._tz(tz_offset_hours)
            day = _day_key(ts, tz)
            state = PlayerState(
                player_id=player_id,
                tz_offset_hours=tz_offset_hours,
                day_key=day,
                week_key=_week_key(ts, tz),
                config_version=self.config.config_version,
                last_reset_ts=ts,
            )
            state.tasks = {
                t.task_id: t for t in
                keep_progress([], self.roll_tasks(day, player_id))
            }
            self._players[player_id] = state
            return state

    def _rollover(self, player: PlayerState, now: float) -> list[str]:
        """按服务器时间与当前配置版本执行滚服，返回发生的事件列表。"""
        tz = self._tz(player.tz_offset_hours)
        new_day = _day_key(now, tz)
        new_week = _week_key(now, tz)
        events: list[str] = []

        version_changed = player.config_version != self.config.config_version
        if version_changed:
            events.append("config_version_bump")
        if new_day != player.day_key:
            events.append("daily_rollover")
        if new_week != player.week_key:
            events.append("weekly_rollover")
        if not events:
            return events

        new_pool = self.roll_tasks(new_day, player.player_id)
        merged = keep_progress(list(player.tasks.values()), new_pool)
        player.tasks = {t.task_id: t for t in merged}
        player.day_key = new_day
        player.week_key = new_week
        player.config_version = self.config.config_version
        player.last_reset_ts = now
        return events

    def heartbeat(self, player_id: int) -> dict:
        """在线玩家心跳/业务请求入口：跨天热刷新，不需要重登。"""
        with self._lock:
            player = self._require_player(player_id)
            now = self.now()
            events = self._rollover(player, now)
            return {
                "player_id": player_id,
                "day_key": player.day_key,
                "week_key": player.week_key,
                "config_version": player.config_version,
                "events": events,
                "refreshed": bool(events),
            }

    # -- 领取 / 邮件 / 补偿 -----------------------------------------------

    def claim(self, player_id: int, reward_id, stage: str | None = None) -> dict:
        """领取奖励：先补滚服再幂等领取，reward_id 自动按天作用域隔离。"""
        with self._lock:
            player = self._require_player(player_id)
            self._rollover(player, self.now())
            scoped_reward = f"{player.day_key}:{reward_id}"
            result = claim_idempotent(player_id, scoped_reward, self.ledger, self.now())
            if result["accepted"] and stage is not None:
                for task in player.tasks.values():
                    if stage not in task.claimed_stages and reward_id in (task.task_id, stage):
                        task.claimed_stages.append(stage)
                        break
            return result

    def send_compensation(self, player_id: int, event_id) -> dict:
        with self._lock:
            return compensation_dedup(player_id, event_id, self.ledger, self.now())

    def send_mail(self, player_id: int, mail_id) -> dict:
        with self._lock:
            return mail_once(player_id, mail_id, self.ledger, self.now())

    # -- 重连 -------------------------------------------------------------

    def reconnect(self, player_id: int, cached_state: dict,
                  cached_config_version: int | None = None) -> dict:
        """以服务器权威状态校准客户端缓存，解决断线重连还是昨天任务的问题。"""
        with self._lock:
            now = self.now()
            player = self._players.get(player_id)
            if player is None:
                tz_offset = float(cached_state.get("tz_offset_hours", 0))
                player = self.register_player(player_id, tz_offset, now)

            sync = reconnect_sync(
                cached_state.get("day_key"), player.day_key,
                cached_config_version, self.config.config_version,
            )
            events = self._rollover(player, now)
            return {
                "sync": sync,
                "rollover_events": events,
                "state": player,
            }

    # -- 调度计划 ----------------------------------------------------------

    def schedule_for(self, player_id: int, tz_offset_hours: float,
                     now: float | None = None) -> dict:
        """返回该玩家错峰后的日常/周常刷新计划时间。"""
        ts = self.now() if now is None else now
        tz = self._tz(tz_offset_hours)
        daily_base = next_reset_at(ts, self.config.reset_hour, tz)
        daily_offset = stagger_window(player_id, self.config.stagger_window_seconds, "daily")
        weekly = week_boundary(ts, 7, tz)
        weekly_offset = stagger_window(player_id, self.config.stagger_window_seconds, "weekly")
        return {
            "daily_reset_ts": daily_base + daily_offset,
            "daily_stagger_offset": daily_offset,
            "weekly_reset_ts": weekly["boundary_ts"] + weekly_offset,
            "weekly_stagger_offset": weekly_offset,
        }

    # -- 内部 -------------------------------------------------------------

    def _require_player(self, player_id: int) -> PlayerState:
        player = self._players.get(player_id)
        if player is None:
            raise KeyError(f"player not registered: {player_id}")
        return player
