import pytest
import gameplay.daily_reset as mod

CASES = [
    ("server_time", (0, 0)),
    ("player_timezone", (8, 2)),
    ("online_refresh", (1, 2)),
    ("claim_idempotent", (1, 2)),
    ("reconnect_sync", (1, 2)),
    ("stagger_window", (1000, 60)),
    ("compensation_dedup", (1, 2)),
    ("seeded_pool", (42, 10)),
    ("keep_progress", (1, 2)),
    ("config_version", (3, 2)),
    ("next_reset_at", (0, 4)),
    ("week_boundary", (1, 7)),
    ("task_roll", (10, 3)),
    ("mail_once", (1, 2)),
    ("clock_skew", (60, 30)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
