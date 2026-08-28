from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_scheduled_service_uses_cli_lock_and_previous_day_timer() -> None:
    unit = (PROJECT_ROOT / "deploy" / "retail-tide-posts.service").read_text(
        encoding="utf-8"
    )
    timer = (PROJECT_ROOT / "deploy" / "retail-tide-posts-yesterday.timer").read_text(
        encoding="utf-8"
    )

    assert "retail-tide scheduled-refresh --limit 50000" in unit
    assert "/usr/bin/flock" not in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=1h" in unit
    assert "StartLimitIntervalSec=0" in unit
    assert "OnCalendar=*-*-* 03:00:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer
    assert "Unit=retail-tide-posts.service" in timer
    assert not (PROJECT_ROOT / "deploy" / "retail-tide-posts@.service").exists()
    assert not (PROJECT_ROOT / "deploy" / "retail-tide-posts-today.timer").exists()


def test_wikimedia_uses_an_independent_utc_timer() -> None:
    unit = (PROJECT_ROOT / "deploy" / "retail-tide-wikimedia.service").read_text(encoding="utf-8")
    timer = (PROJECT_ROOT / "deploy" / "retail-tide-wikimedia-yesterday.timer").read_text(
        encoding="utf-8"
    )

    assert "retail-tide scheduled-wikimedia --limit 50000" in unit
    assert "Restart=on-failure" in unit
    assert "RestartSec=1h" in unit
    assert "OnCalendar=*-*-* 04:00:00 UTC" in timer
    assert "Persistent=true" in timer
    assert "Unit=retail-tide-wikimedia.service" in timer
