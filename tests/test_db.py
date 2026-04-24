"""SQLite 层最小冒烟测试，验证 schema、upsert、查询。"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from storage.db import Db, ToolRow, VideoRow


@pytest.fixture
def db(tmp_path: Path) -> Db:
    return Db(tmp_path / "t.db")


def test_videos_upsert_and_fresh(db: Db) -> None:
    rows = [
        VideoRow(id="dy_1", platform="douyin", url="https://d/1", title="t1", plays=100),
        VideoRow(id="yt_1", platform="youtube_shorts", url="https://y/1", title="t2", plays=200),
    ]
    db.upsert_videos(rows)

    # upsert again — should not duplicate
    rows[0].plays = 999
    db.upsert_videos(rows)

    yesterday = (datetime.now() - timedelta(days=1)).isoformat(timespec="seconds")
    all_fresh = db.fresh_videos(yesterday)
    assert len(all_fresh) == 2
    dy = db.fresh_videos(yesterday, platform="douyin")
    assert len(dy) == 1 and dy[0]["plays"] == 999


def test_video_analysis_update(db: Db) -> None:
    db.upsert_videos([VideoRow(id="v1", platform="douyin", url="u")])
    db.update_video_analysis("v1", hook="开场反转", structure="3 段式",
                             style_tags=["搞笑", "宠物"], score=8.5)
    yesterday = (datetime.now() - timedelta(days=1)).isoformat(timespec="seconds")
    rows = db.fresh_videos(yesterday)
    assert rows[0]["hook"] == "开场反转"
    assert rows[0]["score"] == 8.5


def test_tools_and_report(db: Db) -> None:
    db.upsert_tools([
        ToolRow(id="hf_a", source="huggingface", url="u", name="Sora-clone", metric=12345),
        ToolRow(id="bili_b", source="bilibili", url="u", name="ComfyUI 教程", metric=99999),
    ])
    yesterday = (datetime.now() - timedelta(days=1)).isoformat(timespec="seconds")
    assert len(db.fresh_tools(yesterday)) == 2
    assert db.fresh_tools(yesterday, source="huggingface")[0]["metric"] == 12345

    db.save_report("2026-W17", top_videos=[{"id": "v1"}], top_tools=[{"id": "t1"}],
                   summary="本周 AI 圈聚焦视频生成。")
    latest = db.latest_report()
    assert latest is not None and latest["week"] == "2026-W17"
