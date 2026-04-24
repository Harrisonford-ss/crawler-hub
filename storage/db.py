"""SQLite 数据层。视频/工具/周报三张表，按 platform+id 去重。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator


SCHEMA = """
CREATE TABLE IF NOT EXISTS videos (
    id              TEXT PRIMARY KEY,
    platform        TEXT NOT NULL,
    url             TEXT NOT NULL,
    title           TEXT,
    author          TEXT,
    plays           INTEGER DEFAULT 0,
    likes           INTEGER DEFAULT 0,
    duration_sec    INTEGER,
    publish_time    TEXT,
    crawled_at      TEXT NOT NULL,
    cover_url       TEXT,
    raw_json        TEXT,
    hook            TEXT,
    structure       TEXT,
    style_tags      TEXT,
    score           REAL
);

CREATE INDEX IF NOT EXISTS idx_videos_platform_crawled
    ON videos(platform, crawled_at DESC);

CREATE TABLE IF NOT EXISTS tools (
    id              TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    url             TEXT NOT NULL,
    name            TEXT,
    description     TEXT,
    metric          INTEGER DEFAULT 0,
    publish_time    TEXT,
    crawled_at      TEXT NOT NULL,
    raw_json        TEXT,
    summary         TEXT,
    stage_tags      TEXT,
    score           REAL
);

CREATE INDEX IF NOT EXISTS idx_tools_source_crawled
    ON tools(source, crawled_at DESC);

CREATE TABLE IF NOT EXISTS reports (
    week            TEXT PRIMARY KEY,
    generated_at    TEXT NOT NULL,
    top_videos      TEXT,
    top_tools       TEXT,
    summary         TEXT
);
"""


@dataclass
class VideoRow:
    id: str
    platform: str             # douyin / youtube_shorts
    url: str
    title: str = ""
    author: str = ""
    plays: int = 0
    likes: int = 0
    duration_sec: int | None = None
    publish_time: str = ""    # ISO
    cover_url: str = ""
    raw: dict = field(default_factory=dict)
    # LLM 分析填充
    hook: str = ""
    structure: str = ""
    style_tags: list[str] = field(default_factory=list)
    score: float | None = None


@dataclass
class ToolRow:
    id: str
    source: str               # bilibili / huggingface
    url: str
    name: str = ""
    description: str = ""
    metric: int = 0           # downloads / 播放量
    publish_time: str = ""    # ISO
    raw: dict = field(default_factory=dict)
    summary: str = ""
    stage_tags: list[str] = field(default_factory=list)
    score: float | None = None


class Db:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as c:
            c.executescript(SCHEMA)

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    # --- videos ---

    def upsert_videos(self, rows: list[VideoRow]) -> int:
        if not rows:
            return 0
        now = datetime.now().isoformat(timespec="seconds")
        with self._conn() as c:
            c.executemany(
                """
                INSERT INTO videos (id, platform, url, title, author, plays, likes,
                    duration_sec, publish_time, crawled_at, cover_url, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    plays = excluded.plays,
                    likes = excluded.likes,
                    crawled_at = excluded.crawled_at,
                    raw_json = excluded.raw_json
                """,
                [
                    (r.id, r.platform, r.url, r.title, r.author, r.plays, r.likes,
                     r.duration_sec, r.publish_time, now, r.cover_url,
                     json.dumps(r.raw, ensure_ascii=False))
                    for r in rows
                ],
            )
            return c.total_changes

    def fresh_videos(self, since_iso: str, platform: str | None = None) -> list[dict]:
        sql = "SELECT * FROM videos WHERE crawled_at >= ?"
        args: list = [since_iso]
        if platform:
            sql += " AND platform = ?"
            args.append(platform)
        sql += " ORDER BY plays DESC"
        with self._conn() as c:
            return [dict(row) for row in c.execute(sql, args)]

    def update_video_analysis(self, video_id: str, *, hook: str, structure: str,
                              style_tags: list[str], score: float) -> None:
        with self._conn() as c:
            c.execute(
                """
                UPDATE videos SET hook = ?, structure = ?, style_tags = ?, score = ?
                WHERE id = ?
                """,
                (hook, structure, json.dumps(style_tags, ensure_ascii=False), score, video_id),
            )

    # --- tools ---

    def upsert_tools(self, rows: list[ToolRow]) -> int:
        if not rows:
            return 0
        now = datetime.now().isoformat(timespec="seconds")
        with self._conn() as c:
            c.executemany(
                """
                INSERT INTO tools (id, source, url, name, description, metric,
                    publish_time, crawled_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    metric = excluded.metric,
                    crawled_at = excluded.crawled_at,
                    raw_json = excluded.raw_json
                """,
                [
                    (r.id, r.source, r.url, r.name, r.description, r.metric,
                     r.publish_time, now, json.dumps(r.raw, ensure_ascii=False))
                    for r in rows
                ],
            )
            return c.total_changes

    def fresh_tools(self, since_iso: str, source: str | None = None) -> list[dict]:
        sql = "SELECT * FROM tools WHERE crawled_at >= ?"
        args: list = [since_iso]
        if source:
            sql += " AND source = ?"
            args.append(source)
        sql += " ORDER BY metric DESC"
        with self._conn() as c:
            return [dict(row) for row in c.execute(sql, args)]

    def update_tool_analysis(self, tool_id: str, *, summary: str,
                             stage_tags: list[str], score: float) -> None:
        with self._conn() as c:
            c.execute(
                """
                UPDATE tools SET summary = ?, stage_tags = ?, score = ?
                WHERE id = ?
                """,
                (summary, json.dumps(stage_tags, ensure_ascii=False), score, tool_id),
            )

    # --- reports ---

    def save_report(self, week: str, top_videos: list[dict], top_tools: list[dict],
                    summary: str) -> None:
        now = datetime.now().isoformat(timespec="seconds")
        with self._conn() as c:
            c.execute(
                """
                INSERT INTO reports (week, generated_at, top_videos, top_tools, summary)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(week) DO UPDATE SET
                    generated_at = excluded.generated_at,
                    top_videos = excluded.top_videos,
                    top_tools = excluded.top_tools,
                    summary = excluded.summary
                """,
                (week, now,
                 json.dumps(top_videos, ensure_ascii=False),
                 json.dumps(top_tools, ensure_ascii=False),
                 summary),
            )

    def latest_report(self) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                "SELECT * FROM reports ORDER BY week DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None
