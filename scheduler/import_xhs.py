"""把家用机 scp 过来的 xhs.json 合并进 videos 表。

用法：python scheduler/import_xhs.py [--file /home/ubuntu/incoming/xhs.json]
被 111.229 每周一 08:00 的 run_weekly.sh 在 collect 前调用。
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

from storage.db import Db, VideoRow


DEFAULT_INCOMING = Path("/home/ubuntu/incoming/xhs.json")


def import_from_json(path: Path, db: Db) -> int:
    if not path.exists():
        print(f"[xhs-import] file not found: {path}")
        return 0
    try:
        items = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"[xhs-import] json parse failed: {e}")
        return 0

    rows: list[VideoRow] = []
    for it in items:
        rid = it.get("id")
        if not rid:
            continue
        rows.append(VideoRow(
            id=rid,
            platform=it.get("platform", "xiaohongshu"),
            url=it.get("url", ""),
            title=it.get("title", ""),
            author=it.get("author", ""),
            plays=int(it.get("plays") or 0),
            likes=int(it.get("likes") or 0),
            duration_sec=it.get("duration_sec"),
            publish_time=str(it.get("publish_time") or ""),
            cover_url=it.get("cover_url", ""),
            raw=it.get("raw") or {},
        ))
    n = db.upsert_videos(rows)
    print(f"[xhs-import] imported {len(rows)} xhs videos (changes={n})")
    # mark consumed so we don't re-import
    consumed_path = path.with_suffix(f".json.consumed.{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    try:
        path.rename(consumed_path)
    except OSError:
        pass
    return len(rows)


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=str(DEFAULT_INCOMING))
    ap.add_argument("--db", default="./data/crawler.db")
    args = ap.parse_args()
    db = Db(args.db)
    import_from_json(Path(args.file), db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
