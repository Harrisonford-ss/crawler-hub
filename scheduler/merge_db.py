"""合并另一台服务器的 crawler.db 到本地 DB（upsert）。

用法：python scheduler/merge_db.py /path/to/remote.db
表：videos + tools（reports 每台本地生成，不合并）
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


def merge(remote_path: str | Path, local_path: str | Path) -> dict[str, int]:
    remote = sqlite3.connect(str(remote_path))
    local = sqlite3.connect(str(local_path))
    stats: dict[str, int] = {}
    try:
        for table in ("videos", "tools"):
            cur = remote.execute(f"SELECT * FROM {table}")
            cols = [c[0] for c in cur.description]
            placeholders = ",".join("?" * len(cols))
            sql = f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
            rows = cur.fetchall()
            local.executemany(sql, rows)
            stats[table] = len(rows)
            print(f"[merge] {table}: {len(rows)} rows imported")
        local.commit()
    finally:
        remote.close()
        local.close()
    return stats


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: python scheduler/merge_db.py <remote.db> [local.db]")
        return 1
    remote_path = Path(sys.argv[1])
    local_path = Path(sys.argv[2] if len(sys.argv) > 2 else "./data/crawler.db")
    if not remote_path.exists():
        print(f"[merge] remote db not found: {remote_path}")
        return 2
    if not local_path.exists():
        print(f"[merge] local db not found: {local_path}")
        return 2
    merge(remote_path, local_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
