"""YouTube Shorts 采集器：用 Data API v3 按 AI 关键词搜索。

Quota：search.list=100 / videos.list=1；每周 1 次 30 视频 ≈ 130 quota，完全够用。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from googleapiclient.discovery import build

from storage.db import VideoRow


def _youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)


def fetch_shorts(
    *,
    api_key: str,
    query: str = "AI short film | AI video | Sora | Veo",
    region_code: str = "US",
    max_results: int = 30,
    days_back: int = 14,
) -> list[VideoRow]:
    """搜最近 `days_back` 天的 Shorts，按观看数排序，返回元数据。"""
    yt = _youtube_client(api_key)
    published_after = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).isoformat(timespec="seconds").replace("+00:00", "Z")

    # step 1: search
    search_resp = yt.search().list(
        q=query,
        type="video",
        videoDuration="short",
        part="id,snippet",
        maxResults=min(max_results, 50),
        order="viewCount",
        regionCode=region_code,
        publishedAfter=published_after,
        relevanceLanguage="en",
    ).execute()

    video_ids = [item["id"]["videoId"] for item in search_resp.get("items", [])
                 if item.get("id", {}).get("videoId")]
    if not video_ids:
        return []

    # step 2: batch fetch details (1 quota for up to 50 ids)
    details = yt.videos().list(
        part="snippet,statistics,contentDetails",
        id=",".join(video_ids),
    ).execute()

    rows: list[VideoRow] = []
    for item in details.get("items", []):
        rows.append(_to_row(item))

    # 按 plays 排序
    rows.sort(key=lambda r: r.plays, reverse=True)
    return rows


def _to_row(item: dict[str, Any]) -> VideoRow:
    vid = item["id"]
    sn = item.get("snippet", {})
    st = item.get("statistics", {})
    cd = item.get("contentDetails", {})
    return VideoRow(
        id=f"yt_{vid}",
        platform="youtube_shorts",
        url=f"https://www.youtube.com/shorts/{vid}",
        title=sn.get("title", ""),
        author=sn.get("channelTitle", ""),
        plays=int(st.get("viewCount", 0) or 0),
        likes=int(st.get("likeCount", 0) or 0),
        duration_sec=_parse_iso8601_duration(cd.get("duration", "PT0S")),
        publish_time=sn.get("publishedAt", ""),
        cover_url=(sn.get("thumbnails", {}).get("high") or {}).get("url", ""),
        raw={
            "video_id": vid,
            "channel_id": sn.get("channelId"),
            "tags": sn.get("tags", [])[:20],
            "category_id": sn.get("categoryId"),
            "description": (sn.get("description") or "")[:500],
            "comment_count": int(st.get("commentCount", 0) or 0),
        },
    )


def _parse_iso8601_duration(iso: str) -> int:
    """PT1M30S -> 90；够用的最简解析，只处理 M/S（Shorts 不超过 60s）。"""
    if not iso or not iso.startswith("PT"):
        return 0
    rest = iso[2:]
    seconds = 0
    num = ""
    for ch in rest:
        if ch.isdigit():
            num += ch
        elif ch == "H":
            seconds += int(num) * 3600; num = ""
        elif ch == "M":
            seconds += int(num) * 60; num = ""
        elif ch == "S":
            seconds += int(num); num = ""
    return seconds
