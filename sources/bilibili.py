"""B 站采集器：按 AI 关键词搜索 + 按播放量排序，公开数据。

`bilibili-api-python` 在 sessdata 留空时只能访问公开内容，正好够用。

提供两个入口：
- fetch_keywords(): 用于 **工具/教程** 场景 → ToolRow
- fetch_video_keywords(): 用于 **成片/短剧** 场景 → VideoRow
"""

from __future__ import annotations

import asyncio
from typing import Iterable

from bilibili_api import search
from bilibili_api.search import OrderVideo, SearchObjectType

from storage.db import ToolRow, VideoRow


# 关键词命中哪些 stage（关键词→流水线 stage 标签）
KEYWORD_TO_STAGE = {
    "comfyui": ["keyframe", "videogen"],
    "stable diffusion": ["keyframe"],
    "sd": ["keyframe"],
    "midjourney": ["keyframe"],
    "sora": ["videogen"],
    "ai 视频": ["videogen"],
    "ai 短剧": ["videogen", "script"],
    "runway": ["videogen"],
    "kling": ["videogen"],
    "veo": ["videogen"],
    "tts": ["tts"],
    "voice": ["tts"],
    "lipsync": ["lip_sync"],
    "lip-sync": ["lip_sync"],
}


async def _search_one(keyword: str, *, limit: int) -> list[ToolRow]:
    """B 站搜索一个关键词，按总点击量排序。"""
    rows: list[ToolRow] = []
    try:
        result = await search.search_by_type(
            keyword=keyword,
            search_type=SearchObjectType.VIDEO,
            order_type=OrderVideo.TOTALRANK,   # 综合
            page=1,
        )
    except Exception:
        return rows

    items = (result or {}).get("result", []) or []
    for it in items[:limit]:
        bvid = it.get("bvid") or ""
        if not bvid:
            continue
        rows.append(ToolRow(
            id=f"bili_{bvid}",
            source="bilibili",
            url=f"https://www.bilibili.com/video/{bvid}",
            name=_strip_em(it.get("title", "")),
            description=_strip_em(it.get("description", "")),
            metric=int(it.get("play", 0) or 0),
            publish_time=str(it.get("pubdate", "") or ""),
            raw={
                "bvid": bvid,
                "author": it.get("author"),
                "play": it.get("play"),
                "video_review": it.get("video_review"),
                "favorites": it.get("favorites"),
                "duration": it.get("duration"),
                "tag": it.get("tag"),
                "pic": it.get("pic"),
            },
            stage_tags=_keyword_stages(keyword),
        ))
    return rows


def _strip_em(s: str) -> str:
    # B站搜索结果会用 <em class="keyword"> 高亮关键词，去掉
    return (s or "").replace('<em class="keyword">', "").replace("</em>", "").strip()


def _keyword_stages(keyword: str) -> list[str]:
    k = keyword.lower().strip()
    return KEYWORD_TO_STAGE.get(k, [])


async def _fetch_async(keywords: Iterable[str], per_keyword: int) -> list[ToolRow]:
    results = await asyncio.gather(*[_search_one(kw, limit=per_keyword) for kw in keywords])
    rows = [r for batch in results for r in batch]
    # 按 id 去重，保留 metric 较大的
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen or (r.metric or 0) > (seen[r.id].metric or 0):
            seen[r.id] = r
    return list(seen.values())


def fetch_keywords(keywords: list[str], *, per_keyword: int = 6) -> list[ToolRow]:
    """同步包装，给采集 pipeline 用。（工具/教程场景）"""
    return asyncio.run(_fetch_async(keywords, per_keyword))


# ==================== 视频（成片）搜索 ====================

async def _search_videos_one(keyword: str, *, limit: int) -> list[VideoRow]:
    """B 站搜索一个关键词，返回 VideoRow（成片/短剧场景）。"""
    rows: list[VideoRow] = []
    try:
        result = await search.search_by_type(
            keyword=keyword,
            search_type=SearchObjectType.VIDEO,
            order_type=OrderVideo.CLICK,   # 按播放量排序（适合找爆款）
            page=1,
        )
    except Exception:
        return rows

    items = (result or {}).get("result", []) or []
    for it in items[:limit]:
        bvid = it.get("bvid") or ""
        if not bvid:
            continue
        title = _strip_em(it.get("title", ""))
        pic = it.get("pic") or ""
        if pic and pic.startswith("//"):
            pic = "https:" + pic

        rows.append(VideoRow(
            id=f"bili_v_{bvid}",
            platform="bilibili_video",
            url=f"https://www.bilibili.com/video/{bvid}",
            title=title,
            author=it.get("author", ""),
            plays=int(it.get("play", 0) or 0),
            likes=int(it.get("video_review", 0) or 0),     # B站 video_review = 弹幕数，近似用于互动
            duration_sec=_parse_duration(it.get("duration", "")),
            publish_time=str(it.get("pubdate", "") or ""),
            cover_url=pic,
            raw={
                "bvid": bvid,
                "source_keyword": keyword,
                "favorites": it.get("favorites"),
                "danmaku": it.get("video_review"),
                "tag": it.get("tag"),
                "description": _strip_em(it.get("description", "")),
            },
        ))
    return rows


def _parse_duration(s: str) -> int:
    """B站时长格式 'mm:ss' 或 'hh:mm:ss'。"""
    if not s or not isinstance(s, str):
        return 0
    try:
        parts = [int(p) for p in s.split(":")]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
    except (ValueError, TypeError):
        pass
    return 0


async def _fetch_videos_async(keywords: Iterable[str], per_keyword: int) -> list[VideoRow]:
    results = await asyncio.gather(*[_search_videos_one(kw, limit=per_keyword) for kw in keywords])
    rows = [r for batch in results for r in batch]
    seen: dict[str, VideoRow] = {}
    for r in rows:
        if r.id not in seen or r.plays > seen[r.id].plays:
            seen[r.id] = r
    return list(seen.values())


def fetch_video_keywords(keywords: list[str], *, per_keyword: int = 10) -> list[VideoRow]:
    """按关键词搜 B站成片，入 videos 表。适合找"AI 短剧"类成片。"""
    return asyncio.run(_fetch_videos_async(keywords, per_keyword))
