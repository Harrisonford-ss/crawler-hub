"""抖音采集器：调用本地部署的 Douyin_TikTok_Download_API。

该 API 没有关键词搜索，只能按用户拉作品。因此策略是维护一张「关注 AI 短剧/视频创作者」
的 watch list（sec_user_id 列表），每周拉他们各自最新的 N 条作品做聚合。

sec_user_id 怎么拿：打开 douyin.com/user/xxxx 的用户主页，URL 里的 MS4wLjABAAAA... 就是。
"""

from __future__ import annotations

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.db import VideoRow


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(3), reraise=True)
def _get(client: httpx.Client, url: str, params: dict) -> dict:
    r = client.get(url, params=params, timeout=30.0)
    r.raise_for_status()
    return r.json()


def update_cookie(api_base: str, cookie: str) -> None:
    """将 cookie 写入容器的 TokenManager（service=douyin）。容器重启会丢失，需重调。"""
    r = httpx.post(
        f"{api_base}/api/hybrid/update_cookie",
        json={"service": "douyin", "cookie": cookie},
        timeout=15.0,
    )
    r.raise_for_status()


def fetch_from_accounts(
    *,
    api_base: str,
    sec_user_ids: list[str],
    count_per_user: int = 10,
) -> list[VideoRow]:
    """拉 watch list 里每个账号的最新作品。"""
    if not sec_user_ids:
        return []

    rows: list[VideoRow] = []
    with httpx.Client(headers={"User-Agent": "crawler-hub/0.1"}) as client:
        for sec in sec_user_ids:
            try:
                resp = _get(
                    client,
                    f"{api_base}/api/douyin/web/fetch_user_post_videos",
                    params={"sec_user_id": sec, "max_cursor": 0, "count": count_per_user},
                )
            except Exception as e:
                print(f"[douyin] sec_user_id={sec[:20]}... error: {e}")
                continue
            rows.extend(_parse_user_posts_resp(resp, source_sec=sec))

    # 按 aweme_id 去重
    seen: dict[str, VideoRow] = {}
    for r in rows:
        if r.id not in seen or r.plays > seen[r.id].plays:
            seen[r.id] = r
    return list(seen.values())


def _parse_user_posts_resp(resp: dict, *, source_sec: str) -> list[VideoRow]:
    """返回结构：{code, data: {aweme_list: [...], max_cursor, has_more}}"""
    if not isinstance(resp, dict):
        return []
    data = resp.get("data") or {}
    items = data.get("aweme_list") or []
    rows: list[VideoRow] = []
    for it in items:
        row = _item_to_row(it, source_sec)
        if row:
            rows.append(row)
    return rows


def _item_to_row(aweme: dict, source_sec: str) -> VideoRow | None:
    aweme_id = aweme.get("aweme_id")
    if not aweme_id:
        return None

    author = aweme.get("author") or {}
    stats = aweme.get("statistics") or {}
    video = aweme.get("video") or {}
    cover = (video.get("cover") or {}).get("url_list") or []
    duration = int(video.get("duration", 0) or 0) // 1000   # ms -> s

    return VideoRow(
        id=f"dy_{aweme_id}",
        platform="douyin",
        url=f"https://www.douyin.com/video/{aweme_id}",
        title=aweme.get("desc", ""),
        author=author.get("nickname", ""),
        plays=int(stats.get("play_count", 0) or 0),
        likes=int(stats.get("digg_count", 0) or 0),
        duration_sec=duration,
        publish_time=str(aweme.get("create_time", "")),
        cover_url=cover[0] if cover else "",
        raw={
            "aweme_id": aweme_id,
            "source_sec_user_id": source_sec,
            "comment_count": int(stats.get("comment_count", 0) or 0),
            "share_count": int(stats.get("share_count", 0) or 0),
            "collect_count": int(stats.get("collect_count", 0) or 0),
            "author_uid": author.get("uid"),
            "author_unique_id": author.get("unique_id"),
            "author_follower": author.get("follower_count"),
        },
    )
