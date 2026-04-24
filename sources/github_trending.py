"""GitHub Trending 采集器：拉每日/每周热门仓库（AI 相关）。

GitHub 没有官方 trending API。用 GitHub Search API 按 created/pushed 时间过滤 + AI 关键词。
免费 API：60 reqs/h，加 token 5000/h。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.db import ToolRow


API_URL = "https://api.github.com/search/repositories"

# 关键词 → 我们流水线 stage_tags
KEYWORD_TO_STAGE = {
    "video generation": ["videogen"],
    "text-to-video": ["videogen"],
    "image-to-video": ["videogen"],
    "diffusion": ["keyframe", "videogen"],
    "stable diffusion": ["keyframe"],
    "comfyui": ["keyframe", "videogen"],
    "lora": ["keyframe"],
    "flux": ["keyframe"],
    "tts": ["tts"],
    "text-to-speech": ["tts"],
    "voice clone": ["tts"],
    "lip sync": ["lip_sync"],
    "talking head": ["lip_sync"],
    "audio generation": ["bgm", "sfx"],
    "music generation": ["bgm"],
}


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(3), reraise=True)
def _search(client: httpx.Client, query: str, sort: str, per_page: int) -> dict:
    r = client.get(API_URL, params={
        "q": query,
        "sort": sort,
        "order": "desc",
        "per_page": per_page,
    }, timeout=20.0)
    r.raise_for_status()
    return r.json()


def fetch_trending(
    *,
    token: str | None = None,
    days_back: int = 14,
    per_query: int = 10,
) -> list[ToolRow]:
    """GitHub 上最近 N 天创建/更新的 AI 相关爆款仓库。"""
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    queries = [
        # 视频生成相关
        f'"text to video" pushed:>{since} stars:>10',
        f'"video generation" pushed:>{since} stars:>10',
        # 工作流
        f'comfyui pushed:>{since} stars:>5',
        f'workflow ai-video pushed:>{since} stars:>5',
        # 图像
        f'"flux" pushed:>{since} stars:>20',
        f'"stable diffusion" pushed:>{since} stars:>20',
        # 音频
        f'"voice clone" pushed:>{since} stars:>10',
        f'tts ai pushed:>{since} stars:>10',
    ]

    headers = {"User-Agent": "crawler-hub/0.1", "Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    rows: list[ToolRow] = []
    with httpx.Client(headers=headers) as client:
        for q in queries:
            try:
                data = _search(client, q, sort="stars", per_page=per_query)
            except Exception as e:
                print(f"[github] q={q[:40]}... error: {e}")
                continue
            for item in data.get("items", []):
                rows.append(_repo_to_row(item))

    # dedup by repo full_name
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen or (r.metric or 0) > (seen[r.id].metric or 0):
            seen[r.id] = r
    return list(seen.values())


def _repo_to_row(item: dict[str, Any]) -> ToolRow:
    name = item.get("full_name", "")
    desc = (item.get("description") or "").strip()
    low = (name + " " + desc).lower()
    stage_tags: set[str] = set()
    for kw, ss in KEYWORD_TO_STAGE.items():
        if kw in low:
            for s in ss:
                stage_tags.add(s)
    return ToolRow(
        id=f"gh_{name}".replace("/", "_"),
        source="github",
        url=item.get("html_url", ""),
        name=name,
        description=desc[:200],
        metric=int(item.get("stargazers_count", 0) or 0),
        publish_time=item.get("pushed_at", "") or "",
        raw={
            "language": item.get("language"),
            "watchers": item.get("watchers_count"),
            "forks": item.get("forks_count"),
            "topics": item.get("topics", [])[:10],
        },
        stage_tags=sorted(stage_tags),
    )
