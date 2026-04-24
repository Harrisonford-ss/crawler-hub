"""arXiv 采集器：抓 AI 相关新论文。

直接用 arXiv 官方 RSS + API：
https://export.arxiv.org/api/query?search_query=cat:cs.CV+OR+cat:cs.MM+OR+cat:cs.AI&sortBy=submittedDate&sortOrder=descending

不需要 API key。返回 Atom XML。
"""

from __future__ import annotations

import re
from datetime import datetime
from xml.etree import ElementTree as ET

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.db import ToolRow


API_URL = "https://export.arxiv.org/api/query"

# arXiv 分类 → 我们的 stage_tags（相关度筛选用）
CAT_TO_STAGE = {
    "cs.CV": ["keyframe", "videogen"],   # computer vision
    "cs.MM": ["videogen"],                # multimedia
    "cs.SD": ["tts", "sfx", "bgm"],       # sound
    "cs.CL": ["script"],                  # natural language
    "cs.GR": ["keyframe", "videogen"],    # graphics
    "eess.AS": ["tts", "sfx"],            # audio/speech
    "eess.IV": ["keyframe"],              # image and video processing
}

# 关键词 → stage_tags（在标题/摘要里）
KEYWORD_TO_STAGE = {
    "text-to-video": ["videogen"],
    "text to video": ["videogen"],
    "video generation": ["videogen"],
    "video diffusion": ["videogen"],
    "text-to-image": ["keyframe", "ref_image"],
    "text to image": ["keyframe", "ref_image"],
    "text-to-speech": ["tts"],
    "tts": ["tts"],
    "lip sync": ["lip_sync"],
    "lipsync": ["lip_sync"],
    "face swap": ["lip_sync"],
    "talking head": ["lip_sync"],
    "music generation": ["bgm"],
    "sound effect": ["sfx"],
    "storytelling": ["script"],
    "screenplay": ["script"],
}


ATOM_NS = "{http://www.w3.org/2005/Atom}"


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(3), reraise=True)
def _query(client: httpx.Client, search_query: str, start: int, max_results: int) -> str:
    r = client.get(API_URL, params={
        "search_query": search_query,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
        "start": start,
        "max_results": max_results,
    }, timeout=30.0)
    r.raise_for_status()
    return r.text


def fetch_recent(
    *,
    queries: list[str] | None = None,
    max_per_query: int = 20,
) -> list[ToolRow]:
    """拉 arXiv 最近的 AI 相关论文。

    queries: arXiv 搜索查询，如 `["cat:cs.CV AND all:video", "cat:cs.SD AND all:speech"]`
    """
    queries = queries or [
        # video generation
        'cat:cs.CV AND (abs:"video generation" OR abs:"text-to-video" OR abs:"video diffusion")',
        # image generation
        'cat:cs.CV AND (abs:"text-to-image" OR abs:"diffusion model")',
        # speech / TTS
        '(cat:eess.AS OR cat:cs.SD) AND (abs:"speech synthesis" OR abs:"text-to-speech")',
        # talking head / lip sync
        'cat:cs.CV AND (abs:"talking head" OR abs:"lip sync" OR abs:"face animation")',
    ]

    rows: list[ToolRow] = []
    with httpx.Client(headers={"User-Agent": "crawler-hub/0.1 (research)"}) as client:
        for q in queries:
            try:
                xml = _query(client, q, 0, max_per_query)
            except Exception as e:
                print(f"[arxiv] query={q[:40]}... error: {e}")
                continue
            for row in _parse_atom(xml):
                rows.append(row)

    # dedup by id
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen:
            seen[r.id] = r
    return list(seen.values())


def _parse_atom(xml_text: str) -> list[ToolRow]:
    rows: list[ToolRow] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"[arxiv] parse error: {e}")
        return rows

    for entry in root.findall(f"{ATOM_NS}entry"):
        arxiv_id = _text(entry, f"{ATOM_NS}id")
        if not arxiv_id:
            continue
        # extract short id from url: http://arxiv.org/abs/2404.12345v1 → 2404.12345
        short_id = arxiv_id.rsplit("/", 1)[-1].split("v")[0]
        title = re.sub(r"\s+", " ", _text(entry, f"{ATOM_NS}title") or "").strip()
        summary = re.sub(r"\s+", " ", _text(entry, f"{ATOM_NS}summary") or "").strip()
        published = _text(entry, f"{ATOM_NS}published") or ""
        authors = [
            (_text(a, f"{ATOM_NS}name") or "")
            for a in entry.findall(f"{ATOM_NS}author")
        ]
        cats = [c.get("term", "") for c in entry.findall(f"{ATOM_NS}category")]

        # derive stage_tags from categories + keywords
        stage_tags: set[str] = set()
        for c in cats:
            for s in CAT_TO_STAGE.get(c, []):
                stage_tags.add(s)
        low = (title + " " + summary).lower()
        for kw, ss in KEYWORD_TO_STAGE.items():
            if kw in low:
                for s in ss:
                    stage_tags.add(s)

        rows.append(ToolRow(
            id=f"arxiv_{short_id}",
            source="arxiv",
            url=f"https://arxiv.org/abs/{short_id}",
            name=title[:160],
            description=summary[:300],
            metric=0,   # arXiv 不提供下载/引用数；豆包靠标题摘要打分
            publish_time=published,
            raw={
                "arxiv_id": short_id,
                "authors": authors[:6],
                "categories": cats,
                "full_summary": summary[:2000],
            },
            stage_tags=sorted(stage_tags),
        ))
    return rows


def _text(el: ET.Element, tag: str) -> str:
    node = el.find(tag)
    return (node.text or "").strip() if node is not None else ""
