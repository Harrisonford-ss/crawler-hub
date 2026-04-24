"""通用 RSS 新闻采集器：拉 AI 主题资讯站的 RSS feed 文章。

输入一个 feed URL 列表（含元信息），输出 ToolRow（type=news）。
适用于：36kr / 爱范儿 / 机器之心英文版（synced review）/ HF blog 等。

按 description/title 关键词命中"AI 短剧/视频/语音"才保留——避开非 AI 噪音。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from xml.etree import ElementTree as ET

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.db import ToolRow


# AI 相关关键词（标题/摘要含其中之一才入库）
AI_KEYWORDS = [
    "AI", "GPT", "大模型", "LLM", "Sora", "Veo", "Runway", "Kling", "可灵",
    "Stable Diffusion", "ComfyUI", "Midjourney", "Diffusion",
    "短剧", "视频生成", "图生视频", "文生视频", "数字人", "AIGC",
    "DeepSeek", "豆包", "Qwen", "通义", "MiniMax", "Kimi",
    "AI agent", "智能体", "Claude", "Anthropic",
]

# 关键词 → stage_tags
KEYWORD_TO_STAGE = {
    "短剧": ["script", "videogen"],
    "视频生成": ["videogen"],
    "文生视频": ["videogen"],
    "图生视频": ["videogen"],
    "Sora": ["videogen"], "Veo": ["videogen"], "Runway": ["videogen"], "Kling": ["videogen"],
    "Stable Diffusion": ["keyframe"], "ComfyUI": ["keyframe", "videogen"],
    "Midjourney": ["keyframe"], "Diffusion": ["keyframe", "videogen"],
    "TTS": ["tts"], "数字人": ["lip_sync"],
    "音乐": ["bgm"], "音效": ["sfx"],
    "GPT": ["script"], "LLM": ["script"], "大模型": ["script"],
    "DeepSeek": ["script"], "Qwen": ["script"], "豆包": ["script"],
}


@dataclass
class FeedSource:
    name: str          # 显示名："36kr", "ifanr"
    url: str           # RSS URL


DEFAULT_FEEDS = [
    FeedSource("36kr", "https://36kr.com/feed"),
    FeedSource("ifanr", "https://www.ifanr.com/feed"),
    FeedSource("synced", "https://syncedreview.com/feed/"),
    FeedSource("hf_blog", "https://huggingface.co/blog/feed.xml"),
]


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(2), reraise=False)
def _get(client: httpx.Client, url: str) -> str:
    r = client.get(url, timeout=20.0, headers={"User-Agent": "Mozilla/5.0 crawler-hub"})
    r.raise_for_status()
    return r.text


def fetch_news(
    *,
    feeds: list[FeedSource] | None = None,
    max_per_feed: int = 12,
) -> list[ToolRow]:
    feeds = feeds or DEFAULT_FEEDS
    rows: list[ToolRow] = []
    with httpx.Client(follow_redirects=True) as client:
        for f in feeds:
            try:
                xml = _get(client, f.url)
            except Exception as e:
                print(f"[rss] {f.name} fetch error: {e}")
                continue
            for r in _parse_rss(xml, f.name):
                rows.append(r)
                if sum(1 for x in rows if x.raw.get("feed") == f.name) >= max_per_feed:
                    break
    # dedup
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen:
            seen[r.id] = r
    return list(seen.values())


def _parse_rss(xml_text: str, feed_name: str) -> list[ToolRow]:
    rows: list[ToolRow] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return rows

    # RSS 2.0 (channel/item) + Atom (feed/entry) 都支持
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    for it in items:
        title = _extract(it, ["title", "{http://www.w3.org/2005/Atom}title"]).strip()
        link = _extract_link(it)
        desc = _strip_tags(_extract(it, [
            "description", "summary", "content:encoded",
            "{http://www.w3.org/2005/Atom}summary",
            "{http://www.w3.org/2005/Atom}content",
            "{http://purl.org/rss/1.0/modules/content/}encoded",
        ]))
        pub = _extract(it, ["pubDate", "{http://www.w3.org/2005/Atom}published",
                              "{http://www.w3.org/2005/Atom}updated"])

        # AI 关键词过滤
        haystack = (title + " " + desc).lower()
        if not any(kw.lower() in haystack for kw in AI_KEYWORDS):
            continue

        # 提取 stage_tags
        stage_tags: set[str] = set()
        for kw, ss in KEYWORD_TO_STAGE.items():
            if kw.lower() in haystack:
                for s in ss:
                    stage_tags.add(s)

        # ID：用 link 的 hash
        item_id = re.sub(r"[^\w]", "_", link)[-60:]
        rows.append(ToolRow(
            id=f"news_{feed_name}_{item_id}",
            source="news",
            url=link,
            name=title[:160],
            description=desc[:300],
            metric=0,
            publish_time=pub,
            raw={
                "feed": feed_name,
                "full_summary": desc[:1500],
            },
            stage_tags=sorted(stage_tags),
        ))
    return rows


def _extract(el, tags: list[str]) -> str:
    for t in tags:
        node = el.find(t)
        if node is not None and node.text:
            return node.text
    return ""


def _extract_link(el) -> str:
    # RSS: <link>url</link>; Atom: <link href="url"/>
    node = el.find("link")
    if node is not None:
        if node.text and node.text.strip().startswith("http"):
            return node.text.strip()
        href = node.get("href")
        if href:
            return href
    # Atom
    for n in el.findall("{http://www.w3.org/2005/Atom}link"):
        if n.get("href"):
            return n.get("href")
    return ""


_TAG_RE = re.compile(r"<[^>]+>")
def _strip_tags(s: str) -> str:
    if not s:
        return ""
    return unescape(re.sub(r"\s+", " ", _TAG_RE.sub("", s))).strip()
