"""小红书采集器：用 Playwright 真浏览器走搜索结果页 → 抽 DOM。

小红书反爬最严，必须真浏览器 + 登录 cookie。
搜索 URL: https://www.xiaohongshu.com/search_result?keyword=XXX&type=51

成本：~5-10s/keyword（首次启动 Chromium 慢一点）。
"""

from __future__ import annotations

import time
from typing import Iterable
from urllib.parse import quote

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

from storage.db import VideoRow


def _parse_cookie(cookie_str: str, domain: str = ".xiaohongshu.com") -> list[dict]:
    """把 'k1=v1; k2=v2' 转成 Playwright 接受的 cookie 字典列表。"""
    pairs = []
    for part in cookie_str.split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        name, _, value = part.partition("=")
        pairs.append({
            "name": name.strip(),
            "value": value.strip(),
            "domain": domain,
            "path": "/",
        })
    return pairs


# DOM extraction script (运行在浏览器里)
EXTRACT_JS = r"""
() => {
  const cards = [];
  const items = document.querySelectorAll('section.note-item, a.cover[href*="/search_result"], a[href*="/explore/"]');
  const seen = new Set();
  document.querySelectorAll('a[href*="/explore/"]').forEach(a => {
    const href = a.getAttribute('href') || '';
    const m = href.match(/\/explore\/([0-9a-f]{16,})/);
    if (!m) return;
    const id = m[1];
    if (seen.has(id)) return;
    seen.add(id);
    // try to find parent card
    const card = a.closest('section') || a.closest('.note-item') || a.parentElement;
    if (!card) return;
    const titleEl = card.querySelector('.title, .footer .title, span.title, a.title');
    const authorEl = card.querySelector('.author-wrapper .name, .author .name, span.name, a.author');
    const likeEl = card.querySelector('.like-wrapper .count, .like-count, .count');
    const coverEl = card.querySelector('img');
    cards.push({
      id, href: 'https://www.xiaohongshu.com' + href,
      title: titleEl ? titleEl.textContent.trim() : '',
      author: authorEl ? authorEl.textContent.trim() : '',
      likes_text: likeEl ? likeEl.textContent.trim() : '',
      cover_url: coverEl ? (coverEl.getAttribute('src') || coverEl.getAttribute('data-src') || '') : '',
    });
  });
  return cards;
}
"""


def _likes_to_int(s: str) -> int:
    """'1.2万' → 12000, '15w' → 150000, '2098' → 2098"""
    if not s:
        return 0
    s = s.strip().lower()
    try:
        if "万" in s or "w" in s:
            num = float(s.replace("万", "").replace("w", "").strip())
            return int(num * 10000)
        if "k" in s:
            num = float(s.replace("k", "").strip())
            return int(num * 1000)
        return int(float(s))
    except (ValueError, AttributeError):
        return 0


def fetch_keywords(
    *,
    cookie_file: str,
    keywords: list[str] | None = None,
    per_keyword: int = 8,
    headless: bool = True,
) -> list[VideoRow]:
    """搜小红书。每个关键词等页面渲染完抽 DOM。"""
    keywords = keywords or ["AI 短剧", "AI 视频", "AI 动画", "AI 生成"]
    with open(cookie_file) as f:
        cookie = f.read().strip()
    cookies = _parse_cookie(cookie)

    rows: list[VideoRow] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless,
                                      args=["--disable-blink-features=AutomationControlled"])
        context = browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1440, "height": 900},
        )
        context.add_cookies(cookies)
        page = context.new_page()

        for kw in keywords:
            url = f"https://www.xiaohongshu.com/search_result?keyword={quote(kw)}&type=51"
            try:
                page.goto(url, timeout=20000, wait_until="domcontentloaded")
                # 等结果渲染（等到至少 1 个 explore 卡片出现）
                try:
                    page.wait_for_selector('a[href*="/explore/"]', timeout=10000)
                except PWTimeout:
                    print(f"[xhs] {kw}: no results within 10s")
                    continue
                # 滚一下加载更多
                page.evaluate("window.scrollBy(0, 1500)")
                time.sleep(1.5)
                items = page.evaluate(EXTRACT_JS)
            except Exception as e:
                print(f"[xhs] {kw} error: {e}")
                continue

            for it in items[:per_keyword]:
                rows.append(VideoRow(
                    id=f"xhs_{it['id']}",
                    platform="xiaohongshu",
                    url=it["href"],
                    title=it.get("title", ""),
                    author=it.get("author", ""),
                    plays=0,                      # 小红书不显示播放
                    likes=_likes_to_int(it.get("likes_text", "")),
                    duration_sec=None,
                    publish_time="",
                    cover_url=it.get("cover_url", ""),
                    raw={
                        "note_id": it["id"],
                        "source_keyword": kw,
                        "likes_text": it.get("likes_text", ""),
                    },
                ))

        browser.close()

    # dedup by id
    seen: dict[str, VideoRow] = {}
    for r in rows:
        if r.id not in seen or r.likes > seen[r.id].likes:
            seen[r.id] = r
    return list(seen.values())
