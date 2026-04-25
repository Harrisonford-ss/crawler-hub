"""生成周报：把过去 7 天的 videos + tools 按评分排序，出 markdown + JSON。

Markdown 给人看 + 给 Server酱 推送。
JSON 给 web dashboard 消费。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openai import OpenAI

from storage.db import Db


PLATFORM_EMOJI = {
    "douyin": "🔴",
    "youtube_shorts": "🟡",
    "bilibili": "🔵",
    "huggingface": "🤗",
}

STAGE_LABEL = {
    "world": "世界观",
    "script": "剧本",
    "ref_image": "参考图",
    "keyframe": "关键帧",
    "videogen": "视频生成",
    "quality": "质检",
    "tts": "配音",
    "sfx": "音效",
    "bgm": "配乐",
    "edit": "剪辑",
    "lip_sync": "对口型",
}


def week_code(when: datetime | None = None) -> str:
    when = when or datetime.now()
    return f"{when.isocalendar().year}-W{when.isocalendar().week:02d}"


def generate(
    *,
    db: Db,
    doubao_client: OpenAI,
    doubao_model: str,
    days_back: int = 7,
    per_video_source: int = 5,
    per_tool_source: int = 6,
    out_dir: str | Path = "./data",
) -> dict:
    """取 7 天数据 → verdict 过滤 → 每源 top N → 生成 markdown + JSON。

    过滤规则：
    - verdict=C 直接淘汰
    - relevance<5 直接淘汰
    排序：(quality*0.4 + actionable*0.6) × log10(engagement) × exp(-days/14)
    """
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat(timespec="seconds")
    videos_raw = db.fresh_videos(since)
    tools_raw = db.fresh_tools(since)

    import math
    from datetime import datetime as _dt

    def _passes_filter(item: dict) -> bool:
        """硬门槛：relevance < 5 或 verdict=C 都淘汰。"""
        if (item.get("verdict") or "").upper() == "C":
            return False
        if (item.get("relevance") or 0) < 5:
            return False
        return True

    def _passes_engagement(item: dict, is_video: bool) -> bool:
        """互动量门槛——爆款视频必有人看；工具来源用 metric 区分"""
        if not is_video:
            # 工具：metric 代表 downloads/stars/播放量
            # arxiv/news metric=0 无意义，按 verdict 只保 S/A，B 以下淘汰
            source = item.get("source", "")
            metric = item.get("metric") or 0
            if source in ("arxiv", "news"):
                # 这俩没 metric 可比，靠 verdict 过滤即可
                return (item.get("verdict") or "").upper() in ("S", "A")
            if source == "huggingface":
                return metric >= 10000      # HF 下载 1 万起步
            if source == "modelscope":
                return metric >= 5000       # ModelScope 下载 5 千起步
            if source == "github":
                return metric >= 100        # GitHub 星 100 起步
            if source == "bilibili":
                return metric >= 50000      # B 站教程播放 5 万起步
            return metric >= 100
        # 视频：平台 plays/likes 门槛
        plat = item.get("platform", "")
        plays = item.get("plays") or 0
        likes = item.get("likes") or 0
        engagement = max(plays, likes)
        # 抖音/小红书 plays 被平台隐藏，用 likes 代替；B 站/YT plays 明确
        thresholds = {
            "douyin":         500,      # 点赞 500 起步
            "xiaohongshu":    300,      # 点赞 300 起步
            "bilibili_video": 100000,   # 播放 10 万起步
            "youtube_shorts": 50000,    # 播放 5 万起步
        }
        return engagement >= thresholds.get(plat, 1000)

    def _days_since(iso: str) -> float:
        if not iso:
            return 999.0
        try:
            # 兼容 RFC/ISO/ 纯日期格式
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f"):
                try:
                    dt = _dt.strptime(iso[:19 if len(iso) >= 19 else len(iso)].replace("Z", ""), fmt[:19 if len(fmt) >= 19 else len(fmt)])
                    return max(0.0, (_dt.now() - dt).total_seconds() / 86400.0)
                except ValueError:
                    continue
        except Exception:
            pass
        return 999.0

    def _time_decay(days: float) -> float:
        """exp(-days/14)：2 周前衰减到 1/e，4 周前到 1/e²。"""
        return math.exp(-min(days, 60) / 14.0)

    def _video_key(v: dict) -> float:
        q = v.get("quality") or 0
        a = v.get("actionable") or 0
        engagement = max(1, v.get("plays") or 0, v.get("likes") or 0)
        days = _days_since(v.get("publish_time") or v.get("crawled_at") or "")
        return (q * 0.4 + a * 0.6) * math.log10(engagement) * _time_decay(days)

    def _tool_key(t: dict) -> float:
        q = t.get("quality") or 0
        a = t.get("actionable") or 0
        metric = max(1, t.get("metric") or 1)
        days = _days_since(t.get("publish_time") or t.get("crawled_at") or "")
        return (q * 0.4 + a * 0.6) * math.log10(metric) * _time_decay(days)

    # 过滤两层：verdict + 互动量
    videos = [v for v in videos_raw if _passes_filter(v) and _passes_engagement(v, True)]
    tools = [t for t in tools_raw if _passes_filter(t) and _passes_engagement(t, False)]
    print(f"[report] filter: videos {len(videos_raw)} → {len(videos)} ; tools {len(tools_raw)} → {len(tools)}")

    # 按平台/源分桶 → 每桶取 top N → 合并
    def _topn_per_group(items: list[dict], group_key: str, n: int, sort_key) -> list[dict]:
        from collections import defaultdict
        buckets = defaultdict(list)
        for it in items:
            buckets[it.get(group_key, "?")].append(it)
        out: list[dict] = []
        for grp, items_ in buckets.items():
            items_.sort(key=sort_key, reverse=True)
            out.extend(items_[:n])
        out.sort(key=sort_key, reverse=True)
        return out

    top_v = _topn_per_group(videos, "platform", per_video_source, _video_key)
    top_t = _topn_per_group(tools, "source", per_tool_source, _tool_key)

    # 把所有 http(s) 的 cover URL 下载到本地（避开 CDN referer/expire 问题）
    _localize_covers(top_v, out_dir=Path(out_dir))

    week = week_code()
    summary = _generate_overview(top_v, top_t, doubao_client, doubao_model)
    md = _render_markdown(week, top_v, top_t, summary)
    json_blob = _render_json(week, top_v, top_t, summary)

    # 写文件
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    (out / f"weekly_{week}.md").write_text(md, encoding="utf-8")
    (out / f"weekly_{week}.json").write_text(
        json.dumps(json_blob, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (out / "latest.json").write_text(
        json.dumps(json_blob, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # 写 reports 表
    db.save_report(week=week, top_videos=top_v, top_tools=top_t, summary=summary)

    return {"week": week, "markdown": md, "json": json_blob,
            "video_count": len(top_v), "tool_count": len(top_t)}


def _localize_covers(videos: list[dict], out_dir: Path) -> None:
    """把所有 cover_url 下载到 covers/<platform>/<id>.jpg 本地化。

    避开 CDN referer 校验 + 时效 URL 过期问题。
    家用机已经下载过的 xhs cover 不会重新下载（cover_url 已是相对路径）。
    """
    import urllib.request
    import urllib.error
    import ssl
    import hashlib

    covers_root = out_dir.parent / "covers"   # data/../covers
    covers_root.mkdir(exist_ok=True)
    ctx_ssl = ssl.create_default_context()
    ctx_ssl.check_hostname = False
    ctx_ssl.verify_mode = ssl.CERT_NONE

    n_localized = 0
    n_skipped = 0
    n_failed = 0

    for v in videos:
        url = v.get("cover_url", "")
        if not url or not url.startswith("http"):
            n_skipped += 1   # 已经是本地路径或空
            continue
        plat = v.get("platform", "other")
        # 用 url hash 当文件名，简单稳定
        ext = ".webp" if "webp" in url.lower() else ".jpg"
        fname = hashlib.md5(url.encode()).hexdigest()[:16] + ext
        plat_dir = covers_root / plat
        plat_dir.mkdir(exist_ok=True)
        local_path = plat_dir / fname
        if not local_path.exists():
            try:
                req = urllib.request.Request(url, headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                    # 不发 referer（因为各 CDN 偏好都是无 referer 时放行，有 referer 反而被卡）
                })
                with urllib.request.urlopen(req, timeout=10, context=ctx_ssl) as resp:
                    local_path.write_bytes(resp.read())
                n_localized += 1
            except Exception as e:
                n_failed += 1
                continue
        v["cover_url"] = f"covers/{plat}/{fname}"
    print(f"[report] covers: localized={n_localized} skipped={n_skipped} failed={n_failed}")


def _generate_overview(videos: list[dict], tools: list[dict],
                        client: OpenAI, model: str) -> str:
    """豆包生成一段中文综合观察。"""
    video_gist = [
        f"- {v['platform']}「{v.get('title','')[:40]}」钩子={v.get('hook','')} 评分{v.get('score',0)}"
        for v in videos[:6]
    ]
    tool_gist = [
        f"- {t['source']}「{t.get('name','')[:40]}」适用 stage={t.get('stage_tags','')} 评分{t.get('score',0)}"
        for t in tools[:6]
    ]
    prompt = f"""下面是过去 7 天 AI 短剧圈的爆款内容和工具发现。请用 120-180 字的中文写一段"本周重点"，
指出 1-2 个值得重点学习的爆款选题/打法、1-2 个值得加入流水线的工具/模型，语气直接、
避免套话。不要列表，写一段连贯文字。

## 爆款视频样本
{chr(10).join(video_gist) or '(无)'}

## AI 工具样本
{chr(10).join(tool_gist) or '(无)'}
"""
    try:
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=500,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        return f"(豆包 overview 生成失败: {e})"


def _render_markdown(week: str, videos: list[dict], tools: list[dict],
                      summary: str) -> str:
    lines = [
        f"# AI 短剧情报周报 {week}",
        f"_{datetime.now().strftime('%Y-%m-%d %H:%M')}_",
        "",
        "## 📝 本周重点",
        "",
        summary,
        "",
        "## 📺 爆款短视频 TOP",
        "",
    ]
    for i, v in enumerate(videos, 1):
        emoji = PLATFORM_EMOJI.get(v["platform"], "📺")
        plays = v.get("plays") or 0
        score = v.get("score") or 0
        tags = _json_list(v.get("style_tags"))
        lines += [
            f"### {i}. {emoji} [{v.get('title','(无标题)')}]({v['url']})",
            f"**{v.get('author','')}** · 播放 {plays:,} · 豆包评分 **{score:.1f}**",
            "",
            f"- 🎯 **钩子**：{v.get('hook') or '（未分析）'}",
            f"- 🎬 **结构**：{v.get('structure') or '—'}",
            f"- 🏷️ **标签**：{' / '.join(tags) if tags else '—'}",
            "",
        ]
    lines += [
        "",
        "## 🛠 AI 工具/模型",
        "",
    ]
    for i, t in enumerate(tools, 1):
        emoji = PLATFORM_EMOJI.get(t["source"], "🛠")
        metric = t.get("metric") or 0
        score = t.get("score") or 0
        stage_tags = _json_list(t.get("stage_tags"))
        stage_cn = " / ".join(STAGE_LABEL.get(s, s) for s in stage_tags) if stage_tags else "—"
        lines += [
            f"### {i}. {emoji} [{t.get('name','')}]({t['url']})",
            f"**{t['source']}** · 热度 {metric:,} · 豆包评分 **{score:.1f}**",
            "",
            f"- {t.get('summary') or '—'}",
            f"- 💡 **适用流水线**：{stage_cn}",
            "",
        ]
    return "\n".join(lines)


def _render_json(week: str, videos: list[dict], tools: list[dict],
                  summary: str) -> dict:
    return {
        "week": week,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "summary": summary,
        "videos": [
            {
                "id": v["id"], "platform": v["platform"],
                "url": v["url"], "title": v.get("title", ""), "author": v.get("author", ""),
                "plays": v.get("plays", 0), "likes": v.get("likes", 0),
                "duration_sec": v.get("duration_sec"),
                "cover_url": v.get("cover_url", ""),
                "hook": v.get("hook") or "",
                "structure": v.get("structure") or "",
                "style_tags": _json_list(v.get("style_tags")),
                "score": v.get("score"),
                "relevance": v.get("relevance"),
                "quality": v.get("quality"),
                "actionable": v.get("actionable"),
                "verdict": v.get("verdict"),
                "reason": v.get("reason"),
            }
            for v in videos
        ],
        "tools": [
            {
                "id": t["id"], "source": t["source"],
                "url": t["url"], "name": t.get("name", ""),
                "description": t.get("description", ""),
                "metric": t.get("metric", 0),
                "summary": t.get("summary") or "",
                "stage_tags": _json_list(t.get("stage_tags")),
                "score": t.get("score"),
                "relevance": t.get("relevance"),
                "quality": t.get("quality"),
                "actionable": t.get("actionable"),
                "verdict": t.get("verdict"),
                "reason": t.get("reason"),
            }
            for t in tools
        ],
    }


def _json_list(s) -> list[str]:
    if isinstance(s, list): return s
    if not s: return []
    try:
        out = json.loads(s)
        return out if isinstance(out, list) else []
    except Exception:
        return []
