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
    top_videos: int = 10,
    top_tools: int = 15,
    out_dir: str | Path = "./data",
) -> dict:
    """取 7 天数据 → 生成 markdown + JSON，保存到 data/，并写入 reports 表。"""
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat(timespec="seconds")
    videos = db.fresh_videos(since)
    tools = db.fresh_tools(since)

    # 排序：视频按 豆包分数 * log(播放量)；工具按 豆包分数 * log(metric)
    import math
    def _video_key(v: dict) -> float:
        score = v.get("score") or 0
        plays = max(1, v.get("plays") or 1)
        return score * math.log10(plays)

    def _tool_key(t: dict) -> float:
        score = t.get("score") or 0
        metric = max(1, t.get("metric") or 1)
        return score * math.log10(metric)

    videos.sort(key=_video_key, reverse=True)
    tools.sort(key=_tool_key, reverse=True)

    top_v = videos[:top_videos]
    top_t = tools[:top_tools]

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
