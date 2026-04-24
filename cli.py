"""crawler-hub 主入口：按阶段可分步运行。

用法：
    python cli.py collect              # 只跑采集
    python cli.py analyze              # 只跑豆包分析（需已采集）
    python cli.py report               # 只生成周报
    python cli.py push                 # 推送周报到 Server酱
    python cli.py publish              # git push 数据 repo 触发 Zeabur 部署
    python cli.py all                  # 完整流水线：采集 → 分析 → 周报 → 推送 → 发布

所有阶段都从 config.yaml 读配置。
"""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
from pathlib import Path

import yaml
from openai import OpenAI

from analyzers.tool_analyzer import analyze_tool
from analyzers.video_analyzer import analyze_video
from notifier.server_chan import push as sct_push
from reporter.weekly import generate as generate_weekly
from sources import arxiv, bilibili, douyin, huggingface, modelscope, youtube_shorts
from storage.db import Db, ToolRow, VideoRow


# -------- config --------

def load_config(path: str | Path = "config.yaml") -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def get_doubao(cfg: dict) -> tuple[OpenAI, str, str]:
    d = cfg["doubao"]
    client = OpenAI(api_key=d["api_key"], base_url=d["base_url"])
    return client, d["text_model"], d["vision_model"]


# -------- collect --------

ALL_SOURCES = ["huggingface", "modelscope", "bilibili", "bilibili_video", "youtube_shorts", "douyin", "arxiv"]


def run_collect(cfg: dict, db: Db) -> dict:
    """从启用的源拉数据入库。通过 runtime.enabled_sources 控制哪些源启用。"""
    stats: dict[str, int] = {}
    enabled = set((cfg.get("runtime") or {}).get("enabled_sources") or ALL_SOURCES)
    print(f"[collect] enabled sources: {sorted(enabled)}")

    if "huggingface" in enabled:
        try:
            hf_cfg = cfg.get("huggingface") or {}
            hf_rows = huggingface.fetch_trending(
                token=hf_cfg.get("token") or None,
                limit=hf_cfg.get("trending_limit", 30),
                filter_tags=hf_cfg.get("filter_tags"),
            )
            stats["huggingface"] = db.upsert_tools(hf_rows)
            print(f"[collect] huggingface: {len(hf_rows)} items")
        except Exception as e:
            print(f"[collect] huggingface FAILED: {e}")

    if "modelscope" in enabled:
        try:
            ms_cfg = cfg.get("modelscope") or {}
            ms_rows = modelscope.fetch_trending(
                limit=ms_cfg.get("trending_limit", 40),
                filter_tasks=ms_cfg.get("filter_tasks"),
            )
            stats["modelscope"] = db.upsert_tools(ms_rows)
            print(f"[collect] modelscope: {len(ms_rows)} items")
        except Exception as e:
            print(f"[collect] modelscope FAILED: {e}")

    if "bilibili" in enabled:
        try:
            bi_cfg = cfg.get("bilibili") or {}
            bi_rows = bilibili.fetch_keywords(
                bi_cfg.get("keywords") or [],
                per_keyword=bi_cfg.get("per_keyword", 6),
            )
            stats["bilibili"] = db.upsert_tools(bi_rows)
            print(f"[collect] bilibili: {len(bi_rows)} items")
        except Exception as e:
            print(f"[collect] bilibili FAILED: {e}")

    if "bilibili_video" in enabled:
        try:
            biv_cfg = cfg.get("bilibili_video") or {}
            biv_rows = bilibili.fetch_video_keywords(
                biv_cfg.get("keywords") or [],
                per_keyword=biv_cfg.get("per_keyword", 8),
            )
            stats["bilibili_video"] = db.upsert_videos(biv_rows)
            print(f"[collect] bilibili_video: {len(biv_rows)} videos")
        except Exception as e:
            print(f"[collect] bilibili_video FAILED: {e}")

    if "arxiv" in enabled:
        try:
            ax_cfg = cfg.get("arxiv") or {}
            ax_rows = arxiv.fetch_recent(
                queries=ax_cfg.get("queries"),
                max_per_query=ax_cfg.get("max_per_query", 15),
            )
            stats["arxiv"] = db.upsert_tools(ax_rows)
            print(f"[collect] arxiv: {len(ax_rows)} papers")
        except Exception as e:
            print(f"[collect] arxiv FAILED: {e}")

    if "youtube_shorts" in enabled:
        try:
            yt_cfg = cfg.get("youtube") or {}
            if yt_cfg.get("api_key"):
                yt_rows = youtube_shorts.fetch_shorts(
                    api_key=yt_cfg["api_key"],
                    query=yt_cfg.get("query", "AI short film | AI video"),
                    region_code=yt_cfg.get("region_code", "US"),
                    max_results=yt_cfg.get("max_results", 30),
                )
                stats["youtube_shorts"] = db.upsert_videos(yt_rows)
                print(f"[collect] youtube_shorts: {len(yt_rows)} videos")
        except Exception as e:
            print(f"[collect] youtube_shorts FAILED: {e}")

    if "douyin" in enabled:
        try:
            dy_cfg = cfg.get("douyin") or {}
            sec_ids = dy_cfg.get("sec_user_ids") or []
            if sec_ids and Path(dy_cfg.get("cookie_file", "")).exists():
                cookie = Path(dy_cfg["cookie_file"]).read_text().strip()
                douyin.update_cookie(dy_cfg["api_base"], cookie)
                dy_rows = douyin.fetch_from_accounts(
                    api_base=dy_cfg["api_base"],
                    sec_user_ids=sec_ids,
                    count_per_user=dy_cfg.get("count_per_user", 10),
                )
                stats["douyin"] = db.upsert_videos(dy_rows)
                print(f"[collect] douyin: {len(dy_rows)} videos from {len(sec_ids)} accounts")
            else:
                print(f"[collect] douyin: SKIPPED (no sec_user_ids or cookie file)")
        except Exception as e:
            print(f"[collect] douyin FAILED: {e}")

    return stats


# -------- analyze --------

def run_analyze(cfg: dict, db: Db, *, max_workers: int = 6) -> dict:
    """豆包分析过去 7 天未打分的 videos/tools。"""
    from datetime import datetime, timedelta, timezone
    client, text_model, vision_model = get_doubao(cfg)
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(timespec="seconds")

    # -- videos --
    videos = [v for v in db.fresh_videos(since) if v.get("score") is None]
    print(f"[analyze] {len(videos)} videos to score")
    def _score_video(v: dict) -> None:
        row = VideoRow(
            id=v["id"], platform=v["platform"], url=v["url"],
            title=v.get("title", ""), author=v.get("author", ""),
            plays=v.get("plays") or 0, likes=v.get("likes") or 0,
            duration_sec=v.get("duration_sec"),
            cover_url=v.get("cover_url", "") or "",
        )
        try:
            hook, structure, tags, score = analyze_video(row, client=client, model=vision_model)
            db.update_video_analysis(row.id, hook=hook, structure=structure,
                                     style_tags=tags, score=score)
        except Exception as e:
            print(f"[analyze] video {row.id} FAILED: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_score_video, videos))

    # -- tools --
    tools = [t for t in db.fresh_tools(since) if t.get("score") is None]
    print(f"[analyze] {len(tools)} tools to score")
    def _score_tool(t: dict) -> None:
        row = ToolRow(
            id=t["id"], source=t["source"], url=t["url"],
            name=t.get("name", ""), description=t.get("description", ""),
            metric=t.get("metric") or 0,
        )
        try:
            summary, tags, score = analyze_tool(row, client=client, model=text_model)
            db.update_tool_analysis(row.id, summary=summary, stage_tags=tags, score=score)
        except Exception as e:
            print(f"[analyze] tool {row.id} FAILED: {e}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        list(pool.map(_score_tool, tools))

    return {"videos_scored": len(videos), "tools_scored": len(tools)}


# -------- report --------

def run_report(cfg: dict, db: Db) -> dict:
    client, text_model, _ = get_doubao(cfg)
    out_dir = (cfg.get("runtime") or {}).get("output_dir", "./data")
    return generate_weekly(db=db, doubao_client=client, doubao_model=text_model,
                           out_dir=out_dir)


# -------- push --------

def run_push(cfg: dict, db: Db) -> None:
    latest = db.latest_report()
    if not latest:
        print("[push] no report in DB"); return
    sct = cfg.get("server_chan") or {}
    if not sct.get("sct_key"):
        print("[push] no sct_key"); return
    title = f"AI 短剧情报周报 {latest['week']}"
    desp = _desp_from_report(latest)
    sct_push(sct_key=sct["sct_key"], title=title, desp=desp,
             channel=sct.get("channel"))
    print(f"[push] ✓ pushed {latest['week']}")


def _desp_from_report(report: dict) -> str:
    import json as _json
    videos = _json.loads(report.get("top_videos") or "[]")
    tools = _json.loads(report.get("top_tools") or "[]")
    lines = [
        f"## 本周重点\n\n{report.get('summary', '')}\n",
        "## 📺 爆款短视频 TOP 5\n",
    ]
    for i, v in enumerate(videos[:5], 1):
        lines.append(f"{i}. [{v.get('title','')}]({v.get('url')}) "
                     f"评分 **{v.get('score', 0):.1f}** | 钩子：{v.get('hook','')}")
    lines.append("\n## 🛠 AI 工具 TOP 5\n")
    for i, t in enumerate(tools[:5], 1):
        lines.append(f"{i}. [{t.get('name','')}]({t.get('url')}) "
                     f"评分 **{t.get('score', 0):.1f}** | {t.get('summary','')}")
    lines.append(f"\n\n> 完整周报见 web dashboard")
    return "\n".join(lines)


# -------- publish --------

def run_publish(cfg: dict) -> None:
    """把最新 weekly JSON 推到 crawler-hub-data repo，触发 Zeabur 自动构建。

    策略：克隆 → 覆盖 data/ 目录下的 latest.json + 本周 JSON → commit → push
    """
    import subprocess
    import shutil
    import tempfile
    from pathlib import Path

    gh = cfg.get("github") or {}
    token = gh.get("token")
    repo = gh.get("data_repo")
    branch = gh.get("data_branch", "main")
    if not token or not repo:
        print("[publish] github.token / github.data_repo 未配置，跳过")
        return

    out_dir = (cfg.get("runtime") or {}).get("output_dir", "./data")
    src_latest = Path(out_dir) / "latest.json"
    if not src_latest.exists():
        print(f"[publish] {src_latest} 不存在，跳过")
        return

    with tempfile.TemporaryDirectory() as tmp:
        repo_url = f"https://x-access-token:{token}@github.com/{repo}.git"
        r = subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", branch, repo_url, tmp],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            print(f"[publish] clone failed: {r.stderr[:200]}"); return

        data_dst = Path(tmp) / "data"
        data_dst.mkdir(exist_ok=True)
        shutil.copy(src_latest, data_dst / "latest.json")
        # 同时把本周的 YYYY-WXX.json 也带上
        for f in Path(out_dir).glob("weekly_*.json"):
            shutil.copy(f, data_dst / f.name)

        # commit only if there are diffs
        subprocess.run(["git", "-C", tmp, "add", "data/"], check=True)
        diff = subprocess.run(["git", "-C", tmp, "diff", "--cached", "--quiet"],
                               capture_output=True)
        if diff.returncode == 0:
            print("[publish] nothing to commit (data unchanged)")
            return

        from datetime import datetime
        msg = f"chore: update weekly data {datetime.now().strftime('%Y-%m-%d')}"
        subprocess.run(["git", "-C", tmp, "-c", "user.name=crawler-hub bot",
                       "-c", "user.email=crawler@eliza666.com",
                       "commit", "-m", msg], check=True)
        r = subprocess.run(["git", "-C", tmp, "push", "origin", branch],
                            capture_output=True, text=True)
        if r.returncode != 0:
            print(f"[publish] push failed: {r.stderr[:200]}"); return
        print(f"[publish] ✓ pushed to {repo}@{branch}")


# -------- main --------

def main() -> int:
    parser = argparse.ArgumentParser(prog="crawler-hub")
    parser.add_argument("stage", choices=[
        "collect", "analyze", "report", "push", "publish", "all",
    ])
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)
    db_path = (cfg.get("runtime") or {}).get("output_dir", "./data") + "/crawler.db"
    db = Db(db_path)

    if args.stage in ("collect", "all"):
        run_collect(cfg, db)
    if args.stage in ("analyze", "all"):
        run_analyze(cfg, db)
    if args.stage in ("report", "all"):
        run_report(cfg, db)
    if args.stage in ("push", "all"):
        run_push(cfg, db)
    if args.stage in ("publish", "all"):
        run_publish(cfg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
