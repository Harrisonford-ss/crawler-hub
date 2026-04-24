"""ModelScope（魔搭社区）采集器 —— 国内版 HuggingFace。

API：PUT https://www.modelscope.cn/api/v1/dolphin/models
Body: {"PageSize":..., "PageNumber":..., "SortBy":..., "Criterion":[...]}
Response: {Code, Data: {Model: {Models: [...], TotalCount}}}

没有 token 也能调公开接口。SortBy 可选：Default / Downloads / Stars / GmtRevised / ...
"""

from __future__ import annotations

from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from storage.db import ToolRow


API_URL = "https://www.modelscope.cn/api/v1/dolphin/models"

# ModelScope task name → 我们流水线的 stage_tags
TASK_TO_STAGE = {
    "text-to-video-synthesis": ["videogen"],
    "image-to-video-synthesis": ["videogen"],
    "text-to-image-synthesis": ["keyframe", "ref_image"],
    "image-to-image": ["keyframe"],
    "controllable-image-generation": ["keyframe"],
    "image-text-to-text": ["keyframe"],
    "text-to-speech": ["tts"],
    "auto-speech-recognition": ["tts"],
    "speech-synthesis": ["tts"],
    "text-to-audio": ["bgm", "sfx"],
    "audio-to-audio": ["sfx"],
    "face-detection": ["ref_image"],
    "face-fusion": ["lip_sync"],
    "image-face-swap": ["lip_sync"],
    "video-to-video": ["videogen"],
    "visual-question-answering": ["quality"],
    "text-generation": ["script"],
    "conversational": ["script"],
}


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(3), reraise=True)
def _request(client: httpx.Client, page_no: int, sort_by: str, criterion: list[dict]) -> dict:
    body = {
        "PageSize": 50,
        "PageNumber": page_no,
        "SortBy": sort_by,
        "Target": "",
        "Criterion": criterion,
        "CustomFilter": {},
        "Name": "",
    }
    r = client.put(API_URL, json=body, timeout=30.0)
    r.raise_for_status()
    return r.json()


def fetch_trending(
    *,
    limit: int = 40,
    filter_tasks: list[str] | None = None,
) -> list[ToolRow]:
    """拉 ModelScope 热门模型。按下载量 + 星标两种维度各拉一批。

    filter_tasks: ModelScope 任务名（如 text-to-video-synthesis）。
    """
    rows: list[ToolRow] = []
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0 crawler-hub"}) as client:
        # Default（官方推荐榜，基本是综合热度）+ GmtModified（最近更新）两种视角
        for sort_by in ("Default", "GmtModified"):
            page = 1
            fetched = 0
            while fetched < limit:
                try:
                    data = _request(client, page, sort_by, criterion=[])
                except Exception as e:
                    print(f"[modelscope] {sort_by} page={page} error: {e}")
                    break
                models = (((data.get("Data") or {}).get("Model") or {}).get("Models") or [])
                if not models:
                    break
                for m in models:
                    row = _model_to_row(m)
                    if row:
                        # 任务过滤：如果指定了 filter_tasks，只保留命中的
                        if filter_tasks:
                            ms_tasks = [t.get("Name", "") for t in (m.get("Tasks") or [])]
                            if not any(t in filter_tasks for t in ms_tasks):
                                continue
                        rows.append(row)
                        fetched += 1
                page += 1
                if page > 5:   # safety cap
                    break

    # 按 id 去重（两种排序可能拉到同一个）
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen or (r.metric or 0) > (seen[r.id].metric or 0):
            seen[r.id] = r
    return list(seen.values())


def _model_to_row(m: dict) -> ToolRow | None:
    path = m.get("Path", "")   # namespace e.g. "moonshotai"
    name = m.get("Name", "")   # model e.g. "Kimi-K2.6"
    if not name:
        return None
    full_id = f"{path}/{name}" if path else name

    # task → stage_tags
    tasks = m.get("Tasks") or []
    task_names = [t.get("Name", "") for t in tasks]
    stage_tags: set[str] = set()
    for tn in task_names:
        for s in TASK_TO_STAGE.get(tn, []):
            stage_tags.add(s)

    # description：优先中文 chinese_name，其次 description，再次 readme 摘要
    desc = (m.get("ChineseName") or m.get("Description") or "").strip()
    if not desc:
        # 用任务名做 fallback 描述
        cn_tasks = [t.get("ChineseName") for t in tasks if t.get("ChineseName")]
        desc = "、".join(cn_tasks[:2]) if cn_tasks else "model"

    return ToolRow(
        id=f"ms_{full_id}".replace("/", "_"),
        source="modelscope",
        url=f"https://www.modelscope.cn/models/{full_id}",
        name=full_id,
        description=desc[:200],
        metric=int(m.get("Downloads", 0) or 0),
        publish_time=str(m.get("GmtModified", "") or ""),
        raw={
            "path": path, "name": name, "full_id": full_id,
            "stars": m.get("Stars"),
            "downloads": m.get("Downloads"),
            "tasks": task_names,
            "tags": (m.get("Tags") or [])[:20],
            "chinese_name": m.get("ChineseName"),
        },
        stage_tags=sorted(stage_tags),
    )
