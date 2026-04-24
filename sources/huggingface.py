"""HuggingFace trending 采集器：拿模型 + 数据集 + spaces 的 trending。

不需要 token；如果配置了 token 限速更宽松。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from huggingface_hub import HfApi

from storage.db import ToolRow


# 与本流水线的 stage 标签关联：HF tag → 我们的 stage_tag
HF_TAG_TO_STAGE = {
    "text-to-video": "videogen",
    "image-to-video": "videogen",
    "video-to-video": "videogen",
    "text-to-image": "keyframe",
    "image-to-image": "keyframe",
    "text-to-speech": "tts",
    "automatic-speech-recognition": "tts",
    "audio-to-audio": "sfx",
    "text-to-audio": "bgm",
    "music-generation": "bgm",
    "image-to-3d": "ref_image",
    "lip-sync": "lip_sync",
}


def _hf_tags_to_stages(tags: list[str]) -> list[str]:
    stages: set[str] = set()
    for t in tags:
        s = HF_TAG_TO_STAGE.get(t.lower())
        if s:
            stages.add(s)
    return sorted(stages)


def _model_id(model: Any) -> str:
    return f"hf_model_{model.id}".replace("/", "_")


def _ds_id(ds: Any) -> str:
    return f"hf_ds_{ds.id}".replace("/", "_")


def _space_id(sp: Any) -> str:
    return f"hf_space_{sp.id}".replace("/", "_")


def fetch_trending(
    *,
    token: str | None = None,
    limit: int = 30,
    filter_tags: list[str] | None = None,
) -> list[ToolRow]:
    """抓取 trending models + datasets + spaces，按下载量排序。

    filter_tags 是 HF pipeline_tag，比如 text-to-video / text-to-image / tts。
    设置后只保留命中任一 tag 的模型；不设则不过滤。
    """
    api = HfApi(token=token or None)
    rows: list[ToolRow] = []

    # --- models ---
    if filter_tags:
        for pt in filter_tags:
            for m in api.list_models(
                pipeline_tag=pt, sort="downloads", limit=limit
            ):
                rows.append(_model_to_row(m))
    else:
        for m in api.list_models(sort="downloads", limit=limit):
            rows.append(_model_to_row(m))

    # --- spaces (按 likes 排，信号最强) ---
    try:
        for sp in api.list_spaces(sort="likes", limit=limit):
            rows.append(_space_to_row(sp))
    except Exception:
        # spaces API 偶尔会抖
        pass

    # --- datasets：只看 video / audio / image 这几个跟流水线相关的 ---
    for tag in ["video", "audio", "image-to-text"]:
        try:
            for ds in api.list_datasets(
                filter=tag, sort="downloads", limit=10
            ):
                rows.append(_ds_to_row(ds))
        except Exception:
            continue

    # 按 id 去重（同一模型可能在多个 tag 下出现）
    seen: dict[str, ToolRow] = {}
    for r in rows:
        if r.id not in seen or (r.metric or 0) > (seen[r.id].metric or 0):
            seen[r.id] = r
    return list(seen.values())


def _model_to_row(model: Any) -> ToolRow:
    tags = list(getattr(model, "tags", []) or [])
    stage_tags = _hf_tags_to_stages(tags + [getattr(model, "pipeline_tag", "") or ""])
    return ToolRow(
        id=_model_id(model),
        source="huggingface",
        url=f"https://huggingface.co/{model.id}",
        name=model.id,
        description=(getattr(model, "pipeline_tag", "") or "model").strip(),
        metric=int(getattr(model, "downloads", 0) or 0),
        publish_time=_iso(getattr(model, "last_modified", None)),
        raw={
            "id": model.id,
            "pipeline_tag": getattr(model, "pipeline_tag", None),
            "tags": tags[:30],
            "downloads": getattr(model, "downloads", 0),
            "likes": getattr(model, "likes", 0),
            "kind": "model",
        },
        stage_tags=stage_tags,
    )


def _space_to_row(sp: Any) -> ToolRow:
    return ToolRow(
        id=_space_id(sp),
        source="huggingface",
        url=f"https://huggingface.co/spaces/{sp.id}",
        name=sp.id,
        description="space",
        metric=int(getattr(sp, "likes", 0) or 0),
        publish_time=_iso(getattr(sp, "last_modified", None)),
        raw={
            "id": sp.id,
            "sdk": getattr(sp, "sdk", None),
            "likes": getattr(sp, "likes", 0),
            "tags": list(getattr(sp, "tags", []) or [])[:30],
            "kind": "space",
        },
        stage_tags=[],
    )


def _ds_to_row(ds: Any) -> ToolRow:
    return ToolRow(
        id=_ds_id(ds),
        source="huggingface",
        url=f"https://huggingface.co/datasets/{ds.id}",
        name=ds.id,
        description="dataset",
        metric=int(getattr(ds, "downloads", 0) or 0),
        publish_time=_iso(getattr(ds, "last_modified", None)),
        raw={
            "id": ds.id,
            "tags": list(getattr(ds, "tags", []) or [])[:30],
            "downloads": getattr(ds, "downloads", 0),
            "kind": "dataset",
        },
        stage_tags=[],
    )


def _iso(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(timespec="seconds")
    return str(value)
