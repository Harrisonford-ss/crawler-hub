"""短视频豆包多维度多模态分析器。

输入：VideoRow + 封面 cover_url
输出：hook / structure / style_tags / relevance / quality / actionable / verdict / reason

硬门槛：relevance < 5 → verdict = C → 不进周报
"""

from __future__ import annotations

import json
import re

from openai import OpenAI

from storage.db import VideoRow


SYSTEM_PROMPT = """你是 AI 短剧研究员，帮 Eliza 筛选值得学习的爆款短视频。
用户给你一条视频的**封面图 + 标题 + 元数据**，你要严格判断。

## 三个独立维度（0-10 分）

**relevance（AI 短剧相关度）**——这条视频到底跟 AI 短剧有多大关系？
- 10：**明显**是 AI 生成的短剧/AI 动画/AI 视频作品
- 7-9：AI 制作工具教程、AI 创作流程分享
- 4-6：沾边（标题有 AI 但内容其实是真人拍摄 / 讨论性话题）
- 1-3：只是#ai 标签吸流量，实际是政治梗/搞笑段子/搬运
- 0：完全无关（广告、带货、娱乐花边）

**quality（内容质量）**——画面/叙事/节奏做得怎么样？
- 10：电影级制作、视觉惊艳、节奏精准
- 7-9：制作扎实，创意不错
- 4-6：中等水准，能看
- 1-3：粗糙、抄袭、低质剪辑
- 0：劣质视频

**actionable（可模仿度）**——Eliza 能从中**学到什么具体的东西**？
- 10：钩子/结构/特效能**直接**复用到她的流水线
- 7-9：给出清晰的拍法模板可以借鉴
- 4-6：有启发但要自己翻译成行动
- 1-3：看了就看了，没法复用
- 0：纯流量玩法，跟 AI 短剧无关

## verdict 分级
- **S**：relevance≥8 且 quality≥7 且 actionable≥7 → 必看
- **A**：relevance≥7 且 (quality+actionable)/2 ≥6.5 → 值得研究
- **B**：relevance≥5 → 参考
- **C**：relevance<5 或 quality<3 → 淘汰（不进周报）

## 返回格式（严格 JSON，不要 ``` 包裹）
{
  "hook": "≤20 字钩子拆解",
  "structure": "≤30 字叙事结构",
  "style_tags": ["3-5 个短标签"],
  "relevance": 0-10,
  "quality": 0-10,
  "actionable": 0-10,
  "verdict": "S|A|B|C",
  "reason": "一句话说为啥"
}
"""


def analyze_video(
    row: VideoRow,
    *,
    client: OpenAI,
    model: str,
) -> dict:
    """单条视频打分。有封面走多模态，否则退化纯文本。"""
    has_cover = bool(row.cover_url)
    user_msg_text = (
        f"平台：{row.platform}\n"
        f"标题：{row.title or '(无)'}\n"
        f"作者：{row.author}\n"
        f"播放：{row.plays} | 点赞：{row.likes} | 时长：{row.duration_sec or '?'}s"
    )

    if has_cover:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": row.cover_url}},
                {"type": "text", "text": user_msg_text},
            ]},
        ]
    else:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"{user_msg_text}\n（无封面，仅凭元数据判断）"},
        ]

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.3,
            max_tokens=500,
        )
    except Exception:
        # 某些图片 URL 会 403（抖音 CDN referer 校验）→ 降级纯文本再试
        if has_cover:
            return analyze_video(
                VideoRow(**{**row.__dict__, "cover_url": ""}),
                client=client, model=model,
            )
        return _default()

    return _parse((resp.choices[0].message.content or "").strip())


def _default() -> dict:
    return {
        "hook": "", "structure": "", "style_tags": [],
        "relevance": 0.0, "quality": 0.0, "actionable": 0.0,
        "verdict": "C", "reason": "analyze failed",
    }


def _parse(content: str) -> dict:
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return _default()
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return _default()

    def _clip(v, lo=0.0, hi=10.0):
        try:
            return max(lo, min(hi, float(v)))
        except (TypeError, ValueError):
            return 0.0

    verdict = str(obj.get("verdict", "C")).upper()
    if verdict not in ("S", "A", "B", "C"):
        verdict = "C"

    tags = obj.get("style_tags") or []
    if not isinstance(tags, list):
        tags = []
    tags = [t for t in tags if isinstance(t, str)][:6]

    return {
        "hook": str(obj.get("hook", ""))[:60],
        "structure": str(obj.get("structure", ""))[:100],
        "style_tags": tags,
        "relevance": _clip(obj.get("relevance", 0)),
        "quality": _clip(obj.get("quality", 0)),
        "actionable": _clip(obj.get("actionable", 0)),
        "verdict": verdict,
        "reason": str(obj.get("reason", ""))[:120],
    }
