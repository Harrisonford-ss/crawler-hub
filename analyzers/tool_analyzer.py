"""AI 工具/模型豆包打分器：给 HF / B 站每条记录加 summary + stage_tags + score。

输入已爬的 ToolRow，输出分析结果字段。单条调用，批量在调用层并发。
"""

from __future__ import annotations

import json
import re

from openai import OpenAI

from storage.db import ToolRow


SYSTEM_PROMPT = """你是 AI 短剧制作工作流的选型顾问。用户给你一条"AI 工具/模型/教程"，
你用中文判断它对一条 **10 阶段中文短剧视频流水线** 有多大帮助，给出结构化结果。

10 个 stage 标签（只能从这里选，可多选可空）：
world, script, ref_image, keyframe, videogen, quality, tts, sfx, bgm, edit, lip_sync

评分规则（0-10）：
- 10：直接可插进流水线替换现有方案
- 7-9：本 stage 的 SOTA 或者明显比现有好
- 4-6：相关但不一定比现在好
- 1-3：只是入门教程 / 通用科普
- 0：跟流水线无关

返回 JSON：
{"summary": "20字内精炼中文描述", "stage_tags": ["videogen"], "score": 8.5}
"""


def _client(api_key: str, base_url: str) -> OpenAI:
    return OpenAI(api_key=api_key, base_url=base_url)


def analyze_tool(
    row: ToolRow,
    *,
    client: OpenAI,
    model: str,
) -> tuple[str, list[str], float]:
    """豆包给单条工具打分。返回 (summary, stage_tags, score)。"""
    user_msg = (
        f"来源：{row.source}\n"
        f"名称：{row.name}\n"
        f"描述：{row.description or '(无)'}\n"
        f"热度指标：{row.metric}\n"
        f"URL：{row.url}"
    )

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
        temperature=0.2,
        max_tokens=300,
    )
    content = (resp.choices[0].message.content or "").strip()
    return _parse(content)


def _parse(content: str) -> tuple[str, list[str], float]:
    # 豆包偶尔会把 JSON 包在 ```json ... ``` 里，先剥一下
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return content[:40], [], 0.0
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return content[:40], [], 0.0
    summary = str(obj.get("summary", ""))[:80]
    tags_raw = obj.get("stage_tags") or []
    tags = [t for t in tags_raw if isinstance(t, str)][:5]
    try:
        score = float(obj.get("score", 0.0))
    except (TypeError, ValueError):
        score = 0.0
    return summary, tags, max(0.0, min(10.0, score))
