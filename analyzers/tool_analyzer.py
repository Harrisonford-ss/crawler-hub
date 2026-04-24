"""AI 工具/模型豆包多维度审核器。

输入：ToolRow（HF/ModelScope/B站/arXiv/GitHub/News 的条目）
输出：relevance / quality / actionable / verdict / reason + summary + stage_tags

与之前单一 score 相比的改动：
- 硬门槛：relevance < 5 → verdict = C，直接淘汰
- 三个独立维度，reporter 可按需排序
- verdict 给出 S/A/B/C 分级（S 必看 / A 值得 / B 参考 / C 淘汰）
"""

from __future__ import annotations

import json
import re

from openai import OpenAI

from storage.db import ToolRow


SYSTEM_PROMPT = """你是 AI 短剧制作工作流的严格选型顾问。用户给你一条"AI 工具/模型/教程/论文/新闻"，
你要**严格**判断它对一条 **10 阶段中文短剧视频流水线**（world/script/ref_image/keyframe/
videogen/quality/tts/sfx/bgm/edit/lip_sync）有多大帮助。

## 打分规则（三个独立维度，0-10 分）

**relevance（相关度）**——这个东西**到底**跟 AI 短剧制作有没有关系？
- 10：核心就是 AI 短剧相关（即梦/Sora/可灵/视频生成模型/短剧工作流）
- 7-9：虽然不是专门做短剧，但**明确**可用于短剧某个环节（TTS / 图像生成 / 人物建模）
- 4-6：沾边，比如"通用 LLM"、"图像研究"——可能用得上但不直接
- 1-3：只是带了 #AI 标签但内容无关（如一般科技新闻、商业融资）
- 0：完全无关（股市、电商、政务）

**quality（内容质量）**——它本身做得好不好、信息靠不靠谱？
- 10：行业顶流（Sora、Flux、Kling2）/ 顶会论文 / 大牌教程
- 7-9：大厂或知名独立作者出品，内容扎实
- 4-6：普通水平、有用但不惊艳
- 1-3：水文、标题党、复制粘贴
- 0：虚假信息、过期链接、失效模型

**actionable（可操作度）**——Eliza 能**立刻**从中得到什么可用的东西？
- 10：能直接接进流水线替换现有模块 / 能马上复现的工作流
- 7-9：给出清晰步骤 / 可下载权重 / 可跑代码
- 4-6：有思路但要自己补细节
- 1-3：只是新闻/宏观分析，没法落地
- 0：纯广告 / 通用科普

## verdict 分级

- **S**：relevance≥8 且 quality≥8 且 actionable≥7  → 必看
- **A**：relevance≥7 且 (quality + actionable)/2 ≥7 → 值得研究
- **B**：relevance≥5 → 参考
- **C**：relevance<5 或 quality<3  → 淘汰（不进周报）

## 返回格式

严格 JSON，不要 ``` 包裹：
{
  "relevance": 0-10 浮点,
  "quality": 0-10 浮点,
  "actionable": 0-10 浮点,
  "verdict": "S" | "A" | "B" | "C",
  "summary": "20 字内精炼中文描述",
  "stage_tags": ["videogen", ...],
  "reason": "一句话解释为啥这个 verdict"
}

stage_tags 只能从这里选：world, script, ref_image, keyframe, videogen, quality, tts, sfx, bgm, edit, lip_sync
"""


def analyze_tool(
    row: ToolRow,
    *,
    client: OpenAI,
    model: str,
) -> dict:
    """豆包给单条工具打分。返回 dict 含 5 个维度字段。"""
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
        max_tokens=400,
    )
    content = (resp.choices[0].message.content or "").strip()
    return _parse(content)


def _parse(content: str) -> dict:
    """返回含默认值的 dict。"""
    default = {
        "relevance": 0.0, "quality": 0.0, "actionable": 0.0,
        "verdict": "C", "summary": "", "stage_tags": [], "reason": "parse failed",
    }
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return default
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return default

    def _clip(v, lo=0.0, hi=10.0):
        try:
            return max(lo, min(hi, float(v)))
        except (TypeError, ValueError):
            return 0.0

    verdict = str(obj.get("verdict", "C")).upper()
    if verdict not in ("S", "A", "B", "C"):
        verdict = "C"

    stage_tags = obj.get("stage_tags") or []
    if not isinstance(stage_tags, list):
        stage_tags = []
    stage_tags = [s for s in stage_tags if isinstance(s, str)][:5]

    return {
        "relevance": _clip(obj.get("relevance", 0)),
        "quality": _clip(obj.get("quality", 0)),
        "actionable": _clip(obj.get("actionable", 0)),
        "verdict": verdict,
        "summary": str(obj.get("summary", ""))[:100],
        "stage_tags": stage_tags,
        "reason": str(obj.get("reason", ""))[:120],
    }
