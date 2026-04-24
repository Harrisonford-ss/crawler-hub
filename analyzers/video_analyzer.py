"""短视频豆包多模态分析：看封面 + 标题 + 元数据 → 拆钩子/节奏/风格。

注意：豆包的视频理解接口通常需要提交视频文件或 URL。抖音 CDN 带 referer 校验，
直接 URL 不稳。V1 降级方案：只给豆包看**封面图 + 标题 + 元数据**，其实短视频的钩子
80% 都在前 1 秒（封面往往就是开头画面），完全够用。V2 再上真正的视频帧采样。
"""

from __future__ import annotations

import json
import re

from openai import OpenAI

from storage.db import VideoRow


SYSTEM_PROMPT = """你是 AI 短视频研究员，帮用户拆解爆款短视频的可模仿元素。
用户给你一条视频的**封面图 + 标题 + 核心指标**。请你从封面和标题推断：
1. **hook**: 开场 3 秒钩子是什么？（疑问/反转/金句/视觉冲击/情绪，≤20 字）
2. **structure**: 整条视频的叙事结构猜测（如"悬念→揭秘→反转"、"痛点→方案→结果"，≤30 字）
3. **style_tags**: 3-5 个短标签描述风格（如"悬念开头 神反转 情绪炸裂 AI动画 真人演绎"）
4. **score**: 0-10 的模仿价值分
   - 10：结构清晰、钩子强、AI 短剧能完整复现
   - 7-9：值得重点学习
   - 4-6：一般
   - 1-3：无模仿价值

严格返回 JSON，不要解释：
{"hook": "...", "structure": "...", "style_tags": ["..."], "score": 8.0}
"""


def _client(api_key: str, base_url: str) -> OpenAI:
    return OpenAI(api_key=api_key, base_url=base_url)


def analyze_video(
    row: VideoRow,
    *,
    client: OpenAI,
    model: str,
) -> tuple[str, str, list[str], float]:
    """给单条视频打分/拆解。有封面用多模态，没封面退化到纯文本。

    返回 (hook, structure, style_tags, score)。
    """
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
            {"role": "user", "content": f"{user_msg_text}\n（无封面可看，仅凭标题和指标推断）"},
        ]

    try:
        resp = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.3,
            max_tokens=400,
        )
    except Exception as e:
        # 豆包对某些图片 URL 会 403（抖音 CDN 带 referer）——降级为纯文本再试
        if has_cover:
            return analyze_video(
                VideoRow(**{**row.__dict__, "cover_url": ""}),
                client=client, model=model,
            )
        return "", "", [], 0.0

    content = (resp.choices[0].message.content or "").strip()
    return _parse(content)


def _parse(content: str) -> tuple[str, str, list[str], float]:
    m = re.search(r"\{.*\}", content, re.S)
    if not m:
        return content[:40], "", [], 0.0
    try:
        obj = json.loads(m.group(0))
    except json.JSONDecodeError:
        return content[:40], "", [], 0.0
    hook = str(obj.get("hook", ""))[:40]
    structure = str(obj.get("structure", ""))[:80]
    tags_raw = obj.get("style_tags") or []
    tags = [t for t in tags_raw if isinstance(t, str)][:6]
    try:
        score = float(obj.get("score", 0.0))
    except (TypeError, ValueError):
        score = 0.0
    return hook, structure, tags, max(0.0, min(10.0, score))
