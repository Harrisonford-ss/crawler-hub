"""Server酱 turbo 微信推送。

文档：https://sct.ftqq.com/sendkey
URL：https://sctapi.ftqq.com/<SendKey>.send
参数：title（必填，<32 字符）、desp（markdown 正文，<32KB）、channel（可选）
"""

from __future__ import annotations

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class ServerChanError(RuntimeError):
    pass


@retry(wait=wait_exponential(multiplier=1, min=2, max=10),
       stop=stop_after_attempt(3), reraise=True)
def push(
    *,
    sct_key: str,
    title: str,
    desp: str,
    channel: str | None = None,
) -> dict:
    """向 Server酱 turbo 推送一条消息。返回 API 响应 JSON。"""
    if not sct_key:
        raise ServerChanError("sct_key 为空")
    if len(title) > 32:
        # Server酱 对 title 有硬限制，超了会直接丢
        title = title[:29] + "..."

    url = f"https://sctapi.ftqq.com/{sct_key}.send"
    data = {"title": title, "desp": desp}
    if channel:
        data["channel"] = channel

    r = httpx.post(url, data=data, timeout=15.0)
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0:
        raise ServerChanError(f"Server酱推送失败: {j}")
    return j


def test_push(sct_key: str) -> bool:
    """冒烟测试：推一条最短消息。"""
    try:
        push(sct_key=sct_key,
             title="crawler-hub 测试",
             desp="这是 crawler-hub 的测试推送，收到说明通道正常。")
        return True
    except Exception as e:
        print(f"[server_chan] test failed: {e}")
        return False
