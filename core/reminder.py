"""Roamer 的 system reminder 写入层（进通道，非注入非 Tool）。

设计定位（用户否决了注入与 Tool 后的第三条路）：

框架为「插件 → chatter 传递上下文」预留的**官方机制**是 system reminder：
chatter 构造 LLM 请求时通过 ``with_reminder`` / ``ReminderSourceSpec`` 自动
拾取全局桶与流私有桶（``create_request`` / ``LLMContextManager``），插件只
负责往桶里写内容，**不感知、不耦合任何 chatter 的模板与 prompt 组装**——
NDFC（with_reminder="actor"）、NFC（ReminderSourceSpec bucket="actor"）、
DFC 全部走同一通道，天然全聊天器通用。

本模块维护一条**流私有、单次消费**的「跨群动态 reminder」：

- 写入目标：``stream:{目标流}:actor`` 桶，仅该流可见；
- ``consume=once``：被拾取一次后自动焚毁——每轮都新鲜，不残留不重复；
- ``insert_type=dynamic``：不破坏 NFC 等聊天器苦心维护的 prompt 前缀缓存。
"""

from __future__ import annotations

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api.prompt_api import add_stream_reminder

logger = get_logger("roamer.reminder")

#: reminder 名称（流私有桶内唯一）
_REMINDER_NAME = "roamer_cross_stream"
#: reminder 归属桶（固定取 actor：三家 chatter 都拾取它）
_BUCKET = "actor"

_store_ref = None  # 供测试桩注入


async def set_cross_stream_reminder(stream_id: str, content: str) -> bool:
    """为指定流写下一条「跨群动态」一次性 reminder。

    幂等覆盖语义：多次写入同一流只保留最新内容（同名覆盖）。

    Args:
        stream_id: 目标聊天流。
        content: 跨群动态原文块；空串时跳过写入。

    Returns:
        bool: 实际写入了 reminder 返回 True。
    """
    if not content.strip():
        return False
    try:
        add_stream_reminder(
            stream_id=stream_id,
            bucket=_BUCKET,
            name=_REMINDER_NAME,
            content=content,
            insert_type="dynamic",
            consume="once",
        )
        logger.debug(f"跨群 reminder 已写入 stream={stream_id[:8]} ({len(content)} 字)")
        return True
    except Exception as error:  # noqa: BLE001
        logger.warning(f"写入跨群 reminder 失败: {error}")
        return False


def clear_cross_stream_reminder(stream_id: str) -> None:
    """清除指定流的跨群 reminder（resume 失败/插件停用时兜底）。"""
    try:
        from src.app.plugin_system.api.prompt_api import delete_stream_reminder

        delete_stream_reminder(stream_id, _BUCKET, _REMINDER_NAME)
    except Exception:  # noqa: BLE001
        pass
