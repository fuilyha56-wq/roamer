"""Roamer 跨群原文搬运（消息 + 行为）。

设计目标（用户需求原话）：「框架怎么构建上下文，使用什么工具，就原样搬运」。

搬运两类内容，均零转写：

1. **消息原文**：镜像库中的原始消息（发送者/内容逐字保留）；
2. **行为流水**：Bot 在其他流的工具/动作调用（工具名 + 参数 + 结果摘要）——
   记录「它做了什么」，与框架 ``AFTER_TOOL_CALL`` / ``AFTER_ACTION_CALL``
   事件携带的信息完全一致。

注入通道：流私有 system reminder（``reminder.py``），全聊天器通用。
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Callable

from src.app.plugin_system.api import stream_api
from src.app.plugin_system.api.log_api import get_logger

if TYPE_CHECKING:
    #: 方向披露判定回调：(source_chat_type, target_chat_type) -> 是否允许
    DisclosureCheck = Callable[[str, str], bool]

logger = get_logger("roamer.carry")

#: 目标流类型查不到时的中性回退（披露矩阵对未知类型恒为关，
#: 但群→群默认 detailed 是主路径，缺失时按群处理以免误杀主场景）
_DEFAULT_TARGET_TYPE = "group"

#: 原文块的标题行（告诉 LLM 这是什么）
_BLOCK_HEADER = (
    "# 其他聊天的实时动态（原文搬运）\n\n"
    "以下是你在**其他聊天**里刚看到的最新消息和你自己做过的事，"
    "这是你的一手见闻，不是别人转述给你的：你亲历过这些对话。\n"
    "注意：这些内容都发生在当前聊天之外，仅供你保持记忆连续；"
    "不要在这里回应它们——你的回复只会发到当前聊天。\n"
)

#: 单个源群的小节标题模板
_STREAM_HEADER = "\n## 来自「{name}」的动态（其他聊天，非当前对话）\n"

#: 行为段标题模板（消息后的行为流水小节）
_ACTIONS_HEADER = "\n### 你在这里做过的事\n"


async def collect_carry_block(
    *,
    target_stream: str,
    source_streams: list[str],
    per_stream_count: int = 15,
    max_streams: int = 3,
    lookback_minutes: int = 60,
    since_epoch: float | None = None,
    disclosure: "DisclosureCheck | None" = None,
) -> tuple[str, float]:
    """收集其他漫游域成员的原文（消息+行为）并排版为搬运文本块。

    Args:
        target_stream: 当前会话所在流（排除自身）。
        source_streams: 候选源流列表（漫游域成员）。
        per_stream_count: 每个源群最多搬运的条数。
        max_streams: 单次最多覆盖的源群数。
        lookback_minutes: 只搬运该时间窗口内的内容。
        since_epoch: ACK 游标（只搬运该时刻之后的新内容）；
            None 表示全窗口模式（Tool 拉取/首访）。
        disclosure: 方向披露判定回调 ``(source_chat_type, target_chat_type)
            -> bool``；None 时不做方向过滤。

    Returns:
        (排版好的原文块, 本次搬运的游标上界 epoch)；
        无任何可搬运内容时返回 ``("", 0.0)``。
    """
    if not source_streams:
        return "", 0.0
    # aware cutoff：与 _message_datetime 归一化后的 aware datetime 同型比较
    # （naive now() 与 aware 比较会抛 TypeError——线上 tick 真实故障）
    cutoff = datetime.now().astimezone() - timedelta(
        minutes=max(1, lookback_minutes)
    )
    _ = cutoff  # 增量游标在条目级过滤（见下方 epoch 比较），窗口 cutoff 已下推 SQL

    # 数据源优先级：镜像库（类型安全、毫秒级）→ 主库回退
    from .store import recent_actions, recent_messages  # 局部导入避免环依赖

    upper_epoch = 0.0
    try:
        grouped = await recent_messages(
            exclude_stream=target_stream,
            per_stream_count=per_stream_count,
            max_streams=max_streams,
            lookback_minutes=lookback_minutes,
            since_epoch=since_epoch,
        )
        actions = await recent_actions(
            exclude_stream=target_stream,
            per_stream_count=per_stream_count,
            max_streams=max_streams,
            lookback_minutes=lookback_minutes,
            since_epoch=since_epoch,
        )
    except Exception as error:  # noqa: BLE001  镜像库失败回退主库
        logger.debug(f"镜像库查询失败，回退主库: {error}")
        grouped, actions = {}, {}
    if grouped or actions:
        # 方向披露过滤（chat_type 来自镜像库行；行为流继承其源流的类型）
        if disclosure is not None:
            grouped = {
                sid: items
                for sid, items in grouped.items()
                if disclosure(
                    str(items[0].get("chat_type", "")) if items else "",
                    _target_chat_type_hint(target_stream),
                )
            }
            actions = {
                sid: items
                for sid, items in actions.items()
                if disclosure(_source_chat_type_hint(sid), _target_chat_type_hint(target_stream))
            }
        sections: list[str] = []
        stream_set = set(grouped) | set(actions)
        # 命中展示上限时无法证明本次已经覆盖游标后的所有内容。此时仍返回
        # 原文供模型使用，但不返回 ACK 上界，确保不会因截断跳过未展示记录。
        may_be_truncated = len(stream_set) >= max_streams or any(
            len(items) >= per_stream_count
            for items in [*grouped.values(), *actions.values()]
        )
        # 按最新活跃度排序取前 max_streams 个流
        latest = {
            sid: max(
                [i.get("epoch", 0) for i in grouped.get(sid, [])]
                + [i.get("epoch", 0) for i in actions.get(sid, [])]
            )
            for sid in stream_set
        }
        ranked = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)
        for sid, _ in ranked[:max_streams]:
            display_name = await _stream_display_name(sid)
            parts = [_STREAM_HEADER.format(name=display_name)]
            # 增量模式：条目级游标过滤（epoch 严格大于游标才视为「新」）
            msg_lines = [
                f"{item['speaker_name'] or item['speaker']}：{item['text']}"
                for item in grouped.get(sid, [])
                if str(item.get("text") or "").strip()
                and (since_epoch is None or float(item.get("epoch", 0.0) or 0.0) > since_epoch)
            ]
            if msg_lines:
                parts.append("\n".join(msg_lines))
            act_lines = [
                _format_action(item)
                for item in actions.get(sid, [])
                if since_epoch is None
                or float(item.get("epoch", 0.0) or 0.0) > since_epoch
            ]
            if act_lines:
                parts.append(_ACTIONS_HEADER + "\n".join(act_lines))
            if len(parts) > 1:
                sections.append("\n".join(parts))
                upper_epoch = max(upper_epoch, latest.get(sid, 0.0))
        if sections:
            if may_be_truncated:
                upper_epoch = 0.0
            return _BLOCK_HEADER + "\n".join(sections), upper_epoch

    # ---- 主库回退路径（镜像库为空，如插件刚装、消息尚未流经观察 handler）----
    sections = []
    total_lines = 0
    for sid in source_streams:
        if sid == target_stream:
            continue
        if len(sections) >= max_streams:
            break
        if disclosure is not None and not disclosure(
            _source_chat_type_hint(sid), _target_chat_type_hint(target_stream)
        ):
            continue
        try:
            messages = await get_stream_messages_safe(sid, per_stream_count)
        except Exception as error:  # noqa: BLE001  观测性查询失败不致命
            logger.debug(f"拉取源流消息失败 stream={sid[:8]}: {error}")
            continue
        if not messages:
            continue

        # get_stream_messages 返回按时间升序的历史；只保留窗口内、逐字排版
        from src.core.components.base.chatter import BaseChatter

        lines: list[str] = []
        for msg in messages:
            msg_time = _message_datetime(msg)
            if msg_time is not None and msg_time < cutoff:
                continue
            if since_epoch is not None and (
                msg_time is None or msg_time.timestamp() <= since_epoch
            ):
                continue
            try:
                line = BaseChatter.format_message_line(msg)
            except Exception:  # noqa: BLE001  单条排版失败跳过
                continue
            if line.strip():
                lines.append(line)
        if not lines:
            continue

        # 取最新 N 条（列表尾部），保持时间升序展示
        tail = lines[-per_stream_count:] if len(lines) > per_stream_count else lines
        display_name = await _stream_display_name(sid)
        sections.append(_STREAM_HEADER.format(name=display_name) + "\n".join(tail))
        total_lines += len(tail)

    if not sections or total_lines == 0:
        return "", 0.0
    return _BLOCK_HEADER + "\n".join(sections), 0.0


def _format_action(item: dict) -> str:
    """把一条行为流水格式化为一行展示文本（原样参数与结果摘要）。"""
    kind = str(item.get("kind") or "tool")
    name = str(item.get("name") or "?")
    status = "" if item.get("success") else "（失败）"
    args = str(item.get("args_json") or "{}").strip()
    result = str(item.get("result_text") or "").strip()
    line = f"- 调用{kind}「{name}」{status}，参数：{args}"
    if result:
        line += f"，结果：{result[:160]}"
    return line


async def get_stream_messages_safe(stream_id: str, limit: int) -> list:
    """stream_api.get_stream_messages 的异常安全薄封装（便于测试桩替换）。"""
    return await stream_api.get_stream_messages(
        stream_id=stream_id, limit=max(1, limit), offset=0
    )


def _message_datetime(msg: object) -> datetime | None:
    """把 Message.time 归一化为 aware datetime。

    ``Message.time`` 运行时是 float 时间戳，但经数据库往返后会变成
    offset-naive datetime（SQLite 不保存时区）。naive 值视为**本地时区**
    补全 tzinfo，否则与 aware 的 cutoff 比较会抛 TypeError。

    Returns:
        datetime | None: aware datetime；time 字段缺失/非法返回 None。
    """
    raw = getattr(msg, "time", None)
    if isinstance(raw, datetime):
        # naive（数据库往返）按本地时区补全；aware 直接归一化
        return raw.astimezone() if raw.tzinfo is None else raw.astimezone()
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw)).astimezone()
        except (OSError, OverflowError, ValueError):
            return None
    return None


async def _stream_display_name(stream_id: str) -> str:
    """取流的显示名（失败回退 ID 前 8 位）。"""
    try:
        chat_stream = await stream_api.get_stream(stream_id)
    except Exception:  # noqa: BLE001
        chat_stream = None
    name = getattr(chat_stream, "stream_name", "") if chat_stream else ""
    return name or stream_id[:8]


def _target_chat_type_hint(stream_id: str) -> str:
    """取目标流的聊天类型（planner 登记；未登记回退 group 保主路径）。

    Args:
        stream_id: 目标流 ID。

    Returns:
        str: ``"private"`` / ``"group"``。
    """
    from .service import _SharedState

    registered = _SharedState.planner.chat_type_of(stream_id)
    return registered if registered in ("private", "group") else _DEFAULT_TARGET_TYPE


def _source_chat_type_hint(stream_id: str) -> str:
    """从 planner 读取源流类型；未知类型默认拒绝披露。"""
    from .service import _SharedState

    registered = _SharedState.planner.chat_type_of(stream_id)
    return registered if registered in ("private", "group") else ""
