"""Roamer 插件私有数据库：消息镜像 + 调度事件。

用 :class:`~src.app.plugin_system.api.storage_api.PluginDatabase`（插件独立
SQLite，路径 ``data/roamer/data.db``），与框架主库完全隔离。

**为什么建镜像库**（用户决策：插件可以有自己的数据库）：

- 框架历史查询 ``get_stream_messages`` 从主库捞全量再截断，且 ``Message.time``
  经 SQLite 往返后时区信息丢失（naive），曾导致 offset-naive/aware 比较崩溃；
- 镜像库在**写入时**就存 UTC aware 时间戳（ISO 字符串），读出时还原为 aware
  datetime，类型安全、永不混用；
- tick 唤醒 / 每轮搬运只查镜像库（毫秒级、只捞窗口内条数），
  不再对每个源流打主库。

表设计：

- ``mirror_messages``：漫游域内消息镜像（收到 + 发出），按 ``msg_id`` 幂等；
- ``roam_events``：漫游事件流水（唤醒/释放），供 status 观测（v1 只写不读）。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import String, Text, Integer, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api.storage_api import PluginDatabase

logger = get_logger("roamer.store")

#: 插件数据库文件路径（独立于框架 data/json_storage 与主库）
DB_PATH = "data/roamer/data.db"

#: 消息镜像保留天数（超龄由 cleanup 删除）
RETENTION_DAYS = 7

#: 独立 declarative Base，与核心数据库完全隔离（与 booku_memory 同模式）
Base = declarative_base()


class MirrorMessage(Base):  # type: ignore[misc,valid-type]
    """漫游域消息镜像行。"""

    __tablename__ = "mirror_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    msg_id: Mapped[str] = mapped_column(String(128), index=True)
    stream_id: Mapped[str] = mapped_column(String(128), index=True)
    chat_type: Mapped[str] = mapped_column(String(16), default="")
    speaker: Mapped[str] = mapped_column(String(64), default="bot")  # bot 或用户 ID
    speaker_name: Mapped[str] = mapped_column(String(128), default="")
    #: 原文（processed_plain_text；非文本记占位说明）
    text: Mapped[str] = mapped_column(Text, default="")
    is_bot: Mapped[bool] = mapped_column(Boolean, default=False)
    #: UTC aware ISO 字符串（含时区），类型安全的关键
    time_iso: Mapped[str] = mapped_column(String(40), index=True)
    #: 原始消息时间戳（epoch float，便于排序/清理）
    time_epoch: Mapped[float] = mapped_column(Float, index=True)


class RoamEvent(Base):  # type: ignore[misc,valid-type]
    """漫游事件流水（唤醒/释放）。"""

    __tablename__ = "roam_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stream_id: Mapped[str] = mapped_column(String(128), index=True)
    kind: Mapped[str] = mapped_column(String(16))  # wake / release
    reason: Mapped[str] = mapped_column(String(256), default="")
    time_iso: Mapped[str] = mapped_column(String(40))


class MirrorAction(Base):  # type: ignore[misc,valid-type]
    """Bot 行为镜像行（工具/动作调用流水，含参数与结果摘要）。"""

    __tablename__ = "mirror_actions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stream_id: Mapped[str] = mapped_column(String(128), index=True)
    #: "tool" / "action"
    kind: Mapped[str] = mapped_column(String(16), default="tool")
    name: Mapped[str] = mapped_column(String(128), default="")
    #: 参数 JSON（截断存储）
    args_json: Mapped[str] = mapped_column(Text, default="")
    #: 结果摘要（截断存储）
    result_text: Mapped[str] = mapped_column(Text, default="")
    success: Mapped[bool] = mapped_column(Boolean, default=True)
    #: UTC aware ISO 字符串
    time_iso: Mapped[str] = mapped_column(String(40), index=True)
    time_epoch: Mapped[float] = mapped_column(Float, index=True)


_MODELS: list[type] = [MirrorMessage, RoamEvent, MirrorAction]
_db: PluginDatabase | None = None


def get_db() -> PluginDatabase:
    """返回插件数据库单例（首次调用时创建，需随后 initialize()）。"""
    global _db
    if _db is None:
        _db = PluginDatabase(DB_PATH, _MODELS)
    return _db


def utc_iso(moment: datetime) -> str:
    """把 aware datetime 转为 UTC ISO 字符串（存储格式）。"""
    return moment.astimezone(timezone.utc).isoformat()


def parse_iso(value: str) -> datetime | None:
    """把存储的 ISO 字符串还原为 aware datetime；失败返回 None。"""
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        # 历史脏数据兜底：naive 按本地时区补全
        return parsed.astimezone()
    return parsed


async def mirror_message(
    *,
    msg_id: str,
    stream_id: str,
    chat_type: str,
    speaker: str,
    speaker_name: str,
    text: str,
    is_bot: bool,
    time: datetime,
) -> bool:
    """镜像一条消息（按 msg_id 幂等；重复返回 False）。

    Args:
        msg_id: 消息唯一 ID（去重键）。
        stream_id: 所属流。
        chat_type: 流类型（private/group/...）。
        speaker: ``"bot"`` 或用户平台 ID。
        speaker_name: 显示名。
        text: 消息原文。
        is_bot: 是否 bot 自身发言。
        time: 消息时间（aware 或 naive，统一本地化存储）。

    Returns:
        bool: 真正写入返回 True。
    """
    if not msg_id:
        return False
    db = get_db()
    existing = await db.crud(MirrorMessage).get_by(msg_id=msg_id)
    if existing is not None:
        return False
    normalized = time if time.tzinfo else time.astimezone()
    await db.crud(MirrorMessage).create(
        {
            "msg_id": msg_id,
            "stream_id": stream_id,
            "chat_type": chat_type,
            "speaker": speaker,
            "speaker_name": speaker_name,
            "text": text,
            "is_bot": is_bot,
            "time_iso": utc_iso(normalized),
            "time_epoch": normalized.timestamp(),
        }
    )
    return True


async def recent_messages(
    *,
    exclude_stream: str,
    per_stream_count: int,
    max_streams: int,
    lookback_minutes: int,
    since_epoch: float | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """按流分组返回窗口内的最新消息（搬运块数据源）。

    时间窗口与排序全部下推 SQL（``time_epoch`` 索引列），
    不再全捞千行后内存过滤。

    Args:
        exclude_stream: 排除的目标流（自身）。
        per_stream_count: 每流最多条数。
        max_streams: 最多覆盖的流数。
        lookback_minutes: 时间窗口。

    Returns:
        dict: ``stream_id -> [{speaker, speaker_name, text, time}, ...]``
        （每组按时间升序，最新在尾部）。
    """
    db = get_db()
    cutoff_epoch = (
        datetime.now().astimezone() - timedelta(minutes=max(1, lookback_minutes))
    ).timestamp()
    query = db.query(MirrorMessage).filter(time_epoch__gte=cutoff_epoch)
    if since_epoch is not None:
        query = query.filter(time_epoch__gt=since_epoch)
    rows = (
        await query
        .order_by("-time_epoch")
        .limit(
            max_streams * per_stream_count * 3  # 多拉余量：分组截断前的跨流冗余
        )
        .all(as_dict=True)
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sid = str(row.get("stream_id", ""))
        if not sid or sid == exclude_stream:
            continue
        grouped.setdefault(sid, []).append(
            {
                "speaker": row.get("speaker", ""),
                "speaker_name": row.get("speaker_name", ""),
                "text": row.get("text", ""),
                "chat_type": row.get("chat_type", ""),
                "time": parse_iso(str(row.get("time_iso", ""))),
                "epoch": float(row.get("time_epoch", 0.0) or 0.0),
            }
        )
    # 每流保留最新 N 条（升序），按窗口内最新消息时间排优先级，取前 max_streams 个流
    ranked = sorted(
        grouped.items(),
        key=lambda kv: max(item["epoch"] for item in kv[1]),
        reverse=True,
    )[:max_streams]
    result: dict[str, list[dict[str, Any]]] = {}
    for sid, items in ranked:
        items.sort(key=lambda item: item["epoch"])
        result[sid] = items[-per_stream_count:]
    return result


async def record_event(*, stream_id: str, kind: str, reason: str) -> None:
    """记录一条漫游事件流水（失败不抛出，仅日志）。"""
    try:
        await get_db().crud(RoamEvent).create(
            {
                "stream_id": stream_id,
                "kind": kind,
                "reason": reason[:250],
                "time_iso": utc_iso(datetime.now().astimezone()),
            }
        )
    except Exception as error:  # noqa: BLE001  流水非关键路径
        logger.debug(f"记录漫游事件失败: {error}")


async def mirror_action(
    *,
    stream_id: str,
    kind: str,
    name: str,
    args: dict[str, Any] | None,
    result_text: str,
    success: bool,
) -> bool:
    """镜像一条 Bot 行为（工具/动作调用）。

    Args:
        stream_id: 行为发生所在流。
        kind: ``"tool"`` / ``"action"``。
        name: 工具/动作名。
        args: 调用参数（JSON 序列化后截断存储）。
        result_text: 结果摘要（截断存储）。
        success: 是否成功。

    Returns:
        bool: 写入成功返回 True。
    """
    import json

    now = datetime.now().astimezone()
    try:
        args_json = json.dumps(args or {}, ensure_ascii=False)[:800]
    except (TypeError, ValueError):
        args_json = "{}"
    try:
        await get_db().crud(MirrorAction).create(
            {
                "stream_id": stream_id,
                "kind": kind,
                "name": name[:120],
                "args_json": args_json,
                "result_text": (result_text or "")[:500],
                "success": bool(success),
                "time_iso": utc_iso(now),
                "time_epoch": now.timestamp(),
            }
        )
        return True
    except Exception as error:  # noqa: BLE001  行为镜像非关键路径
        logger.debug(f"行为镜像失败: {error}")
        return False


async def recent_actions(
    *,
    exclude_stream: str,
    per_stream_count: int,
    max_streams: int,
    lookback_minutes: int,
    since_epoch: float | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """按流分组返回窗口内 Bot 的最新行为流水（行为搬运数据源）。

    时间窗口与排序同样下推 SQL。

    Args:
        exclude_stream: 排除的目标流（自身）。
        per_stream_count: 每流最多条数。
        max_streams: 最多覆盖的流数。
        lookback_minutes: 时间窗口。

    Returns:
        dict: ``stream_id -> [{kind, name, args_json, result_text, success, epoch}, ...]``
    """
    db = get_db()
    cutoff_epoch = (
        datetime.now().astimezone() - timedelta(minutes=max(1, lookback_minutes))
    ).timestamp()
    query = db.query(MirrorAction).filter(time_epoch__gte=cutoff_epoch)
    if since_epoch is not None:
        query = query.filter(time_epoch__gt=since_epoch)
    rows = (
        await query
        .order_by("-time_epoch")
        .limit(max_streams * per_stream_count * 3)
        .all(as_dict=True)
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        sid = str(row.get("stream_id", ""))
        if not sid or sid == exclude_stream:
            continue
        grouped.setdefault(sid, []).append(
            {
                "kind": row.get("kind", "tool"),
                "name": row.get("name", ""),
                "args_json": row.get("args_json", ""),
                "result_text": row.get("result_text", ""),
                "success": bool(row.get("success", True)),
                "epoch": float(row.get("time_epoch", 0.0) or 0.0),
            }
        )
    ranked = sorted(
        grouped.items(),
        key=lambda kv: max(item["epoch"] for item in kv[1]),
        reverse=True,
    )[:max_streams]
    result: dict[str, list[dict[str, Any]]] = {}
    for sid, items in ranked:
        items.sort(key=lambda item: item["epoch"])
        result[sid] = items[-per_stream_count:]
    return result


async def cleanup_expired() -> int:
    """删除超过保留期的镜像行，返回删除数（供 tick 周期调用）。

    批量 DELETE ... WHERE 下推 SQL，替代逐行 get_multi + delete。
    """
    from sqlalchemy import delete as sa_delete

    db = get_db()
    cutoff_epoch = (
        datetime.now().astimezone() - timedelta(days=RETENTION_DAYS)
    ).timestamp()
    deleted = 0
    async with db.session() as session:
        for model in (MirrorMessage, MirrorAction):
            result = await session.execute(
                sa_delete(model).where(model.time_epoch < cutoff_epoch)
            )
            deleted += int(result.rowcount or 0)
    if deleted:
        logger.info(f"镜像库清理 {deleted} 条超龄记录")
    return deleted
