"""Roamer 一手发言账本。

设计要点（docs/DESIGN.md §3.1）：

- **数据源唯一**：仅记录 Bot 自己发出的消息（``after_message_sent`` 事件源头
  就只携带 bot 消息），从机制上杜绝跨群搬运用户消息；
- **一手性**：每条记录的 ``stream_id`` 是该消息的真实来源群，记账即一手，
  不经过任何二次转手；
- **窗口老化**：滚动窗口外的条目压缩为按群统计（条数 + 话题标签）；
- **简洁报**：按目标群生成「我最近在其他群说了什么」的简报文本，
  经 ``resume_chatter(extra={"resume_prompt": ...})`` 原生通道注入——
  NDFC 默认 ``:build_resume_prompt`` handler 原生支持读取该字段
  （``build_resume_prompt.py`` 的 ``_build_generic_resume_prompt``）。

并发安全性：asyncio 单线程语义；本类所有方法内部不 ``await``，
读写天然原子，无需加锁。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.app.plugin_system.api.log_api import get_logger

logger = get_logger("roamer.ledger")

#: 存储命名空间（data/json_storage/roamer_ledger/）
_STORE_NAME = "roamer_ledger"
#: 持久化键名
_PERSIST_KEY = "ledger"


def _strip_entry(text: str, max_chars: int) -> str:
    """把单条发言截断到上限内，超长时加省略号。"""
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max(0, max_chars - 1)] + "…"


@dataclass(slots=True)
class LedgerEntry:
    """单条发言记录（bot 自身，或开启追踪后的用户消息）。"""

    stream_id: str
    time: datetime
    text: str
    msg_id: str
    #: "bot" 或平台用户 ID（用户条目仅在 track_user_speech 开启时产生）
    speaker: str = "bot"
    #: 用户显示名（用户条目用；bot 条目为空）
    speaker_name: str = ""

    def to_dict(self) -> dict[str, str]:
        """序列化为可 JSON 持久化的 dict。"""
        return {
            "stream_id": self.stream_id,
            "time": self.time.isoformat(),
            "text": self.text,
            "msg_id": self.msg_id,
            "speaker": self.speaker,
            "speaker_name": self.speaker_name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LedgerEntry | None":
        """从持久化 dict 反序列化；字段缺失或时间非法时返回 None。

        旧版 payload 无 ``speaker`` 字段，缺省视为 bot 条目（向后兼容）。
        """
        try:
            return cls(
                stream_id=str(data["stream_id"]),
                time=datetime.fromisoformat(str(data["time"])),
                text=str(data.get("text", "")),
                msg_id=str(data.get("msg_id", "")),
                speaker=str(data.get("speaker", "bot")),
                speaker_name=str(data.get("speaker_name", "")),
            )
        except (KeyError, TypeError, ValueError):
            return None


@dataclass(slots=True)
class StreamDigest:
    """超龄条目压缩后的按群统计。"""

    stream_id: str
    count: int
    until: datetime


class RoamingLedger:
    """跨群一手发言账本（插件内单例，由 RoamerCore 持有）。"""

    def __init__(
        self,
        *,
        window_hours: int = 6,
        max_entry_chars: int = 200,
        max_entries_per_stream: int = 10,
        max_user_entries_per_stream: int = 5,
    ) -> None:
        """初始化账本。

        Args:
            window_hours: 滚动窗口小时数，窗口外条目老化压缩。
            max_entry_chars: 单条发言截断上限。
            max_entries_per_stream: 简报中单群最多展示的 bot 条数。
            max_user_entries_per_stream: 简报中单群最多展示的用户条数。
        """
        self._window = timedelta(hours=max(1, int(window_hours)))
        self._max_chars = max(4, int(max_entry_chars))
        self._max_per_stream = max(1, int(max_entries_per_stream))
        self._max_user_per_stream = max(0, int(max_user_entries_per_stream))
        # deque(maxlen) 无法按时间老化，这里用 list + 主动裁剪
        self._entries: list[LedgerEntry] = []
        self._digests: dict[str, StreamDigest] = {}
        self._seen_msg_ids: set[str] = set()
        self._dirty = False
        #: 变更版本号：每次实质变更 +1，单调递增。
        #: 持久化端用「快照版本 == 当前版本」判定 await IO 期间内容是否又被改过
        #: （asyncio 单线程，纯同步自增无竞态）。
        self.version: int = 0

    # ------------------------------------------------------------------ 记账

    def record(
        self,
        *,
        stream_id: str,
        text: str,
        msg_id: str,
        time: datetime,
        speaker: str = "bot",
        speaker_name: str = "",
    ) -> bool:
        """记录一条发言（漫游域外或重复消息返回 False）。

        Args:
            stream_id: 发言所在聊天流 ID（真实来源群）。
            text: 发言文本（可为空串）。
            msg_id: 消息 ID，用于去重。
            time: 发言时间。
            speaker: ``"bot"`` 或平台用户 ID。
            speaker_name: 用户显示名（用户条目用）。

        Returns:
            bool: 真正记入账本返回 True；重复消息返回 False。
        """
        if msg_id and msg_id in self._seen_msg_ids:
            return False
        if msg_id:
            self._seen_msg_ids.add(msg_id)
        self._entries.append(
            LedgerEntry(
                stream_id=stream_id,
                time=time,
                text=_strip_entry(text, self._max_chars),
                msg_id=msg_id,
                speaker=speaker,
                speaker_name=speaker_name,
            )
        )
        self._dirty = True
        self.version += 1
        return True

    def record_user(
        self,
        *,
        stream_id: str,
        text: str,
        msg_id: str,
        time: datetime,
        user_id: str,
        user_name: str,
    ) -> bool:
        """记录一条用户消息（仅 track_user_speech 开启时由 handler 调用）。

        Args:
            stream_id: 消息所在聊天流 ID。
            text: 消息文本。
            msg_id: 消息 ID，用于去重。
            time: 消息时间。
            user_id: 发言用户平台 ID。
            user_name: 用户显示名。

        Returns:
            bool: 真正记入账本返回 True；重复消息返回 False。
        """
        return self.record(
            stream_id=stream_id,
            text=text,
            msg_id=msg_id,
            time=time,
            speaker=user_id or "user",
            speaker_name=user_name,
        )

    # ------------------------------------------------------------------ 老化

    def age(self, now: datetime) -> int:
        """把窗口外条目压缩为按群统计，返回压缩掉的数量。

        Args:
            now: 当前时间（调度 tick 调用）。

        Returns:
            int: 本次被老化的条目数。
        """
        cutoff = now - self._window
        kept: list[LedgerEntry] = []
        aged = 0
        for entry in self._entries:
            if entry.time >= cutoff:
                kept.append(entry)
                continue
            digest = self._digests.get(entry.stream_id)
            if digest is None:
                self._digests[entry.stream_id] = StreamDigest(
                    stream_id=entry.stream_id, count=1, until=entry.time
                )
            else:
                digest.count += 1
                digest.until = max(digest.until, entry.time)
            aged += 1
        self._entries = kept
        # 清理过期 digest（窗口外两个窗口周期的统计也丢弃）
        digest_cutoff = now - self._window * 2
        for sid in [s for s, d in self._digests.items() if d.until < digest_cutoff]:
            del self._digests[sid]
        if aged:
            self._dirty = True
            self.version += 1
        return aged

    # ------------------------------------------------------------------ 查询

    def entries_for(self, stream_id: str) -> list[LedgerEntry]:
        """返回指定群的未老化条目（按时间降序）。"""
        return sorted(
            (e for e in self._entries if e.stream_id == stream_id),
            key=lambda e: e.time,
            reverse=True,
        )

    def brief_stats(self) -> dict[str, int]:
        """返回每群未老化条数（运维观测用）。"""
        stats: dict[str, int] = {}
        for entry in self._entries:
            stats[entry.stream_id] = stats.get(entry.stream_id, 0) + 1
        return stats

    # ------------------------------------------------------------------ 简报

    def build_briefing(
        self,
        *,
        target_stream: str,
        since: datetime | None = None,
        stream_names: dict[str, str] | None = None,
    ) -> str:
        """为目标群生成跨群简报文本。

        只包含**其他群**的发言——目标群自己的未读会走 NDFC 原生 fetch_unreads
        路径，不在这里重复（一手信息不转发原则）。

        bot 条目与用户条目（track_user_speech 开启时）混合按时间排序展示。

        Args:
            target_stream: 即将被唤醒的目标聊天流 ID。
            since: 只统计该时间之后的发言（通常是目标群上次活跃时间）。
            stream_names: stream_id → 显示名映射（缺省用 ID）。

        Returns:
            str: 简报文本；漫游域内近期无其他群发言时返回空串。
        """
        names = stream_names or {}
        lines: list[str] = []
        other_streams = sorted(
            {e.stream_id for e in self._entries if e.stream_id != target_stream}
        )
        for sid in other_streams:
            recent_entries = [
                e
                for e in self.entries_for(sid)
                if since is None or e.time >= since
            ]
            bot_entries = [e for e in recent_entries if e.speaker == "bot"][
                : self._max_per_stream
            ]
            user_entries = [e for e in recent_entries if e.speaker != "bot"][
                : self._max_user_per_stream
            ]
            digest = self._digests.get(sid)
            if not bot_entries and not user_entries and digest is None:
                continue
            display = names.get(sid, sid)
            parts: list[str] = []
            if bot_entries:
                mine = "；".join(e.text for e in reversed(bot_entries) if e.text)
                if mine:
                    parts.append(f"你说了：{mine}")
            if user_entries:
                theirs = "；".join(
                    f"{e.speaker_name or '某人'}说「{e.text}」"
                    for e in reversed(user_entries)
                    if e.text
                )
                if theirs:
                    parts.append(theirs)
            if parts:
                head = f"「{display}」：" + "。".join(parts)
            elif digest is not None:
                head = f"「{display}」：（近期有 {digest.count} 条更早的发言）"
            else:
                head = f"「{display}」：（非文本内容）"
            lines.append(head)
        if not lines:
            return ""
        return (
            "（你最近在其他聊天里的动态，供你保持记忆连续：）\n"
            + "\n".join(lines)
            + "\n（以上是你的跨群简报。当前聊天的新消息见下方未读；"
            "你自己决定要不要接话——可以自然参与，也可以只看着不说话。）"
        )

    # ------------------------------------------------------------------ 持久化

    def to_payload(self) -> dict:
        """导出可持久化 payload。"""
        return {
            "entries": [e.to_dict() for e in self._entries],
            "digests": [
                {
                    "stream_id": d.stream_id,
                    "count": d.count,
                    "until": d.until.isoformat(),
                }
                for d in self._digests.values()
            ],
        }

    def load_payload(self, payload: dict | None) -> int:
        """从持久化 payload 恢复账本，返回恢复的条目数。"""
        self._entries.clear()
        self._digests.clear()
        self._seen_msg_ids.clear()
        if not isinstance(payload, dict):
            return 0
        restored = 0
        for item in payload.get("entries", []):
            entry = LedgerEntry.from_dict(item if isinstance(item, dict) else {})
            if entry is None:
                continue
            self._entries.append(entry)
            if entry.msg_id:
                self._seen_msg_ids.add(entry.msg_id)
            restored += 1
        for item in payload.get("digests", []):
            if not isinstance(item, dict):
                continue
            try:
                self._digests[str(item["stream_id"])] = StreamDigest(
                    stream_id=str(item["stream_id"]),
                    count=int(item.get("count", 0)),
                    until=datetime.fromisoformat(str(item["until"])),
                )
            except (KeyError, TypeError, ValueError):
                continue
        self._dirty = False
        return restored

    @property
    def dirty(self) -> bool:
        """自上次持久化以来是否有变更。"""
        return self._dirty

    def mark_persisted(self) -> None:
        """清除脏标记（持久化成功后调用）。"""
        self._dirty = False
