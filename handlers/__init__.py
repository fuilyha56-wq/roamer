"""Roamer 事件 handler。

五个 handler 全部遵循「先过滤、不匹配即 PASS」的非侵入约定：

- :class:`LedgerRecordHandler`：``after_message_sent`` → 账本记账（纯观察）；
- :class:`UnreadObserveHandler`：``on_message_received`` → 喂调度器兴趣分
  + 可选的用户消息追踪记账；
- :class:`FocusGateHandler`：NDFC ``:preprocess`` → 串行专注门（可选，默认关）；
- :class:`CrossStreamInjectHandler`：``on_prompt_build`` → 路径 B 常驻注入
  跨群原文搬运（默认开，``carry.enabled=false`` 或
  ``carry.inject_on_every_turn=false`` 关闭）；
- :class:`BehaviorObserveHandler`：``after_tool_call`` / ``after_action_call``
  → Bot 行为镜像（工具/动作调用流水，供行为搬运）；
- :class:`CarryAckHandler`：``after_llm_request`` / ``on_llm_request_failed``
  → 搬运 ACK 游标推进/回滚（请求成功才推进，失败保留内容下次补发）。

本包共用模块级助手：``_extract_text`` / ``_reply_to_bot`` / ``_MENTION_PATTERN``。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api import service_api
from src.app.plugin_system.base import BaseEventHandler
from src.kernel.event import EventDecision

from ..config import RoamerConfig
from ..core.service import (
    RoamerCore,
    _CARRY_BATCH_PREFIX,
    _CARRY_BATCH_SUFFIX,
    _SharedState,
)
from ..core.reminder import set_cross_stream_reminder
from ..core.store import mirror_action, mirror_message

__all__ = [
    "LedgerRecordHandler",
    "UnreadObserveHandler",
    "FocusGateHandler",
    "CrossStreamInjectHandler",
    "BehaviorObserveHandler",
    "CarryAckHandler",
    "bot_platform_id",
]

logger = get_logger("roamer.handlers")

#: RoamerCore 服务签名（plugin_name:component_type:component_name）
_CORE_SIGNATURE = "roamer:service:roamer_core"

#: NDFC preprocess 事件名（字符串字面量订阅，不跨插件 import，见规范 §2.5）
_NDFC_PREPROCESS = "neo_default_chatter:preprocess"

#: 强提及判定：消息文本中 @ 了 bot（宽松启发式：@ + 非空白片段）
_MENTION_PATTERN = re.compile(r"@[^\s，。,.\u2005]{1,32}")


def _get_core(plugin) -> RoamerCore | None:
    """获取 RoamerCore 服务实例（未注册时返回 None）。"""
    service = service_api.get_service(_CORE_SIGNATURE)
    return service if isinstance(service, RoamerCore) else None


def _extract_text(message: Any) -> str:
    """提取消息可读文本（processed_plain_text 优先，回退 content）。"""
    text = getattr(message, "processed_plain_text", None)
    if isinstance(text, str) and text:
        return text
    content = getattr(message, "content", "")
    return content if isinstance(content, str) else ""


def _reply_to_bot(message: Any, bot_id: str) -> bool:
    """启发式判断消息是否回复了 bot（reply_to 命中 bot ID 前缀）。"""
    reply_to = getattr(message, "reply_to", None)
    return bool(reply_to and bot_id and str(reply_to).startswith(str(bot_id)))


def bot_platform_id() -> str:
    """取 bot 平台 ID（用于回复命中判定；取不到返回空串）。"""
    try:
        from src.core.config import get_core_config

        account = getattr(get_core_config(), "personality", None)
        for attr in ("bot_qq", "bot_id", "qq"):
            value = getattr(account, attr, "")
            if value:
                return str(value)
    except Exception:  # noqa: BLE001  观测性判定失败按无处理
        return ""
    return ""


def _normalize_message_time(message: Any, fallback: datetime) -> datetime:
    """把 Message.time 归一化为 aware datetime。

    ``Message.time`` 运行时是 float 时间戳，但经数据库往返后会变成
    offset-naive datetime——直接与 aware 值比较会抛 TypeError
    （``_tick_once`` 的真实线上故障）。naive 按本地时区补全 tzinfo。

    Args:
        message: 消息对象。
        fallback: time 字段缺失/非法时的时间戳兜底（通常为当前时间）。

    Returns:
        datetime: aware datetime，保证可安全参与比较与算术。
    """
    raw = getattr(message, "time", None)
    if isinstance(raw, datetime):
        return raw.astimezone() if raw.tzinfo is None else raw.astimezone()
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw)).astimezone()
        except (OSError, OverflowError, TypeError, ValueError):
            return fallback
    return fallback


class LedgerRecordHandler(BaseEventHandler):
    """``after_message_sent`` 记账 handler（纯观察，永远 PASS）。

    事件源头（``message_sender._emit_sent_event``）只携带 bot 自己发出的
    消息——从机制上保证账本一手性：不记录任何用户消息。
    """

    name = "ledger_record"
    description = "漫游账本记账：记录 bot 自己的跨群发言（纯观察）"
    weight = 50
    init_subscribe = ["after_message_sent"]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """把漫游域内的 bot 发言记入账本。"""
        core = _get_core(self.plugin)
        if core is None or not core.enabled:
            return EventDecision.PASS, params
        message = params.get("message")
        stream_id = str(getattr(message, "stream_id", "") or "")
        if not stream_id or not core.planner.in_domain(stream_id):
            return EventDecision.PASS, params
        moment = _normalize_message_time(
            message, datetime.now().astimezone()
        )
        recorded = core.ledger.record(
            stream_id=stream_id,
            text=_extract_text(message),
            msg_id=str(getattr(message, "message_id", "") or ""),
            time=moment,
        )
        if recorded:
            # 同步喂调度器：焦点群有会话活动（空闲判定用）
            core.planner.note_chatter_active(stream_id, moment)
        # 镜像到插件私有库（搬运块数据源，幂等）
        await _mirror_from_event(
            message=message,
            stream_id=stream_id,
            chat_type=_message_chat_type(message),
            is_bot=True,
            time=moment,
        )
        return EventDecision.PASS, params


def _message_chat_type(message: Any) -> str:
    """读取消息的聊天类型（private/group/...）。"""
    return str(getattr(message, "chat_type", "") or "")


async def _mirror_from_event(
    *,
    message: Any,
    stream_id: str,
    chat_type: str,
    is_bot: bool,
    time: datetime,
) -> None:
    """把事件携带的 Message 镜像进插件私有库（失败不阻碍记账主流程）。"""
    try:
        await mirror_message(
            msg_id=str(getattr(message, "message_id", "") or ""),
            stream_id=stream_id,
            chat_type=chat_type,
            speaker=(
                "bot"
                if is_bot
                else str(getattr(message, "sender_id", "") or "user")
            ),
            speaker_name=str(
                getattr(message, "sender_cardname", "")
                or getattr(message, "sender_name", "")
                or ""
            ),
            text=_extract_text(message),
            is_bot=is_bot,
            time=time,
        )
    except Exception as error:  # noqa: BLE001  镜像失败不阻断
        logger.debug(f"消息镜像失败: {error}")


class UnreadObserveHandler(BaseEventHandler):
    """``on_message_received`` 未读观察 handler（纯观察，永远 PASS）。

    只在流处于漫游域时喂调度器：累积未读兴趣分 + 强提及标记。
    若 ``ledger.track_user_speech`` 开启，同时把用户消息记入账本
    （供跨群简报展示「某人说：…」）。
    """

    name = "unread_observe"
    description = "漫游调度观察：累积漫游域未读与强提及信号，可选追踪用户发言"
    weight = 50
    init_subscribe = ["on_message_received"]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """把漫游域内的新消息喂给调度器（可选记账）。

        全流域模式下自动登记流类型（private/group）并入域，
        无需在配置文件或命令里逐个 join。
        """
        core = _get_core(self.plugin)
        if core is None or not core.enabled:
            return EventDecision.PASS, params
        message = params.get("message")
        stream_id = str(getattr(message, "stream_id", "") or "")
        if not stream_id:
            return EventDecision.PASS, params
        chat_type = str(getattr(message, "chat_type", "") or "")
        if chat_type:
            # 登记流类型：全流域模式下新流经此自动入域
            core.planner.note_stream_type(stream_id, chat_type)
        if not core.planner.in_domain(stream_id):
            return EventDecision.PASS, params
        strong = self._is_strong_mention(message)
        now = datetime.now().astimezone()
        core.planner.note_unread(
            stream_id=stream_id,
            strong_mention=strong,
            now=now,
        )
        # 一律镜像到插件私有库（搬运块数据源，幂等；用户消息是搬运的主体）
        await _mirror_from_event(
            message=message,
            stream_id=stream_id,
            chat_type=_message_chat_type(message),
            is_bot=False,
            time=_normalize_message_time(message, now),
        )
        # 可选：用户消息记账（跨群简报用）
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if cfg is not None and cfg.ledger.track_user_speech:
            moment = _normalize_message_time(message, now)
            core.ledger.record_user(
                stream_id=stream_id,
                text=_extract_text(message),
                msg_id=str(getattr(message, "message_id", "") or ""),
                time=moment,
                user_id=str(getattr(message, "sender_id", "") or ""),
                user_name=str(
                    getattr(message, "sender_cardname", "")
                    or getattr(message, "sender_name", "")
                    or ""
                ),
            )
        # 维护其他流的跨群 reminder：给漫游域内【最新活跃流的对照面】刷新
        # 候选内容。reminder 是 once 消费制——这里只在「该流恰好是当前焦点」
        # 时刷新它自己的 reminder（焦点群正在对话，下轮拾取的就是新鲜的）。
        # 注意：这是 fire-and-forget 维护，绝不阻塞消息主链路。
        if (
            cfg is not None
            and cfg.roamer.enabled
            and cfg.carry.enabled
            and _SharedState.focus_stream == stream_id
        ):
            try:
                carry_text = await core.build_carry_text(stream_id)
                if carry_text:
                    await set_cross_stream_reminder(stream_id, carry_text)
            except Exception as error:  # noqa: BLE001  维护失败不阻断
                logger.debug(f"刷新跨群 reminder 失败: {error}")
        return EventDecision.PASS, params

    def _is_strong_mention(self, message: Any) -> bool:
        """判断消息是否强提及 bot。

        私聊 = 直接对话，恒为强提及（唤醒插队 + 专注门豁免）；
        群聊 = @ 文本命中或回复了 bot 消息。
        """
        chat_type = str(getattr(message, "chat_type", "") or "")
        if chat_type == "private":
            return True
        if _reply_to_bot(message, bot_platform_id()):
            return True
        text = _extract_text(message)
        return bool(text and _MENTION_PATTERN.search(text))


class CrossStreamInjectHandler(BaseEventHandler):
    """``on_prompt_build`` 路径 B 常驻注入 handler（默认开）。

    对 NDFC 的 user prompt 模板渲染协作追加 ``values["extra"]``——把漫游域内
    其他群的**最新消息原文**（与 NDFC 历史消息同格式，零转写）注入，
    让**普通 @ 回复**也带着跨群一手见闻，不再出现「我看不到别的群」。

    非侵入保证：

    - 仅处理模板名以 ``neo_default_chatter:`` 开头的 NDFC 模板；
    - 原文块为空时不追加任何内容；
    - ``carry.enabled=false`` 或 ``carry.inject_on_every_turn=false`` 时整体 PASS。
    """

    name = "cross_stream_inject"
    description = "路径B常驻注入：每轮 prompt 追加跨群原文搬运"
    weight = 200
    init_subscribe = ["on_prompt_build"]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """向 NDFC user prompt 注入跨群原文。"""
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if (
            cfg is None
            or not cfg.roamer.enabled
            or not cfg.carry.enabled
            or not cfg.carry.inject_on_every_turn
        ):
            return EventDecision.PASS, params
        template_name = str(params.get("name", "") or "")
        # NDFC 模板名前缀是下划线形式（neo_default_chatter_system_prompt 等），
        # 与其事件名前缀（neo_default_chatter:xxx，冒号）不同——两者都要放行。
        if not template_name.startswith("neo_default_chatter"):
            return EventDecision.PASS, params
        values = params.get("values")
        if not isinstance(values, dict):
            return EventDecision.PASS, params
        stream_id = str(values.get("stream_id", "") or "")
        if not stream_id:
            return EventDecision.PASS, params
        core = _get_core(self.plugin)
        if core is None:
            return EventDecision.PASS, params
        carry_text = await core.build_carry_text(stream_id, register_pending=True)
        if not carry_text:
            return EventDecision.SUCCESS, params  # 无内容不追加，但放行默认链
        existing_extra = str(values.get("extra", "") or "")
        values["extra"] = (
            f"{existing_extra}\n{carry_text}" if existing_extra else carry_text
        )
        return EventDecision.SUCCESS, params


class FocusGateHandler(BaseEventHandler):
    """NDFC ``:preprocess`` 串行专注门（可选，默认关）。

    serial 模式的严格执行者：焦点在他群时，把非焦点群的 preprocess 置为
    ``proceed=False``，让该群会话继续 Wait——真人不会同时出现在两个群发言。
    强提及（@/回复）默认豁免，保住响应性底线。

    weight=90：高于 probability_bypass(1) 与 sub_agent_decision(0)，
    在门禁链最前面裁决；沿 ``SUCCESS`` 继续让 NDFC 内置链消化决策字段。
    """

    name = "focus_gate"
    description = "漫游专注门：serial 模式下拦截非焦点群的发言决策"
    weight = 90
    init_subscribe = [_NDFC_PREPROCESS]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """非焦点群的会话决策置为不响应。"""
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if cfg is None or not cfg.roamer.enabled or not cfg.focus_gate.enabled:
            return EventDecision.PASS, params
        core = _get_core(self.plugin)
        if core is None:
            return EventDecision.PASS, params
        snapshot = core.planner.snapshot(focus_stream=core.focus_stream)
        if snapshot.mode != "serial" or snapshot.focus_stream is None:
            return EventDecision.PASS, params
        stream_id = str(params.get("stream_id", "") or "")
        if not stream_id or stream_id == snapshot.focus_stream:
            return EventDecision.PASS, params
        if not core.planner.in_domain(stream_id):
            return EventDecision.PASS, params  # 漫游域外不受约束
        # 私聊永不拦截：直接对话优先于串行专注，私聊也要能用
        if core.planner.is_private_stream(stream_id):
            return EventDecision.PASS, params
        if cfg.focus_gate.strong_mention_exempt and self._has_strong_mention(params):
            return EventDecision.PASS, params
        params["proceed"] = False
        params["reason"] = "roamer: 人在别处（焦点在其他聊天）"
        return EventDecision.SUCCESS, params

    def _has_strong_mention(self, params: dict[str, Any]) -> bool:
        """从 preprocess payload 的未读列表判断是否有强提及。

        私聊消息视为强提及（直接对话）。"""
        bot_id = bot_platform_id()
        for message in params.get("unreads") or []:
            if str(getattr(message, "chat_type", "") or "") == "private":
                return True
            if _reply_to_bot(message, bot_id):
                return True
            text = _extract_text(message)
            if text and _MENTION_PATTERN.search(text):
                return True
        return False


class BehaviorObserveHandler(BaseEventHandler):
    """``after_tool_call`` / ``after_action_call`` 行为镜像 handler（纯观察）。

    用户需求原话：「不仅要搬运原上下文我更希望搬运原上下文行为，包括
    工具调用等等任何内容，任何行为」「框架怎么构建上下文，使用什么工具，
    就原样搬运」。

    机制：订阅框架两个行为后置事件（``tool_manager/tool_use.py`` 与
    ``action_manager.py`` 发布），把 Bot 在漫游域内的工具/动作调用
    （名称 + 参数 + 结果摘要 + 成败）镜像进插件私有库 ``mirror_actions``
    表，供 :mod:`..core.carry` 生成「你在这里做过的事」行为流水段。

    事件 payload 字段（两个事件同构）：

    - ``tool_name`` / ``action_name``：组件名；
    - ``args``：调用参数 dict；
    - ``result``：执行结果（任意类型，取 str 摘要）；
    - ``success``：成败；
    - ``message``：触发调用的 Message（取 stream_id）。

    非侵入保证：永远 PASS、镜像失败仅 debug 日志、域外流直接忽略。
    """

    name = "behavior_observe"
    description = "漫游行为镜像：记录 bot 在漫游域内的工具/动作调用流水"
    weight = 50
    init_subscribe = ["after_tool_call", "after_action_call"]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """把漫游域内的行为调用镜像进插件私有库。"""
        core = _get_core(self.plugin)
        if core is None or not core.enabled:
            return EventDecision.PASS, params
        message = params.get("message")
        stream_id = str(getattr(message, "stream_id", "") or "")
        if not stream_id or not core.planner.in_domain(stream_id):
            return EventDecision.PASS, params
        # tool 事件带 tool_name，action 事件带 action_name（payload 同构）
        if "tool_name" in params:
            kind, name = "tool", str(params.get("tool_name") or "")
        else:
            kind, name = "action", str(params.get("action_name") or "")
        if not name:
            return EventDecision.PASS, params
        args = params.get("args")
        result = params.get("result")
        success = bool(params.get("success"))
        try:
            await mirror_action(
                stream_id=stream_id,
                kind=kind,
                name=name,
                args=args if isinstance(args, dict) else None,
                result_text=_result_summary(result),
                success=success,
            )
        except Exception as error:  # noqa: BLE001  镜像失败不阻断调用链
            logger.debug(f"行为镜像失败 {kind}/{name}: {error}")
        return EventDecision.PASS, params


def _result_summary(result: Any, limit: int = 500) -> str:
    """把任意类型的结果压成单行文本摘要（截断存储）。"""
    if result is None:
        return ""
    try:
        text = str(result)
    except Exception:  # noqa: BLE001  repr 兜底
        text = repr(result)
    return " ".join(text.split())[:limit]


class CarryAckHandler(BaseEventHandler):
    """搬运 ACK 游标提交 handler（纯观察，永远 PASS）。

    生命周期闭环：

    - ``visit()`` / 常驻注入生成搬运块时登记待 ACK 批次
      （``carry_pending[stream] = 游标上界``）；
    - reminder 是 ``consume=once``：payload 构建时即焚毁——若请求失败，
      内容已丢但游标未推，下次 visit 自动补发；
    - ``after_llm_request(success=True)``：模型确实见到了内容 →
      推进 ``carry_cursors[stream]``，后续只搬运更新内容；
    - ``on_llm_request_failed``：丢弃待 ACK 批次（游标不动，内容不丢）。

    流归属由 ``meta_data["stream_id"]`` 判定（chatter 经
    ``create_llm_request(stream_id=...)`` 自动注入，见 ``llm_api.py``）；
    其他插件的不带 stream_id 的请求不受影响。
    """

    name = "carry_ack"
    description = "搬运 ACK：LLM 请求成功后推进跨群搬运游标"
    weight = 50
    init_subscribe = [
        "before_llm_request",
        "after_llm_request",
        "on_llm_request_failed",
    ]

    async def execute(
        self, event_name: str, params: dict[str, Any]
    ) -> tuple[EventDecision, dict[str, Any]]:
        """在请求前绑定 carry 批次，成功或失败时只结算该批次。"""
        core = _get_core(self.plugin)
        if core is None or not core.enabled:
            return EventDecision.PASS, params
        metadata = params.get("meta_data")
        if not isinstance(metadata, dict):
            return EventDecision.PASS, params
        stream_id = str(metadata.get("stream_id", "") or "")
        if not stream_id:
            return EventDecision.PASS, params
        if event_name == "before_llm_request":
            batch_id = _carry_batch_from_payloads(params.get("payloads"))
            if batch_id:
                metadata["roamer_carry_batch"] = batch_id
            return EventDecision.PASS, params
        batch_id = str(metadata.get("roamer_carry_batch", "") or "")
        if not batch_id:
            return EventDecision.PASS, params
        if event_name == "after_llm_request":
            if not params.get("success"):
                core.drop_carry_pending(batch_id, stream_id)
                return EventDecision.PASS, params
            if core.ack_carry_consumed(batch_id, stream_id):
                logger.debug(f"搬运 ACK：游标推进 stream={stream_id[:8]}")
        elif event_name == "on_llm_request_failed":
            core.drop_carry_pending(batch_id, stream_id)
        return EventDecision.PASS, params


def _carry_batch_from_payloads(payloads: Any) -> str:
    """从实际发送的 payload 中提取 roamer carry 批次标识。"""
    if not isinstance(payloads, list):
        return ""
    for payload in payloads:
        for part in getattr(payload, "content", ()):
            text = getattr(part, "text", part)
            if not isinstance(text, str):
                continue
            start = text.find(_CARRY_BATCH_PREFIX)
            if start < 0:
                continue
            end = text.find(_CARRY_BATCH_SUFFIX, start)
            if end < 0:
                continue
            return text[start + len(_CARRY_BATCH_PREFIX):end]
    return ""
