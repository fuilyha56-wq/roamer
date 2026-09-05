"""Roamer 核心 Service：调度循环 + 唤醒执行 + 账本持久化。

生命周期：插件 ``on_plugin_loaded`` 时由 Plugin 启动 tick 循环
（经 ``task_manager`` 托管），``on_plugin_unloaded`` 时取消。

tick 循环职责（docs/DESIGN.md §6 时序）：

1. 账本老化 + 变更时批量持久化（``storage_api`` JSON 存储）；
2. serial 模式下做「聊完判定」→ 释放焦点 → 兴趣分选下一站 →
   ``resume_chatter(source="roamer", extra={"resume_prompt": 简报})``
   唤醒目标群会话；
3. parallel 模式做第 1 步，并在每 tick 按兴趣分并发唤醒至多 ``max_parallel_wakes`` 个流。

自动唤醒的 resume 经 NDFC 默认 ``:build_resume_prompt`` 的
``event.extra["resume_prompt"]`` 原生通道注入简报，零 NDFC 代码修改。
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from src.app.plugin_system.api import storage_api, stream_api
from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseService
from src.core.managers.chatter_manager import get_chatter_manager

from ..config import RoamerConfig
from .carry import collect_carry_block
from .ledger import RoamingLedger
from .planner import RoamingPlanner
from .reminder import set_cross_stream_reminder

if TYPE_CHECKING:
    from src.app.plugin_system.base import BasePlugin

logger = get_logger("roamer.service")

#: resume 触发来源标识（NDFC :build_resume_prompt 会进入 generic 分支读取 extra）
_RESUME_SOURCE = "roamer"
#: 存储命名空间
_STORE_NAME = "roamer_state"
#: 漫游域成员持久化键（命令动态增删的成员）
_DOMAIN_KEY = "domain"
#: 搬运 ACK 游标持久化键
_CARRY_CURSOR_KEY = "carry_cursors"
#: carry 文本中的请求级 ACK 标识；仅内部 handler 解析，不承载用户内容。
_CARRY_BATCH_PREFIX = "<!-- roamer_carry_batch:"
_CARRY_BATCH_SUFFIX = " -->"


class _SharedState:
    """跨 RoamerCore 实例共享的可变调度状态。

    框架的 ``service_manager.get_service()`` 每次调用都会构造**新的** Service
    实例（非单例，见 ``service_manager.py`` 的 ``get_service``）。若把
    ledger/planner/focus 存在实例属性上，插件 ``on_plugin_loaded`` 启动 tick
    循环的实例、各 handler 拿到的实例、命令拿到的实例会各自持有一份互不相通
    的状态。本类以类级单例收敛全部可变状态，保证任何实例读写同一份数据。
    """

    ledger: RoamingLedger = RoamingLedger()
    planner: RoamingPlanner = RoamingPlanner()
    focus_stream: str | None = None
    domain_overrides: set[str] | None = None
    task: asyncio.Task[None] | None = None
    #: 唤醒互斥锁：串行化所有 visit()，防止 tick 与手动命令并发唤醒两个群
    #: （visit 内部有多次 await IO，无锁时 serial 互斥会被击穿）
    visit_lock: asyncio.Lock = asyncio.Lock()
    #: 生命周期锁：串行化 start/stop，防止取消等待期间 start 检查旧 task 误跳过
    lifecycle_lock: asyncio.Lock = asyncio.Lock()
    #: 搬运 ACK 游标（stream_id -> 已确认消费的最大 epoch）：
    #: 只搬运该流上次**成功请求后确认**之后的新内容，消灭重复注入。
    #: LLM 请求失败时游标不动，下次 visit 自动补发（once-reminder
    #: 在 payload 构建时即焚毁，失败即丢，游标不推则内容不丢）。
    carry_cursors: dict[str, float] = {}
    #: 待 ACK 的搬运批次（batch_id -> (target_stream, 游标上界)）。
    #: 只有真正携带对应 batch 标识的 LLM 请求才可以提交或丢弃它。
    carry_pending: dict[str, tuple[str, float]] = {}


class RoamerCore(BaseService):
    """Roamer 调度核心，暴露给本插件 handler / command 共享状态。

    注意：可变状态全部在类级 :class:`_SharedState` 上（原因见该类 docstring），
    本类实例只是无状态视图。
    """

    name = "roamer_core"
    description = "多群漫游调度核心：焦点调度 + 一手账本 + 唤醒注入"

    def __init__(self, plugin: "BasePlugin") -> None:
        """初始化核心组件视图。

        Args:
            plugin: 宿主插件实例。
        """
        super().__init__(plugin)
        # 可变状态全部经 property 转发到类级 _SharedState（见该类 docstring）

    @property
    def ledger(self) -> RoamingLedger:
        """共享账本（类级单例）。"""
        return _SharedState.ledger

    @property
    def planner(self) -> RoamingPlanner:
        """共享调度器（类级单例）。"""
        return _SharedState.planner

    @property
    def focus_stream(self) -> str | None:
        """当前焦点群（serial 模式；parallel 恒 None）。"""
        return _SharedState.focus_stream

    # ------------------------------------------------------------------ 配置

    def apply_config(self, config: RoamerConfig | None) -> None:
        """从配置刷新账本 / 调度器参数与漫游域成员（写入共享状态）。"""
        if config is None:
            return
        roamer = config.roamer
        attraction = config.attraction
        _SharedState.ledger = RoamingLedger(
            window_hours=config.ledger.window_hours,
            max_entry_chars=config.ledger.max_entry_chars,
            max_entries_per_stream=config.ledger.max_entries_per_stream,
            max_user_entries_per_stream=config.ledger.max_user_entries_per_stream,
        )
        _SharedState.planner = RoamingPlanner(
            mode=roamer.normalized_mode(),
            all_streams=roamer.all_streams,
            focus_idle_timeout=roamer.focus_idle_timeout,
            max_focus_hold_minutes=roamer.max_focus_hold_minutes,
            min_return_interval=roamer.min_return_interval,
            at_bot_weight=attraction.at_bot_weight,
            private_weight=attraction.private_weight,
            curiosity_log_base=attraction.curiosity_log_base,
            unread_weight=attraction.unread_weight,
            revisit_weight=attraction.revisit_weight,
        )
        members = list(roamer.roaming_streams)
        if _SharedState.domain_overrides is not None:
            # 命令动态调整优先于（合并于）静态配置
            members = list(
                dict.fromkeys(members + sorted(_SharedState.domain_overrides))
            )
        _SharedState.planner.set_streams(members)

    @property
    def enabled(self) -> bool:
        """插件主开关状态（配置不可用时视为关闭）。"""
        cfg = self.plugin.config
        if isinstance(cfg, RoamerConfig):
            return bool(cfg.roamer.enabled)
        return False

    # ------------------------------------------------------------------ 生命周期

    def start(self) -> None:
        """启动 tick 循环（经 task_manager 托管）。

        与 :meth:`stop` 的竞态由生命周期锁保证：stop 在锁内先摘除 task 引用
        再等待取消完成，因此 stop 等待期间调用 start 不会误用「旧 task 未 done」
        而跳过启动（start 本身不 await，无需持锁）。
        """
        existing = _SharedState.task
        if existing is not None and not existing.done():
            return
        from src.kernel.concurrency import get_task_manager

        task = get_task_manager().create_task(
            self._tick_loop(), name="roamer_scheduler", daemon=True
        )
        _SharedState.task = task.task

    async def stop(self) -> None:
        """停止 tick 循环（幂等，经生命周期锁串行化）。"""
        async with _SharedState.lifecycle_lock:
            task = _SharedState.task
            if task is None:
                return
            _SharedState.task = None  # 先摘引用：锁等待中的 start 会看到 None 并正常启动
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001  停止路径宽容
                pass

    # ------------------------------------------------------------------ tick

    async def _tick_loop(self) -> None:
        """周期调度主循环。"""
        while True:
            try:
                cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
                interval = max(5, int(cfg.roamer.tick_seconds)) if cfg else 60
                await self._tick_once()
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001  循环体防崩
                logger.error(f"Roamer tick 异常: {error}", exc_info=error)
                await asyncio.sleep(30)

    async def _tick_once(self) -> None:
        """单次调度：老化 → 持久化 → serial 焦点判定 → 唤醒。"""
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if cfg is None or not cfg.roamer.enabled:
            return
        now = datetime.now().astimezone()

        # 1. 老化 + 脏时持久化
        aged = self.ledger.age(now)
        if aged:
            logger.debug(f"账本老化压缩 {aged} 条")
        if self.ledger.dirty:
            payload = self.ledger.to_payload()
            # await 期间新记账可能再次置脏：快照版本比对，丢标记仅当内容未变
            version_before = self.ledger.version
            await storage_api.save_json(
                "roamer_ledger", "ledger", payload
            )
            if self.ledger.version == version_before:
                self.ledger.mark_persisted()
        # 镜像库超龄清理（低频：每 ~10 个 tick 执行一次）
        self._cleanup_counter = getattr(self, "_cleanup_counter", 0) + 1
        if self._cleanup_counter >= 10:
            self._cleanup_counter = 0
            try:
                from .store import cleanup_expired

                await cleanup_expired()
            except Exception as error:  # noqa: BLE001
                logger.debug(f"镜像库清理失败: {error}")

        # 2. parallel：并发唤醒批次（受 max_parallel_wakes 上限约束）
        if self.planner.mode != "serial":
            _SharedState.focus_stream = None
            decision = self.planner.pick_parallel_batch(
                now, max_wakes=cfg.roamer.max_parallel_wakes
            )
            if not decision.wake_streams:
                return
            logger.info(f"并行唤醒批次：{decision.reason}")
            await asyncio.gather(
                *(
                    self.visit(sid, reason=decision.reason, now=now)
                    for sid in decision.wake_streams
                )
            )
            return

        # 3. serial：聊完判定 → 释放焦点
        if self.focus_stream is not None:
            state_activity = self._last_activity_of(self.focus_stream)
            should_release, reason = self.planner.should_release(
                focus_stream=self.focus_stream, last_activity=state_activity, now=now
            )
            if should_release:
                logger.info(f"焦点释放 [{self.focus_stream}]：{reason}")
                self.planner.release_focus(now, reason)
                self._record_roam_event_safe(self.focus_stream, "release", reason)
                _SharedState.focus_stream = None
                await self.save_carry_cursors()
            else:
                return  # 焦点占用中，本 tick 不唤醒别人

        # 4. 选下一站并唤醒
        decision = self.planner.pick_next(now)
        if decision.wake_stream is None:
            return
        await self.visit(decision.wake_stream, reason=decision.reason, now=now)

    def _last_activity_of(self, stream_id: str) -> datetime | None:
        """取焦点群最近会话活动时间（账本最新发言时间近似）。"""
        entries = self.ledger.entries_for(stream_id)
        return entries[0].time if entries else None

    # ------------------------------------------------------------------ 唤醒

    async def build_carry_text(
        self,
        target_stream: str,
        *,
        register_pending: bool = False,
    ) -> str:
        """为目标流构建跨群原文搬运块（resume 与每轮通道共用）。

        Args:
            target_stream: 当前会话所在流。
            register_pending: 为 True 时把本次注入的游标上界登记为
                待 ACK 批次（AFTER_LLM_REQUEST 成功后推进游标）。
                reminder 通道（visit 路径）与推送注入路径置 True；
                Tool 拉取路径置 False（LLM 主动拉取即已消费，无需 ACK）。

        Returns:
            str: 原文块文本；搬运关闭或无内容时返回空串。
        """
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if cfg is None or not cfg.carry.enabled:
            return ""
        since_epoch = _SharedState.carry_cursors.get(target_stream)
        carry_text, upper_epoch = await collect_carry_block(
            target_stream=target_stream,
            source_streams=self.planner.domain(),
            per_stream_count=cfg.carry.per_stream_count,
            max_streams=cfg.carry.max_streams_per_inject,
            lookback_minutes=cfg.carry.lookback_minutes,
            since_epoch=since_epoch,
            disclosure=cfg.carry.disclosure_allowed,
        )
        if register_pending and carry_text and upper_epoch > (since_epoch or 0.0):
            batch_id = uuid4().hex
            _SharedState.carry_pending[batch_id] = (target_stream, upper_epoch)
            carry_text = (
                f"{_CARRY_BATCH_PREFIX}{batch_id}{_CARRY_BATCH_SUFFIX}\n"
                f"{carry_text}"
            )
        return carry_text

    def ack_carry_consumed(self, batch_id: str, stream_id: str) -> bool:
        """LLM 请求成功后推进该流的搬运游标（待 ACK 批次提交）。

        Args:
            batch_id: 本次请求在 BEFORE_LLM_REQUEST 绑定的 carry 批次 ID。
            stream_id: AFTER_LLM_REQUEST meta_data 中的流 ID，用于防串流校验。

        Returns:
            bool: 实际推进了游标返回 True。
        """
        pending = _SharedState.carry_pending.get(batch_id)
        if pending is None or pending[0] != stream_id:
            return False
        _SharedState.carry_pending.pop(batch_id, None)
        upper_epoch = pending[1]
        if upper_epoch > _SharedState.carry_cursors.get(stream_id, 0.0):
            _SharedState.carry_cursors[stream_id] = upper_epoch
            return True
        return False

    def drop_carry_pending(self, batch_id: str, stream_id: str) -> None:
        """丢弃指定请求的待 ACK 批次，失败时游标不动、内容下次补发。"""
        pending = _SharedState.carry_pending.get(batch_id)
        if pending is not None and pending[0] == stream_id:
            _SharedState.carry_pending.pop(batch_id, None)

    async def visit(self, stream_id: str, *, reason: str = "", now: datetime | None = None) -> bool:
        """唤醒一个群的 NDFC 会话（带跨群原文搬运）。

        并发安全（docs/DESIGN.md §5）：

        - 经 :data:`_SharedState.visit_lock` 串行化——本方法内部有多次
          ``await`` IO（拉流名 / DB 搬运 / resume 注入），无锁时 tick 决策与
          ``/roamer visit`` 手动命令可并发进行，两次成功会把 focus 互相覆盖、
          同时唤醒两个群，击穿 serial 互斥；
        - 拿到锁后**重查焦点**：等待锁期间焦点可能已被他方占用。

        Args:
            stream_id: 目标聊天流。
            reason: 决策原因（日志）。
            now: 决策时间（缺省取当前）。

        Returns:
            bool: resume 注入成功返回 True。
        """
        moment = now or datetime.now().astimezone()
        async with _SharedState.visit_lock:
            # serial 模式下锁内重查：等待锁期间焦点可能已被占用
            if (
                self.planner.mode == "serial"
                and _SharedState.focus_stream is not None
                and _SharedState.focus_stream != stream_id
            ):
                logger.debug(
                    f"唤醒放弃 [{stream_id[:8]}]：锁等待期间焦点已被 "
                    f"[{_SharedState.focus_stream[:8]}] 占用"
                )
                return False
            names = await self._stream_names()
            carry_text = await self.build_carry_text(
                stream_id, register_pending=True
            )
            # 跨群动态经一次性的 stream reminder 送达（全聊天器通用，
            # chatter 请求时自动拾取，不依赖 NDFC resume extra）
            await set_cross_stream_reminder(stream_id, carry_text)
            # resume 事件文本必须锚定当前流：NDFC 默认 generic resume 是裸文本
            # （不经 user_prompt 模板渲染，无 transport_context），若不锚定，
            # LLM 会把「恢复」关联到搬运块里最显眼的其他流（如私聊）上下文，
            # 误以为自己在私聊里、把私聊口吻的回复发进群聊（线上真实故障）
            injected = await get_chatter_manager().resume_chatter(
                stream_id,
                source=_RESUME_SOURCE,
                extra={
                    "resume_prompt": await self._build_resume_prompt(
                        stream_id, names
                    )
                },
            )
            if not injected:
                logger.debug(
                    f"resume 触发失败（流未挂起或不存在），保留 reminder 待下次拾取: {stream_id}"
                )
                reminder_kept = bool(carry_text)
                return reminder_kept
            # 占用焦点并登记回访（仍在锁内，见上）；
            # parallel 模式无单焦点语义：不占用 focus，
            # 只登记回访/活跃（并行节流依赖 note_visited）
            if self.planner.mode == "serial":
                self.planner.acquire_focus(moment)
                _SharedState.focus_stream = stream_id
            self.planner.note_chatter_active(stream_id, moment)
            self.planner.note_visited(stream_id, moment)
            self._record_roam_event_safe(stream_id, "wake", reason or "手动")
            logger.info(
                f"漫游唤醒 [{names.get(stream_id, stream_id)}]：{reason or '手动'}"
            )
            return True

    def _record_roam_event_safe(self, stream_id: str, kind: str, reason: str) -> None:
        """写漫游事件流水（fire-and-forget，失败仅日志）。"""
        from src.kernel.concurrency import get_task_manager

        try:
            from .store import record_event

            get_task_manager().create_task(
                record_event(stream_id=stream_id, kind=kind, reason=reason),
                name=f"roamer_event_{kind}",
                daemon=True,
            )
        except Exception as error:  # noqa: BLE001  流水非关键路径
            logger.debug(f"漫游事件流水写入失败: {error}")

    def _last_visit_of(self, stream_id: str) -> datetime | None:
        """取目标群上次回访时间（简报的 since 边界）。"""
        return self.planner.last_visit_of(stream_id)

    async def _build_resume_prompt(
        self, stream_id: str, names: dict[str, str] | None = None
    ) -> str:
        """构造 resume 事件文本（经 NDFC ``extra["resume_prompt"]`` 通道注入）。

        NDFC 默认 generic resume 是裸 USER 文本，不经过 user_prompt 模板渲染，
        没有 ``<transport_context>``（对话名/聊天类型）——必须在这里显式锚定
        当前流，否则 LLM 可能把「恢复」关联到搬运块里最显眼的其他流上下文
        （线上故障：群聊被唤醒却以为在私聊，把私聊口吻的回复发进群）。

        Args:
            stream_id: 被唤醒的目标流。
            names: 流显示名映射（缺省现场拉取）。

        Returns:
            str: 带当前流锚点的 resume 提示文本。
        """
        names = names if names is not None else await self._stream_names()
        display = names.get(stream_id) or stream_id[:8]
        registered = self.planner.chat_type_of(stream_id)
        if registered == "private":
            chat_type = "私聊"
        elif registered == "group":
            chat_type = "群聊"
        else:
            chat_type = "聊天"  # 类型未登记（如手动 visit 陌生流）时中性表述
        return (
            f"系统事件：你在「{display}」（{chat_type}）的会话被唤醒。"
            "请基于已有上下文主动决定下一步。"
            "如果现在无需继续处理，请调用 pass_and_wait；"
            "如果需要回复或执行动作，请直接使用相应工具。"
            "注意：你的回复将发送到「{display}」（{chat_type}），"
            "与其他聊天无关。"
        )

    async def _stream_names(self) -> dict[str, str]:
        """取漫游域成员的显示名映射。"""
        names: dict[str, str] = {}
        for sid in self.planner.domain():
            try:
                chat_stream = await stream_api.get_stream(sid)
            except Exception:  # noqa: BLE001  观测性查询失败不致命
                chat_stream = None
            if chat_stream is not None and getattr(chat_stream, "stream_name", ""):
                names[sid] = chat_stream.stream_name
        return names

    # ------------------------------------------------------------------ 域管理（命令入口）

    async def join_domain(self, stream_id: str) -> bool:
        """把流加入漫游域（持久化到插件 KV 存储）。"""
        if _SharedState.domain_overrides is None:
            _SharedState.domain_overrides = set()
        _SharedState.domain_overrides.add(stream_id)
        self.planner.set_streams(self._merge_members())
        return await self._save_domain()

    async def leave_domain(self, stream_id: str) -> bool:
        """把流移出漫游域。"""
        if _SharedState.domain_overrides is not None:
            _SharedState.domain_overrides.discard(stream_id)
        self.planner.set_streams(self._merge_members())
        return await self._save_domain()

    def _merge_members(self) -> list[str]:
        """合并静态配置与动态覆盖的漫游域成员。"""
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        base = list(cfg.roamer.roaming_streams) if cfg else []
        if _SharedState.domain_overrides:
            base = list(
                dict.fromkeys(base + sorted(_SharedState.domain_overrides))
            )
        return base

    async def _save_domain(self) -> bool:
        """持久化动态漫游域成员。"""
        try:
            await storage_api.save_json(
                _STORE_NAME,
                _DOMAIN_KEY,
                {"streams": sorted(_SharedState.domain_overrides or set())},
            )
            return True
        except Exception as error:  # noqa: BLE001
            logger.warning(f"漫游域持久化失败: {error}")
            return False

    async def save_carry_cursors(self) -> None:
        """持久化搬运 ACK 游标（失败仅日志，下次成功请求可补推）。"""
        try:
            await storage_api.save_json(
                _STORE_NAME,
                _CARRY_CURSOR_KEY,
                {"cursors": dict(_SharedState.carry_cursors)},
            )
        except Exception as error:  # noqa: BLE001
            logger.debug(f"搬运游标持久化失败: {error}")

    def _load_carry_cursors(self, payload: dict | None) -> None:
        """从持久化 payload 恢复搬运游标。"""
        _SharedState.carry_cursors = {}
        if isinstance(payload, dict) and isinstance(payload.get("cursors"), dict):
            _SharedState.carry_cursors = {
                str(k): float(v)
                for k, v in payload["cursors"].items()
                if isinstance(v, (int, float))
            }

    async def restore_state(self) -> None:
        """启动时恢复持久化状态（动态域 + 账本 + 搬运游标 + 镜像库初始化）。"""
        try:
            domain = await storage_api.load_json(_STORE_NAME, _DOMAIN_KEY)
        except Exception:  # noqa: BLE001
            domain = None
        if isinstance(domain, dict) and isinstance(domain.get("streams"), list):
            _SharedState.domain_overrides = {str(s) for s in domain["streams"]}
        try:
            self._load_carry_cursors(
                await storage_api.load_json(_STORE_NAME, _CARRY_CURSOR_KEY)
            )
        except Exception:  # noqa: BLE001
            self._load_carry_cursors(None)
        try:
            payload = await storage_api.load_json("roamer_ledger", "ledger")
        except Exception:  # noqa: BLE001
            payload = None
        restored = self.ledger.load_payload(payload)
        if restored:
            logger.info(f"账本恢复 {restored} 条历史发言")
        self.apply_config(self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None)
        # 镜像库初始化（幂等）+ 超龄清理
        try:
            from .store import cleanup_expired, get_db

            await get_db().initialize()
            await cleanup_expired()
            logger.info("镜像库已就绪（data/roamer/data.db）")
        except Exception as error:  # noqa: BLE001  镜像库失败降级为主库回退
            logger.warning(f"镜像库初始化失败，搬运将回退主库: {error}")
