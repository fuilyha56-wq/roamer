"""Roamer 焦点调度器。

设计要点（docs/DESIGN.md §3.3）：

- **两种模式**：``serial``（全域单焦点，最拟人）与 ``parallel``（受限并发唤醒，
  仅账本互通）；
- **聊完判定**：焦点被 ``Stop`` 释放 / 空闲超时 / 硬上限强制释放；
- **兴趣分**：未读数（对数曲线）+ 强提及插队 + 私聊直通 + 距上次回访时长；
- **节流**：``min_return_interval`` 防止高频横跳。

并发安全性：asyncio 单线程语义；本类方法内部不 ``await``。
未读信息通过 :meth:`note_unread` ``ON_MESSAGE_RECEIVED`` 观察喂入。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta


@dataclass(slots=True)
class _StreamState:
    """单个漫游域成员的调度状态。"""

    stream_id: str
    #: 流类型（"group" / "private" / 其他），全流域模式下用于私聊直通判定
    chat_type: str = ""
    unread_count: int = 0
    has_strong_mention: bool = False
    last_visit: datetime | None = None
    last_llm_activity: datetime | None = None


@dataclass(slots=True)
class FocusDecision:
    """一次调度决策的结果。"""

    #: 建议唤醒的 stream_id；None 表示本 tick 不唤醒任何人
    wake_stream: str | None = None
    #: 决策原因（日志观测用）
    reason: str = ""


@dataclass(slots=True)
class ParallelDecision:
    """parallel 模式一次 tick 的并发唤醒批次。"""

    #: 本 tick 应并发唤醒的流列表（已按兴趣分降序）
    wake_streams: list[str] = field(default_factory=list)
    #: 决策原因（日志观测用）
    reason: str = ""


@dataclass(slots=True)
class FocusSnapshot:
    """专注门查询用的调度快照。"""

    #: serial 模式下的当前焦点群；parallel 模式恒为 None
    focus_stream: str | None = None
    #: 模式名
    mode: str = "serial"


class RoamingPlanner:
    """漫游域焦点调度器（插件内单例，由 RoamerCore 持有）。"""

    def __init__(
        self,
        *,
        mode: str = "serial",
        all_streams: bool = False,
        focus_idle_timeout: int = 240,
        max_focus_hold_minutes: int = 20,
        min_return_interval: int = 90,
        at_bot_weight: float = 1000.0,
        private_weight: float = 500.0,
        curiosity_log_base: float = 2.0,
        unread_weight: float = 6.0,
        revisit_weight: float = 1.0,
    ) -> None:
        """初始化调度器。

        Args:
            mode: 调度模式（serial|parallel）。
            all_streams: 是否全流域（适配所有聊天流，含私聊）。
            focus_idle_timeout: 焦点空闲多少秒视为「聊完」。
            max_focus_hold_minutes: 焦点最长占用分钟数，超时强制释放。
            min_return_interval: 同群两次成为焦点的最小间隔秒数。
            at_bot_weight: 强提及固定加分。
            private_weight: 私聊固定加分。
            curiosity_log_base: 好奇心对数底数（>1，非法值回退 2.0）。
            unread_weight: 未读对数得分放大系数。
            revisit_weight: 距上次回访每 10 分钟的加分系数。
        """
        self.mode = mode if mode in ("serial", "parallel") else "serial"
        self.all_streams = bool(all_streams)
        self.focus_idle_timeout = max(30, int(focus_idle_timeout))
        self.max_focus_hold = timedelta(minutes=max(1, int(max_focus_hold_minutes)))
        self.min_return_interval = max(0, int(min_return_interval))
        self.at_bot_weight = max(0.0, float(at_bot_weight))
        self.private_weight = max(0.0, float(private_weight))
        base = float(curiosity_log_base)
        self.curiosity_log_base = base if base > 1.0 else 2.0
        self.unread_weight = max(0.0, float(unread_weight))
        self.revisit_weight = max(0.0, float(revisit_weight))
        self._states: dict[str, _StreamState] = {}
        self._focus_since: datetime | None = None

    # ------------------------------------------------------------------ 域管理

    def set_streams(self, stream_ids: list[str]) -> None:
        """重设漫游域成员（保留已知成员的状态）。"""
        wanted = list(dict.fromkeys(stream_ids))
        for sid in wanted:
            if sid not in self._states:
                self._states[sid] = _StreamState(stream_id=sid)
        for sid in [s for s in self._states if s not in wanted]:
            del self._states[sid]

    def in_domain(self, stream_id: str) -> bool:
        """判断流是否在漫游域内。

        全流域模式（``all_streams=True``）：任何流都属于漫游域；
        成员列表此时只是「已观察到的流」，未观察到的流同样约束生效。
        """
        if self.all_streams:
            return stream_id in self._states or bool(stream_id)
        return stream_id in self._states

    def note_stream_type(self, stream_id: str, chat_type: str) -> None:
        """登记（或更新）流的聊天类型，并确保它在域内。

        全流域模式下未观察过的新流经此自动入域；
        白名单模式下只更新已入域成员的类型，**不会**把未知流拉进域
        （否则白名单形同虚设）。

        Args:
            stream_id: 流 ID。
            chat_type: ``"private"`` / ``"group"`` / 其他。
        """
        state = self._states.get(stream_id)
        if state is None:
            if not self.all_streams:
                return
            self._states[stream_id] = _StreamState(
                stream_id=stream_id, chat_type=chat_type
            )
            return
        if chat_type and state.chat_type != chat_type:
            state.chat_type = chat_type

    def is_private_stream(self, stream_id: str) -> bool:
        """判断流是否为私聊（仅对已登记类型的流可靠）。"""
        state = self._states.get(stream_id)
        return state is not None and state.chat_type == "private"

    def chat_type_of(self, stream_id: str) -> str:
        """返回已登记的流类型（``private`` / ``group`` / 其他）；未登记返回空串。"""
        state = self._states.get(stream_id)
        return state.chat_type if state is not None else ""

    def domain(self) -> list[str]:
        """返回漫游域成员列表。"""
        return list(self._states)

    # ------------------------------------------------------------------ 观测喂入

    def note_unread(
        self,
        *,
        stream_id: str,
        strong_mention: bool,
        now: datetime,
        count: int = 1,
    ) -> None:
        """记录漫游域内一条新到的用户消息。

        Args:
            stream_id: 消息所在流。
            strong_mention: 是否强提及（@bot / 回复 bot）。
            now: 消息时间。
            count: 批量记数（默认 1）。
        """
        state = self._states.get(stream_id)
        if state is None:
            return
        state.unread_count += max(1, count)
        if strong_mention:
            state.has_strong_mention = True

    def note_chatter_active(self, stream_id: str, now: datetime) -> None:
        """记录焦点群内的会话活动（LLM 请求 / 发言），用于空闲判定。"""
        state = self._states.get(stream_id)
        if state is not None:
            state.last_llm_activity = now

    def note_visited(self, stream_id: str, now: datetime) -> None:
        """记录一次成功唤醒（回访），清零未读并重置强提及标记。"""
        state = self._states.get(stream_id)
        if state is None:
            return
        state.unread_count = 0
        state.has_strong_mention = False
        state.last_visit = now

    def last_visit_of(self, stream_id: str) -> datetime | None:
        """返回某群上次回访时间（简报 since 边界）。"""
        state = self._states.get(stream_id)
        return state.last_visit if state else None

    def last_activity_of(self, stream_id: str) -> datetime | None:
        """返回某流最近的会话活动时间（唤醒/LLM 请求/发言，空闲判定用）。"""
        state = self._states.get(stream_id)
        return state.last_llm_activity if state else None

    # ------------------------------------------------------------------ 调度

    def release_focus(self, now: datetime, reason: str = "") -> bool:
        """手动释放焦点（会话产出 Stop 时由 Service 调用）。

        Args:
            now: 当前时间（保留参数以对齐调用方签名）。
            reason: 释放原因（仅日志观测）。

        Returns:
            bool: 之前存在焦点占用返回 True。
        """
        if self._focus_since is None:
            return False
        self._focus_since = None
        _ = reason
        return True

    # focus 的具体群由 Service 持有（单一真相）；Planner 只提供判定与评分。

    def should_release(
        self, *, focus_stream: str | None, last_activity: datetime | None, now: datetime
    ) -> tuple[bool, str]:
        """判定当前焦点是否应释放（聊完判定）。

        Args:
            focus_stream: 当前焦点群（None 表示无焦点）。
            last_activity: 焦点群最近一次会话活动时间。
            now: 当前时间。

        Returns:
            (应释放, 原因)：无焦点时返回 (False, "")。
        """
        if focus_stream is None:
            return False, ""
        if last_activity is not None and now - last_activity > timedelta(
            seconds=self.focus_idle_timeout
        ):
            return True, f"焦点空闲超过 {self.focus_idle_timeout}s"
        if self._focus_since is not None:
            # 硬上限从最近活动起算：活跃会话不被周期性打断
            # （静默会话由上面的空闲判定兜底；活动缺失时退回占用起点）
            anchor = (
                max(self._focus_since, last_activity)
                if last_activity is not None
                else self._focus_since
            )
            if now - anchor > self.max_focus_hold:
                return True, f"焦点占用超过 {self.max_focus_hold}"
        return False, ""

    def acquire_focus(self, now: datetime) -> None:
        """标记焦点占用开始（Service 在唤醒某群前调用）。"""
        self._focus_since = now

    def score(self, stream_id: str, now: datetime) -> float:
        """计算某群的兴趣分（选下一站依据）。

        - 强提及在未读/回访分**之上**再叠加固定插队分（被点名就该回去；
          叠加而非替换，多个群同时 @ 时未读多/等得久的排前面）；
        - 私聊直通信：直接对话的优先级天然高于群聊插队；
        - 未读数按**对数曲线**折算（刷屏群不线性霸占焦点，
          高未读区仍保留区分度）；
        - 越久没回去分越高（每 10 分钟一档，封顶 10 档）。
        """
        state = self._states.get(stream_id)
        if state is None:
            return 0.0
        private_bonus = self.private_weight if state.chat_type == "private" else 0.0
        dynamic = self.unread_weight * math.log(
            state.unread_count + 1, self.curiosity_log_base
        )
        if state.last_visit is None:
            dynamic += 5.0 * self.revisit_weight  # 从没去过：优先探索
        else:
            gap = (now - state.last_visit).total_seconds()
            # 每 10 分钟一档，封顶 10 档（防长期冷宫群分值无限膨胀）
            dynamic += self.revisit_weight * min(
                10.0, max(0.0, gap) / 600.0
            )
        if state.has_strong_mention:
            return self.at_bot_weight + private_bonus + dynamic
        return private_bonus + dynamic

    def pick_next(self, now: datetime) -> FocusDecision:
        """选出本 tick 应唤醒的流（serial 模式的核心）。

        全流域模式下以「已观察到的新消息流」为候选；私聊带直通信加成。

        Returns:
            FocusDecision: ``wake_stream=None`` 表示无人可唤醒。
        """
        if self.mode != "serial":
            return FocusDecision(None, "parallel 模式不做焦点调度")
        candidates: list[tuple[float, str]] = []
        for sid in self._states:
            state = self._states[sid]
            # 节流：上次刚回访过的流暂不考虑（强提及/私聊豁免节流）
            throttle_exempt = state.has_strong_mention or state.chat_type == "private"
            if (
                not throttle_exempt
                and state.last_visit is not None
                and (now - state.last_visit).total_seconds() < self.min_return_interval
            ):
                continue
            # 没有未读且没被提及：去了也没事干（真人不会空转跑群）
            if state.unread_count <= 0 and not state.has_strong_mention:
                continue
            # 平分时按未读数决胜（而非 stream_id 序），sid 只作最终稳定排序
            candidates.append((self.score(sid, now), state.unread_count, sid))
        if not candidates:
            return FocusDecision(None, "无可唤醒候选")
        candidates.sort(reverse=True)
        best_score, _, best_sid = candidates[0]
        return FocusDecision(best_sid, f"兴趣分 {best_score:.1f}")

    def pick_parallel_batch(
        self, now: datetime, *, max_wakes: int
    ) -> ParallelDecision:
        """选出 parallel 模式本 tick 应并发唤醒的流批次。

        与 serial 的 :meth:`pick_next` 共用同一套兴趣分与节流规则；
        区别在于不设全域单焦点，而是按分数降序取前 ``max_wakes`` 个
        互不相同的流并发唤醒。

        Args:
            now: 当前时间。
            max_wakes: 单次 tick 的并发唤醒上限（<=0 视为 1）。

        Returns:
            ParallelDecision: ``wake_streams`` 为空表示本 tick 不唤醒。
        """
        if self.mode != "parallel":
            return ParallelDecision([], "非 parallel 模式不做并发唤醒")
        cap = max(1, int(max_wakes))
        candidates: list[tuple[float, str]] = []
        for sid in self._states:
            state = self._states[sid]
            throttle_exempt = state.has_strong_mention or state.chat_type == "private"
            if (
                not throttle_exempt
                and state.last_visit is not None
                and (now - state.last_visit).total_seconds() < self.min_return_interval
            ):
                continue
            if state.unread_count <= 0 and not state.has_strong_mention:
                continue
            candidates.append((self.score(sid, now), state.unread_count, sid))
        candidates.sort(reverse=True)
        picked = [sid for _, _, sid in candidates[:cap]]
        reason = (
            f"并发唤醒 {len(picked)} 个流（兴趣分 "
            f"{', '.join(f'{s:.1f}' for s, _, _ in candidates[:cap])}）"
            if picked
            else "无可唤醒候选"
        )
        return ParallelDecision(picked, reason)

    # ------------------------------------------------------------------ 快照

    def snapshot(self, *, focus_stream: str | None) -> FocusSnapshot:
        """返回专注门查询用的调度快照。"""
        if self.mode != "serial":
            return FocusSnapshot(focus_stream=None, mode=self.mode)
        return FocusSnapshot(focus_stream=focus_stream, mode=self.mode)

    def status_lines(self, *, focus_stream: str | None, now: datetime) -> list[str]:
        """返回 /roamer status 展示用的状态行。"""
        lines = [f"模式：{self.mode}" + ("（全流域）" if self.all_streams else "")]
        lines.append(f"当前焦点：{focus_stream or '（无）'}")
        for sid, state in sorted(self._states.items()):
            visit = (
                state.last_visit.strftime("%H:%M:%S") if state.last_visit else "从未"
            )
            mention = " [@提及]" if state.has_strong_mention else ""
            kind = " [私聊]" if state.chat_type == "private" else ""
            lines.append(
                f"- {sid[:12]}…{kind}：未读 {state.unread_count}{mention}，"
                f"上次回访 {visit}，兴趣分 {self.score(sid, now):.1f}"
            )
        return lines
