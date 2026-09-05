"""Roamer 单元测试。

覆盖 v0.2.0 新增机制：

- 对数吸引力曲线（高未读区分度、权重配置传递）；
- 搬运 ACK 游标（visit 登记 → 成功推进 → 失败保留）；
- 方向披露矩阵（私聊默认不出群）；
- cross_stream_feed Tool（拉取即消费推进游标）。

框架依赖（storage/stream/service_api）全部以桩替换，纯逻辑验证。
"""

from __future__ import annotations

import importlib
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

PACKAGE = "plugins.roamer"

config_module = importlib.import_module(f"{PACKAGE}.config")
planner_module = importlib.import_module(f"{PACKAGE}.core.planner")
carry_module = importlib.import_module(f"{PACKAGE}.core.carry")
service_module = importlib.import_module(f"{PACKAGE}.core.service")

RoamerConfig = config_module.RoamerConfig
RoamingPlanner = planner_module.RoamingPlanner


@pytest.fixture()
def fresh_shared_state() -> Any:
    """每个测试前重置 service 的类级共享状态，避免跨测试污染。"""
    service_module._SharedState.carry_cursors = {}
    service_module._SharedState.carry_pending = {}
    yield service_module._SharedState
    service_module._SharedState.carry_cursors = {}
    service_module._SharedState.carry_pending = {}


# ---------------------------------------------------------------- 对数吸引力


class TestLogAttraction:
    """对数吸引力曲线的行为验证。"""

    def test_high_unread_keeps_discrimination(self) -> None:
        """高未读区仍保留区分度：100 未读应明显低于 1000 未读。"""
        planner = RoamingPlanner(unread_weight=6.0, curiosity_log_base=2.0)
        now = datetime.now().astimezone()
        planner.set_streams(["a", "b"])
        planner.note_unread(stream_id="a", strong_mention=False, now=now, count=100)
        planner.note_unread(stream_id="b", strong_mention=False, now=now, count=1000)
        # 两流均从未回访（+5 探索分相等），不影响相对比较
        assert planner.score("b", now) > planner.score("a", now)

    def test_mention_beats_unread(self) -> None:
        """强提及仍是最强插队信号。"""
        planner = RoamingPlanner(
            at_bot_weight=1000.0, unread_weight=6.0
        )
        now = datetime.now().astimezone()
        planner.set_streams(["a", "b"])
        planner.note_unread(stream_id="a", strong_mention=True, now=now, count=1)
        planner.note_unread(stream_id="b", strong_mention=False, now=now, count=500)
        assert planner.score("a", now) > planner.score("b", now)

    def test_invalid_log_base_falls_back(self) -> None:
        """非法对数底数（<=1）回退 2.0 而非抛错。"""
        planner = RoamingPlanner(curiosity_log_base=0.5)
        assert planner.curiosity_log_base == 2.0

    def test_weights_flow_from_config(self) -> None:
        """attraction 配置节经 apply_config 传入 planner。"""
        cfg = RoamerConfig()
        cfg.attraction.at_bot_weight = 1234.0
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        core.apply_config(cfg)
        assert core.planner.at_bot_weight == 1234.0


# ---------------------------------------------------------------- 并行调度


class TestParallelScheduling:
    """parallel 模式的并发唤醒批次。"""

    def test_parallel_batch_respects_cap(self) -> None:
        """并发唤醒数量受上限约束，且按兴趣分降序。"""
        planner = RoamingPlanner(mode="parallel")
        now = datetime.now().astimezone()
        planner.set_streams(["low", "mid", "high"])
        planner.note_unread(stream_id="low", strong_mention=False, now=now, count=1)
        planner.note_unread(stream_id="mid", strong_mention=False, now=now, count=5)
        planner.note_unread(stream_id="high", strong_mention=False, now=now, count=50)
        decision = planner.pick_parallel_batch(now, max_wakes=2)
        assert decision.wake_streams == ["high", "mid"]

    def test_parallel_batch_throttles_recent_visits(self) -> None:
        """刚回访过的流（无强提及/私聊豁免）本 tick 不再唤醒。"""
        planner = RoamingPlanner(mode="parallel", min_return_interval=90)
        now = datetime.now().astimezone()
        planner.set_streams(["a", "b"])
        planner.note_unread(stream_id="a", strong_mention=False, now=now, count=3)
        planner.note_unread(stream_id="b", strong_mention=False, now=now, count=3)
        planner.note_visited("a", now)
        decision = planner.pick_parallel_batch(now, max_wakes=2)
        assert decision.wake_streams == ["b"]

    def test_serial_mode_rejects_parallel_batch(self) -> None:
        """serial 模式不产生并发批次。"""
        planner = RoamingPlanner(mode="serial")
        now = datetime.now().astimezone()
        planner.set_streams(["a"])
        planner.note_unread(stream_id="a", strong_mention=False, now=now, count=1)
        decision = planner.pick_parallel_batch(now, max_wakes=2)
        assert decision.wake_streams == []

    @pytest.mark.asyncio
    async def test_parallel_tick_wakes_multiple_streams(
        self, fresh_shared_state: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """parallel 模式 tick 并发唤醒多个流且不设焦点。"""
        state = fresh_shared_state
        cfg = RoamerConfig()
        cfg.roamer.mode = "parallel"
        cfg.roamer.max_parallel_wakes = 2
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        core.apply_config(cfg)
        now = datetime.now().astimezone()
        core.planner.note_stream_type("s1", "group")
        core.planner.note_stream_type("s2", "group")
        core.planner.note_unread(stream_id="s1", strong_mention=False, now=now, count=2)
        core.planner.note_unread(stream_id="s2", strong_mention=False, now=now, count=2)

        resumed: list[str] = []

        async def fake_resume(stream_id: str, *args: Any, **kwargs: Any) -> bool:
            resumed.append(stream_id)
            return True

        async def fake_reminder(stream_id: str, content: str) -> bool:
            return True

        async def fake_names(self: Any) -> dict[str, str]:
            return {}

        async def fake_prompt(self: Any, stream_id: str, names: Any = None) -> str:
            return "resume"

        async def fake_carry(self: Any, target_stream: str, **kwargs: Any) -> str:
            return ""

        async def fake_carry_no_self(target_stream: str, **kwargs: Any) -> str:
            return ""

        monkeypatch.setattr(
            service_module, "get_chatter_manager", lambda: SimpleNamespace(resume_chatter=fake_resume)
        )
        monkeypatch.setattr(service_module, "set_cross_stream_reminder", fake_reminder)
        monkeypatch.setattr(service_module.RoamerCore, "_stream_names", fake_names)
        monkeypatch.setattr(service_module.RoamerCore, "_build_resume_prompt", fake_prompt)
        monkeypatch.setattr(core, "build_carry_text", fake_carry_no_self)

        await core._tick_once()
        assert sorted(resumed) == ["s1", "s2"]
        assert state.focus_stream is None


# ---------------------------------------------------------------- ACK 游标


class TestCarryCursorAck:
    """搬运 ACK 游标生命周期。"""

    def test_ack_advances_cursor(self, fresh_shared_state: Any) -> None:
        """成功 ACK 推进游标到登记上界。"""
        state = fresh_shared_state
        state.carry_pending["batch-1"] = ("s1", 1000.0)
        cfg = RoamerConfig()
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        assert core.ack_carry_consumed("batch-1", "s1") is True
        assert state.carry_cursors["s1"] == 1000.0
        assert "batch-1" not in state.carry_pending

    def test_failed_request_keeps_cursor(self, fresh_shared_state: Any) -> None:
        """请求失败丢 pending 但游标不动（内容下次补发）。"""
        state = fresh_shared_state
        state.carry_cursors["s1"] = 500.0
        state.carry_pending["batch-1"] = ("s1", 900.0)
        cfg = RoamerConfig()
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        core.drop_carry_pending("batch-1", "s1")
        assert "batch-1" not in state.carry_pending
        assert state.carry_cursors["s1"] == 500.0

    def test_ack_without_pending_is_noop(self, fresh_shared_state: Any) -> None:
        """无待 ACK 批次时不推进。"""
        cfg = RoamerConfig()
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        assert core.ack_carry_consumed("unknown", "s1") is False

    def test_ack_rejects_wrong_stream(self, fresh_shared_state: Any) -> None:
        """批次不能被另一个流的请求提交。"""
        state = fresh_shared_state
        state.carry_pending["batch-1"] = ("s1", 1000.0)
        core = service_module.RoamerCore(SimpleNamespace(config=RoamerConfig()))
        assert core.ack_carry_consumed("batch-1", "other") is False
        assert "batch-1" in state.carry_pending

    def test_cursor_persist_roundtrip(self, fresh_shared_state: Any) -> None:
        """游标持久化 payload 往返。"""
        state = fresh_shared_state
        state.carry_cursors = {"s1": 111.5, "s2": 222.0}
        cfg = RoamerConfig()
        core = service_module.RoamerCore(SimpleNamespace(config=cfg))
        core._load_carry_cursors({"cursors": {"s1": 111.5, "s2": 222.0}})
        assert state.carry_cursors == {"s1": 111.5, "s2": 222.0}
        # 非法 payload 清空不抛错
        core._load_carry_cursors(None)
        assert state.carry_cursors == {}


# ---------------------------------------------------------------- 披露矩阵


class TestDisclosureMatrix:
    """carry 方向披露控制。"""

    def test_private_never_leaks_to_group_by_default(self) -> None:
        """默认配置：私聊消息不进群。"""
        carry = RoamerConfig().carry
        assert carry.disclosure_allowed("private", "group") is False

    def test_group_to_group_default_detailed(self) -> None:
        """默认配置：群→群允许。"""
        carry = RoamerConfig().carry
        assert carry.disclosure_allowed("group", "group") is True

    def test_group_to_private_default_detailed(self) -> None:
        """默认配置：群→私聊允许（bot 在私聊记得群里的事）。"""
        carry = RoamerConfig().carry
        assert carry.disclosure_allowed("group", "private") is True

    def test_private_to_private_always_off(self) -> None:
        """私聊→私聊无配置项，恒为关。"""
        carry = RoamerConfig().carry
        assert carry.disclosure_allowed("private", "private") is False

    def test_explicit_enable_private_to_group(self) -> None:
        """显式开启后私聊→群允许。"""
        cfg = RoamerConfig()
        cfg.carry.private_to_group = "detailed"
        assert cfg.carry.disclosure_allowed("private", "group") is True


# ---------------------------------------------------------------- 搬运块增量


class TestCarryIncremental:
    """collect_carry_block 的增量过滤（镜像库路径）。"""

    @pytest.mark.asyncio
    async def test_since_epoch_filters_old_items(
        self, fresh_shared_state: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """游标之前的消息与行为被过滤，之后保留。"""
        now_epoch = datetime.now().timestamp()
        grouped = {
            "src": [
                {
                    "speaker": "u1",
                    "speaker_name": "用户一",
                    "text": "旧消息",
                    "chat_type": "group",
                    "epoch": now_epoch - 100,
                },
                {
                    "speaker": "u2",
                    "speaker_name": "用户二",
                    "text": "新消息",
                    "chat_type": "group",
                    "epoch": now_epoch - 10,
                },
            ]
        }
        actions = {
            "src": [
                {
                    "kind": "tool",
                    "name": "old_tool",
                    "args_json": "{}",
                    "result_text": "",
                    "success": True,
                    "epoch": now_epoch - 100,
                },
                {
                    "kind": "tool",
                    "name": "new_tool",
                    "args_json": "{}",
                    "result_text": "",
                    "success": True,
                    "epoch": now_epoch - 5,
                },
            ]
        }

        async def fake_recent_messages(**kwargs: Any) -> dict:
            return grouped

        async def fake_recent_actions(**kwargs: Any) -> dict:
            return actions

        store = importlib.import_module(f"{PACKAGE}.core.store")
        monkeypatch.setattr(store, "recent_messages", fake_recent_messages)
        monkeypatch.setattr(store, "recent_actions", fake_recent_actions)

        async def fake_name(sid: str) -> str:
            return "源群"

        monkeypatch.setattr(carry_module, "_stream_display_name", fake_name)

        text, upper = await carry_module.collect_carry_block(
            target_stream="tgt",
            source_streams=["src"],
            since_epoch=now_epoch - 50,
        )
        assert "新消息" in text
        assert "旧消息" not in text
        assert "new_tool" in text
        assert "old_tool" not in text
        assert upper == pytest.approx(now_epoch - 5)

    @pytest.mark.asyncio
    async def test_disclosure_blocks_private_source(
        self, fresh_shared_state: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """披露矩阵过滤私聊源（默认关）。"""
        grouped = {
            "pm": [
                {
                    "speaker": "u1",
                    "speaker_name": "私聊者",
                    "text": "隐私内容",
                    "chat_type": "private",
                    "epoch": datetime.now().timestamp(),
                }
            ]
        }

        async def fake_recent_messages(**kwargs: Any) -> dict:
            return grouped

        async def fake_recent_actions(**kwargs: Any) -> dict:
            return {}

        store = importlib.import_module(f"{PACKAGE}.core.store")
        monkeypatch.setattr(store, "recent_messages", fake_recent_messages)
        monkeypatch.setattr(store, "recent_actions", fake_recent_actions)

        text, _ = await carry_module.collect_carry_block(
            target_stream="tgt",
            source_streams=["pm"],
            disclosure=RoamerConfig().carry.disclosure_allowed,
        )
        assert "隐私内容" not in text

    @pytest.mark.asyncio
    async def test_capped_result_does_not_offer_ack_cursor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """命中单流条数上限时不能 ACK，避免跳过未返回的数据。"""
        now_epoch = datetime.now().timestamp()

        async def fake_recent_messages(**kwargs: Any) -> dict:
            return {
                "src": [
                    {
                        "speaker": "u1",
                        "speaker_name": "用户一",
                        "text": "较早消息",
                        "chat_type": "group",
                        "epoch": now_epoch - 2,
                    },
                    {
                        "speaker": "u2",
                        "speaker_name": "用户二",
                        "text": "较新消息",
                        "chat_type": "group",
                        "epoch": now_epoch - 1,
                    },
                ]
            }

        async def fake_recent_actions(**kwargs: Any) -> dict:
            return {}

        store = importlib.import_module(f"{PACKAGE}.core.store")
        monkeypatch.setattr(store, "recent_messages", fake_recent_messages)
        monkeypatch.setattr(store, "recent_actions", fake_recent_actions)

        async def fake_name(sid: str) -> str:
            return "源群"

        monkeypatch.setattr(carry_module, "_stream_display_name", fake_name)

        text, upper = await carry_module.collect_carry_block(
            target_stream="tgt",
            source_streams=["src"],
            per_stream_count=2,
        )
        assert "较早消息" in text
        assert "较新消息" in text
        assert upper == 0.0

    @pytest.mark.asyncio
    async def test_action_only_stream_is_kept(
        self, fresh_shared_state: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """只有行为流水而没有消息时仍应展示该源流。"""
        fresh_shared_state.planner.note_stream_type("src", "group")

        async def fake_recent_messages(**kwargs: Any) -> dict:
            return {}

        async def fake_recent_actions(**kwargs: Any) -> dict:
            return {
                "src": [
                    {
                        "kind": "tool",
                        "name": "weather",
                        "args_json": "{}",
                        "result_text": "晴天",
                        "success": True,
                        "epoch": datetime.now().timestamp(),
                    }
                ]
            }

        store = importlib.import_module(f"{PACKAGE}.core.store")
        monkeypatch.setattr(store, "recent_messages", fake_recent_messages)
        monkeypatch.setattr(store, "recent_actions", fake_recent_actions)

        async def fake_name(sid: str) -> str:
            return "源群"

        monkeypatch.setattr(carry_module, "_stream_display_name", fake_name)
        text, upper = await carry_module.collect_carry_block(
            target_stream="tgt",
            source_streams=["src"],
            disclosure=RoamerConfig().carry.disclosure_allowed,
        )
        assert "weather" in text
        assert upper > 0.0


# ---------------------------------------------------------------- Tool


class TestCrossStreamFeedTool:
    """cross_stream_feed 拉取即消费语义。"""

    @pytest.mark.asyncio
    async def test_pull_advances_cursor(
        self,
        fresh_shared_state: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Tool 拉取成功后推进该流游标（与推送共享）。"""
        state = fresh_shared_state
        now_epoch = datetime.now().timestamp()
        tools = importlib.import_module(f"{PACKAGE}.tools")

        async def fake_collect(**kwargs: Any) -> tuple[str, float]:
            return "跨群内容", now_epoch

        monkeypatch.setattr(
            carry_module, "collect_carry_block", fake_collect
        )

        fake_core = SimpleNamespace(
            planner=SimpleNamespace(
                in_domain=lambda sid: True,
                domain=lambda: ["a", "b"],
            ),
        )
        monkeypatch.setattr(tools, "_get_core", lambda: fake_core)

        cfg = RoamerConfig()
        plugin = SimpleNamespace(config=cfg)
        tool = tools.CrossStreamFeedTool(plugin)
        tool._bind_runtime_context(stream_id="tgt")
        ok, result = await tool.execute()
        assert ok is True
        assert state.carry_cursors == {}
