"""Roamer 插件入口。

生命周期（docs/DESIGN.md §8）：

- ``on_plugin_loaded``：恢复持久化状态 → 启动调度循环；
- ``on_plugin_unloaded``：停止调度循环。

组件注册由 manifest ``include`` 驱动；本插件不依赖任何 NDFC 代码，
全部通过事件字符串字面量与框架公开 manager 交互（零侵入约束）。
"""

from __future__ import annotations

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BasePlugin, register_plugin

from .commands import RoamerCommand
from .config import RoamerConfig
from .core.service import RoamerCore
from .handlers import (
    BehaviorObserveHandler,
    CarryAckHandler,
    CrossStreamInjectHandler,
    FocusGateHandler,
    LedgerRecordHandler,
    UnreadObserveHandler,
)
from .tools import CrossStreamFeedTool

logger = get_logger("roamer.plugin")


@register_plugin
class RoamerPlugin(BasePlugin):
    """多群漫游调度层：串行焦点 + 一手发言账本 + 跨群互通简报。"""

    plugin_name = "roamer"
    configs = [RoamerConfig]

    def get_components(self) -> list[type]:
        """按配置返回组件列表。"""
        cfg = self.config if isinstance(self.config, RoamerConfig) else RoamerConfig()
        if not cfg.roamer.enabled:
            logger.info("Roamer 插件未启用")
            return []
        # reminder 通道（全聊天器通用）为主路径；
        # 推送注入 handler 仅在 carry.inject_on_every_turn 开启时注册；
        # cross_stream_feed Tool（拉模型）与 carry ACK 游标 handler 常驻。
        components: list[type] = [
            RoamerCore,
            LedgerRecordHandler,
            UnreadObserveHandler,
            BehaviorObserveHandler,
            FocusGateHandler,
            CarryAckHandler,
            CrossStreamFeedTool,
        ]
        if cfg.carry.enabled and cfg.carry.inject_on_every_turn:
            components.append(CrossStreamInjectHandler)
        if cfg.roamer.command_enabled:
            components.append(RoamerCommand)
        return components

    async def on_plugin_loaded(self) -> None:
        """恢复状态并启动调度循环。"""
        core = self._core()
        if core is None:
            logger.info("RoamerCore 服务未注册，跳过调度循环启动")
            return
        await core.restore_state()
        core.start()
        logger.info(
            f"Roamer 已启动：模式={core.planner.mode}，"
            f"漫游域={len(core.planner.domain())} 个流"
        )

    async def on_plugin_unloaded(self) -> None:
        """停止调度循环。"""
        core = self._core()
        if core is not None:
            await core.stop()
        logger.info("Roamer 已停止")

    def _core(self) -> RoamerCore | None:
        """获取已注册的 RoamerCore 服务实例（可能尚未注册）。"""
        from src.app.plugin_system.api import service_api

        service = service_api.get_service("roamer:service:roamer_core")
        return service if isinstance(service, RoamerCore) else None
