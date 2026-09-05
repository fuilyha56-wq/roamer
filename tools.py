"""Roamer 工具组件。

`cross_stream_feed`：把其他漫游域成员的最新消息原文与行为流水交给 LLM
（拉模型）。Bot 想知道其他聊天动态时**自己调用**，而不是被动等待推送——

- 省 token：只在 Bot 主动需要时携带跨群内容，不占每轮 prompt；
- 全聊天器通用：Tool 是框架标准组件，NDFC / NFC / DFC 一视同仁；
- ACK 语义：拉取即消费，直接推进该流的搬运游标（与推送路径共享同一游标，
  避免拉取过的内容在下次 visit 时重复推送）。
"""

from __future__ import annotations

from typing import Annotated, Any

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.api import service_api
from src.core.components.base.tool import BaseTool

from .config import RoamerConfig
from .core.service import RoamerCore

logger = get_logger("roamer.tools")

#: RoamerCore 服务签名（plugin_name:component_type:component_name）
_CORE_SIGNATURE = "roamer:service:roamer_core"


def _get_core() -> RoamerCore | None:
    """获取 RoamerCore 服务实例（未注册时返回 None）。"""
    service = service_api.get_service(_CORE_SIGNATURE)
    return service if isinstance(service, RoamerCore) else None


class CrossStreamFeedTool(BaseTool):
    """跨群动态拉取工具：查看其他聊天流的最新消息与自己的行为流水。

    拉取结果与漫游唤醒时自动注入的「跨群动态」同源同格式（镜像库原文，
    零转写）；区别在于工具拉取不受推送节流限制，Bot 随时可查。
    """

    name = "cross_stream_feed"
    description = (
        "查看你在其他聊天流里错过的最新动态（消息原文与你自己的行为流水）。"
        "当你在当前聊天中需要回忆其他聊天的近况、或想确认那边有没有新消息时调用。"
    )

    async def execute(
        self,
        limit_per_stream: Annotated[
            int, "每个源流最多返回的消息条数（1-30，默认 10）"
        ] = 10,
        max_streams: Annotated[
            int, "最多覆盖的源流数量（1-5，默认 3）"
        ] = 3,
    ) -> tuple[bool, str | dict]:
        """拉取其他漫游域成员的最新动态。

        Args:
            limit_per_stream: 每个源流最多返回的消息条数。
            max_streams: 最多覆盖的源流数量。

        Returns:
            tuple[bool, str | dict]: (是否成功, 动态文本或错误说明)。
        """
        cfg = self.plugin.config if isinstance(self.plugin.config, RoamerConfig) else None
        if cfg is None or not cfg.roamer.enabled:
            return False, "Roamer 未启用，无跨群动态可查。"
        core = _get_core()
        if core is None:
            return False, "RoamerCore 服务未注册，无跨群动态可查。"
        current_stream = self.get_current_stream_id()
        if not current_stream:
            return False, "无法确定当前聊天流。"
        if not core.planner.in_domain(current_stream):
            return False, "当前聊天不在漫游域内，无跨群动态。"

        per_stream = max(1, min(30, int(limit_per_stream)))
        stream_cap = max(1, min(5, int(max_streams)))

        from .core.carry import collect_carry_block

        try:
            text, upper_epoch = await collect_carry_block(
                target_stream=current_stream,
                source_streams=core.planner.domain(),
                per_stream_count=per_stream,
                max_streams=stream_cap,
                lookback_minutes=max(
                    cfg.carry.lookback_minutes, 120
                ),  # 拉取窗口放宽：主动查询值得多回看一些
                since_epoch=None,  # 全窗口模式：主动拉取要看完整近况
                disclosure=cfg.carry.disclosure_allowed,
            )
        except Exception as error:  # noqa: BLE001  工具失败降级为提示
            logger.warning(f"cross_stream_feed 拉取失败: {error}", exc_info=error)
            return False, f"拉取跨群动态失败：{error}"

        if not text:
            return True, "（其他聊天近期没有可查看的新动态。）"

        # Tool 结果受每流与流数量上限约束，无法证明已覆盖游标之前的全部内容；
        # 因此不推进自动注入游标，宁可后续重复而绝不跳过未返回的动态。
        return True, text
