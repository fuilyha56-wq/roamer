"""Roamer 运维命令。

路由结构（``BaseCommand.execute`` 收到的是去掉前缀与命令名的子路由文本）：

- ``/roamer`` 或 ``/roamer help``：帮助；
- ``/roamer status``：漫游域 / 焦点 / 账本状态；
- ``/roamer visit <stream_id>``：手动唤醒一个群（带简报）；
- ``/roamer pause`` / ``/roamer resume``：暂停 / 恢复调度。
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from src.app.plugin_system.api.log_api import get_logger
from src.app.plugin_system.base import BaseCommand, cmd_route
from src.app.plugin_system.types import PermissionLevel

from .config import RoamerConfig
from .core.service import RoamerCore

if TYPE_CHECKING:
    pass

logger = get_logger("roamer.command")

#: RoamerCore 服务签名
_CORE_SIGNATURE = "roamer:service:roamer_core"


class RoamerCommand(BaseCommand):
    """Roamer 运维命令。"""

    name = "roamer"
    description = "Roamer 漫游调度运维命令"
    command_prefix = "/"
    permission_level = PermissionLevel.OPERATOR

    def _core(self) -> RoamerCore | None:
        """获取 RoamerCore 服务（未注册时返回 None）。"""
        from src.app.plugin_system.api import service_api

        service = service_api.get_service(_CORE_SIGNATURE)
        return service if isinstance(service, RoamerCore) else None

    def _config(self) -> RoamerConfig | None:
        """获取当前配置。"""
        cfg = self.plugin.config
        return cfg if isinstance(cfg, RoamerConfig) else None

    @cmd_route()
    async def handle_help(self) -> tuple[bool, str]:
        """显示完整命令帮助。"""
        return True, (
            "Roamer 命令：\n"
            "/roamer status\n"
            "/roamer join [stream_id]\n"
            "/roamer leave [stream_id]\n"
            "/roamer visit <stream_id>\n"
            "/roamer pause\n"
            "/roamer resume"
        )

    @cmd_route("status")
    async def handle_status(self) -> tuple[bool, str]:
        """展示漫游调度状态。"""
        cfg = self._config()
        if cfg is None or not cfg.roamer.enabled:
            return True, "Roamer 未启用。"
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        now = datetime.now().astimezone()
        lines = core.planner.status_lines(
            focus_stream=core.focus_stream, now=now
        )
        stats = core.ledger.brief_stats()
        if stats:
            stats_text = "，".join(f"{k}:{v}条" for k, v in sorted(stats.items()))
            lines.append(f"账本（未老化）：{stats_text}")
        else:
            lines.append("账本（未老化）：空")
        from .core.service import _SharedState

        if _SharedState.carry_cursors:
            cursor_text = "，".join(
                f"{k[:8]}:{datetime.fromtimestamp(v).strftime('%H:%M')}"
                for k, v in sorted(_SharedState.carry_cursors.items())
            )
            lines.append(f"搬运游标（ACK）：{cursor_text}")
        else:
            lines.append("搬运游标（ACK）：无")
        pending = len(_SharedState.carry_pending)
        lines.append(f"待 ACK 批次：{pending} 个")
        return True, "\n".join(lines)

    @cmd_route("visit")
    async def handle_visit(self, stream_id: str = "") -> tuple[bool, str]:
        """手动唤醒一个群（带跨群简报）。"""
        target = stream_id.strip()
        if not target:
            return False, "用法：/roamer visit <stream_id>"
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        if not core.planner.in_domain(target):
            return False, f"{target} 不在漫游域内。"
        ok = await core.visit(target, reason="手动 visit")
        return (True, f"已唤醒 {target}。") if ok else (False, f"唤醒失败：{target}（会话未挂起或流不存在）")

    @cmd_route("pause")
    async def handle_pause(self) -> tuple[bool, str]:
        """暂停调度循环。"""
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        await core.stop()
        return True, "Roamer 调度已暂停。"

    @cmd_route("resume")
    async def handle_resume(self) -> tuple[bool, str]:
        """恢复调度循环。"""
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        core.start()
        return True, "Roamer 调度已恢复。"

    @cmd_route("join")
    async def handle_join(self, stream_id: str = "") -> tuple[bool, str]:
        """把当前聊天流加入漫游域（省略参数时用命令所在流）。"""
        target = stream_id.strip() or self.stream_id
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        if core.planner.in_domain(target):
            return False, f"{target} 已在漫游域内。"
        if not await core.join_domain(target):
            return False, "漫游域持久化失败（见日志）。"
        return True, f"已加入漫游域：{target}（当前成员 {len(core.planner.domain())} 个）"

    @cmd_route("leave")
    async def handle_leave(self, stream_id: str = "") -> tuple[bool, str]:
        """把聊天流移出漫游域（省略参数时用命令所在流）。"""
        target = stream_id.strip() or self.stream_id
        core = self._core()
        if core is None:
            return False, "RoamerCore 服务未注册。"
        if not core.planner.in_domain(target):
            return False, f"{target} 不在漫游域内。"
        if not await core.leave_domain(target):
            return False, "漫游域持久化失败（见日志）。"
        return True, f"已移出漫游域：{target}"
