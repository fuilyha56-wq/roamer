"""Roamer 配置定义。

对应设计文档 ``docs/DESIGN.md`` §7：

- ``roamer``：主开关 / 模式（serial|parallel）/ 漫游域成员 / 调度参数；
- ``ledger``：一手发言账本窗口与简报粒度；
- ``focus_gate``：串行专注门（可选，默认关）。
"""

from __future__ import annotations

from typing import ClassVar

from src.app.plugin_system.base import BaseConfig, Field, SectionBase, config_section

#: 支持的调度模式。serial=全域单焦点；parallel=受限并发唤醒（单 tick 批次受 max_parallel_wakes 约束）。
_SUPPORTED_MODES = ("serial", "parallel")


def _validate_mode(value: str) -> str:
    """校验并规范化调度模式字符串。

    Args:
        value: 配置原始值。

    Returns:
        str: 规范化后的模式名；非法值回退 ``serial``。
    """
    normalized = (value or "").strip().lower()
    if normalized not in _SUPPORTED_MODES:
        return "serial"
    return normalized


class RoamerConfig(BaseConfig):
    """Roamer 配置。"""

    name: ClassVar[str] = "config"
    description: ClassVar[str] = "Roamer 配置"

    @config_section("roamer", title="漫游调度", tag="plugin")
    class RoamerSection(SectionBase):
        """漫游调度主配置节。"""

        enabled: bool = Field(
            default=True,
            description="是否启用 Roamer",
            label="启用漫游",
            tag="plugin",
        )
        mode: str = Field(
            default="serial",
            description="调度模式：serial=全域单焦点（最拟人）；parallel=受限并发唤醒（见 max_parallel_wakes）",
            label="调度模式",
            tag="plugin",
            hint="只影响漫游域内部；非漫游域流不受任何约束。",
        )
        all_streams: bool = Field(
            default=True,
            description="全流域模式：适配所有聊天流（含私聊），无需逐个 join",
            label="全流域",
            tag="plugin",
            hint="开启后 roaming_streams 仅作备注；私聊享有最高优先级。",
        )
        roaming_streams: list[str] = Field(
            default_factory=list,
            description="漫游域成员聊天流 ID 列表（全流域关闭时的成员白名单）",
            label="漫游域成员",
            tag="plugin",
        )
        tick_seconds: int = Field(
            default=60,
            description="调度循环间隔（秒）",
            label="调度间隔",
            tag="plugin",
        )
        focus_idle_timeout: int = Field(
            default=240,
            description="焦点群空闲多久后视为「聊完」（秒）",
            label="焦点空闲超时",
            tag="plugin",
        )
        max_focus_hold_minutes: int = Field(
            default=20,
            description="焦点群最长占用时间（分钟），超时强制释放",
            label="焦点硬上限",
            tag="plugin",
        )
        min_return_interval: int = Field(
            default=90,
            description="同一群两次被选为焦点的最小间隔（秒），防高频横跳",
            label="回访节流",
            tag="plugin",
        )
        max_parallel_wakes: int = Field(
            default=2,
            description="parallel 模式单次 tick 并发唤醒的流数量上限",
            label="并行唤醒上限",
            tag="plugin",
            hint="serial 模式忽略此值；建议 2-3，过高会同时占用多个 LLM 请求预算。",
        )
        command_enabled: bool = Field(
            default=True,
            description="是否注册 /roamer 运维命令",
            label="启用命令",
            tag="plugin",
        )

        def normalized_mode(self) -> str:
            """返回规范化后的调度模式。"""
            return _validate_mode(self.mode)

    @config_section("attraction", title="吸引力系数", tag="ai")
    class AttractionSection(SectionBase):
        """兴趣分打分系数（docs/DESIGN.md §3.3 的可调版）。

        未读数折算采用**对数曲线**：刷屏群不会线性霸占焦点，
        与线性封顶相比在 10+ 未读时仍保留区分度。
        """

        at_bot_weight: float = Field(
            default=1000.0,
            description="强提及（@bot/回复bot）的固定加分",
            label="强提及权重",
            tag="ai",
        )
        private_weight: float = Field(
            default=500.0,
            description="私聊消息的固定加分（直接对话优先）",
            label="私聊权重",
            tag="ai",
        )
        curiosity_log_base: float = Field(
            default=2.0,
            description="好奇心对数底数（必须大于 1，越大未读增益越平缓）",
            label="好奇心底数",
            tag="ai",
            hint="unread 得分 = log(unread+1, base)×unread_weight；线性封顶在高未读时失去区分度。",
        )
        unread_weight: float = Field(
            default=6.0,
            description="未读数对数得分的放大系数",
            label="未读权重",
            tag="ai",
        )
        revisit_weight: float = Field(
            default=1.0,
            description="距上次回访每 10 分钟的加分系数",
            label="回访权重",
            tag="ai",
        )

    @config_section("ledger", title="发言账本", tag="ai")
    class LedgerSection(SectionBase):
        """一手发言账本配置节。"""

        window_hours: int = Field(
            default=6,
            description="账本滚动窗口（小时），超龄条目压缩为按群统计",
            label="账本窗口",
            tag="ai",
        )
        max_entry_chars: int = Field(
            default=200,
            description="单条发言记录的最大字符数",
            label="单条截断",
            tag="ai",
        )
        max_entries_per_stream: int = Field(
            default=10,
            description="简报中每个群最多展示的 bot 发言条数",
            label="单群条数上限",
            tag="ai",
        )
        always_inject: bool = Field(
            default=True,
            description="是否在每轮 prompt 都注入账本跨群简报（路径B）",
            label="常驻简报注入",
            tag="ai",
            hint="关闭后简报仅随漫游唤醒（resume）注入；路径B常驻注入目前仅对 NDFC 生效。",
        )
        track_user_speech: bool = Field(
            default=False,
            description="是否记录漫游域内的用户消息用于跨群简报",
            label="追踪用户发言",
            tag="ai",
            hint="开启后简报会包含「某人说：…」；涉及用户消息跨群可见，请酌情开启。",
        )
        max_user_entries_per_stream: int = Field(
            default=5,
            description="简报中每个群最多展示的用户发言条数",
            label="用户条数上限",
            tag="ai",
        )

    @config_section("focus_gate", title="专注门", tag="ai")
    class FocusGateSection(SectionBase):
        """串行专注门配置节（可选，默认关）。"""

        enabled: bool = Field(
            default=False,
            description="是否严格拦截非焦点群的 preprocess（人在别处时不发言）",
            label="启用专注门",
            tag="ai",
            hint="serial 模式的严格执行者；强提及（@/回复）默认豁免。",
        )
        strong_mention_exempt: bool = Field(
            default=True,
            description="被强提及（@bot/回复bot）时是否豁免专注门",
            label="强提及豁免",
            tag="ai",
        )

    @config_section("carry", title="上下文搬运", tag="ai")
    class CarrySection(SectionBase):
        """跨群原始上下文搬运配置节。

        把其他漫游域成员的**最新消息原文**（与 NDFC 历史消息同格式）
        逐字注入当前会话，不做任何摘要转述。
        """

        enabled: bool = Field(
            default=True,
            description="是否启用跨群原文搬运",
            label="启用搬运",
            tag="ai",
        )
        inject_on_every_turn: bool = Field(
            default=False,
            description="推送注入：每轮 prompt 自动追加跨群动态（默认关，主路径是 cross_stream_feed 工具拉取）",
            label="推送注入",
            tag="ai",
            hint="默认关闭：Bot 需要时自己调 cross_stream_feed 工具，省 token 且全聊天器通用。注意：推送注入通道目前仅对 NDFC 模板生效，其他聊天器请走工具拉取主路径。",
        )
        per_stream_count: int = Field(
            default=15,
            description="每个源群单次搬运的最大消息条数",
            label="单群条数",
            tag="ai",
        )
        max_streams_per_inject: int = Field(
            default=3,
            description="单次注入最多覆盖的源群数量",
            label="单次群数上限",
            tag="ai",
        )
        lookback_minutes: int = Field(
            default=60,
            description="只搬运该时间窗口内的消息（分钟）",
            label="回看窗口",
            tag="ai",
        )
        group_to_group: str = Field(
            default="detailed",
            description="群→群搬运披露等级：off/detailed",
            label="群到群披露",
            tag="ai",
        )
        private_to_group: str = Field(
            default="off",
            description="私聊→群搬运披露等级：off/detailed（默认关，防隐私泄漏）",
            label="私聊到群披露",
            tag="ai",
            hint="开启后群聊可见私聊消息原文，谨慎启用。",
        )
        group_to_private: str = Field(
            default="detailed",
            description="群→私聊搬运披露等级：off/detailed",
            label="群到私聊披露",
            tag="ai",
        )

        def disclosure_allowed(self, source_chat_type: str, target_chat_type: str) -> bool:
            """判定 source→target 方向是否允许搬运。

            方向矩阵：群→群 / 群→私聊 / 私聊→群 三向可配，
            私聊→私聊无配置项，恒为关（最强隐私默认）。

            Args:
                source_chat_type: 源流类型（private/group/...）。
                target_chat_type: 目标流类型。

            Returns:
                bool: 允许 detailed 搬运返回 True。
            """
            if source_chat_type == target_chat_type == "group":
                return self.group_to_group == "detailed"
            if source_chat_type == "group" and target_chat_type == "private":
                return self.group_to_private == "detailed"
            if source_chat_type == "private" and target_chat_type == "group":
                return self.private_to_group == "detailed"
            return False

    roamer: RoamerSection = Field(default_factory=RoamerSection)
    attraction: AttractionSection = Field(default_factory=AttractionSection)
    ledger: LedgerSection = Field(default_factory=LedgerSection)
    focus_gate: FocusGateSection = Field(default_factory=FocusGateSection)
    carry: CarrySection = Field(default_factory=CarrySection)
