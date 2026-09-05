"""Roamer：多群「辗转腾挪」调度层。

不依赖任何具体 chatter——全兼容（NDFC / NFC / DFC / 其他）：

- 核心路径：全局注册的 ``cross_stream_feed`` Tool（拉模型），
  任何聊天器的 LLM 想知道其他聊天动态时自己调用；
- 可选路径：``on_prompt_build`` 推送注入（默认关，``carry.inject_on_every_turn``）；
- 调度：serial（全域单焦点）与 parallel（受限并发唤醒，受 max_parallel_wakes 上限约束）两档；
- 零侵入：不修改框架 / 任何 chatter 插件的文件。
"""

from .config import RoamerConfig
from .core.ledger import RoamingLedger
from .core.planner import RoamingPlanner
from .core.service import RoamerCore

#: RoamerCore 服务签名（plugin_name:component_type:component_name）
SERVICE_SIGNATURE = "roamer:service:roamer_core"

__all__ = [
    "SERVICE_SIGNATURE",
    "RoamerConfig",
    "RoamerCore",
    "RoamingLedger",
    "RoamingPlanner",
]
