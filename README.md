# Roamer

Roamer 是一个面向 Neo-MoFox 的多群「辗转腾挪」调度层插件。当前版本为 `0.1.0`。

它解决的核心问题：**Bot 同时在多个群和私聊里"生活"，如何决定此刻出现在哪里、如何带着跨群记忆自然接话**——而不是机械轮询或全并发造成"人格分裂"。

Roamer 不依赖任何具体聊天器插件（NDFC / NFC / DFC / 其他均可）：

- **核心路径**：全局注册的 `cross_stream_feed` Tool（拉模型），任何聊天器的 LLM 想知道其他聊天动态时自己调用；
- **唤醒路径**：`resume_chatter` 经 framework 通道注入漫游简报，零聊天器代码修改；
- **零侵入**：不修改框架 / 任何聊天器插件的文件，全部通过事件字符串字面量与框架公开 manager 交互。

## 核心能力

### 双模式漫游调度

| 模式 | 行为 | 适用 |
|---|---|---|
| `serial`（默认） | 全域单焦点：同一时刻只"身在"一个流，聊完（空闲超时/硬上限）才去下一站 | 最拟人，防多群同时说话的语气冲突 |
| `parallel` | 受限并发唤醒：每 tick 按兴趣分选至多 `max_parallel_wakes` 个流并发 resume | 高响应场景，并发度受上限约束可预测 |

两种模式共用同一套兴趣分与回访节流规则，切换模式无需改其他配置。

### 对数吸引力选站

下一站不是轮询，而是打分：

- **强提及**（@bot / 回复 bot）→ 固定高分直接插队，被点名就该回去；
- **私聊** → 直通信加成，直接对话天然优先于群聊插队；
- **未读数按对数曲线折算**——刷屏群不会线性霸占焦点，高未读区（10+）仍保留区分度；
- **回访间隔**每 10 分钟一档加分，封顶 10 档，长期冷宫群不会分值无限膨胀。

所有系数（`at_bot_weight` / `private_weight` / `curiosity_log_base` / `unread_weight` / `revisit_weight`）均可配。

### 一手发言账本

只记 Bot 自己在各群的发言（可选记用户发言），滚动窗口超龄自动压缩为按群统计。账本简报经 `resume_prompt` 注入唤醒的会话——"你在 C 群刚聊了装机预算"，让接话有连续性。

### 跨群原文搬运（v0.2）

插件私有 SQLite 镜像（`data/roamer/data.db`，与框架主库完全隔离）存储漫游域内消息原文与 Bot 动作流水：

- **披露方向矩阵**：群→群 / 群→私聊 / 私聊→群三向独立配置，私聊→私聊恒关；私聊到群默认关闭，防隐私泄漏；
- **请求级 ACK 游标**：每个跨群内容块带独立批次标识，只有实际携带它的 LLM 请求成功才推进游标，失败下次补发，无关请求不能确认它；
- **截断保守策略**：命中展示上限时内容仍展示但批次不生成可提交游标——数据完整性优先于去重，宁可重复不可跳过；
- **`cross_stream_feed` Tool**：拉模型，LLM 需要时主动查询，Tool 拉取不推进自动注入游标。

### 串行专注门（可选，默认关）

serial 模式的严格执行者：非焦点群的发言请求被拦截（"人在别处"），强提及默认豁免。防止调度层说"身在 A 群"而 LLM 却在 B 群插话的割裂。

## 命令

| 命令 | 说明 |
|---|---|
| `/roamer status` | 漫游域 / 焦点 / 账本 / 游标 / 待 ACK 批次状态 |
| `/roamer join [stream_id]` | 加入漫游域（全流域关闭时使用） |
| `/roamer leave [stream_id]` | 退出漫游域 |
| `/roamer visit <stream_id>` | 手动唤醒一个流（带简报） |
| `/roamer pause` / `resume` | 暂停 / 恢复调度 |

## 配置速览

配置文件：`config/plugins/roamer/config.toml`（首次加载自动生成）。

| 节 | 关键项 | 默认 | 说明 |
|---|---|---|---|
| `roamer` | `mode` | `serial` | 调度模式 |
| | `all_streams` | `true` | 全流域模式（含私聊），无需逐个 join |
| | `tick_seconds` | `60` | 调度循环间隔 |
| | `max_parallel_wakes` | `2` | parallel 模式单 tick 并发唤醒上限 |
| | `min_return_interval` | `90` | 同群两次被选焦点最小间隔（秒） |
| `attraction` | `curiosity_log_base` | `2.0` | 未读对数底数，越大增益越平缓 |
| `ledger` | `always_inject` | `true` | 每轮 prompt 注入跨群足迹 |
| | `track_user_speech` | `false` | 记录用户消息用于简报（涉跨群可见，酌情开） |
| `focus_gate` | `enabled` | `false` | 串行专注门开关 |
| `carry` | `inject_on_every_turn` | `false` | 推送注入（默认关，主路径是 Tool 拉取） |
| | `private_to_group` | `off` | 私聊→群披露（默认关，防泄漏） |

完整字段说明见配置文件内注释或 `docs/DESIGN.md` §7。

## 前置要求

- Neo-MoFox `>= 1.0.0`
- Python `>= 3.11`
- 无其他插件硬依赖（不依赖任何具体聊天器）

## 数据与隐私

- **镜像库**：`data/roamer/data.db`，仅存漫游域内消息（保留 7 天自动清理），与框架主库隔离；
- **持久化状态**：`data/json_storage/roamer_state/`（漫游域成员、ACK 游标）；
- **披露默认最保守**：私聊内容默认不出现在任何群聊，私聊↔私聊恒关；
- **绝不搬运**用户消息到未授权方向，即使开启 `track_user_speech` 也受披露矩阵约束。

## 架构与测试

设计细节（竞态治理、ACK 契约、并行调度契约、时序图）见 `docs/DESIGN.md`。

```text
tests/
└── test_roamer.py    # 23 项：对数吸引力 / ACK 游标 / 披露矩阵 / 并行调度 / Tool
```

运行测试：

```powershell
& ".\.venv\Scripts\python.exe" -m pytest "plugins\roamer\tests\test_roamer.py" -q -p no:randomly --no-cov
```

## License

本项目采用 [AGPL-3.0](./LICENSE) 许可证发布。

基于本插件修改或分发（含网络服务形式）时，必须以相同许可证开源完整对应源码。
