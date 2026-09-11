# Astrbot_Plugin_BotBrother OneBot账号状态监视

一个为 AstrBot 打造的 NapCat/OneBot 在线状态监视插件。


## 目录

- [使用须知](#使用须知)
- [第一次使用](#第一次使用)
- [功能特性](#功能特性)
- [安装](#安装)
- [NapCat 反向 WebSocket 配置要点](#napcat-反向-websocket-配置要点)
- [配置项](#配置项)
- [platform_id 与 target_self_id 的区别](#platform_id-与-target_self_id-的区别)
- [如何取得 notify_targets](#如何取得-notify_targets)
- [状态判定与去抖语义](#状态判定与去抖语义)
- [常见问题](#常见问题)
- [兼容性与已知限制](#兼容性与已知限制)
- [测试](#测试)
- [更新日志](#更新日志)
- [问题反馈](#问题反馈)
- [许可证](#许可证)

## 使用须知

`platform_id` 必填，且要与 AstrBot Bots 配置中该 OneBot 平台实例的唯一 ID 一致。填错时插件会持续判定「连接/服务不可达」。

故障状态会写入 `data/plugin_data/astrbot_plugin_botbrother/state.json`以实现持久化监视。插件重载后不会对持续中的故障重复告警。

存在多个OneBot实例时请显式配置 `platform_id`。插件按该 ID 精确绑定平台实例，只监视指定实例。

## 第一次使用

1. 把 `astrbot_plugin_botbrother` 目录放进 AstrBot 的 `data/plugins/`，重启 AstrBot，或在插件管理页启用。
2. 在插件配置页填写三项必填内容：
   - `target_self_id`：被监视机器人的 QQ 号；
   - `platform_id`：AstrBot Bots 中该 OneBot 平台实例的 ID；
   - `notify_targets`：至少一个推送目标（群聊或私聊）。
3. 保存配置，插件会立即按 `interval_seconds` 周期开始探测。
4. 验证：让目标群或好友给机器人发一条消息（事件到达即视为在线证据）；或关停 NapCat，观察在 `debounce` 次探测后是否收到告警，重启后是否收到恢复通知。



## 功能特性

- 账号掉线与连接不可达分开提示：QQ 登录掉线和 NapCat 未运行、网络断开会收到不同文案的告警，一眼能看出问题出在哪一侧。
- 网络波动不误报：探测结果连续 N 次（默认 3 次）异常才告警一次，偶发抖动不打扰。
- 恢复后只通知一次：确认机器人恢复在线后补发一次恢复通知，不会反复刷屏。
- 告警通道不依赖被监视账号：`notify_targets` 可填 AstrBot 中任意平台的会话（如微信、Telegram、另一个 OneBot 实例），即使被监视的 QQ 账号掉线，告警也能通过独立通道送达。
- 只监视指定的机器人：多实例部署时按配置锁定监控对象，各实例互不干扰。
- 重载不重复提醒：插件重启或 AstrBot 重载后，仍在持续的故障不会重新告警。
- 插件异常不影响运行：内部出错只记录日志，AstrBot 和机器人照常工作。



## 安装

前置条件：

- AstrBot ≥ 4.x；
- NapCat（或任意 OneBot v11 实例）已通过反向 WebSocket 接入 AstrBot。

步骤：

```bash
# 1. 复制插件目录到 AstrBot 插件目录
cp -r astrbot_plugin_botbrother <AstrBot目录>/data/plugins/

# 2. 重启 AstrBot 或在插件管理页启用
# 3. 在插件配置页填写配置（见「配置项」）
```


## 配置项

所有配置在 AstrBot 插件配置页中设置（由 `_conf_schema.json` 生成）。

| 配置项 | 必填 | 默认 | 说明 |
| --- | --- | --- | --- |
| `enabled` | 否 | `true` | 是否启用在线监视。 |
| `target_self_id` | 是 | `""` | 被监视机器人的 self_id（QQ 号）。 |
| `platform_id` | 是 | `""` | AstrBot Bots 中该 OneBot 平台实例的 ID。 |
| `interval_seconds` | 否 | `30` | 探测间隔（秒），范围 5–86400，越界自动夹取。 |
| `debounce` | 否 | `3` | 连续 N 次故障/恢复才推送，范围 1–10，越界自动夹取。 |
| `probe_timeout_seconds` | 否 | `5` | 单次 `get_status` 超时（秒），范围 1–60，越界自动夹取。 |
| `notify_targets` | 是 | `[]` | 推送目标（`unified_msg_origin`）列表，至少一个；可为 AstrBot 中任意平台的会话，不限于被监视的 OneBot 实例。 |
| `persist_state` | 否 | `true` | 是否持久化故障状态。 |

### platform_id 与 target_self_id 的区别

| | `platform_id` | `target_self_id` |
| --- | --- | --- |
| 本质 | AstrBot Bots 中平台实例的唯一 ID（添加 Bot 时自定义） | 被监视机器人的 QQ 号（self_id） |
| 作用 | 定位平台实例，发起 `get_status` 探测 | 过滤事件，只处理该机器人的事件 |
| 典型取值 | `napcat1` | `123456789` |
| 为什么必须显式配置 | 多实例时只有显式指定才能保证目标唯一 | 用它确认事件确实来自目标机器人 |

### 如何取得 notify_targets

`notify_targets` 的每一项都是 AstrBot 的 `unified_msg_origin`（统一会话标识），格式为 `{platform_id}:{消息类型}:{会话ID}`。

推荐选择与被监视 QQ 账号**相互独立**的通道（例如平台实例 `weixin_personal_icry` 的微信会话）：这样即使被监视账号掉线、NapCat 崩溃，告警依然能送达。

两种拿法：

1. 从 AstrBot 日志复制：目标会话收发消息时，AstrBot 日志的事件详情会带 `unified_msg_origin`，直接复制。
2. 按格式拼接：`{platform_id}:{消息类型}:{会话ID}`
   - QQ 群聊：`napcat1:GroupMessage:<群号>`
   - QQ 私聊：`napcat1:FriendMessage:<对方QQ号>`
   - 微信（示例，以实际平台实例 ID 为准）：`weixin_personal_icry:FriendMessage:<wxid>`

推送目标必须真实可达：对应平台实例已在 AstrBot 中启用，且该会话能收到平台消息（机器人在群内、与对方为好友等）。

注意：QQ 官方 API 平台（`qq_official`）不支持主动发送消息，请勿选作告警目标。



## 状态判定与去抖语义

状态判定：

探测经 AstrBot 平台 API 调用 aiocqhttp 的 `get_status`，复用现有 WebSocket 连接，不使用 HTTP 轮询、`requests` 或直接调用 NapCat HTTP 接口。

| `get_status` 结果 | 判定 | 说明 |
| --- | --- | --- |
| 成功且 `online=true` | 在线 | 正常 |
| 成功但 `online=false` | 账号下线 | QQ 登录态掉线，连接正常 |
| 失败 / 超时 / 找不到平台实例 | 连接/服务不可达 | WS 断开、NapCat 未运行等 |

去抖语义（设 `debounce = N`）：

- 在线时连续 N 次探测为故障才进入故障态并告警一次；期间任意一次正常会清零计数（cross-clear）。
- 故障态内同类故障不重复告警；故障种类切换（如账号下线 → 连接不可达）按新种类重新累计 N 次后再告警。
- 恢复：连续 N 次正常探测，或收到一次匹配 self_id 的事件（事件能到达即证明在线，立即恢复）；同一故障段内恢复通知只补发一次。
- 告警与恢复通知都带 `self_id`，方便确认对象。



## 常见问题

### 为什么一直报「连接/服务不可达」？

先查 `platform_id` 是否与 AstrBot Bots 中的平台实例 ID 完全一致，再确认 NapCat 反向 WS 已连上、AstrBot 侧平台实例状态为已连接。

### 机器人掉线为什么没有立刻告警？

去抖（默认连续 3 次才告警）加上探测间隔（默认 30s）会造成一定延迟。目的是避免网络抖动误报。

### 需要配置 NapCat 心跳吗？

不需要。

### 能同时监视多个NapCat/OneBot实例吗？

不能。本插件面向单实例场景设计。

### 账号掉线后收不到推送？

逐项检查 `notify_targets`：格式是否为 `{platform_id}:{消息类型}:{会话ID}`；`platform_id` 段是否对应 AstrBot 中一个**已启用**的平台实例（不要求等于被监视的 `platform_id` 配置）；目标会话是否真实可达（机器人在群内、与对方为好友等）。

若把告警目标设在被监视账号自己参与的 QQ 会话里，账号掉线期间该目标自然收不到告警——建议改用与被监视账号独立的通道（见「如何取得 notify_targets」）。



## 问题反馈

遇到问题或有功能建议，可在项目仓库提交 Issue，或联系插件作者。反馈时请附上：AstrBot 版本、Python 版本、插件配置、AstrBot 日志中相关的错误行。



## 许可证

本项目基于 MIT 许可证开源。
