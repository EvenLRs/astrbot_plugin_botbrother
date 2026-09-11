"""astrbot_plugin_botbrother —— OneBot QQ账号在线监视插件。

目标等价于 BotBrother 核心功能（在线/账号下线/连接不可达监视、去抖告警、
恢复通知），但不做 HTTP 轮询、WebUI、Docker 与多端点。

设计要点
--------
- 事件只接受 AIOCQHTTP 平台：通过 ``register_platform_adapter_type(AIOCQHTTP)``
  声明式过滤，并在处理器内校验 ``event.get_self_id() == target_self_id``。
- 被动活性：任何匹配 target_self_id 的 AIOCQHTTP 事件都证明该机器人在线。
- 主动探测：按 interval_seconds 周期通过 AstrBot 平台 API 调用 aiocqhttp
  客户端（平台自身维护的 WebSocket API 连接）的 ``get_status`` —— 不使用
  requests，也不自行调用 NapCat HTTP 接口。
- 告警区分两类故障：
  * 账号下线（OFFLINE）：get_status 成功但 online=false（QQ 登录态掉线）；
  * 连接/服务不可达（UNREACHABLE）：调用失败/超时/无平台实例。
- 推送一律走 AstrBot Context/平台 API（``context.send_message``）；目标可为 AstrBot 中任意平台的会话（unified_msg_origin）。逐目标独立尝试并带超时，单个目标失败/挂起仅记录日志，不影响其余目标，取消信号照常向上传播。
- 管理员指令 ``/botbrother_test``：仅管理员可用，向已配置的 notify_targets 发送测试通知并返回逐目标 发送调用完成/未找到目标平台/超时/失败 摘要；不依赖被监视账号在线、不要求监视已启动，也不改动状态机/快照、不触发故障告警。
- 后台任务可取消、无泄漏；所有异常仅记录，不令插件崩溃。
- 持久化写入 AstrBot 的 data/plugin_data/astrbot_plugin_botbrother/。
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from astrbot.api import logger
from astrbot.api.all import (
    AstrBotConfig,
    AstrMessageEvent,
    Context,
    MessageChain,
    MessageEventResult,
    Plain,
    Star,
    register,
)
from astrbot.api.star import StarTools
from astrbot.core.star.filter.permission import PermissionType
from astrbot.core.star.filter.platform_adapter_type import PlatformAdapterType
from astrbot.core.star.register import (
    register_command,
    register_permission_type,
    register_platform_adapter_type,
)

from .config_schema import validate_config
from .state_machine import (
    ACTIVITY,
    OFFLINE,
    ONLINE,
    UNREACHABLE,
    MonitorStateMachine,
)

PLUGIN_NAME = "astrbot_plugin_botbrother"

# 单目标推送超时（秒）：send_message 可能因目标平台阻塞/半开连接长时间不返回，
# 必须有界，否则会卡住整条监视循环与后续目标。
PUSH_TIMEOUT_SECONDS = 15

# 管理员测试通知指令：无需被监视账号在线，也不要求监视任务已启动；
# 只向已配置的 notify_targets 发送，不接受调用者指定的收件人。
TEST_COMMAND_NAME = "botbrother_test"
TEST_NOTIFICATION_TEXT = (
    "[BotBrother] 测试通知：用于验证通知渠道配置，不代表账号状态变化。"
)


@register(
    name=PLUGIN_NAME,
    author="AstrBot Team",
    desc="单实例 NapCat/OneBot 在线监视：区分账号下线与连接/服务不可达，去抖告警与恢复通知。",
    version="1.1.0",
)
class BotBrotherMonitor(Star):
    """BotBrother 单实例在线监视插件。"""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        # 注意：AstrBot 的 Star 基类不保存 config，插件需自行保存
        self.config = config
        self._task: asyncio.Task | None = None
        self._machine: MonitorStateMachine | None = None
        self._config: dict = {}
        self._platform_id = (
            ""  # 启动后由配置 platform_id 赋值（AstrBot Bots 中的平台实例 ID）
        )
        self._probe_timeout = 5
        self._data_dir: Path | None = None
        self._state_file: Path | None = None
        # 测试通知防重入：指令处理期间置 True，避免并发/重复触发叠加发送。
        self._test_in_flight = False

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    async def initialize(self) -> None:
        """插件启用入口：校验配置、恢复快照、启动后台监视任务。"""
        try:
            cfg, errors = validate_config(dict(self.config))
        except Exception as e:  # 校验器自身异常也不得让初始化崩溃；不启动即不伪造成功
            logger.error("%s 配置解析异常，监视未启动：%s", PLUGIN_NAME, e)
            return
        self._config = cfg
        if errors:
            logger.error("%s 配置无效，监视未启动：%s", PLUGIN_NAME, "; ".join(errors))
            return
        if not cfg["enabled"]:
            logger.info("%s 未启用（enabled=false）。", PLUGIN_NAME)
            return

        self._machine = MonitorStateMachine(debounce=cfg["debounce"])
        self._probe_timeout = cfg["probe_timeout_seconds"]
        # platform_id：AstrBot Bots 中 OneBot 平台实例的唯一 ID（用户自定义值，
        # 不一定是 "aiocqhttp"）；必须与 get_platform_inst 的匹配键一致。
        self._platform_id = cfg["platform_id"]

        if cfg["persist_state"]:
            self._setup_data_dir()
            self._load_snapshot()

        self._task = asyncio.create_task(
            self._monitor_loop(), name=f"{PLUGIN_NAME}-monitor"
        )
        logger.info(
            "%s 已启动：监视 self_id=%s（平台 id=%s），间隔 %ds，去抖 %d 次，推送目标 %d 个",
            PLUGIN_NAME,
            cfg["target_self_id"],
            cfg["platform_id"],
            cfg["interval_seconds"],
            cfg["debounce"],
            len(cfg["notify_targets"]),
        )

    async def terminate(self) -> None:
        """插件停用/热重载入口：取消后台任务，保存状态快照。"""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # 我们主动取消，吞掉自身取消信号是预期行为
            except Exception as e:  # 任务收尾异常只记录，不中断停用流程
                logger.warning("%s 监视任务退出时异常（已忽略）：%s", PLUGIN_NAME, e)
        self._save_snapshot()
        logger.info("%s 已停止。", PLUGIN_NAME)

    # ------------------------------------------------------------------ #
    # 事件入口（仅 AIOCQHTTP）
    # ------------------------------------------------------------------ #
    @register_platform_adapter_type(PlatformAdapterType.AIOCQHTTP)
    async def on_aiocqhttp_event(self, event: AstrMessageEvent):
        """只接受 AIOCQHTTP 平台事件；匹配 target_self_id 的事件视为在线证据。

        事件能到达本处理器，说明该机器人的 WS 连接可用且账号在线。
        """
        try:
            if self._machine is None:
                return None
            if event.get_self_id() != self._config.get("target_self_id"):
                return None
            await self._feed_and_notify(ACTIVITY, "")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # 仅记录，不令插件崩溃
            logger.error("%s 事件处理异常（已忽略）：%s", PLUGIN_NAME, e)
        return None

    # ------------------------------------------------------------------ #
    # 后台监视
    # ------------------------------------------------------------------ #
    async def _monitor_loop(self) -> None:
        """周期探测循环。只 await，不阻塞事件循环；可取消、无泄漏。"""
        interval = self._config["interval_seconds"]
        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                raise
            try:
                state, detail = await self._probe()
                await self._feed_and_notify(state, detail)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # 仅记录，不令插件崩溃
                logger.error("%s 监视循环异常（已忽略）：%s", PLUGIN_NAME, e)

    def _resolve_platform(self):
        """按配置的 ``platform_id``（AstrBot Bots 中平台实例的唯一 ID）定位平台实例。

        ``platform_id`` 是用户自定义值（不一定等于 "aiocqhttp"），
        与 ``Context.get_platform_inst`` 的匹配键一致。只解析该 ID，
        并校验其适配器类型确为 aiocqhttp（``meta().name == "aiocqhttp"``），
        防止多实例/多平台场景下错绑；查不到即返回 None，由调用方判定为
        "连接/服务不可达"，从而暴露配置错误而非掩盖。
        """
        if not self._platform_id:
            return None
        platform = self.context.get_platform_inst(self._platform_id)
        if platform is None:
            return None
        try:
            meta = platform.meta()
        except Exception:
            return None
        if getattr(meta, "name", None) != "aiocqhttp":
            return None
        return platform

    async def _probe(self) -> tuple[str, str]:
        """通过 AstrBot 平台 API 探测目标机器人状态。

        Returns:
            (state, detail)，state 取值见 state_machine：
            - online        get_status 成功且 online=true
            - offline       get_status 成功但 online=false（账号下线）
            - unreachable   调用失败/超时/无平台实例（连接/服务不可达）
        """
        platform = self._resolve_platform()
        bot = getattr(platform, "bot", None) if platform is not None else None
        if bot is None:
            return (
                UNREACHABLE,
                f"未找到平台实例（platform_id={self._platform_id}）或其 bot 客户端（平台未连接？）。",
            )
        try:
            info = await asyncio.wait_for(
                bot.call_action("get_status"),
                timeout=self._probe_timeout,
            )
        except asyncio.TimeoutError:
            return (
                UNREACHABLE,
                f"get_status 超时（>{self._probe_timeout}s），连接/服务不可达。",
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            return (
                UNREACHABLE,
                f"get_status 调用失败（{type(e).__name__}: {e}），连接/服务不可达。",
            )
        if isinstance(info, dict):
            if info.get("online"):
                return ONLINE, ""
            return (
                OFFLINE,
                f"get_status 返回 online=false（QQ 登录态掉线）：{info}",
            )
        return (UNREACHABLE, f"get_status 返回异常数据：{info!r}")

    # ------------------------------------------------------------------ #
    # 状态机与推送
    # ------------------------------------------------------------------ #
    async def _feed_and_notify(self, signal: str, detail: str = "") -> None:
        if self._machine is None:
            return
        try:
            notices = self._machine.feed(
                signal, detail, self_id=self._config.get("target_self_id", "")
            )
        except Exception as e:
            logger.error("%s 状态机处理失败（已忽略）：%s", PLUGIN_NAME, e)
            return
        if notices:
            self._save_snapshot()
            for msg, _body in notices:
                # 通知是含 [BotBrother] 前缀与 self_id 的完整文案，直接发送
                await self._push(msg)

    async def _send_to_target(
        self, target: str, chain: MessageChain
    ) -> tuple[str, str]:
        """向单个推送目标发送，返回 (status, detail)。

        status：``ok`` 已完成发送调用；``false`` 未找到目标平台；
        ``timeout`` 超时；``error`` 抛异常。

        注意：AstrBot ``Context.send_message`` 的返回值语义是「是否找到匹配的
        平台」，``True`` 只代表调用已完成、目标平台已定位——**不代表消息真实
        可达、用户已读或最终投递成功**，需结合适配器响应/日志确认。
        取消信号照常向上传播。
        """
        try:
            ok = await asyncio.wait_for(
                self.context.send_message(target, chain),
                timeout=PUSH_TIMEOUT_SECONDS,
            )
        except asyncio.CancelledError:
            raise  # 取消必须向上传播，不得当作普通发送失败吞掉
        except asyncio.TimeoutError:
            return "timeout", f"超时（>{PUSH_TIMEOUT_SECONDS}s）"
        except Exception as e:
            # 不回传原始异常文本（可能含 token/URL 等敏感信息），只给类型。
            return "error", f"{type(e).__name__}（详情见日志）"
        if ok:
            return "ok", ""
        return "false", "未找到目标平台（send_message 返回 False）"

    async def _push(self, text: str) -> None:
        """通过 AstrBot Context/平台 API 推送通知（不使用 requests）。

        text 为状态机给出的完整文案（含 [BotBrother] 前缀与 self_id），
        此处不再拼接任何内容。目标可为 AstrBot 中任意平台的
        unified_msg_origin（不限于被监视的 OneBot 实例）；逐目标独立尝试并带
        超时（PUSH_TIMEOUT_SECONDS），单个目标失败/挂起仅记录日志，不影响
        其余目标；取消信号照常向上传播。
        """
        targets = self._config.get("notify_targets") or []
        chain = MessageChain([Plain(text=text)])
        for target in targets:
            status, detail = await self._send_to_target(target, chain)
            if status == "ok":
                continue
            if status == "false":
                logger.warning(
                    "%s 推送目标 %s 未找到对应平台，消息未发送。",
                    PLUGIN_NAME,
                    target,
                )
            elif status == "timeout":
                logger.error(
                    "%s 推送超时 target=%s（>%ds，已跳过，继续后续目标）。",
                    PLUGIN_NAME,
                    target,
                    PUSH_TIMEOUT_SECONDS,
                )
            else:
                logger.error(
                    "%s 推送失败 target=%s（已忽略，继续后续目标）：%s",
                    PLUGIN_NAME,
                    target,
                    detail,
                )

    # ------------------------------------------------------------------ #
    # 管理员测试通知指令（/botbrother_test）
    # ------------------------------------------------------------------ #
    def _resolve_notify_targets(self) -> list[str]:
        """解析已配置的推送目标，用于测试通知。

        直接读取原始配置（而不是只依赖 initialize 校验后的 self._config）：
        即使监视因 target_self_id/platform_id 缺失而未启动、或插件被禁用，
        只要 notify_targets 本身配置合理，测试通知依然可用。绝不接受调用者
        指定的收件人——目标只能是配置里的 notify_targets。

        任何异常（含 config 非 Mapping）一律返回空列表，不做未校验的兜底解析。
        """
        try:
            cfg, _errors = validate_config(dict(self.config))
        except Exception:
            return []
        return list(cfg.get("notify_targets") or [])

    @register_permission_type(PermissionType.ADMIN)
    @register_command(TEST_COMMAND_NAME)
    async def on_test_notification(self, event: AstrMessageEvent) -> MessageEventResult:
        """管理员指令 ``/botbrother_test``：向已配置的 notify_targets 发送测试通知。

        - 仅管理员可用（声明式权限过滤器 + 运行时复核双保险）。
        - 只发给已配置的 notify_targets，不接受调用者指定收件人。
        - 不依赖被监视账号在线、不要求监视任务已启动；不改动状态机/快照，
          不触发任何故障告警。
        - 返回逐目标 发送调用完成/未找到目标平台/超时/失败 摘要；「发送调用
          完成」仅表示已调用适配器且目标平台已定位，不代表真实可达或用户已读，
          需结合适配器响应/日志确认。
        """
        # 运行时管理员复核（fail closed）：缺方法、非 callable、返回假值或
        # 调用抛异常，一律拒绝。声明式权限过滤器之外的兜底（如 alter_cmd 被
        # 改为 member / 事件缺 is_admin）。
        is_admin = getattr(event, "is_admin", None)
        try:
            allowed = callable(is_admin) and bool(is_admin())
        except Exception:
            allowed = False
        if not allowed:
            return event.plain_result("[BotBrother] 该指令仅限管理员使用。")

        if self._test_in_flight:
            return event.plain_result("[BotBrother] 已有测试通知正在进行，请稍后再试。")

        targets = self._resolve_notify_targets()
        if not targets:
            return event.plain_result(
                "[BotBrother] 未配置有效的 notify_targets，无法测试。"
                "请先在插件配置中填写至少一个推送目标。"
            )

        self._test_in_flight = True
        chain = MessageChain([Plain(text=TEST_NOTIFICATION_TEXT)])
        labels = {
            "ok": "发送调用完成（请核对收件）",
            "false": "未找到目标平台",
            "timeout": "超时",
            "error": "失败",
        }
        statuses: list[str] = []
        lines: list[str] = []
        try:
            for target in targets:
                status, detail = await self._send_to_target(target, chain)
                statuses.append(status)
                suffix = f"（{detail}）" if detail else ""
                lines.append(f"- {labels.get(status, status)}：{target}{suffix}")
        finally:
            self._test_in_flight = False

        ok_n = statuses.count("ok")
        timeout_n = statuses.count("timeout")
        fail_n = len(statuses) - ok_n - timeout_n
        summary = (
            f"发送调用完成={ok_n}，超时={timeout_n}，失败={fail_n}，"
            f"共 {len(statuses)} 个目标"
        )
        header = (
            "[BotBrother] 测试通知发送调用已完成（「发送调用完成」仅表示已调用"
            "适配器且目标平台已定位，不代表真实可达或用户已读，请核对收件并查看日志）：\n"
        )
        return event.plain_result(header + "\n".join(lines) + "\n汇总：" + summary)

    # ------------------------------------------------------------------ #
    # 持久化（data/plugin_data/astrbot_plugin_botbrother/）
    # ------------------------------------------------------------------ #
    def _setup_data_dir(self) -> None:
        try:
            self._data_dir = StarTools.get_data_dir(PLUGIN_NAME)
            self._data_dir.mkdir(parents=True, exist_ok=True)
            self._state_file = self._data_dir / "state.json"
        except Exception as e:
            logger.warning("%s 无法创建数据目录，持久化已禁用：%s", PLUGIN_NAME, e)
            self._data_dir = None
            self._state_file = None

    def _save_snapshot(self) -> None:
        if self._state_file is None or self._machine is None:
            return
        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._state_file.with_name("state.json.tmp")
            tmp.write_text(
                json.dumps(self._machine.to_snapshot(), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self._state_file)
        except Exception as e:
            logger.warning("%s 状态持久化失败（已忽略）：%s", PLUGIN_NAME, e)

    def _load_snapshot(self) -> None:
        if self._state_file is None or self._machine is None:
            return
        try:
            if self._state_file.exists():
                from .state_machine import MonitorStateMachine

                snap = json.loads(self._state_file.read_text(encoding="utf-8"))
                self._machine = MonitorStateMachine.from_snapshot(
                    snap, debounce=self._config["debounce"]
                )
                logger.info(
                    "%s 已从快照恢复状态：state=%s",
                    PLUGIN_NAME,
                    self._machine.state,
                )
        except Exception as e:
            logger.warning(
                "%s 状态快照加载失败，使用全新状态（已忽略）：%s", PLUGIN_NAME, e
            )
