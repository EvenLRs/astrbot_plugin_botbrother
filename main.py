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
- 告警可靠性：探测/事件只把告警写入待发送队列（始终外置 data 目录），由生命周期受管的独立 worker 负责有限并发发送，按“每目标 FIFO + 有界指数退避”重试；发送与探测解耦，单目标失败/挂起不拖慢探测或其它目标。该兜底不是“保证不丢”，也不等于及时预警。
- 微信个人号（weixin_oc）：本插件走与定时任务/agent 相同的 ``Context.send_message`` 路径（不自建适配器、不使用 requests、不伪造/刷新 token）。适配器要求存在缓存的 context_token 且仅入站刷新，但“prepare failed = 过期”的结论已撤回，原始失败原因未证实；本插件的补发是可靠性加固，不等于根因解决。
- 管理员指令 ``/botbrother_test``：仅管理员可用，向已配置的 notify_targets 发送测试通知并返回逐目标 发送调用完成/未找到目标平台/超时/失败 摘要；不依赖被监视账号在线、不要求监视已启动，也不改动状态机/快照、不触发故障告警。
- 后台任务可取消、无泄漏；所有异常仅记录，不令插件崩溃。
- 持久化写入 AstrBot 的 data/plugin_data/astrbot_plugin_botbrother/。
"""

from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
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

# 告警待发送队列：交付失败的告警持久化于 data 目录，按目标 FIFO、有界重试。
# 说明：这是“失败尽量不丢”的兜底，不是“保证不丢失”，也不等于及时预警；
# 容量满/磁盘写失败均有边界（见 README）。发送由独立 worker 负责，与探测解耦。
PENDING_FILE_NAME = "pending_notifications.json"
PENDING_VERSION = 2
PENDING_MAX_ENTRIES = 50
# worker 每轮最多发起的发送尝试数（含各目标），避免一轮内无限尝试。
PENDING_MAX_ATTEMPTS_PER_CYCLE = 3
# worker 单轮的最大并发发送数（单目标超时/错误不拖慢其它目标）。
PENDING_MAX_CONCURRENT_SENDS = 3
# 单目标失败退避上限（秒）；退避为 interval * 2^(failures-1)，封顶此值。
PENDING_BACKOFF_CAP_SECONDS = 3600
# 退避指数前先截断失败次数，避免长期失败导致 2**n 指数爆炸/溢出。
PENDING_BACKOFF_MAX_FAILURES = 10


@register(
    name=PLUGIN_NAME,
    author="AstrBot Team",
    desc="单实例 NapCat/OneBot 在线监视：区分账号下线与连接/服务不可达，去抖告警与恢复通知。",
    version="0.1.2",
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
        # 待发送告警队列（内存 + data 目录持久化），按目标 FIFO、有界重试。
        self._pending_file: Path | None = None
        self._pending: list[dict] = []
        # 每目标退避态：target -> {"failures": int, "next_at": epoch}
        self._target_state: dict[str, dict] = {}
        # 通知 worker：生命周期受管、只负责发送，与探测/事件路径解耦。
        self._worker_task: asyncio.Task | None = None
        self._worker_wakeup: asyncio.Event | None = None
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

        # pending 队列始终外置到 data 目录（persist_state 只控制 state.json 快照）。
        self._setup_data_dir(persist_state=cfg["persist_state"])
        if cfg["persist_state"]:
            self._load_snapshot()
        self._load_pending()

        # 通知 worker：与探测解耦，负责从队列取件发送（可取消、可重载）。
        self._worker_wakeup = asyncio.Event()
        self._worker_task = asyncio.create_task(
            self._worker_loop(), name=f"{PLUGIN_NAME}-notify"
        )
        self._wake_worker()  # 立即冲刷上次遗留的待发送队列

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
        """插件停用/热重载入口：取消后台任务与通知 worker，保存状态与队列。"""
        for attr in ("_task", "_worker_task"):
            task = getattr(self, attr)
            setattr(self, attr, None)
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # 我们主动取消，吞掉自身取消信号是预期行为
            except Exception as e:  # 任务收尾异常只记录，不中断停用流程
                logger.warning("%s 后台任务退出时异常（已忽略）：%s", PLUGIN_NAME, e)
        self._save_pending()
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
                # 通知是含 [BotBrother] 前缀与 self_id 的完整文案；此处只入队，
                # 由独立 worker 负责发送，避免发送阻塞探测/事件路径。
                self._handle_alert(msg)

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

    def _wake_worker(self) -> None:
        """唤醒通知 worker（若已启动）。"""
        if self._worker_wakeup is not None:
            self._worker_wakeup.set()

    def _handle_alert(self, title: str) -> None:
        """告警通知：仅按目标入队（追加队尾），由独立 worker 异步发送。

        新告警排在该目标既有待发条目之后（每目标 FIFO），不会跨过更早的
        故障/恢复通知；不改变状态机/快照语义（快照已在调用前保存）。
        此路径不做任何网络请求，因此不会阻塞探测与事件处理。
        注意：队列是“尽量不丢”的兜底，不是“保证不丢失”，也不等于及时预警。
        """
        targets = list(self._config.get("notify_targets") or [])
        if not targets:
            logger.warning("%s 无可用推送目标，告警未发送。", PLUGIN_NAME)
            return
        self._enqueue_pending(title, targets)
        self._wake_worker()

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
        - 复用与 worker 相同的诊断包装（每个目标输出 id，便于在日志中定位该次
          发送）；测试发送不入队、不改退避计数、不影响补发队列。
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
                # 与 worker 复用同一诊断包装；测试发送不入队、不改退避计数。
                status, detail, send_id = await self._send_with_diagnostics(
                    target, TEST_NOTIFICATION_TEXT, attempt=1, source="test"
                )
                statuses.append(status)
                suffix = f"（{detail}）" if detail else ""
                lines.append(
                    f"- {labels.get(status, status)}：{target}{suffix} [id={send_id}]"
                )
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
    def _setup_data_dir(self, persist_state: bool = True) -> None:
        try:
            self._data_dir = StarTools.get_data_dir(PLUGIN_NAME)
            self._data_dir.mkdir(parents=True, exist_ok=True)
            # state.json 快照受 persist_state 控制；pending 队列始终外置。
            self._state_file = (
                (self._data_dir / "state.json") if persist_state else None
            )
            self._pending_file = self._data_dir / PENDING_FILE_NAME
        except Exception as e:
            logger.warning(
                "%s 无法创建数据目录，快照与待发送队列降级为内存：%s",
                PLUGIN_NAME,
                e,
            )
            self._data_dir = None
            self._state_file = None
            self._pending_file = None

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

    # ------------------------------------------------------------------ #
    # 告警待发送队列（data 外置、按目标 FIFO、有界退避重试）
    # ------------------------------------------------------------------ #
    def _enqueue_pending(self, title: str, targets: list[str]) -> None:
        """把告警追加到队尾（每目标 FIFO）；超容量时丢弃最旧并明确记录。"""
        target_list = [t for t in targets if isinstance(t, str) and t.strip()]
        if not target_list:
            return
        self._pending.append(
            {
                "id": uuid.uuid4().hex,
                "ts": time.time(),
                "title": title,
                "targets": target_list,
            }
        )
        dropped = 0
        while len(self._pending) > PENDING_MAX_ENTRIES:
            self._pending.pop(0)
            dropped += 1
        if dropped:
            logger.error(
                "%s 待发送告警超过容量 %d，已丢弃最旧的 %d 条（不保证不丢失）。",
                PLUGIN_NAME,
                PENDING_MAX_ENTRIES,
                dropped,
            )
        self._save_pending()

    def _save_pending(self) -> None:
        if self._pending_file is None:
            return
        payload = {
            "version": PENDING_VERSION,
            "entries": self._pending,
            "target_state": self._target_state,
        }
        try:
            self._pending_file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._pending_file.with_name(self._pending_file.name + ".tmp")
            tmp.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self._pending_file)
        except Exception as e:
            # 写盘失败只记日志：内存队列仍有效，但该次持久化丢失（有边界）。
            logger.warning("%s 待发送队列持久化失败（已忽略）：%s", PLUGIN_NAME, e)

    @staticmethod
    def _clean_pending_entries(raw_entries: object) -> list[dict]:
        clean: list[dict] = []
        if not isinstance(raw_entries, list):
            return clean
        for item in raw_entries:
            if not isinstance(item, dict):
                continue
            title = item.get("title")
            targets = item.get("targets")
            if not isinstance(title, str) or not title:
                continue
            if not isinstance(targets, list):
                continue
            target_list = [t for t in targets if isinstance(t, str) and t.strip()]
            if not target_list:
                continue
            ts = item.get("ts")
            clean.append(
                {
                    "id": str(item.get("id") or uuid.uuid4().hex),
                    "ts": ts
                    if isinstance(ts, (int, float)) and not isinstance(ts, bool)
                    else time.time(),
                    "title": title,
                    "targets": target_list,
                }
            )
        return clean

    @staticmethod
    def _clean_target_state(raw_state: object) -> dict[str, dict]:
        clean: dict[str, dict] = {}
        if not isinstance(raw_state, dict):
            return clean
        for target, state in raw_state.items():
            if not isinstance(target, str) or not target:
                continue
            if not isinstance(state, dict):
                continue
            failures = state.get("failures")
            next_at = state.get("next_at")
            # failures 必须是有界非负 int；next_at 必须是有限非负数值。
            # 否则视为损坏：丢弃该退避态（而不是接受 inf/巨大值导致永久停发）。
            failures_ok = (
                isinstance(failures, int)
                and not isinstance(failures, bool)
                and 0 <= failures <= PENDING_BACKOFF_MAX_FAILURES
            )
            next_at_value: float | None = None
            if isinstance(next_at, (int, float)) and not isinstance(next_at, bool):
                try:
                    candidate = float(next_at)
                except (OverflowError, ValueError):
                    # 例如合法 JSON 里超大的 int：float() 会 OverflowError，必须兜底。
                    candidate = None
                if (
                    candidate is not None
                    and math.isfinite(candidate)
                    and candidate >= 0
                ):
                    next_at_value = candidate
            if failures_ok and next_at_value is not None:
                clean[target] = {"failures": failures, "next_at": next_at_value}
        return clean

    def _load_pending(self) -> None:
        """载入待发送队列（兼容 v1 列表）；损坏/结构非法严格回退空。"""
        if self._pending_file is None or not self._pending_file.exists():
            return
        try:
            raw = json.loads(self._pending_file.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning("%s 待发送队列加载失败（已忽略）：%s", PLUGIN_NAME, e)
            self._pending = []
            self._target_state = {}
            return
        try:
            if isinstance(raw, dict) and isinstance(raw.get("entries"), list):
                entries = raw["entries"]
                target_state = self._clean_target_state(raw.get("target_state"))
            elif isinstance(raw, list):  # 兼容旧 v1 列表格式
                entries = raw
                target_state = {}
            else:
                self._pending = []
                self._target_state = {}
                return
            self._pending = self._clean_pending_entries(entries)[-PENDING_MAX_ENTRIES:]
            self._target_state = target_state
        except Exception as e:
            # 任何清洗异常（含极端数值转换）都不得让初始化失败。
            logger.warning("%s 待发送队列解析异常（已忽略）：%s", PLUGIN_NAME, e)
            self._pending = []
            self._target_state = {}

    def _backoff_delay(self, failures: int) -> float:
        base = max(int(self._config.get("interval_seconds", 30) or 30), 5)
        # 先截断指数，避免长期失败时 2**n 指数爆炸/溢出。
        capped = min(max(int(failures), 1), PENDING_BACKOFF_MAX_FAILURES)
        return min(base * (2 ** (capped - 1)), PENDING_BACKOFF_CAP_SECONDS)

    # ------------------------------------------------------------------ #
    # 通知 worker（生命周期受管；与探测解耦）
    # ------------------------------------------------------------------ #
    def _next_wakeup_delay(self) -> float | None:
        """worker 下次醒来延迟：None=队列空；0=有可立即尝试；>0=最近退避到期。"""
        now = time.time()
        soonest: float | None = None
        for entry in self._pending:
            for target in entry.get("targets") or []:
                state = self._target_state.get(target) or {}
                next_at = float(state.get("next_at", 0) or 0)
                if next_at <= now:
                    return 0.0
                soonest = next_at if soonest is None else min(soonest, next_at)
        if soonest is None:
            return None
        return max(0.0, soonest - now)

    async def _worker_loop(self) -> None:
        """通知 worker：只从队列取件发送；可取消、无泄漏、不参与探测。"""
        wakeup = self._worker_wakeup
        assert wakeup is not None
        while True:
            try:
                delay = self._next_wakeup_delay()
                if delay is None:
                    await wakeup.wait()
                elif delay > 0:
                    try:
                        await asyncio.wait_for(wakeup.wait(), timeout=delay)
                    except asyncio.TimeoutError:
                        pass
                wakeup.clear()
                await self._drain_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # 仅记录，worker 自身不崩
                logger.error("%s 通知 worker 异常（已忽略）：%s", PLUGIN_NAME, e)
                await asyncio.sleep(1)

    async def _send_with_diagnostics(
        self,
        target: str,
        text: str,
        *,
        attempt: int = 1,
        source: str = "worker",
    ) -> tuple[str, str, str]:
        """所有发送路径共享的诊断包装：返回 ``(status, detail, send_id)``。

        - 输出结构化诊断日志（source / 关联 id / 目标 / 平台实例 / 尝试次数 /
          耗时 / 状态 / 异常类型）。
        - 不记录任何凭据、完整请求或 token；原始 ret/errmsg 经公开 API 不可得。
        - 不做入队、不改退避计数——状态处理由调用方决定。
        """
        send_id = uuid.uuid4().hex[:12]
        platform = target.split(":", 1)[0] if isinstance(target, str) else "?"
        started = time.monotonic()
        logger.info(
            "%s 发送开始 id=%s source=%s target=%s platform=%s attempt=%d",
            PLUGIN_NAME,
            send_id,
            source,
            target,
            platform,
            attempt,
        )
        status, detail = await self._send_to_target(
            target, MessageChain([Plain(text=text)])
        )
        elapsed = time.monotonic() - started
        if status == "ok":
            logger.info(
                "%s 发送结果 id=%s source=%s target=%s status=ok elapsed=%.2fs"
                "（仅表示适配器调用返回，不代表已读/最终投递）",
                PLUGIN_NAME,
                send_id,
                source,
                target,
                elapsed,
            )
        else:
            logger.warning(
                "%s 发送结果 id=%s source=%s target=%s status=%s elapsed=%.2fs "
                "detail=%s（原始 ret/errmsg 经公开 API 不可得；"
                "请按时间/平台关联适配器日志）",
                PLUGIN_NAME,
                send_id,
                source,
                target,
                status,
                elapsed,
                detail,
            )
        return status, detail, send_id

    async def _attempt_send(self, entry: dict, target: str) -> tuple[str, str]:
        """worker 单次发送：共享诊断包装 + 队列所需 (status, detail)。

        诊断 id 仅写日志；FIFO/退避由 _drain_once 统一维护。
        """
        attempt = int((self._target_state.get(target) or {}).get("failures", 0)) + 1
        status, detail, _send_id = await self._send_with_diagnostics(
            target, entry.get("title", ""), attempt=attempt, source="worker"
        )
        return status, detail

    async def _drain_once(self) -> None:
        """单轮有界投递：每目标 FIFO + 有限并发 + 退避 + 公平。

        - 每个目标每轮只尝试其最早的一条待发条目（保持 FIFO，坏目标不越级）。
        - 每轮最多 PENDING_MAX_ATTEMPTS_PER_CYCLE 次尝试、至多
          PENDING_MAX_CONCURRENT_SENDS 个并发；单目标超时/错误不影响其它目标。
        - 失败目标进入指数退避；成功目标移除；不再配置的目标清除。
        - 取消信号照常上抛（不吞 CancelledError）。
        """
        if not self._pending:
            return
        now = time.time()
        configured = set(self._config.get("notify_targets") or [])
        changed = False

        # 1) 清理：空标题、已删除目标、孤立退避态
        for entry in list(self._pending):
            title = entry.get("title")
            if not isinstance(title, str) or not title:
                self._pending.remove(entry)
                changed = True
                continue
            kept = [
                t
                for t in (entry.get("targets") or [])
                if isinstance(t, str) and t in configured
            ]
            if kept != entry.get("targets"):
                entry["targets"] = kept
                changed = True
            if not entry["targets"]:
                self._pending.remove(entry)
                changed = True
        for target in list(self._target_state):
            if target not in configured:
                self._target_state.pop(target, None)
                changed = True

        # 2) 选取本轮可尝试的 (entry, target)：每目标取其最早条目
        scheduled: list[tuple[dict, str]] = []
        scheduled_targets: set[str] = set()
        for entry in self._pending:
            for target in entry.get("targets") or []:
                if target in scheduled_targets:
                    continue
                scheduled_targets.add(target)
                state = self._target_state.get(target) or {}
                if now < float(state.get("next_at", 0) or 0):
                    continue  # 退避中：该目标本轮不尝试，其更新条目也不越级
                scheduled.append((entry, target))
                if len(scheduled) >= PENDING_MAX_ATTEMPTS_PER_CYCLE:
                    break
            if len(scheduled) >= PENDING_MAX_ATTEMPTS_PER_CYCLE:
                break

        if not scheduled:
            if changed:
                self._save_pending()
            return

        # 3) 有限并发执行：return_exceptions 确保单子任务意外异常不影响其它子任务
        semaphore = asyncio.Semaphore(PENDING_MAX_CONCURRENT_SENDS)

        async def _guarded(entry: dict, target: str) -> tuple[str, str]:
            async with semaphore:
                try:
                    return await self._attempt_send(entry, target)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # 意外异常按失败处理，其它目标继续
                    logger.error(
                        "%s 发送尝试意外异常 target=%s（已按失败处理）：%s",
                        PLUGIN_NAME,
                        target,
                        type(e).__name__,
                    )
                    return "error", f"{type(e).__name__}（详情见日志）"

        outcomes = await asyncio.gather(
            *(_guarded(entry, target) for entry, target in scheduled),
            return_exceptions=True,
        )

        # 4) 串行应用结果（避免并发改表）；成功结果必须保留
        cancelled: asyncio.CancelledError | None = None
        for (entry, target), outcome in zip(scheduled, outcomes):
            if isinstance(outcome, asyncio.CancelledError):
                cancelled = outcome  # 取消：等待所有子任务结束后再传播
                continue
            if isinstance(outcome, BaseException):
                logger.error(
                    "%s 发送结果异常 target=%s（按失败处理）：%s",
                    PLUGIN_NAME,
                    target,
                    type(outcome).__name__,
                )
                status = "error"
            else:
                status, _detail = outcome
            if status == "ok":
                if target in entry.get("targets", []):
                    entry["targets"].remove(target)
                self._target_state.pop(target, None)
            else:
                prev = int(
                    (self._target_state.get(target) or {}).get("failures", 0) or 0
                )
                failures = min(prev + 1, PENDING_BACKOFF_MAX_FAILURES)
                self._target_state[target] = {
                    "failures": failures,
                    "next_at": time.time() + self._backoff_delay(failures),
                }
        self._pending = [e for e in self._pending if e.get("targets")]
        self._save_pending()
        if cancelled is not None:
            raise cancelled
