"""集成冒烟测试（可选）：需要真实 AstrBot 环境。

- 未安装 AstrBot 时自动跳过（skip）。
- 覆盖：插件注册（@register + AIOCQHTTP 平台过滤器）、initialize/terminate
  生命周期、_probe 探测映射（online/offline/unreachable）、告警推送、任务取消。

运行（在有 AstrBot 的 Python 环境中）：
    python3 -m unittest discover -s tests -v
"""

import asyncio
import importlib
import importlib.util
import os
import sys
import unittest
from types import SimpleNamespace

PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PLUGIN_DIR)

HAS_ASTRBOT = importlib.util.find_spec("astrbot") is not None

if HAS_ASTRBOT:
    from astrbot.api.all import AstrBotConfig, MessageChain
    from astrbot.core.star.context import Context
    from astrbot.core.star.filter.platform_adapter_type import (
        PlatformAdapterType,
        PlatformAdapterTypeFilter,
    )
    from astrbot.core.star.register.star_handler import get_handler_full_name
    from astrbot.core.star.star_handler import EventType, star_handlers_registry

    from state_machine import OFFLINE, ONLINE, UNREACHABLE


def _load_plugin_main():
    """以包形式加载插件 main（模拟 AstrBot 的 data.plugins.<name>.main 导入）。"""
    pkg_name = "astrbot_plugin_botbrother"
    parent_spec = importlib.util.spec_from_file_location(
        pkg_name, os.path.join(PLUGIN_DIR, "__init__.py")
    )
    parent = importlib.util.module_from_spec(parent_spec)
    parent.__path__ = [PLUGIN_DIR]
    sys.modules[pkg_name] = parent
    spec = importlib.util.spec_from_file_location(
        f"{pkg_name}.main", os.path.join(PLUGIN_DIR, "main.py")
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"{pkg_name}.main"] = mod
    spec.loader.exec_module(mod)
    return mod


def _plain_text(chain):
    """从 MessageChain 提取纯文本。

    AstrBot 4.x 中 str(MessageChain) 返回对象 repr 而非文本，必须走
    get_plain_text()（或兜底拼接各段 text）才能拿到实际推送内容。
    """
    getter = getattr(chain, "get_plain_text", None)
    if callable(getter):
        return getter()
    parts = []
    for seg in chain:
        text = getattr(seg, "text", None)
        if text is not None:
            parts.append(text)
    return "".join(parts)


@unittest.skipUnless(HAS_ASTRBOT, "AstrBot 未安装，跳过集成冒烟测试")
class PluginRegistrationTest(unittest.TestCase):
    """插件必须以 @register 注册，且事件处理器仅接受 AIOCQHTTP。"""

    @classmethod
    def setUpClass(cls):
        cls.main = _load_plugin_main()

    def test_star_class_registered(self):
        from astrbot.core.star import star_map

        md = star_map.get("astrbot_plugin_botbrother.main")
        self.assertIsNotNone(md, "@register 应把插件类注册进 star_map")
        self.assertTrue(issubclass(md.star_cls_type, self.main.Star))

    def test_handler_only_aiocqhttp(self):
        full_name = get_handler_full_name(
            self.main.BotBrotherMonitor.on_aiocqhttp_event
        )
        md = star_handlers_registry.get_handler_by_full_name(full_name)
        self.assertIsNotNone(md, "AIOCQHTTP 事件处理器应已注册")
        self.assertEqual(md.event_type, EventType.AdapterMessageEvent)
        filters = [
            f
            for f in md.event_filters
            if isinstance(f, PlatformAdapterTypeFilter)
        ]
        self.assertTrue(filters, "应存在平台适配器过滤器")
        self.assertTrue(
            filters[0].platform_type & PlatformAdapterType.AIOCQHTTP,
            "过滤器应只接受 AIOCQHTTP",
        )


@unittest.skipUnless(HAS_ASTRBOT, "AstrBot 未安装，跳过集成冒烟测试")
class LifecycleSmokeTest(unittest.TestCase):
    """生命周期 + 探测 + 推送 冒烟。"""

    @classmethod
    def setUpClass(cls):
        cls.main = _load_plugin_main()

    def setUp(self):
        self.sent = []

        class FakeBot:
            def __init__(self):
                self.calls = []
                self.responses = [{"online": True}]

            async def call_action(self, action, **kwargs):
                self.calls.append(action)
                if self.responses:
                    r = self.responses.pop(0)
                    if isinstance(r, Exception):
                        raise r
                    return r
                return {"online": True}

        class FakePlatform:
            def __init__(self, bot):
                self._bot = bot
                self.sent = []

            def meta(self):
                return SimpleNamespace(id="aiocqhttp", name="aiocqhttp")

            @property
            def bot(self):
                return self._bot

            async def send_by_session(self, session, chain):
                self.sent.append((str(session), chain))

        self.bot = FakeBot()
        self.platform = FakePlatform(self.bot)

        class FakePlatformManager:
            platform_insts = []

        ctx = object.__new__(Context)
        ctx.platform_manager = FakePlatformManager()
        ctx.platform_manager.platform_insts = [self.platform]
        self.context = ctx

    def _make_plugin(self, **overrides):
        # 生产环境由 AstrBot 传入由 _conf_schema.json 构建的 AstrBotConfig(dict 子类)；
        # 测试直接传普通 dict（插件内部使用 dict(self.config)，两者兼容）。
        cfg = {
            "enabled": True,
            "target_self_id": "123456",
            "platform_id": "aiocqhttp",
            "interval_seconds": 5,
            "debounce": 3,
            "probe_timeout_seconds": 5,
            "notify_targets": ["aiocqhttp:GroupMessage:10001"],
            "persist_state": False,
            **overrides,
        }
        return self.main.BotBrotherMonitor(self.context, cfg)

    def test_probe_mapping(self):
        async def run():
            plugin = self._make_plugin()
            await plugin.initialize()
            # online
            self.bot.responses = [{"online": True}]
            state, _ = await plugin._probe()
            self.assertEqual(state, ONLINE)
            # account offline
            self.bot.responses = [{"online": False}]
            state, detail = await plugin._probe()
            self.assertEqual(state, OFFLINE)
            self.assertIn("online=false", detail)
            # unreachable: exception
            self.bot.responses = [ConnectionError("ws down")]
            state, detail = await plugin._probe()
            self.assertEqual(state, UNREACHABLE)
            self.assertIn("ws down", detail)
            # unreachable: no platform instance
            ctx2 = object.__new__(Context)
            ctx2.platform_manager = SimpleNamespace(platform_insts=[])
            plugin2 = self.main.BotBrotherMonitor(ctx2, {
                "enabled": True, "target_self_id": "1",
                "platform_id": "aiocqhttp",
                "notify_targets": ["a:b:c"], "persist_state": False,
            })
            await plugin2.initialize()
            state, detail = await plugin2._probe()
            self.assertEqual(state, UNREACHABLE)
            await plugin.terminate()
            await plugin2.terminate()

        asyncio.run(run())

    def test_alert_pushed_once_after_debounce(self):
        async def run():
            plugin = self._make_plugin()
            await plugin.initialize()
            self.bot.responses = [{"online": False}] * 10
            # 模拟 3 次连续故障后告警一次
            for _ in range(3):
                state, detail = await plugin._probe()
                await plugin._feed_and_notify(state, detail)
            self.assertEqual(len(self.platform.sent), 1, "去抖后应只推送一次告警")
            # 推送负载必须是单条完整文案：不得重复附加 self_id/标题/详情
            self.assertEqual(
                _plain_text(self.platform.sent[0][1]),
                "[BotBrother] 警告：123456账号离线。如本次离线为您主动触发，请忽略本信息。",
            )
            # 后续同故障不重复推送
            state, detail = await plugin._probe()
            await plugin._feed_and_notify(state, detail)
            self.assertEqual(len(self.platform.sent), 1)
            await plugin.terminate()

        asyncio.run(run())

    def test_recovery_pushed_once(self):
        async def run():
            plugin = self._make_plugin()
            await plugin.initialize()
            self.bot.responses = [{"online": False}] * 3 + [{"online": True}] * 10
            for _ in range(3):
                state, detail = await plugin._probe()
                await plugin._feed_and_notify(state, detail)
            self.assertEqual(len(self.platform.sent), 1)
            for _ in range(3):  # 连续 3 次 online -> 恢复
                state, _ = await plugin._probe()
                await plugin._feed_and_notify(state)
            self.assertEqual(len(self.platform.sent), 2, "恢复通知应补发一次")
            self.assertEqual(
                _plain_text(self.platform.sent[1][1]),
                "[BotBrother] 123456已恢复在线。",
                "恢复推送必须是单条完整文案，不得重复附加 self_id/标题/详情",
            )
            await plugin.terminate()

        asyncio.run(run())

    def test_terminate_cancels_task(self):
        async def run():
            plugin = self._make_plugin()
            await plugin.initialize()
            self.assertIsNotNone(plugin._task)
            await plugin.terminate()
            self.assertIsNone(plugin._task)
            # 确认任务已取消，无泄漏
            await asyncio.sleep(0.05)

        asyncio.run(run())

    def test_invalid_config_stays_inert(self):
        async def run():
            plugin = self._make_plugin(target_self_id="", notify_targets=[])
            await plugin.initialize()
            self.assertIsNone(plugin._task, "配置无效时不应启动后台任务")

        asyncio.run(run())

    def test_push_isolated_per_target(self):
        """单目标发送异常只记录错误，不得阻塞/影响其他目标。"""
        sent, tried = [], []

        class FlakyPlatform:
            def __init__(self, pid, boom):
                self._meta = SimpleNamespace(id=pid, name=pid)
                self._boom = boom

            def meta(self):
                return self._meta

            async def send_by_session(self, session, chain):
                tried.append(str(session))
                if self._boom:
                    raise RuntimeError("send failed")
                sent.append(str(session))

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": False}

        async def run():
            ctx = object.__new__(Context)
            ctx.platform_manager = SimpleNamespace(platform_insts=[
                FlakyPlatform("bad_channel", True),
                FlakyPlatform("good_channel", False),
            ])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": [
                    "bad_channel:GroupMessage:1",
                    "good_channel:FriendMessage:2",
                ],
                "persist_state": False,
            })
            await plugin.initialize()
            for _ in range(3):
                state, detail = await plugin._probe()
                await plugin._feed_and_notify(state, detail)
            self.assertEqual(len(sent), 1, "正常目标应收到告警")
            self.assertEqual(sent[0], "good_channel:FriendMessage:2")
            self.assertEqual(tried[0], "bad_channel:GroupMessage:1",
                             "故障目标也应被尝试过")
            self.assertEqual(tried, [
                "bad_channel:GroupMessage:1",
                "good_channel:FriendMessage:2",
            ], "故障目标不得阻塞后续目标")
            await plugin.terminate()

        asyncio.run(run())

    def test_push_unknown_platform_returns_false_no_crash(self):
        """send_message 返回 False（找不到平台）时只记日志，不崩溃、不阻塞。"""

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": False}

        class FakePlatform:
            def __init__(self):
                self._meta = SimpleNamespace(id="napcat", name="aiocqhttp")
                self._bot = FakeBot()

            def meta(self):
                return self._meta

        async def run():
            ctx = object.__new__(Context)
            ctx.platform_manager = SimpleNamespace(
                platform_insts=[FakePlatform()])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": ["ghost_platform:GroupMessage:1"],
                "persist_state": False,
            })
            await plugin.initialize()
            for _ in range(3):
                state, detail = await plugin._probe()
                await plugin._feed_and_notify(state, detail)
            await plugin.terminate()  # 未崩溃即通过

        asyncio.run(run())

    def test_push_invalid_umo_string_no_crash(self):
        """send_message 对不合法 UMO 抛 ValueError：插件须吞掉并继续。"""

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": False}

        class FakePlatform:
            def __init__(self):
                self._meta = SimpleNamespace(id="napcat", name="aiocqhttp")
                self._bot = FakeBot()

            def meta(self):
                return self._meta

        async def run():
            ctx = object.__new__(Context)
            ctx.platform_manager = SimpleNamespace(
                platform_insts=[FakePlatform()])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "776916629",
                "platform_id": "napcat",
                # 结构合法（3 段），但消息类型枚举非法 -> from_str 抛 ValueError
                "notify_targets": ["napcat:NotAType:1"],
                "persist_state": False,
            })
            await plugin.initialize()
            for _ in range(3):
                state, detail = await plugin._probe()
                await plugin._feed_and_notify(state, detail)
            await plugin.terminate()  # 未崩溃即通过

        asyncio.run(run())

    def test_custom_platform_id_success_path(self):
        """平台实例 id 非 aiocqhttp（用户自定义）时，按配置的 platform_id 探测成功。

        对应审计要求：platform_id 为必填配置，取 AstrBot Bots 中的平台实例 ID，
        不能假设默认等于 "aiocqhttp"。
        """

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": True}

        class FakePlatform:
            def __init__(self, mid, name):
                self._meta = SimpleNamespace(id=mid, name=name)
                self._bot = FakeBot()

            def meta(self):
                return self._meta

            @property
            def bot(self):
                return self._bot

        async def run():
            ctx = object.__new__(Context)
            insts = [FakePlatform("my_napcat", "aiocqhttp")]
            ctx.platform_manager = SimpleNamespace(platform_insts=insts)
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "123456",
                "platform_id": "my_napcat",  # 用户自定义的平台实例 ID
                "notify_targets": ["my_napcat:GroupMessage:10001"],
                "persist_state": False,
            })
            await plugin.initialize()
            self.assertEqual(plugin._platform_id, "my_napcat")
            state, _ = await plugin._probe()
            self.assertEqual(state, ONLINE, "应按配置的 platform_id 找到平台并探测成功")
            await plugin.terminate()

        asyncio.run(run())

    def test_wrong_platform_id_reports_unreachable(self):
        """配置的 platform_id 与任何平台实例都不匹配时，判定连接/服务不可达。"""

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": True}

        class FakePlatform:
            def __init__(self, mid, name):
                self._meta = SimpleNamespace(id=mid, name=name)
                self._bot = FakeBot()

            def meta(self):
                return self._meta

            @property
            def bot(self):
                return self._bot

        async def run():
            ctx = object.__new__(Context)
            # 唯一的 aiocqhttp 平台实例 id 是 "real_napcat"，配置却填了 "other"
            ctx.platform_manager = SimpleNamespace(
                platform_insts=[FakePlatform("real_napcat", "aiocqhttp")]
            )
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "123456",
                "platform_id": "other",
                "notify_targets": ["a:b:c"],
                "persist_state": False,
            })
            await plugin.initialize()
            state, detail = await plugin._probe()
            self.assertEqual(state, UNREACHABLE)
            self.assertIn("other", detail)
            await plugin.terminate()

        asyncio.run(run())

    def test_two_instances_only_target_called(self):
        """多实例场景：配置的 platform_id 只调用目标实例，绝不扫描/错绑。"""

        class FakeBot:
            def __init__(self, name):
                self.name = name
                self.calls = []

            async def call_action(self, action, **kwargs):
                self.calls.append(action)
                return {"online": True}

        class FakePlatform:
            def __init__(self, mid, name, bot):
                self._meta = SimpleNamespace(id=mid, name=name)
                self._bot = bot

            def meta(self):
                return self._meta

            @property
            def bot(self):
                return self._bot

        async def run():
            bot_a = FakeBot("bot_a")
            bot_b = FakeBot("bot_b")
            ctx = object.__new__(Context)
            ctx.platform_manager = SimpleNamespace(platform_insts=[
                FakePlatform("napcat_a", "aiocqhttp", bot_a),
                FakePlatform("napcat_b", "aiocqhttp", bot_b),
            ])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "123456",
                "platform_id": "napcat_b",
                "notify_targets": ["napcat_b:GroupMessage:10001"],
                "persist_state": False,
            })
            await plugin.initialize()
            self.assertEqual(plugin._platform_id, "napcat_b")
            state, _ = await plugin._probe()
            self.assertEqual(state, ONLINE)
            self.assertEqual(bot_b.calls, ["get_status"], "应只调用目标实例")
            self.assertEqual(bot_a.calls, [], "不得调用/错绑其他实例")
            await plugin.terminate()

        asyncio.run(run())

    def test_same_id_non_aiocqhttp_platform_not_bound(self):
        """ID 相同但适配器类型不是 aiocqhttp 的平台实例不得被绑定。"""

        class FakeBot:
            async def call_action(self, action, **kwargs):
                return {"online": True}

        class FakePlatform:
            def __init__(self, mid, name, bot=None):
                self._meta = SimpleNamespace(id=mid, name=name)
                self._bot = bot or FakeBot()

            def meta(self):
                return self._meta

            @property
            def bot(self):
                return self._bot

        async def run():
            ctx = object.__new__(Context)
            ctx.platform_manager = SimpleNamespace(platform_insts=[
                FakePlatform("shared_id", "qqchannel"),  # 同 ID 但非 aiocqhttp
                FakePlatform("napcat1", "aiocqhttp"),
            ])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "123456",
                "platform_id": "shared_id",
                "notify_targets": ["a:b:c"],
                "persist_state": False,
            })
            await plugin.initialize()
            state, detail = await plugin._probe()
            self.assertEqual(state, UNREACHABLE, "非 aiocqhttp 平台不得被绑定")
            self.assertIn("shared_id", detail)
            await plugin.terminate()

        asyncio.run(run())

    def test_no_aiocqhttp_platform_reports_unreachable(self):
        async def run():
            ctx = object.__new__(Context)
            other = SimpleNamespace(
                meta=lambda: SimpleNamespace(id="qqchannel", name="qqchannel"),
            )
            ctx.platform_manager = SimpleNamespace(platform_insts=[other])
            plugin = self.main.BotBrotherMonitor(ctx, {
                "enabled": True,
                "target_self_id": "1",
                "platform_id": "some_id",
                "notify_targets": ["a:b:c"],
                "persist_state": False,
            })
            await plugin.initialize()
            state, detail = await plugin._probe()
            self.assertEqual(state, UNREACHABLE)
            self.assertIn("some_id", detail)
            await plugin.terminate()

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
