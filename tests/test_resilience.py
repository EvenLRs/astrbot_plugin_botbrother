"""故障注入回归测试：异常隔离、超时、取消传播、坏快照、写盘失败。

本文件不依赖真实 AstrBot：未安装时注入最小桩模块后加载插件 main，
因此可在纯开发环境运行；若已安装 AstrBot 则直接使用真实模块
（此时 tests/test_integration_smoke.py 亦会执行）。
"""

import asyncio
import importlib
import importlib.util
import logging
import os
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

PLUGIN_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PLUGIN_DIR)

from state_machine import OFFLINE, ONLINE, MonitorStateMachine  # noqa: E402

HAS_ASTRBOT = importlib.util.find_spec("astrbot") is not None


def _install_astrbot_stub():
    """安装最小 astrbot 桩，供 main.py 的 import 通过（不模拟真实行为）。"""

    def mod(name):
        m = types.ModuleType(name)
        m.__path__ = []
        sys.modules[name] = m
        return m

    astrbot = mod("astrbot")
    api = mod("astrbot.api")
    api.logger = logging.getLogger("astrbot.stub")
    api.logger.addHandler(logging.NullHandler())

    allm = mod("astrbot.api.all")

    class Star:
        def __init__(self, context, config):
            self.context = context
            self.config = config

    class MessageChain(list):
        def __init__(self, segs=()):
            super().__init__(segs)

        def get_plain_text(self):
            return "".join(getattr(s, "text", "") for s in self)

    class Plain:
        def __init__(self, text):
            self.text = text

    class Context:
        pass

    class AstrBotConfig(dict):
        pass

    class AstrMessageEvent:
        pass

    class MessageEventResult:
        def __init__(self, text=""):
            self.text = text

        def message(self, text):
            self.text = text
            return self

    def register(**kwargs):
        def deco(cls):
            cls._stub_registered = kwargs
            return cls

        return deco

    allm.Star = Star
    allm.MessageChain = MessageChain
    allm.Plain = Plain
    allm.Context = Context
    allm.AstrBotConfig = AstrBotConfig
    allm.AstrMessageEvent = AstrMessageEvent
    allm.MessageEventResult = MessageEventResult
    allm.register = register
    api.all = allm

    starmod = mod("astrbot.api.star")

    class StarTools:
        @staticmethod
        def get_data_dir(name):
            return Path(tempfile.mkdtemp(prefix="botbrother-stub-")) / name

    starmod.StarTools = StarTools
    api.star = starmod

    core = mod("astrbot.core")
    star_pkg = mod("astrbot.core.star")
    filter_pkg = mod("astrbot.core.star.filter")
    filt = mod("astrbot.core.star.filter.platform_adapter_type")

    class PlatformAdapterType:
        AIOCQHTTP = 1

    class PermissionType:
        ADMIN = 1
        MEMBER = 2

    filt.PlatformAdapterType = PlatformAdapterType
    perm = mod("astrbot.core.star.filter.permission")
    perm.PermissionType = PermissionType
    regmod = mod("astrbot.core.star.register")

    def register_command(command_name=None, alias=None, **kwargs):
        def deco(func):
            func._stub_command = command_name
            return func

        return deco

    def register_permission_type(permission_type, raise_error=True):
        def deco(func):
            func._stub_permission = permission_type
            return func

        return deco

    def register_platform_adapter_type(*args, **kwargs):
        def deco(func):
            return func

        return deco

    regmod.register_command = register_command
    regmod.register_permission_type = register_permission_type
    regmod.register_platform_adapter_type = register_platform_adapter_type

    core.star = star_pkg
    star_pkg.filter = filter_pkg
    filter_pkg.platform_adapter_type = filt
    filter_pkg.permission = perm
    star_pkg.register = regmod

    astrbot.api = api
    astrbot.core = core


if not HAS_ASTRBOT:
    _install_astrbot_stub()


def _load_plugin_main():
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


@contextmanager
def _push_timeout(seconds):
    """临时改写推送超时，测试用。"""
    main = _load_plugin_main()
    old = main.PUSH_TIMEOUT_SECONDS
    main.PUSH_TIMEOUT_SECONDS = seconds
    try:
        yield main
    finally:
        main.PUSH_TIMEOUT_SECONDS = old


def _make_plugin(main, send_impl, targets, debounce=1):
    class Ctx:
        send_message = staticmethod(send_impl)

    plugin = main.BotBrotherMonitor(Ctx(), {"notify_targets": list(targets)})
    plugin._config = {"notify_targets": list(targets), "target_self_id": "1"}
    plugin._machine = MonitorStateMachine(debounce=debounce)
    return plugin


class PushIsolationTest(unittest.TestCase):
    """单目标挂起/失败不得阻塞后续目标，也不得吞掉取消。"""

    def test_hang_target_times_out_and_next_target_still_sent(self):
        calls, sent = [], []

        async def send(target, chain):
            calls.append(target)
            if target == "hang":
                await asyncio.sleep(60)
            sent.append(target)
            return True

        with _push_timeout(0.05) as main:
            plugin = _make_plugin(main, send, ["hang", "ok"])
            asyncio.run(plugin._push("msg"))

        self.assertEqual(calls, ["hang", "ok"], "挂起目标之后的目标仍须被尝试")
        self.assertEqual(sent, ["ok"], "正常目标应真正收到消息")

    def test_failing_target_then_next_succeeds(self):
        calls, sent = [], []

        async def send(target, chain):
            calls.append(target)
            if target.startswith("bad"):
                raise RuntimeError("boom")
            sent.append(target)
            return True

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["bad", "good"])
            asyncio.run(plugin._push("msg"))

        self.assertEqual(calls, ["bad", "good"])
        self.assertEqual(sent, ["good"])

    def test_cancelled_error_propagates(self):
        async def send(target, chain):
            raise asyncio.CancelledError()

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["a"])
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(plugin._push("msg"))

    def test_feed_and_notify_survives_hanging_push(self):
        """推送挂起（超时后）不得让 _feed_and_notify 抛异常。"""

        async def send(target, chain):
            await asyncio.sleep(60)
            return True

        with _push_timeout(0.05) as main:
            plugin = _make_plugin(main, send, ["hang"], debounce=1)
            asyncio.run(
                plugin._feed_and_notify(
                    OFFLINE,
                    "down",
                )
            )
        self.assertEqual(plugin._machine.state, OFFLINE)


class MonitorLoopSurvivalTest(unittest.TestCase):
    """一轮推送被挂起后，监视循环必须继续后续轮次。"""

    def test_polling_continues_after_hung_push(self):
        probes = []

        with _push_timeout(0.05) as main:

            async def send(target, chain):
                await asyncio.sleep(60)
                return True

            plugin = _make_plugin(main, send, ["hang"], debounce=1)

            async def fake_probe():
                probes.append(1)
                return OFFLINE, "down"

            plugin._probe = fake_probe
            plugin._config["interval_seconds"] = 0.01

            async def run():
                task = asyncio.create_task(plugin._monitor_loop())
                await asyncio.sleep(0.3)
                alive = not task.done()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                return alive

            alive = asyncio.run(run())

        self.assertTrue(alive, "循环不得因单次推送挂起而退出")
        self.assertGreaterEqual(len(probes), 2, "挂起后应继续探测后续轮次")


class MalformedConfigTest(unittest.TestCase):
    """畸形配置返回错误且不启动，不伪造运行成功。"""

    def test_non_iterable_notify_targets_stays_inert(self):
        with _push_timeout(1) as main:
            plugin = main.BotBrotherMonitor(
                SimpleNamespace(),
                {
                    "enabled": True,
                    "target_self_id": "1",
                    "platform_id": "napcat",
                    "notify_targets": 123,
                    "persist_state": False,
                },
            )
            asyncio.run(plugin.initialize())
        self.assertIsNone(plugin._task, "配置无效时不得启动后台任务")


class SnapshotWriteFailureTest(unittest.TestCase):
    """写盘失败仅告警，不抛异常、不中断监视。"""

    def test_save_snapshot_swallows_write_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            ro = Path(tmp) / "ro"
            ro.mkdir()
            os.chmod(ro, 0o555)
            try:
                with _push_timeout(1) as main:
                    plugin = _make_plugin(main, _noop_send, ["a"])
                    plugin._state_file = ro / "state.json"
                    plugin._machine.feed(OFFLINE)  # 使快照非空
                    plugin._save_snapshot()  # 只读目录 -> 内部 OSError 必须被吞
            finally:
                os.chmod(ro, 0o755)

    def test_feed_and_notify_survives_write_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            ro = Path(tmp) / "ro"
            ro.mkdir()
            os.chmod(ro, 0o555)
            try:
                with _push_timeout(1) as main:
                    plugin = _make_plugin(main, _noop_send, ["a"], debounce=1)
                    plugin._state_file = ro / "state.json"
                    asyncio.run(plugin._feed_and_notify(OFFLINE, "down"))
                self.assertEqual(plugin._machine.state, OFFLINE)
            finally:
                os.chmod(ro, 0o755)


class CorruptSnapshotRecoveryTest(unittest.TestCase):
    """坏快照恢复后监视仍可启动且不崩溃。"""

    def test_load_corrupt_snapshot_file_falls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state_file.write_text('{"state": ["bogus"], "pending_fault": "x"}', "utf-8")
            with _push_timeout(1) as main:
                plugin = _make_plugin(main, _noop_send, ["a"])
                plugin._machine = MonitorStateMachine(debounce=1)
                plugin._state_file = state_file
                plugin._load_snapshot()
            self.assertEqual(plugin._machine.state, ONLINE)


class FakeEvent:
    """最小事件桩：仅提供 handler 用到的 is_admin / plain_result。"""

    def __init__(self, admin=True):
        self._admin = admin
        self.replies = []

    def is_admin(self):
        return self._admin

    def plain_result(self, text):
        result = SimpleNamespace(text=text)
        self.replies.append(text)
        return result


def _command_plugin(main, send_impl, **overrides):
    class Ctx:
        send_message = staticmethod(send_impl)

    cfg = {
        "enabled": True,
        "target_self_id": "1",
        "platform_id": "napcat",
        "notify_targets": ["napcat:GroupMessage:1"],
        "persist_state": False,
        **overrides,
    }
    return main.BotBrotherMonitor(Ctx(), cfg)


class TestNotificationCommandTest(unittest.TestCase):
    """管理员测试通知指令：鉴权、目标限制、状态隔离、容错、防重入。"""

    @unittest.skipIf(HAS_ASTRBOT, "桩属性仅在未安装 AstrBot 时存在；真实环境见集成测试")
    def test_decorators_registered(self):
        with _push_timeout(1) as main:
            fn = main.BotBrotherMonitor.on_test_notification
            self.assertEqual(fn._stub_command, main.TEST_COMMAND_NAME)
            self.assertEqual(fn._stub_permission, main.PermissionType.ADMIN)

    def test_non_admin_denied_without_sending(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send)
            result = asyncio.run(plugin.on_test_notification(FakeEvent(admin=False)))
        self.assertIn("仅限管理员", result.text)
        self.assertEqual(calls, [], "非管理员不得触发任何发送")

    def test_missing_is_admin_method_fails_closed(self):
        """拿不到 is_admin 方法时必须拒绝（fail closed），不得放行。"""
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        event = SimpleNamespace(plain_result=lambda text: SimpleNamespace(text=text))
        self.assertFalse(hasattr(event, "is_admin"))
        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send)
            result = asyncio.run(plugin.on_test_notification(event))
        self.assertIn("仅限管理员", result.text)
        self.assertEqual(calls, [])

    def test_is_admin_raises_fails_closed(self):
        """is_admin() 抛异常时必须拒绝（fail closed），不得放行或崩溃。"""
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        class ExplodingEvent:
            def is_admin(self):
                raise RuntimeError("role lookup failed")

            def plain_result(self, text):
                return SimpleNamespace(text=text)

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send)
            result = asyncio.run(plugin.on_test_notification(ExplodingEvent()))
        self.assertIn("仅限管理员", result.text)
        self.assertEqual(calls, [])

    def test_success_text_matches_required_wording(self):
        async def send(target, chain):
            return True

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send, notify_targets=["p:GroupMessage:1"])
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertIn("发送调用完成（请核对收件）", result.text)

    def test_non_mapping_config_returns_no_targets(self):
        """config 非 Mapping 时 _resolve_notify_targets 安全返回空，不再抛异常。"""
        with _push_timeout(1) as main:
            plugin = main.BotBrotherMonitor(
                SimpleNamespace(), {"notify_targets": ["a:b:c"]}
            )
            plugin.config = object()  # 非 Mapping
            self.assertEqual(plugin._resolve_notify_targets(), [])

    def test_only_configured_targets_are_used(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        targets = ["napcat:GroupMessage:1", "wechat:FriendMessage:2"]
        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send, notify_targets=targets)
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertEqual(calls, targets, "只能发给配置的 notify_targets")
        self.assertIn("发送调用完成=2", result.text)

    def test_multi_target_status_summary(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            if target == "p:GroupMessage:t_false":
                return False
            if target == "p:GroupMessage:t_error":
                raise RuntimeError("boom")
            if target == "p:GroupMessage:t_timeout":
                await asyncio.sleep(60)
            return True

        targets = [
            "p:GroupMessage:t_ok",
            "p:GroupMessage:t_false",
            "p:GroupMessage:t_error",
            "p:GroupMessage:t_timeout",
        ]
        with _push_timeout(0.05) as main:
            plugin = _command_plugin(main, send, notify_targets=targets)
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertEqual(calls, targets, "每个目标都应被尝试且不互相阻塞")
        self.assertIn("发送调用完成=1", result.text)
        self.assertIn("超时=1", result.text)
        self.assertIn("失败=2", result.text)
        self.assertIn("未找到目标平台", result.text)
        self.assertIn("不代表真实可达或用户已读", result.text)
        self.assertIn("RuntimeError", result.text, "失败应显示异常类型")
        self.assertNotIn("boom", result.text, "不得原样回传可能敏感的异常文本")

    def test_does_not_touch_state_machine_or_alert(self):
        async def send(target, chain):
            return True

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send, notify_targets=["a:b:c"])
            plugin._machine = main.MonitorStateMachine(debounce=2)
            before = plugin._machine.to_snapshot()
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
            after = plugin._machine.to_snapshot()
        self.assertEqual(before, after, "测试通知不得改动状态机/快照")
        self.assertIn("测试通知", result.text)

    def test_works_when_monitor_disabled_and_not_started(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        with _push_timeout(1) as main:
            plugin = _command_plugin(
                main,
                send,
                enabled=False,
                target_self_id="",
                platform_id="",
                notify_targets=["wechat:FriendMessage:9"],
            )
            asyncio.run(plugin.initialize())
            self.assertIsNone(plugin._task, "禁用/缺必填时不应启动监视")
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertEqual(calls, ["wechat:FriendMessage:9"])
        self.assertIn("发送调用完成=1", result.text)

    def test_no_targets_configured(self):
        async def send(target, chain):
            return True

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send, notify_targets=[])
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertIn("未配置", result.text)

    def test_reentrancy_blocked(self):
        started, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def send(target, chain):
            calls.append(target)
            started.set()
            await release.wait()
            return True

        async def run():
            with _push_timeout(5) as main:
                plugin = _command_plugin(main, send, notify_targets=["a:b:c"])
                first = asyncio.create_task(plugin.on_test_notification(FakeEvent()))
                await started.wait()
                second = await plugin.on_test_notification(FakeEvent())
                release.set()
                return plugin, await first, second

        plugin, first, second = asyncio.run(run())
        self.assertIn("正在进行", second.text)
        self.assertEqual(calls, ["a:b:c"], "重复触发不得叠加发送")
        self.assertIn("发送调用完成=1", first.text)
        self.assertFalse(plugin._test_in_flight)

    def test_cancellation_propagates_and_resets_flag(self):
        async def send(target, chain):
            await asyncio.sleep(60)
            return True

        async def run():
            with _push_timeout(5) as main:
                plugin = _command_plugin(main, send, notify_targets=["a:b:c"])
                task = asyncio.create_task(plugin.on_test_notification(FakeEvent()))
                await asyncio.sleep(0.02)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                return plugin

        plugin = asyncio.run(run())
        self.assertFalse(plugin._test_in_flight, "取消后防重入标志应复位")


async def _noop_send(target, chain):
    return True


if __name__ == "__main__":
    unittest.main()
