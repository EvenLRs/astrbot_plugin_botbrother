"""故障注入回归测试：异常隔离、超时、取消传播、坏快照、写盘失败。

本文件不依赖真实 AstrBot：未安装时注入最小桩模块后加载插件 main，
因此可在纯开发环境运行；若已安装 AstrBot 则直接使用真实模块
（此时 tests/test_integration_smoke.py 亦会执行）。
"""

import asyncio
import importlib
import importlib.util
import json
import logging
import os
import sys
import tempfile
import time
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


class _Recorder:
    """简易日志记录器，替换 main.logger 以断言诊断内容。"""

    def __init__(self):
        self.messages = []

    def _record(self, fmt, *args):
        try:
            self.messages.append(fmt % args if args else str(fmt))
        except Exception:
            self.messages.append(f"{fmt} {args}")

    info = _record
    warning = _record
    error = _record
    debug = _record


class DrainIsolationTest(unittest.TestCase):
    """worker 单轮：挂起/失败目标不阻塞其它目标；取消上抛。"""

    @staticmethod
    def _entry(targets, title="m"):
        return {"id": "1", "ts": 0, "title": title, "targets": list(targets)}

    def test_hang_target_does_not_block_healthy_target(self):
        calls, sent = [], []

        async def send(target, chain):
            calls.append(target)
            if target == "hang:GroupMessage:1":
                await asyncio.sleep(60)
            sent.append(target)
            return True

        with _push_timeout(0.05) as main:
            plugin = _make_plugin(
                main,
                send,
                ["hang:GroupMessage:1", "ok:GroupMessage:1"],
                debounce=1,
            )
            plugin._pending = [
                self._entry(["hang:GroupMessage:1", "ok:GroupMessage:1"])
            ]
            asyncio.run(plugin._drain_once())

        self.assertEqual(set(calls), {"hang:GroupMessage:1", "ok:GroupMessage:1"})
        self.assertEqual(sent, ["ok:GroupMessage:1"], "健康目标应完成")
        self.assertEqual(plugin._pending[0]["targets"], ["hang:GroupMessage:1"])
        self.assertIn("hang:GroupMessage:1", plugin._target_state, "失败目标应退避")

    def test_failing_target_removed_after_retry(self):
        async def send(target, chain):
            return target != "bad:GroupMessage:1"

        with _push_timeout(1) as main:
            plugin = _make_plugin(
                main,
                send,
                ["bad:GroupMessage:1", "good:GroupMessage:1"],
                debounce=1,
            )
            plugin._pending = [
                self._entry(["bad:GroupMessage:1", "good:GroupMessage:1"])
            ]
            asyncio.run(plugin._drain_once())
        self.assertEqual(plugin._pending[0]["targets"], ["bad:GroupMessage:1"])

    def test_cancelled_error_propagates_from_drain(self):
        async def send(target, chain):
            raise asyncio.CancelledError()

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["a:b:c"], debounce=1)
            plugin._pending = [self._entry(["a:b:c"])]
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(plugin._drain_once())

    def test_feed_and_notify_only_enqueues_without_sending(self):
        """事件/探测路径只入队，不得直接发送，也不得被发送阻塞。"""
        calls = []

        async def send(target, chain):
            calls.append(target)
            await asyncio.sleep(60)
            return True

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            started = time.monotonic()
            asyncio.run(plugin._feed_and_notify(OFFLINE, "down"))
            elapsed = time.monotonic() - started
        self.assertEqual(plugin._pending[0]["targets"], ["p:GroupMessage:1"])
        self.assertEqual(calls, [], "事件路径不得直接发送")
        self.assertLess(elapsed, 0.5, "事件路径不得被发送阻塞")


class WorkerLifecycleTest(unittest.TestCase):
    """worker 与探测解耦；生命周期可取消、无泄漏；重启可恢复队列。"""

    def test_probe_not_blocked_by_hung_worker(self):
        probes = []

        async def send(target, chain):
            await asyncio.sleep(60)
            return True

        with _push_timeout(0.05) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)

            async def fake_probe():
                probes.append(1)
                return OFFLINE, "down"

            plugin._probe = fake_probe
            plugin._config["interval_seconds"] = 0.01

            async def run():
                plugin._worker_wakeup = asyncio.Event()
                worker = asyncio.create_task(plugin._worker_loop())
                monitor = asyncio.create_task(plugin._monitor_loop())
                await asyncio.sleep(0.3)
                alive = not monitor.done() and not worker.done()
                for task in (monitor, worker):
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                return alive

            alive = asyncio.run(run())

        self.assertTrue(alive, "探测/worker 不应因单次发送挂起而退出")
        self.assertGreaterEqual(len(probes), 2, "挂起后应继续探测后续轮次")

    def test_initialize_starts_worker_and_terminate_cancels_it(self):
        async def send(target, chain):
            return True

        async def run():
            with _push_timeout(1) as main:
                full_cfg = {
                    "enabled": True,
                    "target_self_id": "1",
                    "platform_id": "napcat",
                    "notify_targets": ["p:GroupMessage:1"],
                    "interval_seconds": 30,
                    "debounce": 1,
                    "probe_timeout_seconds": 5,
                    "persist_state": False,
                    "send_message": send,
                }
                plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
                plugin.config = full_cfg  # initialize 读取原始 config
                await plugin.initialize()
                worker = plugin._worker_task
                self.assertIsNotNone(worker, "initialize 应启动 worker")
                await plugin.terminate()
                self.assertIsNone(plugin._worker_task)
                self.assertIsNone(plugin._task)
                return worker

        worker = asyncio.run(run())
        self.assertTrue(worker.done(), "terminate 后 worker 应已结束")

    def test_restart_recovers_and_drains_pending(self):
        sent = []

        async def send(target, chain):
            sent.append(target)
            return True

        async def run():
            with _push_timeout(1) as main:
                plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
                # 模拟重启后从磁盘恢复的队列
                plugin._pending = [
                    {"id": "1", "ts": 0, "title": "m", "targets": ["p:GroupMessage:1"]}
                ]
                plugin._worker_wakeup = asyncio.Event()
                worker = asyncio.create_task(plugin._worker_loop())
                plugin._wake_worker()
                await asyncio.sleep(0.1)
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass
                return plugin

        plugin = asyncio.run(run())
        self.assertEqual(sent, ["p:GroupMessage:1"])
        self.assertEqual(plugin._pending, [])

    def test_terminate_leaves_no_running_tasks(self):
        async def send(target, chain):
            await asyncio.sleep(60)
            return True

        async def run():
            with _push_timeout(5) as main:
                full_cfg = {
                    "enabled": True,
                    "target_self_id": "1",
                    "platform_id": "napcat",
                    "notify_targets": ["p:GroupMessage:1"],
                    "interval_seconds": 30,
                    "debounce": 1,
                    "probe_timeout_seconds": 5,
                    "persist_state": False,
                }
                plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
                plugin.config = full_cfg
                await plugin.initialize()
                plugin._pending = [
                    {"id": "1", "ts": 0, "title": "m", "targets": ["p:GroupMessage:1"]}
                ]
                plugin._wake_worker()
                await asyncio.sleep(0.05)  # worker 挂在发送上
                await plugin.terminate()
                leftover = [
                    t for t in asyncio.all_tasks() if t is not asyncio.current_task()
                ]
                return plugin, leftover

        plugin, leftover = asyncio.run(run())
        self.assertIsNone(plugin._worker_task)
        self.assertIsNone(plugin._task)
        self.assertTrue(all(t.done() for t in leftover), "terminate 后不应有遗留任务")

    def test_bad_target_does_not_starve_healthy_target(self):
        """坏目标占满本轮后，健康目标在后续轮次仍被送达（不被长期饿死）。"""
        calls = []
        ctl = {"bad_ok": False}

        async def send(target, chain):
            calls.append(target)
            if target.startswith("bad"):
                await asyncio.sleep(60)
                return ctl["bad_ok"]
            return True

        with _push_timeout(0.02) as main:
            plugin = _make_plugin(main, send, ["a:b:c"], debounce=1)
            bad = [f"bad{i}:GroupMessage:1" for i in range(5)]
            plugin._config["notify_targets"] = bad + ["good:GroupMessage:1"]
            plugin._pending = [
                {"id": "b", "ts": 0, "title": "m1", "targets": bad},
                {"id": "g", "ts": 1, "title": "m2", "targets": ["good:GroupMessage:1"]},
            ]
            asyncio.run(plugin._drain_once())  # 第一轮：被 bad 占满
            asyncio.run(plugin._drain_once())  # 第二轮：bad 退避，good 应被尝试
        self.assertIn("good:GroupMessage:1", calls, "健康目标不得被坏目标饿死")


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

    def test_test_command_diagnostics_redacted_and_not_queued(self):
        secret = "SECRET-TOKEN-xyz"

        async def send(target, chain):
            raise ValueError(secret)

        with _push_timeout(1) as main:
            recorder = _Recorder()
            main.logger = recorder
            plugin = _command_plugin(main, send, notify_targets=["p:GroupMessage:1"])
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        combined = result.text + "\n" + "\n".join(recorder.messages)
        self.assertIn("id=", result.text, "摘要应包含可查询的诊断 id")
        self.assertNotIn(secret, combined, "测试命令发送日志/摘要不得泄露异常原文")
        self.assertEqual(plugin._pending, [], "测试发送不得入队")
        self.assertEqual(plugin._target_state, {}, "测试发送不得改退避计数")

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


class PendingQueueTest(unittest.TestCase):
    """告警队列：有界尝试、退避、每目标 FIFO、删除目标、持久化、边界。"""

    def test_failed_alert_queued_then_retried_after_backoff(self):
        ctl = {"ok": False}

        async def send(target, chain):
            return ctl["ok"]

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            asyncio.run(plugin._feed_and_notify(OFFLINE, "down"))
            self.assertEqual(len(plugin._pending), 1)
            self.assertEqual(plugin._pending[0]["targets"], ["p:GroupMessage:1"])
            asyncio.run(plugin._drain_once())  # 失败 -> 进入退避
            self.assertEqual(len(plugin._pending), 1)
            ctl["ok"] = True
            asyncio.run(plugin._drain_once())  # 退避期内不再尝试
            self.assertEqual(len(plugin._pending), 1, "退避期内不应重试")
            plugin._target_state.clear()
            asyncio.run(plugin._drain_once())
        self.assertEqual(plugin._pending, [], "退避清除后重试成功应移出队列")

    def test_partial_target_failure_only_failed_stays(self):
        async def send(target, chain):
            return target != "bad:GroupMessage:1"

        with _push_timeout(1) as main:
            plugin = _make_plugin(
                main,
                send,
                ["good:GroupMessage:1", "bad:GroupMessage:1"],
                debounce=1,
            )
            asyncio.run(plugin._feed_and_notify(OFFLINE, "down"))
            asyncio.run(plugin._drain_once())
        self.assertEqual(len(plugin._pending), 1)
        self.assertEqual(plugin._pending[0]["targets"], ["bad:GroupMessage:1"])

    def test_attempts_are_bounded_per_cycle(self):
        calls = []

        async def fake_send(target, chain):
            calls.append(target)
            return ("error", "x")

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
            plugin._send_to_target = fake_send
            plugin._pending = [
                {
                    "id": "1",
                    "ts": 0,
                    "title": "m",
                    "targets": [f"t{i}:GroupMessage:1" for i in range(10)],
                }
            ]
            plugin._config["notify_targets"] = [
                f"t{i}:GroupMessage:1" for i in range(10)
            ]
            asyncio.run(plugin._drain_once())
            self.assertEqual(len(calls), main.PENDING_MAX_ATTEMPTS_PER_CYCLE)

    def test_backoff_blocks_immediate_retry(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            return False

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            plugin._pending = [
                {"id": "1", "ts": 0, "title": "m", "targets": ["p:GroupMessage:1"]}
            ]
            asyncio.run(plugin._drain_once())
            self.assertEqual(len(calls), 1)
            asyncio.run(plugin._drain_once())  # 立刻再来一轮
            self.assertEqual(len(calls), 1, "退避未到点不应再次尝试")

    def test_per_target_fifo_order(self):
        calls = []
        ctl = {"ok": False}

        async def send(target, chain):
            calls.append(
                chain.get_plain_text() if hasattr(chain, "get_plain_text") else ""
            )
            return ctl["ok"]

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            plugin._pending = [
                {
                    "id": "1",
                    "ts": 1,
                    "title": "offline",
                    "targets": ["p:GroupMessage:1"],
                },
                {
                    "id": "2",
                    "ts": 2,
                    "title": "recovery",
                    "targets": ["p:GroupMessage:1"],
                },
            ]
            asyncio.run(plugin._drain_once())
            self.assertEqual(calls, ["offline"], "旧条目失败时不得越级发新条目")
            ctl["ok"] = True
            plugin._target_state.clear()
            asyncio.run(plugin._drain_once())  # 先发最早条目 offline
            asyncio.run(plugin._drain_once())  # 再发更新的 recovery
            self.assertEqual(calls, ["offline", "offline", "recovery"])

    def test_deleted_target_cleared_from_queue(self):
        calls = []

        async def send(target, chain):
            calls.append(target)
            return True

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["keep:GroupMessage:1"], debounce=1)
            plugin._pending = [
                {
                    "id": "1",
                    "ts": 0,
                    "title": "m",
                    "targets": ["gone:GroupMessage:1", "keep:GroupMessage:1"],
                }
            ]
            plugin._target_state = {
                "gone:GroupMessage:1": {"failures": 1, "next_at": 0}
            }
            asyncio.run(plugin._drain_once())
        self.assertNotIn("gone:GroupMessage:1", calls, "已删除目标不应再发送")
        self.assertIn("keep:GroupMessage:1", calls)
        self.assertEqual(plugin._pending, [])
        self.assertNotIn("gone:GroupMessage:1", plugin._target_state)

    def test_cancellation_propagates_and_keeps_queue(self):
        async def send(target, chain):
            await asyncio.sleep(60)
            return True

        async def run():
            with _push_timeout(1) as main:
                plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
                plugin._pending = [
                    {"id": "1", "ts": 0, "title": "m", "targets": ["p:GroupMessage:1"]}
                ]
                task = asyncio.create_task(plugin._drain_once())
                await asyncio.sleep(0.02)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                return plugin

        plugin = asyncio.run(run())
        self.assertEqual(len(plugin._pending), 1, "取消不得破坏队列内容")

    def test_pending_cap_drops_oldest(self):
        async def send(target, chain):
            return False

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            for i in range(main.PENDING_MAX_ENTRIES + 5):
                plugin._enqueue_pending(f"msg{i}", ["p:GroupMessage:1"])
            self.assertEqual(len(plugin._pending), main.PENDING_MAX_ENTRIES)
            titles = [entry["title"] for entry in plugin._pending]
            self.assertNotIn("msg0", titles)
            self.assertIn(f"msg{main.PENDING_MAX_ENTRIES + 4}", titles)

    def test_pending_persist_and_load_v2(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pending_notifications.json"
            with _push_timeout(1) as main:
                saver = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
                saver._pending_file = path
                saver._enqueue_pending("alert-1", ["x:GroupMessage:1"])
                saver._target_state = {
                    "x:GroupMessage:1": {"failures": 2, "next_at": 123.0}
                }
                saver._save_pending()

                raw = json.loads(path.read_text("utf-8"))
                self.assertEqual(raw["version"], main.PENDING_VERSION)

                loader = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
                loader._pending_file = path
                loader._load_pending()
                self.assertEqual(len(loader._pending), 1)
                self.assertEqual(loader._pending[0]["title"], "alert-1")
                self.assertEqual(
                    loader._target_state,
                    {"x:GroupMessage:1": {"failures": 2, "next_at": 123.0}},
                )

    def test_corrupt_or_invalid_pending_file_falls_back(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pending_notifications.json"
            with _push_timeout(1) as main:
                plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
                plugin._pending_file = path

                path.write_text("{not-json", "utf-8")
                plugin._pending = [{"x": 1}]
                plugin._target_state = {"a": {"failures": 1, "next_at": 1}}
                plugin._load_pending()
                self.assertEqual(plugin._pending, [])
                self.assertEqual(plugin._target_state, {})

                path.write_text(
                    json.dumps(
                        {
                            "version": 2,
                            "entries": [
                                {"title": 123},
                                {"targets": ["a:b:c"]},
                                {"title": "ok", "targets": ["a:b:c"]},
                                "junk",
                            ],
                            "target_state": {
                                "ok": {"failures": 1, "next_at": 2},
                                "badf": {"failures": "x", "next_at": 2},
                                "badn": {"failures": 1, "next_at": "y"},
                            },
                        }
                    ),
                    "utf-8",
                )
                plugin._load_pending()
                self.assertEqual(len(plugin._pending), 1)
                self.assertEqual(plugin._pending[0]["title"], "ok")
                self.assertEqual(
                    plugin._target_state, {"ok": {"failures": 1, "next_at": 2.0}}
                )

    def test_persist_state_false_still_externalizes_pending(self):
        with _push_timeout(1) as main:
            plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
            plugin._setup_data_dir(persist_state=False)
            if plugin._pending_file is None:
                self.skipTest("当前环境无法创建 data 目录")
            self.assertIsNone(plugin._state_file, "persist_state=false 不应写快照")
            plugin._enqueue_pending("t", ["a:b:c"])
            self.assertTrue(plugin._pending_file.exists(), "pending 仍应外置")

    def test_write_failure_is_isolated(self):
        with tempfile.TemporaryDirectory() as tmp:
            ro = Path(tmp) / "ro"
            ro.mkdir()
            os.chmod(ro, 0o555)
            try:
                with _push_timeout(1) as main:
                    plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
                    plugin._pending_file = ro / "pending_notifications.json"
                    plugin._enqueue_pending("m", ["a:b:c"])  # 写失败必须被吞
            finally:
                os.chmod(ro, 0o755)

    def test_successful_target_not_resent(self):
        counts = {}

        async def send(target, chain):
            counts[target] = counts.get(target, 0) + 1
            return target != "bad:GroupMessage:1"

        with _push_timeout(1) as main:
            plugin = _make_plugin(
                main, send, ["good:GroupMessage:1", "bad:GroupMessage:1"], debounce=1
            )
            plugin._pending = [
                {
                    "id": "1",
                    "ts": 0,
                    "title": "m",
                    "targets": ["good:GroupMessage:1", "bad:GroupMessage:1"],
                }
            ]
            asyncio.run(plugin._drain_once())
            plugin._target_state.clear()
            asyncio.run(plugin._drain_once())
        self.assertEqual(counts["good:GroupMessage:1"], 1, "成功目标不得重发")

    def test_backoff_truncates_huge_failure_count(self):
        with _push_timeout(1) as main:
            plugin = _make_plugin(main, _noop_send, ["p:GroupMessage:1"], debounce=1)
            plugin._config["interval_seconds"] = 30
            for huge in (10**6, 10**9, 10**18):
                delay = plugin._backoff_delay(huge)
                self.assertGreater(delay, 0)
                self.assertLessEqual(delay, main.PENDING_BACKOFF_CAP_SECONDS)

    def test_diagnostics_redact_secret_and_tag_fields(self):
        secret = "SECRET-TOKEN-abc"

        async def send(target, chain):
            raise ValueError(secret)

        with _push_timeout(1) as main:
            recorder = _Recorder()
            main.logger = recorder
            plugin = _make_plugin(main, send, ["p:GroupMessage:1"], debounce=1)
            plugin._pending = [
                {"id": "1", "ts": 0, "title": "m", "targets": ["p:GroupMessage:1"]}
            ]
            asyncio.run(plugin._drain_once())
        joined = "\n".join(recorder.messages)
        self.assertNotIn(secret, joined, "日志不得包含异常原文/凭据")
        self.assertIn("id=", joined)
        self.assertIn("p:GroupMessage:1", joined)
        self.assertIn("elapsed=", joined)

    def test_worker_bounded_concurrency(self):
        active = 0
        peak = 0

        async def send(target, chain):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1
            return True

        with _push_timeout(1) as main:
            main.PENDING_MAX_ATTEMPTS_PER_CYCLE = 6
            main.PENDING_MAX_CONCURRENT_SENDS = 2
            targets = [f"t{i}:GroupMessage:1" for i in range(6)]
            plugin = _make_plugin(main, send, targets, debounce=1)
            plugin._pending = [{"id": "1", "ts": 0, "title": "m", "targets": targets}]
            asyncio.run(plugin._drain_once())
        self.assertLessEqual(peak, 2, "并发不得超过 PENDING_MAX_CONCURRENT_SENDS")

    def test_target_state_rejects_nonfinite_negative_and_huge(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "pending_notifications.json"
            with _push_timeout(1) as main:
                plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
                plugin._pending_file = path
                path.write_text(
                    json.dumps(
                        {
                            "version": 2,
                            "entries": [],
                            "target_state": {
                                "inf": {"failures": 1, "next_at": float("inf")},
                                "nan": {"failures": 1, "next_at": float("nan")},
                                "neg": {"failures": 1, "next_at": -5},
                                "huge": {"failures": 10**9, "next_at": 2},
                                "hugeint": {"failures": 1, "next_at": 10**400},
                                "ok": {"failures": 2, "next_at": 2},
                            },
                        }
                    ),
                    "utf-8",
                )
                plugin._load_pending()
        self.assertEqual(plugin._target_state, {"ok": {"failures": 2, "next_at": 2.0}})

    def test_unexpected_attempt_exception_does_not_block_or_resend_healthy(self):
        calls = {}

        async def fake_attempt(entry, target):
            calls[target] = calls.get(target, 0) + 1
            if target.startswith("bad"):
                raise RuntimeError("unexpected")
            return ("ok", "")

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, _noop_send, ["a:b:c"], debounce=1)
            plugin._attempt_send = fake_attempt
            plugin._config["notify_targets"] = [
                "bad:GroupMessage:1",
                "good:GroupMessage:1",
            ]
            plugin._pending = [
                {
                    "id": "1",
                    "ts": 0,
                    "title": "m",
                    "targets": ["bad:GroupMessage:1", "good:GroupMessage:1"],
                }
            ]
            asyncio.run(plugin._drain_once())
            self.assertEqual(
                plugin._pending[0]["targets"],
                ["bad:GroupMessage:1"],
                "单目标意外异常不得影响健康目标成功",
            )
            asyncio.run(plugin._drain_once())  # bad 退避，good 已移除
        self.assertEqual(calls["good:GroupMessage:1"], 1, "成功目标不得重发")

    def test_test_notification_is_not_queued(self):
        async def send(target, chain):
            return False

        with _push_timeout(1) as main:
            plugin = _command_plugin(main, send, notify_targets=["p:GroupMessage:1"])
            result = asyncio.run(plugin.on_test_notification(FakeEvent()))
        self.assertEqual(plugin._pending, [], "测试通知不应进入告警重试队列")
        self.assertIn("失败=1", result.text)

    def test_alert_without_targets_no_crash_no_queue(self):
        async def send(target, chain):
            return True

        with _push_timeout(1) as main:
            plugin = _make_plugin(main, send, [], debounce=1)
            asyncio.run(plugin._feed_and_notify(OFFLINE, "down"))
        self.assertEqual(plugin._pending, [])


async def _noop_send(target, chain):
    return True


if __name__ == "__main__":
    unittest.main()
