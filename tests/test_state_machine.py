"""astrbot_plugin_botbrother 单元测试：状态机（纯逻辑，无需 AstrBot）。"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from state_machine import (  # noqa: E402
    ACTIVITY,
    OFFLINE,
    ONLINE,
    RECOVERY_TITLE,
    UNREACHABLE,
    ALERT_TITLES,
    MonitorStateMachine,
)


class OnlineToOfflineTest(unittest.TestCase):
    """在线 → 账号下线：连续 N 次才告警一次，恢复仅补发一次。"""

    def test_alert_only_after_debounce(self):
        m = MonitorStateMachine(debounce=3)
        # 前两次故障不告警（去抖中）
        self.assertEqual(m.feed(OFFLINE, "d1"), [])
        self.assertEqual(m.feed(OFFLINE, "d2"), [])
        self.assertEqual(m.state, ONLINE)
        # 第三次才进入故障态并告警一次
        notices = m.feed(OFFLINE, "d3", self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(
            notices[0][0],
            "[BotBrother] 警告：776916629账号离线。如本次离线为您主动触发，请忽略本信息。",
        )
        self.assertEqual(notices[0][1], "", "body 恒为空，通知是单条完整文案")
        self.assertEqual(m.state, OFFLINE)

    def test_cross_clear_online(self):
        """去抖期间出现一次 online 即清零计数。"""
        m = MonitorStateMachine(debounce=3)
        m.feed(OFFLINE)
        m.feed(OFFLINE)
        m.feed(ONLINE)  # cross-clear
        self.assertEqual(m.feed(OFFLINE), [])
        self.assertEqual(m.state, ONLINE)

    def test_recovery_after_debounce_and_only_once(self):
        m = MonitorStateMachine(debounce=3)
        m.feed(OFFLINE)
        m.feed(OFFLINE)
        m.feed(OFFLINE)  # 进入故障态并告警
        self.assertEqual(m.state, OFFLINE)
        # 恢复去抖
        self.assertEqual(m.feed(ONLINE, "ok1"), [])
        self.assertEqual(m.feed(ONLINE, "ok2"), [])
        notices = m.feed(ONLINE, "ok3", self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0][0], "[BotBrother] 776916629已恢复在线。")
        self.assertEqual(m.state, ONLINE)
        # 恢复通知只补发一次
        self.assertEqual(m.feed(ONLINE), [])
        self.assertEqual(m.feed(ONLINE), [])


class UnreachableTest(unittest.TestCase):
    """连接/服务不可达：与账号下线区分开。"""

    def test_unreachable_alert_distinct_title(self):
        m = MonitorStateMachine(debounce=2)
        self.assertEqual(m.feed(UNREACHABLE, "conn lost"), [])
        notices = m.feed(UNREACHABLE, "conn lost again", self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(
            notices[0][0],
            "[BotBrother] 警告：776916629所在客户端连接失败，请检查客户端是否离线。",
        )
        self.assertEqual(m.state, UNREACHABLE)

    def test_no_duplicate_alert_in_same_fault(self):
        m = MonitorStateMachine(debounce=2)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)
        # 同种类故障不重复告警
        self.assertEqual(m.feed(UNREACHABLE), [])
        self.assertEqual(m.feed(UNREACHABLE), [])

    def test_offline_then_unreachable_escalates(self):
        """故障种类切换：按新种类重新去抖后发出对应告警。"""
        m = MonitorStateMachine(debounce=2)
        m.feed(OFFLINE)
        m.feed(OFFLINE)  # 账号下线告警
        self.assertEqual(m.state, OFFLINE)
        # 切换为不可达：第一次不告警（重新去抖），第二次告警
        self.assertEqual(m.feed(UNREACHABLE, "conn lost"), [])
        notices = m.feed(UNREACHABLE, "conn lost again", self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(
            notices[0][0],
            "[BotBrother] 警告：776916629所在客户端连接失败，请检查客户端是否离线。",
        )
        self.assertEqual(m.state, UNREACHABLE)

    def test_unreachable_then_online_recovery(self):
        """连接/服务不可达告警后，连续 online 信号恢复：补发恢复通知。

        恢复语义必须同时覆盖「账号离线」与「连接/服务不可达」两类故障。
        """
        m = MonitorStateMachine(debounce=2)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)  # 进入不可达并告警
        self.assertEqual(m.state, UNREACHABLE)
        self.assertEqual(m.feed(ONLINE, "ok1"), [])  # 恢复去抖中
        notices = m.feed(ONLINE, "ok2", self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0][0], "[BotBrother] 776916629已恢复在线。")
        self.assertEqual(notices[0][1], "", "body 恒为空，通知是单条完整文案")
        self.assertEqual(m.state, ONLINE)


class ActivityTest(unittest.TestCase):
    """被动活性信号：事件到达即在线，立即恢复。"""

    def test_activity_immediate_recovery(self):
        m = MonitorStateMachine(debounce=3)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)  # 进入不可达并告警
        self.assertEqual(m.state, UNREACHABLE)
        notices = m.feed_activity(self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0][0], "[BotBrother] 776916629已恢复在线。")
        self.assertEqual(m.state, ONLINE)
        # 恢复通知只发一次
        self.assertEqual(m.feed_activity(), [])

    def test_activity_online_no_notice(self):
        m = MonitorStateMachine(debounce=3)
        self.assertEqual(m.feed_activity(), [])

    def test_activity_clears_pending_fault(self):
        m = MonitorStateMachine(debounce=3)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)
        m.feed_activity()  # 事件到达，清零去抖
        self.assertEqual(m.feed(UNREACHABLE), [])
        self.assertEqual(m.state, ONLINE)


class SnapshotTest(unittest.TestCase):
    """快照：真实 to_snapshot 往返兼容 + 非法快照整体回退。"""

    def test_snapshot_roundtrip_resumes_without_duplicate_alert(self):
        m = MonitorStateMachine(debounce=3)
        m.feed(OFFLINE)
        m.feed(OFFLINE)
        m.feed(OFFLINE)  # 已告警
        m2 = MonitorStateMachine.from_snapshot(m.to_snapshot(), debounce=3)
        self.assertEqual(m2.state, OFFLINE)
        self.assertTrue(m2.ever_alerted)
        # 恢复后仍只补发一次恢复
        self.assertEqual(m2.feed(ONLINE), [])
        self.assertEqual(m2.feed(ONLINE), [])
        notices = m2.feed(ONLINE, self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0][0], "[BotBrother] 776916629已恢复在线。")

    def test_roundtrip_preserves_all_fields_exactly(self):
        """在线待定故障 / 故障态 / 恢复去抖中三类真实快照，往返字段一致。"""
        online_pending = MonitorStateMachine(debounce=5)
        online_pending.feed(OFFLINE)
        online_pending.feed(OFFLINE)  # 在线但累计待定故障
        fault = MonitorStateMachine(debounce=2)
        fault.feed(UNREACHABLE)
        fault.feed(UNREACHABLE)  # 故障态
        recovering = MonitorStateMachine(debounce=2)
        recovering.feed(OFFLINE)
        recovering.feed(OFFLINE)  # 故障
        recovering.feed(ONLINE)  # 恢复去抖中
        for m in (online_pending, fault, recovering):
            with self.subTest(state=m.state):
                snap = m.to_snapshot()
                m2 = MonitorStateMachine.from_snapshot(snap, debounce=m.debounce)
                self.assertEqual(m2.to_snapshot(), snap)

    def test_pending_fault_counters_roundtrip(self):
        m = MonitorStateMachine(debounce=5)
        m.feed(OFFLINE)
        m.feed(OFFLINE)
        snap = m.to_snapshot()
        self.assertEqual(snap["pending_fault"], 2)
        m2 = MonitorStateMachine.from_snapshot(snap, debounce=5)
        self.assertEqual(m2._counters.pending_fault, 2)
        self.assertEqual(m2._counters.pending_kind, OFFLINE)
        self.assertEqual(m2.to_snapshot(), snap)

    def test_debounce_shrink_keeps_legal_old_counters(self):
        """用户调小 debounce 后，旧快照更大的合法计数须继续生效，不被拒绝。"""
        m = MonitorStateMachine(debounce=5)
        for _ in range(4):  # 4/5，未告警
            m.feed(OFFLINE)
        snap = m.to_snapshot()
        m2 = MonitorStateMachine.from_snapshot(snap, debounce=2)
        self.assertEqual(m2.to_snapshot(), snap, "合法旧计数不得被 debounce 卡掉")
        notices = m2.feed(OFFLINE, self_id="1")  # 5 >= 2 → 立即告警
        self.assertEqual(len(notices), 1)
        self.assertEqual(m2.state, OFFLINE)

    def test_illegal_snapshots_fall_back_to_fresh(self):
        valid = MonitorStateMachine(debounce=3).to_snapshot()
        bad_cases = {
            "non-dict": ["not", "a", "dict"],
            "empty-dict": {},
            "missing-field": {k: v for k, v in valid.items() if k != "pending_ok"},
            "state-unknown": {**valid, "state": "garbage"},
            "ever_alerted-str": {**valid, "ever_alerted": "false"},
            "ever_alerted-int": {**valid, "ever_alerted": 1},
            "ever_recovered-str": {**valid, "ever_recovered": "true"},
            "pending_fault-bool": {**valid, "pending_fault": True},
            "pending_fault-neg": {**valid, "pending_fault": -1},
            "pending_fault-float": {**valid, "pending_fault": 2.0},
            "pending_fault-inf": {**valid, "pending_fault": float("inf")},
            "pending_fault-str": {**valid, "pending_fault": "3"},
            "pending_ok-bool": {**valid, "pending_ok": False},
            "pending_kind-online": {**valid, "pending_kind": ONLINE},
            "pending_kind-int": {**valid, "pending_kind": 7},
            "last_detail-int": {**valid, "last_detail": 123},
            "fault-without-alert": {**valid, "state": OFFLINE, "ever_alerted": False},
            "recovered-without-alert": {
                **valid,
                "ever_alerted": False,
                "ever_recovered": True,
            },
        }
        for name, snap in bad_cases.items():
            with self.subTest(case=name):
                m = MonitorStateMachine.from_snapshot(snap, debounce=3)
                self.assertEqual(m.state, ONLINE)
                self.assertFalse(m.ever_alerted)
                self.assertFalse(m.ever_recovered)
                self.assertEqual(m._counters.pending_fault, 0)
                self.assertEqual(m._counters.pending_kind, "")
                self.assertEqual(m._counters.pending_ok, 0)

    def test_valid_fault_snapshot_still_recovers_once(self):
        """合法故障快照仍按语义补发一次恢复通知。"""
        m = MonitorStateMachine(debounce=2)
        m.feed(UNREACHABLE)
        m.feed(UNREACHABLE)  # 进入故障并告警
        m2 = MonitorStateMachine.from_snapshot(m.to_snapshot(), debounce=2)
        notices = m2.feed(ONLINE, self_id="776916629")
        notices += m2.feed(ONLINE, self_id="776916629")
        self.assertEqual(len(notices), 1)
        self.assertEqual(notices[0][0], "[BotBrother] 776916629已恢复在线。")


class DebounceEdgeTest(unittest.TestCase):
    def test_debounce_at_least_one(self):
        m = MonitorStateMachine(debounce=0)
        self.assertEqual(m.debounce, 1)
        notices = m.feed(OFFLINE, "d")
        self.assertEqual(len(notices), 1)
        self.assertEqual(m.state, OFFLINE)

    def test_unknown_signal_raises(self):
        m = MonitorStateMachine(debounce=3)
        with self.assertRaises(ValueError):
            m.feed("bogus")


if __name__ == "__main__":
    unittest.main()
