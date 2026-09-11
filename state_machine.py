"""BotBrother 核心监视状态机（纯 Python，零 AstrBot 依赖，可独立单测）。

状态
----
- ``ONLINE``       在线：账号在线且连接/服务可用。
- ``OFFLINE``      账号下线：连接/服务正常，但 QQ 登录态掉线（get_status 成功且 online=false）。
- ``UNREACHABLE``  连接/服务不可达：WebSocket 断开、NapCat 未运行、探测超时/失败。

输入信号（``feed``）
--------------------
- ``"online"``       探测成功且 online=true
- ``"offline"``      探测成功但 online=false（账号下线）
- ``"unreachable"``  探测失败/超时（连接/服务不可达）
- ``"activity"``     被动活性信号：收到匹配 self_id 的 AIOCQHTTP 事件（事件能到达即证明在线）

语义（与 BotBrother 核心等价）
------------------------------
* 在线态：连续 ``debounce`` 次非在线信号才进入对应故障态（按故障种类各自累计；
  期间任意一次在线信号清零计数，即 cross-clear）。进入故障态即告警一次。
* 故障态：同种类信号不再重复告警；故障种类切换时按新种类重新累计 ``debounce``
  次后才发出针对新种类的告警（从而区分"账号下线"与"连接/服务不可达"）。
* 恢复：故障态中连续 ``debounce`` 次 "online"，或收到一次 "activity"
  （事件到达即铁证在线），恢复 ONLINE 并补发一次恢复通知；同一故障段内只补发一次。

``feed`` 返回需要推送的通知列表 ``[(title, body), ...]``，空列表表示无需推送。
"""

from __future__ import annotations

from dataclasses import dataclass, field

# 状态常量
ONLINE = "online"
OFFLINE = "offline"
UNREACHABLE = "unreachable"

# 输入信号常量
ACTIVITY = "activity"

# 通知文案（单条完整文案，含 [BotBrother] 前缀；self_id 由调用方填入）
ALERT_TITLES = {
    OFFLINE: "[BotBrother] 警告：{self_id}账号离线。如本次离线为您主动触发，请忽略本信息。",
    UNREACHABLE: "[BotBrother] 警告：{self_id}所在客户端连接失败，请检查客户端是否离线。",
}
RECOVERY_TITLE = "[BotBrother] {self_id}已恢复在线。"


@dataclass
class _Counters:
    """去抖计数器。

    - 在线态累计故障：pending_kind 记录当前累计的故障种类，pending_fault 为次数。
    - 故障态累计恢复：pending_ok 为连续 online 信号的次数。
    """

    pending_fault: int = 0
    pending_kind: str = ""
    pending_ok: int = 0


class MonitorStateMachine:
    """单实例在线监视状态机。纯逻辑，无 IO，可单测。"""

    def __init__(self, debounce: int = 3) -> None:
        self.debounce = max(1, int(debounce))
        self.state: str = ONLINE
        self._ever_alerted: bool = False  # 当前故障段是否已告警
        self._ever_recovered: bool = False  # 当前故障段是否已补发恢复
        self._counters = _Counters()
        self._last_detail: str = ""

    # ------------------------------------------------------------------ #
    # 对外接口
    # ------------------------------------------------------------------ #
    def feed(
        self, signal: str, detail: str = "", self_id: str = ""
    ) -> list[tuple[str, str]]:
        """喂入一个探测/活性信号，返回需要推送的通知列表 [(msg, ""), ...]。

        通知为单条完整文案（含 [BotBrother] 前缀与 self_id），body 恒为空串，
        调用方直接把 title 当整条消息发送，勿再拼接 self_id/标题/详情。

        Raises:
            ValueError: 未知信号。
        """
        if signal == ACTIVITY:
            return self._on_activity(self_id)
        if signal not in (ONLINE, OFFLINE, UNREACHABLE):
            raise ValueError(f"未知状态信号: {signal!r}")
        if signal == ONLINE:
            return self._on_online(self_id)
        return self._on_fault_signal(signal, self_id)

    def feed_activity(self, self_id: str = "") -> list[tuple[str, str]]:
        """收到匹配 self_id 的平台事件，视为机器人在线。"""
        return self._on_activity(self_id)

    # ------------------------------------------------------------------ #
    # 内部转移
    # ------------------------------------------------------------------ #
    def _on_activity(self, self_id: str = "") -> list[tuple[str, str]]:
        """事件能到达 = 账号在线且连接可用，立即恢复（无需去抖）。"""
        if self.state != ONLINE:
            self.state = ONLINE
            self._reset_counters()
            if self._ever_alerted and not self._ever_recovered:
                self._ever_recovered = True
                return [(RECOVERY_TITLE.format(self_id=self_id), "")]
        else:
            self._reset_counters()
        return []

    def _on_online(self, self_id: str = "") -> list[tuple[str, str]]:
        if self.state == ONLINE:
            # 在线态收到在线信号：清零故障去抖计数（cross-clear）
            self._reset_counters()
            return []
        # 故障态：累计恢复去抖
        self._counters.pending_ok += 1
        self._counters.pending_fault = 0
        self._counters.pending_kind = ""
        if self._counters.pending_ok >= self.debounce:
            self.state = ONLINE
            self._reset_counters()
            if self._ever_alerted and not self._ever_recovered:
                self._ever_recovered = True
                return [(RECOVERY_TITLE.format(self_id=self_id), "")]
        return []

    def _on_fault_signal(self, kind: str, self_id: str = "") -> list[tuple[str, str]]:
        """收到 offline / unreachable 信号。"""
        if self.state == ONLINE:
            # 在线态去抖：按故障种类各自累计
            if self._counters.pending_kind != kind:
                self._counters.pending_kind = kind
                self._counters.pending_fault = 1
            else:
                self._counters.pending_fault += 1
            if self._counters.pending_fault >= self.debounce:
                self._enter_fault(kind, "")
                return [(ALERT_TITLES[kind].format(self_id=self_id), "")]
            return []

        # 故障态
        if kind == self.state:
            # 同种类故障：已告警过，不重复
            return []

        # 故障种类切换：按新种类重新去抖，避免抖动导致重复告警
        if self._counters.pending_kind != kind:
            self._counters.pending_kind = kind
            self._counters.pending_fault = 1
        else:
            self._counters.pending_fault += 1
        self._counters.pending_ok = 0  # cross-clear 恢复计数
        if self._counters.pending_fault >= self.debounce:
            self._enter_fault(kind, "")
            return [(ALERT_TITLES[kind].format(self_id=self_id), "")]
        return []

    def _enter_fault(self, kind: str, detail: str) -> None:
        self.state = kind
        self._last_detail = detail
        self._reset_counters()
        self._ever_alerted = True
        self._ever_recovered = False

    def _reset_counters(self) -> None:
        self._counters.pending_fault = 0
        self._counters.pending_kind = ""
        self._counters.pending_ok = 0

    # ------------------------------------------------------------------ #
    # 快照（持久化用，纯数据）
    # ------------------------------------------------------------------ #
    @property
    def ever_alerted(self) -> bool:
        return self._ever_alerted

    @property
    def ever_recovered(self) -> bool:
        return self._ever_recovered

    def to_snapshot(self) -> dict:
        """导出当前状态，供持久化。"""
        return {
            "state": self.state,
            "ever_alerted": self._ever_alerted,
            "ever_recovered": self._ever_recovered,
            "pending_fault": self._counters.pending_fault,
            "pending_kind": self._counters.pending_kind,
            "pending_ok": self._counters.pending_ok,
            "last_detail": self._last_detail,
        }

    @classmethod
    def from_snapshot(cls, snapshot: dict, debounce: int = 3) -> "MonitorStateMachine":
        """从快照恢复状态机（用于插件重载后避免重复告警）。"""
        machine = cls(debounce=debounce)
        machine.state = snapshot.get("state", ONLINE)
        machine._ever_alerted = bool(snapshot.get("ever_alerted", False))
        machine._ever_recovered = bool(snapshot.get("ever_recovered", False))
        machine._counters.pending_fault = int(snapshot.get("pending_fault", 0))
        machine._counters.pending_kind = snapshot.get("pending_kind", "")
        machine._counters.pending_ok = int(snapshot.get("pending_ok", 0))
        machine._last_detail = str(snapshot.get("last_detail", ""))
        return machine
