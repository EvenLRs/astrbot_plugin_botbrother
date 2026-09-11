"""BotBrother 核心监视状态机。

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

语义
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

# 合法状态集合（快照校验用）
VALID_STATES = (ONLINE, OFFLINE, UNREACHABLE)
# 故障态 / 允许的 pending_kind（不得为 ONLINE：在线态不会作为待定故障种类）
_FAULT_STATES = (OFFLINE, UNREACHABLE)
_PENDING_KINDS = ("", OFFLINE, UNREACHABLE)

# 快照字段清单（to_snapshot 的输出即验收基准；多余未知键忽略，缺失即视为坏快照）
_SNAPSHOT_FIELDS = (
    "state",
    "ever_alerted",
    "ever_recovered",
    "pending_fault",
    "pending_kind",
    "pending_ok",
    "last_detail",
)


def _is_nonneg_int(value) -> bool:
    """是否合法的非负计数器：仅接受真正的 int（排除 bool），不做事后强转。

    - bool 是 int 子类，必须显式排除（True 会被误当 1）。
    - 不接受 str/float 等强转（"3"、3.0、float('inf') 一律视为非法，
      同时避免 int(float('inf')) 抛 OverflowError）。
    """
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _snapshot_is_valid(snapshot) -> bool:
    """快照是否可安全恢复：字段齐全 + 类型严格 + 无自相矛盾。

    计数不按 debounce 卡上限：用户调小 debounce 后，旧快照里更大的合法
    计数应继续生效（下一轮即触发），不得因“超出当前 debounce”被拒绝。
    """
    if not isinstance(snapshot, dict):
        return False
    if any(key not in snapshot for key in _SNAPSHOT_FIELDS):
        return False
    if snapshot["state"] not in VALID_STATES:
        return False
    if not isinstance(snapshot["ever_alerted"], bool):
        return False
    if not isinstance(snapshot["ever_recovered"], bool):
        return False
    if not _is_nonneg_int(snapshot["pending_fault"]):
        return False
    if not _is_nonneg_int(snapshot["pending_ok"]):
        return False
    if snapshot["pending_kind"] not in _PENDING_KINDS:
        return False
    if not isinstance(snapshot["last_detail"], str):
        return False
    # 矛盾一：故障态却从未告警（正常进入故障态必置 ever_alerted=True）
    if snapshot["state"] in _FAULT_STATES and not snapshot["ever_alerted"]:
        return False
    # 矛盾二：从未告警却标记已恢复
    if snapshot["ever_recovered"] and not snapshot["ever_alerted"]:
        return False
    return True


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
        """从快照恢复状态机（用于插件重载后避免重复告警）。

        快照来自磁盘，可能被截断/手改/版本不兼容。验收标准：
        - ``to_snapshot()`` 的真实往返必须原样恢复（含故障态与去抖计数）；
        - 任何字段缺失、类型非法（如 ever_alerted 为 str/1）、计数为
          bool/负数/float、pending_kind 为 ONLINE，或状态与标志自相矛盾时，
          **整体回退全新状态机**——绝不部分继承损坏的计数或历史标志，
          也不因坏快照让插件初始化失败。
        - 计数不按 debounce 卡上限，debounce 调小后旧快照的合法大计数继续生效。
        """
        if _snapshot_is_valid(snapshot):
            machine = cls(debounce=debounce)
            machine.state = snapshot["state"]
            machine._ever_alerted = snapshot["ever_alerted"]
            machine._ever_recovered = snapshot["ever_recovered"]
            machine._counters.pending_fault = snapshot["pending_fault"]
            machine._counters.pending_kind = snapshot["pending_kind"]
            machine._counters.pending_ok = snapshot["pending_ok"]
            machine._last_detail = snapshot["last_detail"]
            return machine
        # 非法或矛盾快照：全新状态机（state=ONLINE，计数/标志清零）
        return cls(debounce=debounce)
