"""插件配置 schema 与校验（纯 Python，零 AstrBot 依赖，可独立单测）。

``CONF_SCHEMA`` 与 ``_conf_schema.json`` 保持同步。
"""

from __future__ import annotations

CONF_SCHEMA: dict = {
    "enabled": {
        "type": "bool",
        "description": "是否启用在线监视。",
        "default": True,
    },
    "target_self_id": {
        "type": "string",
        "description": "要监视的机器人 self_id（QQ 号），必填。",
        "default": "",
    },
    "platform_id": {
        "type": "string",
        "description": (
            "AstrBot Bots 中 OneBot 平台实例的 ID（即平台实例的唯一 id，"
            "在 AstrBot 配置 Bots 时自定义，默认不一定等于 aiocqhttp），必填；"
            "用于定位平台实例并发起 get_status 探测。"
        ),
        "default": "",
    },
    "interval_seconds": {
        "type": "int",
        "description": "状态探测间隔（秒），范围 5–86400。",
        "default": 30,
    },
    "debounce": {
        "type": "int",
        "description": "连续 N 次故障（或恢复）才推送通知，范围 1–10。",
        "default": 3,
    },
    "probe_timeout_seconds": {
        "type": "int",
        "description": "单次状态探测超时（秒），范围 1–60。",
        "default": 5,
    },
    "notify_targets": {
        "type": "list",
        "description": (
            "告警推送目标列表（unified_msg_origin），格式 {platform_id}:{消息类型}:{会话ID}。"
            "可用 AstrBot 中任意平台的会话（不限于被监视的 OneBot/NapCat 实例），"
            "推荐使用与被监视账号独立的通道（如微信），确保其离线时告警仍能送达。"
            "至少填一个。"
        ),
        "default": [],
    },
    "persist_state": {
        "type": "bool",
        "description": "在 data 目录持久化故障状态，避免插件重载后重复告警。",
        "default": True,
    },
}

DEFAULTS: dict = {k: v["default"] for k, v in CONF_SCHEMA.items()}

INTERVAL_MIN, INTERVAL_MAX = 5, 86400
DEBOUNCE_MIN, DEBOUNCE_MAX = 1, 10
TIMEOUT_MIN, TIMEOUT_MAX = 1, 60

TARGET_ORIGIN_SEP = ":"
# unified_msg_origin 结构：{platform_id}:{消息类型}:{会话ID}
# 消息类型段由各平台适配器自行定义（GroupMessage/FriendMessage/OtherMessage/…），
# 平台段可为 AstrBot 中任意平台，不做 aiocqhttp 限定，因此只校验分段结构。
MIN_SEGMENTS = 3


def _clamp_int(value, lo: int, hi: int, default: int) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, v))


def _as_bool(value, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return default


def validate_config(raw: dict | None) -> tuple[dict, list[str]]:
    """规范化并校验配置。

    Returns:
        (normalized_config, errors)。
        errors 非空时，调用方不应启动监视任务。
    """
    raw = raw or {}
    cfg = dict(DEFAULTS)
    errors: list[str] = []

    cfg["enabled"] = _as_bool(raw.get("enabled"), DEFAULTS["enabled"])

    target = str(raw.get("target_self_id", "") or "").strip()
    cfg["target_self_id"] = target
    if not target:
        errors.append("target_self_id 不能为空：请填写要监视的机器人 QQ 号（self_id）。")

    platform_id = str(raw.get("platform_id", "") or "").strip()
    cfg["platform_id"] = platform_id
    if not platform_id:
        errors.append(
            "platform_id 不能为空：请填写 AstrBot Bots 中 OneBot 平台实例的 ID"
            "（不是适配器类型名 aiocqhttp）。"
        )

    cfg["interval_seconds"] = _clamp_int(
        raw.get("interval_seconds"), INTERVAL_MIN, INTERVAL_MAX, DEFAULTS["interval_seconds"]
    )
    cfg["debounce"] = _clamp_int(
        raw.get("debounce"), DEBOUNCE_MIN, DEBOUNCE_MAX, DEFAULTS["debounce"]
    )
    cfg["probe_timeout_seconds"] = _clamp_int(
        raw.get("probe_timeout_seconds"),
        TIMEOUT_MIN,
        TIMEOUT_MAX,
        DEFAULTS["probe_timeout_seconds"],
    )

    targets = raw.get("notify_targets")
    if targets is None:
        targets = []
    if isinstance(targets, str):
        targets = [targets]
    clean: list[str] = []
    for t in targets:
        s = str(t).strip()
        if not s:
            continue
        parts = s.split(TARGET_ORIGIN_SEP, MIN_SEGMENTS - 1)
        # 三段均须 strip 后非空：空段（如 wechat::id、:FriendMessage:id、
        # wechat:FriendMessage:）会在 MessageSession.from_str 或发送阶段
        # 必然失败，仅 count(':') 检查会把它们漏成“表面有效”。
        if len(parts) == MIN_SEGMENTS and all(p.strip() for p in parts):
            clean.append(s)
        else:
            errors.append(
                f"notify_targets 中 {s!r} 格式非法，"
                "应为 platform_id:消息类型:会话ID（三段均非空），"
                "例如 napcat:GroupMessage:123456789。"
            )
    cfg["notify_targets"] = clean
    if not clean:
        errors.append("notify_targets 至少需要一个推送目标（unified_msg_origin）。")

    cfg["persist_state"] = _as_bool(raw.get("persist_state"), DEFAULTS["persist_state"])
    return cfg, errors
