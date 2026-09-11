"""astrbot_plugin_botbrother 单元测试：配置 schema 与校验（纯逻辑，无需 AstrBot）。"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_schema import (  # noqa: E402
    CONF_SCHEMA,
    DEFAULTS,
    validate_config,
)


class SchemaConsistencyTest(unittest.TestCase):
    """_conf_schema.json 与 config_schema.CONF_SCHEMA 保持同步。"""

    def test_json_matches_python_schema(self):
        here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(here, "_conf_schema.json"), encoding="utf-8") as f:
            on_disk = json.load(f)
        self.assertEqual(set(on_disk.keys()), set(CONF_SCHEMA.keys()))
        for key, meta in CONF_SCHEMA.items():
            self.assertEqual(on_disk[key]["type"], meta["type"], key)
            self.assertEqual(on_disk[key]["default"], meta["default"], key)


class ValidConfigTest(unittest.TestCase):
    def test_defaults_applied(self):
        cfg, errors = validate_config(
            {"target_self_id": "1", "platform_id": "napcat01", "notify_targets": ["a:b:c"]}
        )
        self.assertEqual(errors, [])
        self.assertEqual(cfg["interval_seconds"], 30)
        self.assertEqual(cfg["debounce"], 3)
        self.assertEqual(cfg["probe_timeout_seconds"], 5)
        self.assertTrue(cfg["enabled"])
        self.assertTrue(cfg["persist_state"])

    def test_empty_config_is_invalid(self):
        """默认配置缺少必填项（target_self_id / platform_id / notify_targets），必须报错。"""
        cfg, errors = validate_config(None)
        self.assertEqual(cfg["interval_seconds"], 30)  # 默认值仍被应用
        self.assertTrue(any("target_self_id" in e for e in errors))
        self.assertTrue(any("platform_id" in e for e in errors))
        self.assertTrue(any("notify_targets" in e for e in errors))

    def test_valid_full_config(self):
        raw = {
            "target_self_id": "123456789",
            "platform_id": "my_napcat",
            "notify_targets": ["my_napcat:GroupMessage:10001"],
        }
        cfg, errors = validate_config(raw)
        self.assertEqual(errors, [])
        self.assertEqual(cfg["target_self_id"], "123456789")
        self.assertEqual(cfg["platform_id"], "my_napcat")
        self.assertEqual(cfg["notify_targets"], ["my_napcat:GroupMessage:10001"])


class InvalidConfigTest(unittest.TestCase):
    def test_missing_target_self_id(self):
        cfg, errors = validate_config(
            {"platform_id": "napcat1", "notify_targets": ["aiocqhttp:GroupMessage:1"]}
        )
        self.assertTrue(any("target_self_id" in e for e in errors))

    def test_missing_platform_id(self):
        cfg, errors = validate_config(
            {"target_self_id": "1", "notify_targets": ["aiocqhttp:GroupMessage:1"]}
        )
        self.assertTrue(any("platform_id" in e for e in errors))

    def test_empty_notify_targets(self):
        cfg, errors = validate_config(
            {"target_self_id": "1", "platform_id": "napcat1", "notify_targets": []}
        )
        self.assertTrue(any("notify_targets" in e for e in errors))

    def test_bad_target_format(self):
        cfg, errors = validate_config(
            {
                "target_self_id": "1",
                "platform_id": "napcat1",
                "notify_targets": ["no-colons-here"],
            }
        )
        self.assertTrue(any("格式非法" in e for e in errors))

    def test_empty_segment_targets_rejected(self):
        """含空段的 unified_msg_origin 必须拒绝（否则表面启动、实际无法发送）。"""
        for bad in (
            "wechat::id",           # 消息类型段为空
            ":FriendMessage:id",    # 平台段为空
            "wechat:FriendMessage:",  # 会话段为空
            "wechat: :id",          # 消息类型段仅空白
        ):
            cfg, errors = validate_config(
                {
                    "target_self_id": "1",
                    "platform_id": "napcat1",
                    "notify_targets": [bad],
                }
            )
            self.assertTrue(
                any("格式非法" in e for e in errors),
                f"{bad!r} 应被判为格式非法",
            )
        # 拒绝后不得进入生效配置
        cfg, errors = validate_config(
            {
                "target_self_id": "1",
                "platform_id": "napcat1",
                "notify_targets": ["wechat::id"],
            }
        )
        self.assertEqual(cfg["notify_targets"], [])

    def test_cross_platform_targets_accepted(self):
        """notify_targets 可为 AstrBot 任意平台的 unified_msg_origin，不限于被监视的 OneBot 实例。"""
        cfg, errors = validate_config(
            {
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": [
                    "weixin_personal_icry:FriendMessage:wxid_abc123",
                    "telegram:FriendMessage:10001",
                    "wecom:OtherMessage:room-1",
                ],
            }
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            cfg["notify_targets"],
            [
                "weixin_personal_icry:FriendMessage:wxid_abc123",
                "telegram:FriendMessage:10001",
                "wecom:OtherMessage:room-1",
            ],
        )

    def test_notify_target_not_required_to_match_platform_id(self):
        """推送目标的 platform 段不要求等于被监视实例的 platform_id。"""
        cfg, errors = validate_config(
            {
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": ["other_platform:GroupMessage:42"],
            }
        )
        self.assertEqual(errors, [])

    def test_notify_target_wrong_segment_count_rejected(self):
        """段数不足（非 platform:类型:会话 结构）仍须拒绝——只校验结构，不校验取值。"""
        cfg, errors = validate_config(
            {
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": ["napcat:GroupMessage"],  # 只有 2 段
            }
        )
        self.assertTrue(any("格式非法" in e for e in errors))

    def test_extra_colon_session_id_still_accepted(self):
        """会话 ID 本身可含冒号（split 上限 2 段），结构与非空校验即可。"""
        cfg, errors = validate_config(
            {
                "target_self_id": "776916629",
                "platform_id": "napcat",
                "notify_targets": ["napcat:GroupMessage:group:with:colons"],
            }
        )
        self.assertEqual(errors, [])
        self.assertEqual(
            cfg["notify_targets"], ["napcat:GroupMessage:group:with:colons"]
        )

    def test_multi_targets_ok(self):
        cfg, errors = validate_config(
            {
                "target_self_id": "1",
                "platform_id": "napcat1",
                "notify_targets": [
                    "napcat1:GroupMessage:10001",
                    "napcat1:FriendMessage:20002",
                ],
            }
        )
        self.assertEqual(errors, [])
        self.assertEqual(len(cfg["notify_targets"]), 2)


class ClampTest(unittest.TestCase):
    def _valid(self, **overrides):
        base = {"target_self_id": "1", "platform_id": "napcat1", "notify_targets": ["a:b:c"]}
        base.update(overrides)
        return base

    def test_interval_clamped(self):
        cfg, _ = validate_config(self._valid(interval_seconds=999999))
        self.assertEqual(cfg["interval_seconds"], 86400)
        cfg, _ = validate_config(self._valid(interval_seconds=1))
        self.assertEqual(cfg["interval_seconds"], 5)

    def test_debounce_clamped(self):
        cfg, _ = validate_config(self._valid(debounce=99))
        self.assertEqual(cfg["debounce"], 10)
        cfg, _ = validate_config(self._valid(debounce=0))
        self.assertEqual(cfg["debounce"], 1)

    def test_bad_types_fall_back_to_defaults(self):
        cfg, _ = validate_config(
            self._valid(interval_seconds="abc", debounce=None)
        )
        self.assertEqual(cfg["interval_seconds"], DEFAULTS["interval_seconds"])
        self.assertEqual(cfg["debounce"], DEFAULTS["debounce"])


if __name__ == "__main__":
    unittest.main()
