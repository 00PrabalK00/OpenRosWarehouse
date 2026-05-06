import sys
from pathlib import Path


MODULE_DIR = Path(__file__).resolve().parents[1] / 'src'
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

from topic_catalog import DEFAULT_TOPICS, sanitize_topic_overrides  # noqa: E402


def test_sanitize_topic_overrides_blocks_manual_wheel_cmd_bypass():
    cleaned = sanitize_topic_overrides(
        {
            'ui_manual_cmd_vel_publish': '/wheel_controller/cmd_vel_unstamped',
            'subscription_odom': '/odom',
        }
    )

    assert cleaned['ui_manual_cmd_vel_publish'] == DEFAULT_TOPICS['ui_manual_cmd_vel_publish']
    assert cleaned['subscription_odom'] == '/odom'


def test_sanitize_topic_overrides_blocks_manual_sim_wheel_cmd_bypass():
    cleaned = sanitize_topic_overrides(
        {'ui_manual_cmd_vel_publish': '/diff_cont/cmd_vel_unstamped'}
    )

    assert cleaned['ui_manual_cmd_vel_publish'] == DEFAULT_TOPICS['ui_manual_cmd_vel_publish']
