#!/usr/bin/env python3
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, DurabilityPolicy, HistoryPolicy
from std_msgs.msg import Bool, String

# Match the publisher QoS in control_mode_manager so this node receives the
# current mode immediately even if it starts after the first publish.
_MODE_QOS = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1,
)


class ModeSwitcher(Node):
    def __init__(self):
        super().__init__('mode_switcher')

        # Publishers to lock/unlock twist_mux topics
        self.lock_pubs = {
            'navigation': self.create_publisher(Bool, '/twist_mux/locks/navigation', 10),
            'tracker': self.create_publisher(Bool, '/twist_mux/locks/tracker', 10),
            'joystick': self.create_publisher(Bool, '/twist_mux/locks/joystick', 10),
        }

        self.mode_map = {
            'manual': 'joystick',
            'zones': 'navigation',
            'sequence': 'tracker',
            'path': 'navigation',
        }

        # Canonical mode input — transient-local so the current mode is delivered
        # even if mode_switcher starts after control_mode_manager.
        self.mode_sub = self.create_subscription(String, '/control_mode', self.mode_callback, _MODE_QOS)

        # Safe startup default until first /control_mode message arrives.
        self.current_mode = None
        self.activate_mode('zones')

        self.get_logger().info('Mode Switcher started')
        self.get_logger().info('Consumes /control_mode and applies twist_mux locks')
        self.get_logger().info('Available modes: manual, zones, sequence, path')

    @staticmethod
    def _normalize_mode(mode: str) -> str:
        mode = (mode or '').strip().lower()
        aliases = {
            'auto': 'zones',
            'autonomous': 'zones',
            'nav': 'zones',
        }
        return aliases.get(mode, mode)

    def activate_mode(self, mode: str):
        """Apply mode-specific lock policy for twist_mux sources."""
        if mode not in self.mode_map:
            self.get_logger().warn(f'Unknown mode: {mode}')
            return

        # NOTE:
        # twist_mux locks are threshold-based by lock priority, not per-topic masks.
        # Locking high-priority sources (joystick/tracker) can unintentionally block
        # lower-priority navigation, causing Nav2 "Failed to make progress" aborts.
        lock_policy = {
            # Manual driving: block only navigation commands.
            'manual': {'navigation': True, 'tracker': False, 'joystick': False},
            # Autonomous zone/path navigation: keep navigation unblocked.
            'zones': {'navigation': False, 'tracker': False, 'joystick': False},
            'path': {'navigation': False, 'tracker': False, 'joystick': False},
            # Sequence mode uses Nav2 via GoToZone, so navigation must remain free.
            'sequence': {'navigation': False, 'tracker': False, 'joystick': False},
        }
        selected = lock_policy.get(mode, {'navigation': False, 'tracker': False, 'joystick': False})

        for topic_name, pub in self.lock_pubs.items():
            msg = Bool()
            msg.data = bool(selected.get(topic_name, False))
            pub.publish(msg)

        nav_state = 'LOCKED' if selected.get('navigation') else 'FREE'
        trk_state = 'LOCKED' if selected.get('tracker') else 'FREE'
        joy_state = 'LOCKED' if selected.get('joystick') else 'FREE'
        self.get_logger().info(
            f'{mode.upper()} MODE ACTIVE - locks: navigation={nav_state}, tracker={trk_state}, joystick={joy_state}'
        )
        self.current_mode = mode

    def mode_callback(self, msg: String):
        """Apply mode switches only from canonical /control_mode topic."""
        mode = self._normalize_mode(msg.data)
        if mode not in self.mode_map:
            self.get_logger().warn(
                f'Ignoring invalid /control_mode "{msg.data}". '
                f'Allowed modes: {", ".join(self.mode_map.keys())}'
            )
            return
        if mode == self.current_mode:
            return
        self.activate_mode(mode)


def main(args=None):
    rclpy.init(args=args)
    node = ModeSwitcher()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.destroy_node()
        except Exception:
            pass
        try:
            rclpy.shutdown()
        except Exception:
            pass


if __name__ == '__main__':
    main()
