#!/usr/bin/env python3

import json
import os
import threading
from typing import Any, Dict

import rclpy
import yaml
from rclpy.node import Node

from next_ros2ws_interfaces.srv import GetUiMappings, SaveUiMappings
from .db_manager import DatabaseManager
from .action_registry import default_action_mappings, merge_action_mappings
from .topic_catalog import (
    default_topics as catalog_default_topics,
    extract_topic_overrides,
    merge_topic_overrides,
)


class SettingsManager(Node):
    """Owns UI topic-mapping persistence."""

    DEFAULT_MAPPINGS: Dict[str, Dict[str, str]] = {
        'battery': {
            'topic': '/battery_state',
            'field': 'percentage',
            'type': 'sensor_msgs/msg/BatteryState',
        },
        'speed': {
            'topic': '/odometry/filtered',
            'field': 'twist.twist.linear.x',
            'type': 'nav_msgs/msg/Odometry',
        },
        'localization': {
            'topic': '/amcl_pose',
            'field': 'pose.pose',
            'type': 'geometry_msgs/msg/PoseWithCovarianceStamped',
        },
        'robot_pose': {
            'topic': '/tf',
            'field': 'map->base_link',
            'type': 'tf2_msgs/msg/TFMessage',
        },
        'map': {
            'topic': '/map',
            'field': 'OccupancyGrid',
            'type': 'nav_msgs/msg/OccupancyGrid',
        },
        'cmd_vel': {
            'topic': '/wheel_controller/cmd_vel_unstamped',
            'field': 'linear.x',
            'type': 'geometry_msgs/msg/Twist',
        },
        'battery_temperature': {
            'topic': '/battery_temperature',
            'field': 'data',
            'type': 'std_msgs/msg/Float32',
        },
        'battery_charge_cycles': {
            'topic': '/battery_charge_cycles',
            'field': 'data',
            'type': 'std_msgs/msg/UInt32',
        },
        **default_action_mappings(),
    }

    DEFAULT_TOPICS: Dict[str, str] = catalog_default_topics()

    def __init__(self):
        super().__init__('settings_manager')

        # Initialize database manager
        db_path = os.path.expanduser(
            str(self.declare_parameter('db_path', '~/DB/robot_data.db').value)
        )
        self.db_manager = DatabaseManager(db_path=db_path)
        
        # Legacy file path for migration fallback
        self.declare_parameter('mappings_file', os.path.expanduser('~/ui_mappings.yaml'))
        configured = str(self.get_parameter('mappings_file').value or '').strip()
        self.mappings_file = os.path.expanduser(configured or '~/ui_mappings.yaml')

        self._lock = threading.RLock()
        self._mappings: Dict[str, Dict[str, str]] = {
            key: dict(value) for key, value in self.DEFAULT_MAPPINGS.items()
        }
        self._topics: Dict[str, str] = dict(self.DEFAULT_TOPICS)
        self._load_from_database()
        with self._lock:
            try:
                # Ensure defaults are saved to database
                self._save_to_database_locked()
            except Exception as exc:
                self.get_logger().warn(f'Failed writing default mappings to database: {exc}')

        self.get_mappings_srv = self.create_service(GetUiMappings, '/settings/get_mappings', self.get_mappings_callback)
        self.save_mappings_srv = self.create_service(SaveUiMappings, '/settings/save_mappings', self.save_mappings_callback)

        self.get_logger().info('SettingsManager started')
        self.get_logger().info(f'Database: {db_path}')
        self.get_logger().info(f'Legacy mappings file: {self.mappings_file}')

    @staticmethod
    def _sanitize_mappings(raw: Any) -> Dict[str, Dict[str, str]]:
        if not isinstance(raw, dict):
            return {}

        cleaned: Dict[str, Dict[str, str]] = {}
        for key, value in raw.items():
            field_name = str(key or '').strip()
            if not field_name:
                continue
            if not isinstance(value, dict):
                continue

            topic = str(value.get('topic', '') or '').strip()
            field = str(value.get('field', '') or '').strip()
            msg_type = str(value.get('type', '') or '').strip()

            cleaned[field_name] = {
                'topic': topic,
                'field': field,
            }
            if msg_type:
                cleaned[field_name]['type'] = msg_type
        return cleaned

    @staticmethod
    def _sanitize_topics(raw: Any) -> Dict[str, str]:
        if not isinstance(raw, dict):
            return {}

        cleaned: Dict[str, str] = {}
        for key, value in raw.items():
            name = str(key or '').strip()
            if not name:
                continue
            cleaned[name] = str(value or '').strip()
        return cleaned

    @classmethod
    def _merge_mappings_with_defaults(cls, loaded: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        merged: Dict[str, Dict[str, str]] = {}
        for key, default_value in cls.DEFAULT_MAPPINGS.items():
            entry = dict(default_value)
            if key in loaded:
                entry.update(loaded[key])
            merged[key] = entry

        for key, value in loaded.items():
            if key not in merged:
                merged[key] = dict(value)
        return merge_action_mappings(merged)

    @classmethod
    def _merge_topics_with_defaults(cls, loaded: Dict[str, str]) -> Dict[str, str]:
        return merge_topic_overrides(loaded)

    def _load_from_database(self):
        """Load UI mappings from database with legacy file fallback."""
        try:
            db_data = self.db_manager.get_ui_mappings()
            
            # If database is empty, try loading from legacy file
            if not db_data:
                self.get_logger().info('Database empty, attempting legacy file migration...')
                if os.path.exists(self.mappings_file):
                    try:
                        with open(self.mappings_file, 'r', encoding='utf-8') as f:
                            file_data = yaml.safe_load(f) or {}
                        
                        if isinstance(file_data, dict) and ('mappings' in file_data or 'topics' in file_data):
                            mappings = self._sanitize_mappings(file_data.get('mappings', {}))
                            topics = self._sanitize_topics(file_data.get('topics', {}))
                        else:
                            # Legacy format: root object directly stores mappings
                            mappings = self._sanitize_mappings(file_data)
                            topics = {}
                        
                        # Save to database
                        db_data = {'mappings': mappings, 'topics': topics}
                        self.db_manager.save_ui_mappings(db_data)
                        self.get_logger().info('Migrated UI mappings from legacy file to database')
                    except Exception as exc:
                        self.get_logger().warn(f'Failed to load legacy mappings file: {exc}')
                        return
            
            with self._lock:
                mappings = self._sanitize_mappings(db_data.get('mappings', {}))
                topics = self._sanitize_topics(db_data.get('topics', {}))
                self._mappings = self._merge_mappings_with_defaults(mappings)
                self._topics = self._merge_topics_with_defaults(topics)
        except Exception as exc:
            self.get_logger().error(f'Failed to load UI mappings from database: {exc}')

    def _save_to_database_locked(self):
        """Save UI mappings to database."""
        payload = {
            'mappings': self._mappings,
            'topics': extract_topic_overrides(self._topics),
        }
        self.db_manager.save_ui_mappings(payload)

    def get_mappings_callback(self, _request, response):
        with self._lock:
            response.ok = True
            response.message = 'Mappings loaded'
            response.mappings_json = json.dumps(
                {
                    'mappings': self._mappings,
                    'topics': self._topics,
                }
            )
        return response

    def save_mappings_callback(self, request, response):
        try:
            parsed = json.loads(str(request.mappings_json or '{}'))
        except Exception as exc:
            response.ok = False
            response.message = f'Invalid mappings JSON: {exc}'
            response.require_confirmation = False
            return response

        parsed_dict = parsed if isinstance(parsed, dict) else {}
        if 'mappings' in parsed_dict or 'topics' in parsed_dict:
            mappings_raw = parsed_dict.get('mappings', {})
            topics_raw = parsed_dict.get('topics', {})
        else:
            mappings_raw = parsed_dict
            topics_raw = {}

        sanitized_mappings = self._sanitize_mappings(mappings_raw)
        sanitized_topics = self._sanitize_topics(topics_raw)

        with self._lock:
            self._mappings = self._merge_mappings_with_defaults(sanitized_mappings)
            self._topics = self._merge_topics_with_defaults(sanitized_topics)
            try:
                self._save_to_database_locked()
            except Exception as exc:
                response.ok = False
                response.message = f'Failed to save mappings: {exc}'
                response.require_confirmation = False
                return response

        response.ok = True
        response.message = 'Mappings saved'
        response.require_confirmation = False
        return response


def main(args=None):
    rclpy.init(args=args)
    node = SettingsManager()
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
