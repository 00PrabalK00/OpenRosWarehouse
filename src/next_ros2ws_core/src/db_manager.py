#!/usr/bin/env python3
"""
SQLite database manager for robot navigation data.

Provides thread-safe database operations for zones, paths, layouts,
UI mappings, mission state, and map layers.
"""

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple


class DatabaseManager:
    """Thread-safe SQLite database manager for robot data."""
    
    def __init__(self, db_path: str = None):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file. Defaults to ~/DB/robot_data.db
        """
        if db_path is None:
            db_path = os.path.expanduser('~/DB/robot_data.db')
        
        self.db_path = db_path
        self._lock = threading.Lock()
        self._schema_version = 1
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        # Initialize schema
        self._initialize_schema()
    
    @contextmanager
    def _get_connection(self):
        """Get a short-lived database connection for one operation."""
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            timeout=10.0
        )
        conn.row_factory = sqlite3.Row

        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    def _initialize_schema(self):
        """Create database schema if it doesn't exist."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Zones table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS zones (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    position_x REAL NOT NULL,
                    position_y REAL NOT NULL,
                    position_z REAL NOT NULL DEFAULT 0.0,
                    orientation_x REAL NOT NULL DEFAULT 0.0,
                    orientation_y REAL NOT NULL DEFAULT 0.0,
                    orientation_z REAL NOT NULL,
                    orientation_w REAL NOT NULL,
                    frame_id TEXT NOT NULL DEFAULT 'map',
                    type TEXT DEFAULT 'normal',
                    speed REAL DEFAULT 0.5,
                    action TEXT,
                    charge_duration REAL,
                    zone_order INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Paths table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paths (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    frame_id TEXT NOT NULL DEFAULT 'map',
                    points TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Layouts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS layouts (
                    id TEXT PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    map_path TEXT,
                    zones TEXT,
                    paths TEXT,
                    filters TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # UI mappings table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ui_mappings (
                    category TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (category, key)
                )
            ''')
            
            # Mission state table (single row)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS mission_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_zone TEXT DEFAULT '',
                    data TEXT DEFAULT '[]',
                    interrupted_by TEXT DEFAULT '',
                    message TEXT DEFAULT '',
                    pending_resume INTEGER DEFAULT 0,
                    progress INTEGER DEFAULT 0,
                    running INTEGER DEFAULT 0,
                    type TEXT DEFAULT '',
                    updated_at REAL DEFAULT 0.0
                )
            ''')
            
            # Map layers table (single row)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS map_layers (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    no_go_zones TEXT DEFAULT '[]',
                    restricted TEXT DEFAULT '[]',
                    slow_zones TEXT DEFAULT '[]',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Robot profiles table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS robot_profiles (
                    robot_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    profile_version INTEGER DEFAULT 1,
                    schema_version INTEGER DEFAULT 1,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Active robot profile state (single row)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS robot_profile_registry (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    active_robot_id TEXT DEFAULT '',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Recognition template assets
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS recognition_templates (
                    template_id TEXT PRIMARY KEY,
                    family_key TEXT NOT NULL,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    geometry_type TEXT NOT NULL,
                    version INTEGER DEFAULT 1,
                    status TEXT DEFAULT 'draft',
                    parent_template_id TEXT DEFAULT '',
                    payload TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Shelf-aware action point metadata keyed by zone/action-point name
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS action_point_configs (
                    zone_name TEXT PRIMARY KEY,
                    point_type TEXT NOT NULL DEFAULT 'generic',
                    template_id TEXT DEFAULT '',
                    action_id TEXT DEFAULT '',
                    recognize INTEGER DEFAULT 0,
                    payload TEXT DEFAULT '{}',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            ''')

            if self._get_schema_version(cursor) < self._schema_version:
                self._ensure_zone_order_column(cursor)
                self._set_schema_version(cursor, self._schema_version)

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_zones_name ON zones(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_zones_order ON zones(zone_order)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_paths_name ON paths(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_layouts_name ON layouts(name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_robot_profiles_robot_id ON robot_profiles(robot_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recognition_templates_family ON recognition_templates(family_key, version DESC)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_recognition_templates_category ON recognition_templates(category, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_action_point_configs_template ON action_point_configs(template_id)')
            
            conn.commit()

    @staticmethod
    def _get_schema_version(cursor) -> int:
        try:
            cursor.execute("SELECT value FROM schema_meta WHERE key = 'schema_version'")
            row = cursor.fetchone()
            if not row:
                return 0
            value = row['value'] if isinstance(row, sqlite3.Row) else row[0]
            return int(value)
        except Exception:
            return 0

    @staticmethod
    def _set_schema_version(cursor, version: int):
        cursor.execute(
            '''
            INSERT INTO schema_meta(key, value)
            VALUES('schema_version', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            ''',
            (str(int(version)),),
        )

    def _ensure_zone_order_column(self, cursor):
        """Ensure zones.zone_order exists and has deterministic sequential values."""
        cursor.execute('PRAGMA table_info(zones)')
        cols = cursor.fetchall()
        col_names = set()
        for col in cols:
            try:
                col_names.add(str(col['name']))
            except Exception:
                if len(col) > 1:
                    col_names.add(str(col[1]))

        if 'zone_order' not in col_names:
            cursor.execute('ALTER TABLE zones ADD COLUMN zone_order INTEGER')

        cursor.execute('''
            SELECT name, zone_order, created_at, rowid AS _rowid
            FROM zones
        ''')
        rows = cursor.fetchall()
        if not rows:
            return

        def _safe_int(value, default):
            try:
                return int(value)
            except Exception:
                return int(default)

        sorted_rows = sorted(
            rows,
            key=lambda row: (
                0 if row['zone_order'] is not None else 1,
                _safe_int(row['zone_order'], 0),
                str(row['created_at'] or ''),
                str(row['name'] or ''),
                _safe_int(row['_rowid'], 0),
            ),
        )

        for idx, row in enumerate(sorted_rows):
            current = row['zone_order']
            if current is None or _safe_int(current, -1) != idx:
                cursor.execute(
                    'UPDATE zones SET zone_order = ? WHERE name = ?',
                    (idx, str(row['name'] or '')),
                )
    
    # ==================== Zone Operations ====================
    
    def get_all_zones(self) -> Dict[str, Dict[str, Any]]:
        """Get all zones as a dictionary keyed by zone name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT *
                FROM zones
                ORDER BY
                    CASE WHEN zone_order IS NULL THEN 1 ELSE 0 END,
                    zone_order ASC,
                    created_at ASC,
                    name ASC
            ''')
            rows = cursor.fetchall()
            
            zones = {}
            for row in rows:
                zone_data = {
                    'position': {
                        'x': row['position_x'],
                        'y': row['position_y'],
                        'z': row['position_z']
                    },
                    'orientation': {
                        'x': row['orientation_x'],
                        'y': row['orientation_y'],
                        'z': row['orientation_z'],
                        'w': row['orientation_w']
                    },
                    'frame_id': row['frame_id']
                }
                
                if row['type'] and row['type'] != 'normal':
                    zone_data['type'] = row['type']
                if row['speed'] is not None:
                    zone_data['speed'] = row['speed']
                if row['action']:
                    zone_data['action'] = row['action']
                if row['charge_duration'] is not None:
                    zone_data['charge_duration'] = row['charge_duration']
                
                zones[row['name']] = zone_data
            
            return zones

    @staticmethod
    def _safe_int(value, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return int(default)

    def _save_zone_with_cursor(self, cursor, name: str, zone_data: Dict[str, Any]):
        zone_id = zone_data.get('id', name)
        position = zone_data.get('position', {})
        orientation = zone_data.get('orientation', {})

        cursor.execute('SELECT zone_order FROM zones WHERE name = ?', (name,))
        existing_by_name = cursor.fetchone()

        existing_by_id = None
        if existing_by_name is None and zone_id:
            cursor.execute('SELECT name, zone_order FROM zones WHERE id = ?', (zone_id,))
            existing_by_id = cursor.fetchone()

        cursor.execute('SELECT COALESCE(MAX(zone_order), -1) + 1 AS next_order FROM zones')
        next_row = cursor.fetchone()
        next_zone_order = self._safe_int(next_row['next_order'] if next_row else 0, 0)

        if existing_by_name is not None:
            zone_order = self._safe_int(existing_by_name['zone_order'], next_zone_order)
            cursor.execute('''
                UPDATE zones
                SET
                    id = ?,
                    position_x = ?,
                    position_y = ?,
                    position_z = ?,
                    orientation_x = ?,
                    orientation_y = ?,
                    orientation_z = ?,
                    orientation_w = ?,
                    frame_id = ?,
                    type = ?,
                    speed = ?,
                    action = ?,
                    charge_duration = ?,
                    zone_order = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE name = ?
            ''', (
                zone_id,
                position.get('x', 0.0),
                position.get('y', 0.0),
                position.get('z', 0.0),
                orientation.get('x', 0.0),
                orientation.get('y', 0.0),
                orientation.get('z', 0.0),
                orientation.get('w', 1.0),
                zone_data.get('frame_id', 'map'),
                zone_data.get('type', 'normal'),
                zone_data.get('speed', 0.5),
                zone_data.get('action'),
                zone_data.get('charge_duration'),
                zone_order,
                name,
            ))
            return

        if existing_by_id is not None:
            zone_order = self._safe_int(existing_by_id['zone_order'], next_zone_order)
            cursor.execute('''
                UPDATE zones
                SET
                    name = ?,
                    position_x = ?,
                    position_y = ?,
                    position_z = ?,
                    orientation_x = ?,
                    orientation_y = ?,
                    orientation_z = ?,
                    orientation_w = ?,
                    frame_id = ?,
                    type = ?,
                    speed = ?,
                    action = ?,
                    charge_duration = ?,
                    zone_order = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                name,
                position.get('x', 0.0),
                position.get('y', 0.0),
                position.get('z', 0.0),
                orientation.get('x', 0.0),
                orientation.get('y', 0.0),
                orientation.get('z', 0.0),
                orientation.get('w', 1.0),
                zone_data.get('frame_id', 'map'),
                zone_data.get('type', 'normal'),
                zone_data.get('speed', 0.5),
                zone_data.get('action'),
                zone_data.get('charge_duration'),
                zone_order,
                zone_id,
            ))
            return

        zone_order = next_zone_order
        cursor.execute('''
            INSERT INTO zones (
                id, name, position_x, position_y, position_z,
                orientation_x, orientation_y, orientation_z, orientation_w,
                frame_id, type, speed, action, charge_duration, zone_order, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            zone_id,
            name,
            position.get('x', 0.0),
            position.get('y', 0.0),
            position.get('z', 0.0),
            orientation.get('x', 0.0),
            orientation.get('y', 0.0),
            orientation.get('z', 0.0),
            orientation.get('w', 1.0),
            zone_data.get('frame_id', 'map'),
            zone_data.get('type', 'normal'),
            zone_data.get('speed', 0.5),
            zone_data.get('action'),
            zone_data.get('charge_duration'),
            zone_order,
        ))

    def _save_path_with_cursor(self, cursor, name: str, path_data: Dict[str, Any]):
        path_id = path_data.get('id', name)
        points = path_data.get('points', [])
        cursor.execute('''
            INSERT OR REPLACE INTO paths (
                id, name, frame_id, points, updated_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            path_id,
            name,
            path_data.get('frame_id', 'map'),
            json.dumps(points)
        ))

    def save_zone(self, name: str, zone_data: Dict[str, Any]) -> bool:
        """Save or update a zone."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            self._save_zone_with_cursor(cursor, name, zone_data)
            conn.commit()
            return True
    
    def delete_zone(self, name: str) -> bool:
        """Delete a zone by name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM zones WHERE name = ?', (name,))
            conn.commit()
            return cursor.rowcount > 0
    
    def reorder_zones(self, zone_names: List[str]) -> bool:
        """Persist zone display/order sequence in the zones table."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT name
                FROM zones
                ORDER BY
                    CASE WHEN zone_order IS NULL THEN 1 ELSE 0 END,
                    zone_order ASC,
                    created_at ASC,
                    name ASC
            ''')
            existing = [str(row['name'] or '') for row in cursor.fetchall() if str(row['name'] or '').strip()]
            if not existing:
                return True

            existing_set = set(existing)
            ordered = []
            seen = set()

            for raw_name in zone_names or []:
                name = str(raw_name or '').strip()
                if not name or name in seen or name not in existing_set:
                    continue
                ordered.append(name)
                seen.add(name)

            # Keep any zones not included by caller in their previous relative order.
            for name in existing:
                if name not in seen:
                    ordered.append(name)
                    seen.add(name)

            cursor.executemany(
                'UPDATE zones SET zone_order = ? WHERE name = ?',
                [(idx, name) for idx, name in enumerate(ordered)],
            )
            conn.commit()
            return True
    
    # ==================== Path Operations ====================
    
    def get_all_paths(self) -> Dict[str, Dict[str, Any]]:
        """Get all paths as a dictionary keyed by path name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM paths')
            rows = cursor.fetchall()
            
            paths = {}
            for row in rows:
                paths[row['name']] = {
                    'frame_id': row['frame_id'],
                    'points': json.loads(row['points'])
                }
            
            return paths
    
    def save_path(self, name: str, path_data: Dict[str, Any]) -> bool:
        """Save or update a path."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            self._save_path_with_cursor(cursor, name, path_data)
            conn.commit()
            return True
    
    def delete_path(self, name: str) -> bool:
        """Delete a path by name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM paths WHERE name = ?', (name,))
            conn.commit()
            return cursor.rowcount > 0

    def replace_zones_and_paths(
        self,
        zones: Dict[str, Dict[str, Any]],
        paths: Dict[str, Dict[str, Any]],
    ) -> bool:
        """Atomically replace the working zone/path set."""
        zones_payload = zones if isinstance(zones, dict) else {}
        paths_payload = paths if isinstance(paths, dict) else {}

        with self._lock:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('DELETE FROM zones')
                cursor.execute('DELETE FROM paths')

                for zone_name, zone_data in zones_payload.items():
                    if not isinstance(zone_data, dict):
                        continue
                    self._save_zone_with_cursor(cursor, str(zone_name), zone_data)

                for path_name, path_data in paths_payload.items():
                    if not isinstance(path_data, dict):
                        continue
                    self._save_path_with_cursor(cursor, str(path_name), path_data)

                conn.commit()
        return True
    
    # ==================== Layout Operations ====================
    
    def get_all_layouts(self) -> Dict[str, Dict[str, Any]]:
        """Get all layouts as a dictionary keyed by layout name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM layouts')
            rows = cursor.fetchall()
            
            layouts = {}
            for row in rows:
                layout_data = {
                    'description': row['description'] or '',
                    'map_path': row['map_path'] or ''
                }
                
                if row['zones']:
                    layout_data['zones'] = json.loads(row['zones'])
                if row['paths']:
                    layout_data['paths'] = json.loads(row['paths'])
                if row['filters']:
                    layout_data['filters'] = json.loads(row['filters'])
                
                # Add created timestamp if available
                if row['created_at']:
                    layout_data['created'] = int(time.mktime(time.strptime(
                        row['created_at'], '%Y-%m-%d %H:%M:%S'
                    )))
                
                layouts[row['name']] = layout_data
            
            return layouts
    
    def save_layout(self, name: str, layout_data: Dict[str, Any]) -> bool:
        """Save or update a layout."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            layout_id = layout_data.get('id', name)
            
            cursor.execute('''
                INSERT OR REPLACE INTO layouts (
                    id, name, description, map_path, zones, paths, filters, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                layout_id,
                name,
                layout_data.get('description', ''),
                layout_data.get('map_path', ''),
                json.dumps(layout_data.get('zones', {})),
                json.dumps(layout_data.get('paths', {})),
                json.dumps(layout_data.get('filters', {}))
            ))
            
            conn.commit()
            return True
    
    def delete_layout(self, name: str) -> bool:
        """Delete a layout by name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM layouts WHERE name = ?', (name,))
            conn.commit()
            return cursor.rowcount > 0
    
    # ==================== UI Mappings Operations ====================
    
    def get_ui_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Get all UI mappings organized by category."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT category, key, value FROM ui_mappings')
            rows = cursor.fetchall()
            
            mappings = {}
            for row in rows:
                category = row['category']
                if category not in mappings:
                    mappings[category] = {}
                
                # Try to parse as JSON, otherwise use as string
                try:
                    value = json.loads(row['value'])
                except (json.JSONDecodeError, TypeError):
                    value = row['value']
                
                mappings[category][row['key']] = value
            
            return mappings
    
    def save_ui_mappings(self, mappings: Dict[str, Dict[str, Any]]) -> bool:
        """Save UI mappings (replaces all existing mappings)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            # Clear existing mappings
            cursor.execute('DELETE FROM ui_mappings')
            
            # Insert new mappings
            for category, items in mappings.items():
                for key, value in items.items():
                    # Serialize complex values as JSON
                    if isinstance(value, (dict, list)):
                        value_str = json.dumps(value)
                    else:
                        value_str = str(value)
                    
                    cursor.execute('''
                        INSERT INTO ui_mappings (category, key, value)
                        VALUES (?, ?, ?)
                    ''', (category, key, value_str))
            
            conn.commit()
            return True
    
    # ==================== Mission State Operations ====================
    
    def get_mission_state(self) -> Dict[str, Any]:
        """Get current mission state."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM mission_state WHERE id = 1')
            row = cursor.fetchone()
            
            if row is None:
                # Return default state
                return {
                    'current_zone': '',
                    'data': [],
                    'interrupted_by': '',
                    'message': '',
                    'pending_resume': False,
                    'progress': 0,
                    'running': False,
                    'type': '',
                    'updated_at': 0.0
                }
            
            return {
                'current_zone': row['current_zone'],
                'data': json.loads(row['data']),
                'interrupted_by': row['interrupted_by'],
                'message': row['message'],
                'pending_resume': bool(row['pending_resume']),
                'progress': row['progress'],
                'running': bool(row['running']),
                'type': row['type'],
                'updated_at': row['updated_at']
            }
    
    def save_mission_state(self, state: Dict[str, Any]) -> bool:
        """Save mission state."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO mission_state (
                    id, current_zone, data, interrupted_by, message,
                    pending_resume, progress, running, type, updated_at
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                state.get('current_zone', ''),
                json.dumps(state.get('data', [])),
                state.get('interrupted_by', ''),
                state.get('message', ''),
                1 if state.get('pending_resume', False) else 0,
                state.get('progress', 0),
                1 if state.get('running', False) else 0,
                state.get('type', ''),
                state.get('updated_at', time.time())
            ))
            
            conn.commit()
            return True
    
    # ==================== Map Layers Operations ====================
    
    def get_map_layers(self) -> Dict[str, List[Any]]:
        """Get map layers (no-go zones, slow zones, restricted areas)."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM map_layers WHERE id = 1')
            row = cursor.fetchone()
            
            if row is None:
                return {
                    'no_go_zones': [],
                    'restricted': [],
                    'slow_zones': []
                }
            
            return {
                'no_go_zones': json.loads(row['no_go_zones']),
                'restricted': json.loads(row['restricted']),
                'slow_zones': json.loads(row['slow_zones'])
            }
    
    def save_map_layers(self, layers: Dict[str, List[Any]]) -> bool:
        """Save map layers."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO map_layers (
                    id, no_go_zones, restricted, slow_zones, updated_at
                ) VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (
                json.dumps(layers.get('no_go_zones', [])),
                json.dumps(layers.get('restricted', [])),
                json.dumps(layers.get('slow_zones', []))
            ))
            
            conn.commit()
            return True

    # ==================== Robot Profile Persistence ====================

    def get_robot_profiles(self) -> Dict[str, Dict[str, Any]]:
        """Get all persisted robot profiles keyed by robot_id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT robot_id, payload FROM robot_profiles ORDER BY robot_id')
            rows = cursor.fetchall()

            profiles: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                robot_id = str(row['robot_id'] or '').strip()
                if not robot_id:
                    continue
                payload_raw = row['payload']
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else {}
                except Exception:
                    payload = {}
                if isinstance(payload, dict):
                    profiles[robot_id] = payload
            return profiles

    def save_robot_profile(self, robot_id: str, payload: Dict[str, Any]) -> bool:
        """Save/update one robot profile payload."""
        target_robot_id = str(robot_id or '').strip()
        if not target_robot_id:
            return False
        profile_payload = payload if isinstance(payload, dict) else {}
        profile_version = 1
        schema_version = 1
        try:
            profile_version = max(1, int(profile_payload.get('profile_version', 1) or 1))
        except Exception:
            profile_version = 1
        try:
            schema_version = max(1, int(profile_payload.get('schema_version', 1) or 1))
        except Exception:
            schema_version = 1

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO robot_profiles (
                    robot_id, payload, profile_version, schema_version, updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    target_robot_id,
                    json.dumps(profile_payload),
                    profile_version,
                    schema_version,
                ),
            )
            conn.commit()
            return True

    def save_robot_profiles(self, profiles: Dict[str, Dict[str, Any]]) -> bool:
        """Replace all robot profiles with provided payloads."""
        if not isinstance(profiles, dict):
            return False
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM robot_profiles')
            for robot_id, payload in profiles.items():
                target_robot_id = str(robot_id or '').strip()
                if not target_robot_id or not isinstance(payload, dict):
                    continue
                profile_version = 1
                schema_version = 1
                try:
                    profile_version = max(1, int(payload.get('profile_version', 1) or 1))
                except Exception:
                    profile_version = 1
                try:
                    schema_version = max(1, int(payload.get('schema_version', 1) or 1))
                except Exception:
                    schema_version = 1
                cursor.execute(
                    '''
                    INSERT OR REPLACE INTO robot_profiles (
                        robot_id, payload, profile_version, schema_version, updated_at
                    ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''',
                    (
                        target_robot_id,
                        json.dumps(payload),
                        profile_version,
                        schema_version,
                    ),
                )
            conn.commit()
            return True

    def get_active_robot_profile_id(self) -> str:
        """Get currently active robot_id from registry state."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT active_robot_id FROM robot_profile_registry WHERE id = 1')
            row = cursor.fetchone()
            if row is None:
                return ''
            return str(row['active_robot_id'] or '').strip()

    def save_active_robot_profile_id(self, robot_id: str) -> bool:
        """Persist currently active robot_id to registry state."""
        target_robot_id = str(robot_id or '').strip()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO robot_profile_registry (
                    id, active_robot_id, updated_at
                ) VALUES (1, ?, CURRENT_TIMESTAMP)
                ''',
                (target_robot_id,),
            )
            conn.commit()
            return True

    # ==================== Recognition Template Persistence ====================

    def get_recognition_templates(self) -> Dict[str, Dict[str, Any]]:
        """Get all persisted recognition templates keyed by template_id."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT *
                FROM recognition_templates
                ORDER BY
                    category ASC,
                    family_key ASC,
                    version DESC,
                    updated_at DESC,
                    template_id ASC
                '''
            )
            rows = cursor.fetchall()

            templates: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                template_id = str(row['template_id'] or '').strip()
                if not template_id:
                    continue
                payload_raw = row['payload']
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else {}
                except Exception:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                payload.update({
                    'template_id': template_id,
                    'family_key': str(row['family_key'] or '').strip(),
                    'name': str(row['name'] or '').strip(),
                    'category': str(row['category'] or '').strip(),
                    'geometry_type': str(row['geometry_type'] or '').strip(),
                    'version': self._safe_int(row['version'], 1),
                    'status': str(row['status'] or 'draft').strip(),
                    'parent_template_id': str(row['parent_template_id'] or '').strip(),
                    'created_at': str(row['created_at'] or ''),
                    'updated_at': str(row['updated_at'] or ''),
                })
                templates[template_id] = payload
            return templates

    def save_recognition_template(self, template_id: str, payload: Dict[str, Any]) -> bool:
        """Save or update one recognition template payload."""
        target_template_id = str(template_id or '').strip()
        if not target_template_id:
            return False

        template_payload = payload if isinstance(payload, dict) else {}
        family_key = str(template_payload.get('family_key', target_template_id) or '').strip() or target_template_id
        name = str(template_payload.get('name', target_template_id) or '').strip() or target_template_id
        category = str(template_payload.get('category', 'shelves') or 'shelves').strip().lower()
        geometry_type = str(template_payload.get('geometry_type', 'recognition') or 'recognition').strip().lower()
        status = str(template_payload.get('status', 'draft') or 'draft').strip().lower()
        if status not in {'draft', 'published', 'deprecated'}:
            status = 'draft'
        version = max(1, self._safe_int(template_payload.get('version', 1), 1))
        parent_template_id = str(template_payload.get('parent_template_id', '') or '').strip()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO recognition_templates (
                    template_id, family_key, name, category, geometry_type,
                    version, status, parent_template_id, payload, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    target_template_id,
                    family_key,
                    name,
                    category,
                    geometry_type,
                    version,
                    status,
                    parent_template_id,
                    json.dumps(template_payload),
                ),
            )
            conn.commit()
            return True

    def delete_recognition_template(self, template_id: str) -> bool:
        """Delete one recognition template payload."""
        target_template_id = str(template_id or '').strip()
        if not target_template_id:
            return False
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM recognition_templates WHERE template_id = ?', (target_template_id,))
            conn.commit()
            return cursor.rowcount > 0

    def get_action_point_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get persisted action point config payloads keyed by zone_name."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT *
                FROM action_point_configs
                ORDER BY zone_name ASC
                '''
            )
            rows = cursor.fetchall()

            configs: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                zone_name = str(row['zone_name'] or '').strip()
                if not zone_name:
                    continue
                payload_raw = row['payload']
                try:
                    payload = json.loads(payload_raw) if isinstance(payload_raw, str) else {}
                except Exception:
                    payload = {}
                if not isinstance(payload, dict):
                    payload = {}
                payload.update({
                    'zone_name': zone_name,
                    'point_type': str(row['point_type'] or 'generic').strip().lower() or 'generic',
                    'template_id': str(row['template_id'] or '').strip(),
                    'action_id': str(row['action_id'] or '').strip(),
                    'recognize': bool(row['recognize']),
                    'updated_at': str(row['updated_at'] or ''),
                })
                configs[zone_name] = payload
            return configs

    def save_action_point_config(self, zone_name: str, payload: Dict[str, Any]) -> bool:
        """Save or update one action point config payload."""
        target_zone_name = str(zone_name or '').strip()
        if not target_zone_name:
            return False

        config_payload = payload if isinstance(payload, dict) else {}
        point_type = str(config_payload.get('point_type', 'generic') or 'generic').strip().lower()
        template_id = str(config_payload.get('template_id', '') or '').strip()
        action_id = str(config_payload.get('action_id', config_payload.get('action', '')) or '').strip()
        recognize = 1 if bool(config_payload.get('recognize', False)) else 0

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                INSERT OR REPLACE INTO action_point_configs (
                    zone_name, point_type, template_id, action_id, recognize, payload, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''',
                (
                    target_zone_name,
                    point_type or 'generic',
                    template_id,
                    action_id,
                    recognize,
                    json.dumps(config_payload),
                ),
            )
            conn.commit()
            return True

    def delete_action_point_config(self, zone_name: str) -> bool:
        """Delete one action point config by zone name."""
        target_zone_name = str(zone_name or '').strip()
        if not target_zone_name:
            return False
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM action_point_configs WHERE zone_name = ?', (target_zone_name,))
            conn.commit()
            return cursor.rowcount > 0
    
    # ==================== Utility Methods ====================
    
    def get_registry_data(self) -> Dict[str, Any]:
        """
        Get all registry data (zones, paths, layouts) in the format
        compatible with the legacy registry.yaml structure.
        """
        return {
            'version': 2,
            'zones': self.get_all_zones(),
            'paths': self.get_all_paths(),
            'layouts': self.get_all_layouts()
        }
    
    def close(self):
        """Close database connection."""
        if hasattr(self._local, 'connection'):
            self._local.connection.close()
            delattr(self._local, 'connection')
