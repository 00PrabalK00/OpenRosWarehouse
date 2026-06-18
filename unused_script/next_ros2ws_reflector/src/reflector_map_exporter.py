#!/usr/bin/env python3
import json
import os
import sqlite3
import yaml

def export_reflector_map(db_path, output_path):
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return False
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Try to find reflectors in map_layers
    try:
        cursor.execute("SELECT no_go_zones, restricted, slow_zones, safety_zones FROM map_layers WHERE id=1")
        row = cursor.fetchone()
        if not row:
            print("No map layers found")
            return False
            
        # Wait, if 'reflector' column is missing, we might have stored it in 'no_go_zones' or similar?
        # Actually, let's check if the column exists first.
        cursor.execute("PRAGMA table_info(map_layers)")
        columns = [c[1] for c in cursor.fetchall()]
        
        reflectors = []
        if 'reflector' in columns:
            cursor.execute("SELECT reflector FROM map_layers WHERE id=1")
            r = cursor.fetchone()
            if r and r['reflector']:
                reflectors = json.loads(r['reflector'])
        else:
            print("Warning: 'reflector' column missing in map_layers table.")
            # Maybe it's in a different table?
            # For now, we'll use a placeholder if empty.
            
        if not reflectors:
            print("No reflectors found in database.")
            # If we're on the real robot and this is a fresh setup, we might need a default.
            return False

        map_data = {"reflectors": []}
        for r in reflectors:
            map_data["reflectors"].append({
                "id": r.get("reflector_id", r.get("id", "0")),
                "x": float(r["x"]),
                "y": float(r["y"]),
                "group": r.get("group", "default")
            })
            
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            yaml.safe_dump(map_data, f)
        print(f"Exported {len(reflectors)} reflectors to {output_path}")
        return True
        
    except Exception as e:
        print(f"Export failed: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    db = os.path.expanduser("~/DB/robot_data.db")
    out = os.path.expanduser("~/.gemini/reflector_map.yaml") # Temporary location
    export_reflector_map(db, out)
