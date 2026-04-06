#!/usr/bin/env python3
import sys
import os
import time

# Add current directory to path so we can import the package
sys.path.append(os.getcwd())

try:
    from next_ros2ws_core.db_manager import DatabaseManager
    print("SUCCESS: Imported DatabaseManager")
except ImportError as e:
    print(f"ERROR: Could not import DatabaseManager: {e}")
    print(f"Current path: {sys.path}")
    sys.exit(1)

def test_database():
    db_path = os.path.expanduser("~/DB/robot_data.db")
    print(f"Testing database at: {db_path}")
    
    if not os.path.exists(db_path):
        print(f"ERROR: Database file not found at {db_path}")
        return False

    db = DatabaseManager(db_path=db_path)
    
    # Check tables
    print("\n--- Checking Data ---")
    zones = db.get_all_zones()
    print(f"Zones count: {len(zones)}")
    
    paths = db.get_all_paths()
    print(f"Paths count: {len(paths)}")
    
    layouts = db.get_all_layouts()
    print(f"Layouts count: {len(layouts)}")
    
    mappings = db.get_ui_mappings()
    print(f"UI Mappings count (sections): {len(mappings)}")
    
    # Test CRUD
    print("\n--- Testing CRUD ---")
    test_zone_name = "test_verification_zone"
    test_data = {
        "type": "keepout",
        "points": [{"x": 1.0, "y": 1.0}, {"x": 2.0, "y": 2.0}]
    }
    
    # Create
    print(f"Creating zone '{test_zone_name}'...")
    db.save_zone(test_zone_name, test_data)
    
    # Read
    zones = db.get_all_zones()
    if test_zone_name in zones:
        print(f"SUCCESS: Zone '{test_zone_name}' found in database")
    else:
        print(f"ERROR: Zone '{test_zone_name}' NOT found in database")
        return False
        
    # Delete
    print(f"Deleting zone '{test_zone_name}'...")
    db.delete_zone(test_zone_name)
    
    # Verify Delete
    zones = db.get_all_zones()
    if test_zone_name not in zones:
        print(f"SUCCESS: Zone '{test_zone_name}' successfully deleted")
    else:
        print(f"ERROR: Zone '{test_zone_name}' still exists after delete")
        return False

    print("\n✅ DATABASE VERIFICATION PASSED")
    return True

if __name__ == "__main__":
    test_database()
