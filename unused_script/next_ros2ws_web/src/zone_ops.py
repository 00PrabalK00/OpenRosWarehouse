"""Zone metadata helper operations for zone web UI."""


def apply_zone_metadata(zones, name, zone_type='normal', speed=0.5, action=None, charge_duration=None):
    """Apply zone metadata fields on an in-memory zones dict."""
    zones[name]['type'] = zone_type
    zones[name]['speed'] = speed

    zones[name].pop('action', None)
    zones[name].pop('charge_duration', None)

    if zone_type == 'action' and action:
        zones[name]['action'] = action

    if charge_duration is not None:
        try:
            duration = float(charge_duration)
        except (TypeError, ValueError):
            duration = 0.0
        if duration > 0.0:
            zones[name]['charge_duration'] = duration
