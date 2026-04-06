"""Safety helper operations for zone web UI routes."""


def remap_setbool_result(result, unavailable_msg, timeout_msg):
    """Map generic SetBool helper messages to route-specific messages."""
    if not result.get('ok') and result.get('message') == 'SetBool service not available':
        return {'ok': False, 'message': unavailable_msg}
    if not result.get('ok') and result.get('message') == 'Timeout waiting for SetBool response':
        return {'ok': False, 'message': timeout_msg}
    return result
