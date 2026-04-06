"""Mode-switching helpers for zone web UI."""


VALID_STACK_MODES = {'nav', 'slam', 'stop'}


def set_mode_with_log(ros_node, mode, log_message):
    """Set a control mode through canonical service and emit route-specific logs."""
    result = ros_node.publish_mode(mode)
    if result.get('ok'):
        ros_node.get_logger().info(log_message)
    else:
        ros_node.get_logger().warn(
            f'{log_message} failed: {result.get("message", "unknown error")}'
        )
    return result


def is_valid_stack_mode(mode):
    """Check if a stack mode value is accepted by the stack route."""
    return mode in VALID_STACK_MODES
