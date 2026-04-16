#!/usr/bin/env python3

import asyncio
import threading
import time
from typing import Any, Dict, Optional, Tuple

import rclpy
from action_msgs.msg import GoalStatus
from action_msgs.srv import CancelGoal
from rclpy.action import ActionClient, ActionServer, CancelResponse, GoalResponse
from rclpy.callback_groups import ReentrantCallbackGroup
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.task import Future

from next_ros2ws_interfaces.action import GoToZone as GoToZoneAction, FollowPath as FollowPathAction
from next_ros2ws_interfaces.srv import CancelNavigation, RequestNavigation


class NavigationArbitrator(Node):
    """Single ingress for public navigation goals.

    The arbitrator owns the public navigation endpoints and forwards accepted
    requests to ZoneManager's private action servers. This prevents multiple
    external nodes from racing directly against ZoneManager.
    """

    @staticmethod
    def _normalize_robot_namespace(raw_value) -> str:
        ns = str(raw_value or '').strip()
        return ns.strip('/')

    def _endpoint(self, base_name: str) -> str:
        name = str(base_name or '').strip()
        if not name:
            return name
        if not name.startswith('/'):
            name = '/' + name
        if not self.robot_namespace:
            return name
        return f'/{self.robot_namespace}{name}'

    def __init__(self):
        super().__init__('navigation_arbitrator')

        self.robot_namespace = self._normalize_robot_namespace(
            self.declare_parameter('robot_namespace', '').value
        )
        self.cb_group = ReentrantCallbackGroup()

        self.go_to_zone_client = ActionClient(
            self,
            GoToZoneAction,
            self._endpoint('/zone_manager/go_to_zone'),
            callback_group=self.cb_group,
        )
        self.follow_path_client = ActionClient(
            self,
            FollowPathAction,
            self._endpoint('/zone_manager/zone_follow_path'),
            callback_group=self.cb_group,
        )

        self.go_to_zone_server = ActionServer(
            self,
            GoToZoneAction,
            self._endpoint('/go_to_zone'),
            execute_callback=self.execute_go_to_zone,
            goal_callback=self.go_to_zone_goal_callback,
            cancel_callback=self.go_to_zone_cancel_callback,
            callback_group=self.cb_group,
        )
        self.follow_path_server = ActionServer(
            self,
            FollowPathAction,
            self._endpoint('/zone_follow_path'),
            execute_callback=self.execute_follow_path,
            goal_callback=self.follow_path_goal_callback,
            cancel_callback=self.follow_path_cancel_callback,
            callback_group=self.cb_group,
        )

        self.request_goal_srv = self.create_service(
            RequestNavigation,
            self._endpoint('/arbitrator/request_goal'),
            self.request_goal_callback,
            callback_group=self.cb_group,
        )
        self.cancel_goal_srv = self.create_service(
            CancelNavigation,
            self._endpoint('/arbitrator/cancel_goal'),
            self.cancel_goal_callback,
            callback_group=self.cb_group,
        )

        self._state_lock = threading.RLock()
        self._request_counter = 0
        self._reserved_request: Optional[Dict[str, Any]] = None
        self._active_request: Optional[Dict[str, Any]] = None

        self.get_logger().info(
            'NavigationArbitrator ready: public goals are serialized through a single owner.'
        )
        self.get_logger().info(
            f'Private action targets: go_to_zone={self._endpoint("/zone_manager/go_to_zone")} '
            f'follow_path={self._endpoint("/zone_manager/zone_follow_path")}'
        )

    def _next_token_locked(self) -> int:
        self._request_counter += 1
        return int(self._request_counter)

    def _busy_message_locked(self) -> str:
        current = self._active_request or self._reserved_request
        if not current:
            return 'NavigationArbitrator is idle'
        kind = str(current.get('kind', 'navigation') or 'navigation')
        source = str(current.get('source', 'unknown') or 'unknown')
        summary = str(current.get('summary', '') or '').strip()
        if summary:
            return f'NavigationArbitrator busy with {kind} from {source} ({summary})'
        return f'NavigationArbitrator busy with {kind} from {source}'

    def _reserve_request(
        self,
        *,
        kind: str,
        source: str,
        summary: str,
    ) -> Tuple[bool, str, int]:
        with self._state_lock:
            if self._active_request is not None or self._reserved_request is not None:
                return False, self._busy_message_locked(), 0
            token = self._next_token_locked()
            self._reserved_request = {
                'token': token,
                'kind': str(kind),
                'source': str(source),
                'summary': str(summary),
                'mode': 'action',
            }
            return True, '', token

    def _activate_request(
        self,
        *,
        kind: str,
        source: str,
        summary: str,
        mode: str,
        upstream_goal_handle=None,
    ) -> Tuple[bool, str, int]:
        with self._state_lock:
            if self._active_request is not None:
                return False, self._busy_message_locked(), 0

            token = 0
            if self._reserved_request is not None:
                reserved = dict(self._reserved_request)
                if str(reserved.get('kind', '')) != str(kind):
                    return False, self._busy_message_locked(), 0
                token = int(reserved.get('token', 0) or 0)
                source = str(reserved.get('source', source) or source)
                summary = str(reserved.get('summary', summary) or summary)
                mode = str(reserved.get('mode', mode) or mode)
                self._reserved_request = None
            else:
                token = self._next_token_locked()

            self._active_request = {
                'token': token,
                'kind': str(kind),
                'source': str(source),
                'summary': str(summary),
                'mode': str(mode),
                'upstream_goal_handle': upstream_goal_handle,
                'downstream_goal_handle': None,
                'started_at': time.monotonic(),
            }
            return True, '', token

    def _set_downstream_goal_handle(self, token: int, goal_handle) -> bool:
        with self._state_lock:
            if self._active_request is None:
                return False
            if int(self._active_request.get('token', 0) or 0) != int(token):
                return False
            self._active_request['downstream_goal_handle'] = goal_handle
            return True

    def _clear_request(self, token: int):
        with self._state_lock:
            if self._reserved_request is not None:
                reserved_token = int(self._reserved_request.get('token', 0) or 0)
                if reserved_token == int(token):
                    self._reserved_request = None
            if self._active_request is not None:
                active_token = int(self._active_request.get('token', 0) or 0)
                if active_token == int(token):
                    self._active_request = None

    def _wait_future_blocking(self, future, timeout_sec: float):
        timeout = max(0.0, float(timeout_sec))
        start = time.monotonic()
        while rclpy.ok():
            if future.done():
                try:
                    return future.result()
                except Exception as exc:
                    self.get_logger().warn(f'Async operation failed: {exc}')
                    return None
            if timeout > 0.0 and (time.monotonic() - start) >= timeout:
                return None
            time.sleep(0.02)
        return None

    async def _non_blocking_wait(self, seconds: float):
        delay = max(0.0, float(seconds))
        if delay <= 0.0:
            return

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            wake = Future()
            timer_ref = {'timer': None}

            def _wake():
                timer = timer_ref['timer']
                if timer is not None:
                    try:
                        timer.cancel()
                    except Exception:
                        pass
                    try:
                        self.destroy_timer(timer)
                    except Exception:
                        pass
                    timer_ref['timer'] = None
                if not wake.done():
                    wake.set_result(True)

            timer_ref['timer'] = self.create_timer(delay, _wake)
            try:
                await wake
            finally:
                timer = timer_ref['timer']
                if timer is not None:
                    try:
                        timer.cancel()
                    except Exception:
                        pass
                    try:
                        self.destroy_timer(timer)
                    except Exception:
                        pass
                    timer_ref['timer'] = None
                if not wake.done():
                    wake.set_result(True)
            return

        await asyncio.sleep(delay)

    async def _wait_future_with_timeout(self, future, timeout_sec: float):
        timeout = max(0.0, float(timeout_sec))
        deadline_ns = self.get_clock().now().nanoseconds + int(timeout * 1e9)

        while not future.done():
            if timeout > 0.0 and self.get_clock().now().nanoseconds >= deadline_ns:
                return None
            await self._non_blocking_wait(0.02)

        try:
            return future.result()
        except Exception as exc:
            self.get_logger().warn(f'Async future failed: {exc}')
            return None

    @staticmethod
    def _classify_cancel_response(cancel_response) -> Tuple[str, str]:
        if cancel_response is None:
            return 'timeout', 'cancel request timed out'

        try:
            code = int(
                getattr(
                    cancel_response,
                    'return_code',
                    CancelGoal.Response.ERROR_REJECTED,
                )
            )
        except Exception:
            code = int(CancelGoal.Response.ERROR_REJECTED)

        if code == int(CancelGoal.Response.ERROR_NONE):
            goals_canceling = list(getattr(cancel_response, 'goals_canceling', []) or [])
            if goals_canceling:
                return 'accepted', f'cancel accepted ({len(goals_canceling)} goal(s) canceling)'
            return 'accepted', 'cancel accepted'
        if code == int(CancelGoal.Response.ERROR_GOAL_TERMINATED):
            return 'terminal', 'goal already terminal'
        if code == int(CancelGoal.Response.ERROR_UNKNOWN_GOAL_ID):
            return 'unknown', 'goal no longer known by downstream action server'
        if code == int(CancelGoal.Response.ERROR_REJECTED):
            return 'rejected', 'downstream action server rejected cancel request'
        return 'error', f'downstream cancel failed with code {code}'

    @staticmethod
    def _follow_summary(goal_request) -> str:
        count = len(list(getattr(goal_request, 'waypoints', []) or []))
        return f'{count} waypoints'

    def go_to_zone_goal_callback(self, goal_request):
        summary = str(goal_request.name or '').strip() or 'unnamed zone'
        ok, message, _token = self._reserve_request(
            kind='go_to_zone',
            source='action:/go_to_zone',
            summary=summary,
        )
        if not ok:
            self.get_logger().warn(f'Rejecting GoToZone request: {message}')
            return GoalResponse.REJECT
        return GoalResponse.ACCEPT

    def follow_path_goal_callback(self, goal_request):
        summary = self._follow_summary(goal_request)
        ok, message, _token = self._reserve_request(
            kind='follow_path',
            source='action:/zone_follow_path',
            summary=summary,
        )
        if not ok:
            self.get_logger().warn(f'Rejecting FollowPath request: {message}')
            return GoalResponse.REJECT
        return GoalResponse.ACCEPT

    def go_to_zone_cancel_callback(self, goal_handle):
        del goal_handle
        return CancelResponse.ACCEPT

    def follow_path_cancel_callback(self, goal_handle):
        del goal_handle
        return CancelResponse.ACCEPT

    def _build_follow_path_goal(
        self,
        *,
        waypoints,
        zone_names=None,
        shelf_checks=None,
    ):
        goal = FollowPathAction.Goal()
        goal.waypoints = list(waypoints or [])
        goal.zone_names = [str(name or '') for name in list(zone_names or [])]
        goal.shelf_checks = [bool(flag) for flag in list(shelf_checks or [])]
        return goal

    def _start_service_dispatch(self, token: int, request: RequestNavigation.Request) -> Tuple[bool, str]:
        goal_type = str(request.goal_type or '').strip().lower()

        if goal_type == 'go_to_zone':
            goal_name = str(request.goal_name or '').strip()
            if not goal_name:
                return False, 'goal_name is required for go_to_zone requests'
            if not self.go_to_zone_client.wait_for_server(timeout_sec=2.0):
                return False, 'Private GoToZone action server is unavailable'
            goal = GoToZoneAction.Goal()
            goal.name = goal_name
            send_future = self.go_to_zone_client.send_goal_async(goal)
        elif goal_type == 'follow_path':
            waypoints = list(request.waypoints or [])
            if len(waypoints) == 0:
                return False, 'At least one waypoint is required for follow_path requests'
            if not self.follow_path_client.wait_for_server(timeout_sec=2.0):
                return False, 'Private FollowPath action server is unavailable'
            goal = self._build_follow_path_goal(
                waypoints=waypoints,
                zone_names=request.zone_names,
                shelf_checks=request.shelf_checks,
            )
            send_future = self.follow_path_client.send_goal_async(goal)
        else:
            return False, f'Unsupported goal_type "{goal_type}"'

        goal_handle = self._wait_future_blocking(send_future, 2.0)
        if goal_handle is None:
            return False, f'Timeout sending {goal_type} through arbitrator'
        if not bool(getattr(goal_handle, 'accepted', False)):
            return False, f'Arbitrator downstream {goal_type} goal was rejected'
        if not self._set_downstream_goal_handle(token, goal_handle):
            try:
                goal_handle.cancel_goal_async()
            except Exception:
                pass
            return False, 'Arbitrator request state changed during dispatch'

        watcher = threading.Thread(
            target=self._watch_service_result,
            args=(token, goal_type, goal_handle),
            daemon=True,
        )
        watcher.start()
        return True, f'Queued {goal_type} via navigation arbitrator'

    def _watch_service_result(self, token: int, goal_type: str, goal_handle):
        result_future = goal_handle.get_result_async()
        wrapped = self._wait_future_blocking(result_future, 0.0)
        status = GoalStatus.STATUS_UNKNOWN
        message = ''
        success = False

        if wrapped is not None:
            try:
                status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))
                result = getattr(wrapped, 'result', None)
                if result is not None:
                    success = bool(getattr(result, 'success', False))
                    message = str(getattr(result, 'message', '') or '').strip()
            except Exception as exc:
                message = f'Service-dispatched {goal_type} result error: {exc}'

        self._clear_request(token)
        self.get_logger().info(
            f'Service-dispatched {goal_type} finished with status={status} '
            f'success={success} message="{message}"'
        )

    def request_goal_callback(self, request, response):
        requester = str(request.requester or '').strip() or 'service'
        goal_type = str(request.goal_type or '').strip().lower()
        summary = str(request.goal_name or '').strip()
        if goal_type == 'follow_path':
            summary = f'{len(list(request.waypoints or []))} waypoints'

        ok, message, token = self._activate_request(
            kind=goal_type,
            source=f'service:{requester}',
            summary=summary,
            mode='service',
            upstream_goal_handle=None,
        )
        if not ok:
            response.ok = False
            response.message = message
            return response

        start_ok, start_message = self._start_service_dispatch(token, request)
        if not start_ok:
            self._clear_request(token)
        response.ok = bool(start_ok)
        response.message = str(start_message)
        return response

    def cancel_goal_callback(self, request, response):
        requester = str(request.requester or '').strip() or 'service'
        reason = str(request.reason or '').strip() or 'cancel requested'
        with self._state_lock:
            reserved = dict(self._reserved_request) if self._reserved_request is not None else None
            active = dict(self._active_request) if self._active_request is not None else None

            if active is None and reserved is None:
                response.ok = True
                response.message = 'No active navigation goal'
                return response

            if active is None and reserved is not None:
                token = int(reserved.get('token', 0) or 0)
                self._clear_request(token)
                response.ok = True
                response.message = (
                    f'Cleared pending navigation reservation for {reserved.get("kind", "goal")}'
                )
                return response

            downstream_goal_handle = active.get('downstream_goal_handle')
            active_kind = str(active.get('kind', 'navigation') or 'navigation')

        if downstream_goal_handle is None:
            response.ok = False
            response.message = (
                f'Navigation dispatch for {active_kind} is still starting; retry cancel shortly'
            )
            return response

        cancel_future = downstream_goal_handle.cancel_goal_async()
        cancel_response = self._wait_future_blocking(cancel_future, 2.0)
        outcome, detail = self._classify_cancel_response(cancel_response)
        if outcome not in ('accepted', 'terminal'):
            response.ok = False
            response.message = (
                f'Cancel failed for active {active_kind}: {detail}'
            )
            return response

        response.ok = True
        response.message = (
            f'Cancel forwarded by {requester}: {active_kind} ({reason}) - {detail}'
        )
        return response

    async def execute_go_to_zone(self, goal_handle):
        result = GoToZoneAction.Result()
        summary = str(goal_handle.request.name or '').strip() or 'unnamed zone'
        ok, message, token = self._activate_request(
            kind='go_to_zone',
            source='action:/go_to_zone',
            summary=summary,
            mode='action',
            upstream_goal_handle=goal_handle,
        )
        if not ok:
            result.success = False
            result.message = message
            goal_handle.abort()
            return result

        try:
            if not self.go_to_zone_client.wait_for_server(timeout_sec=2.0):
                result.success = False
                result.message = 'Private GoToZone action server is unavailable'
                goal_handle.abort()
                return result

            downstream_goal = GoToZoneAction.Goal()
            downstream_goal.name = goal_handle.request.name

            def _forward_feedback(feedback_msg):
                feedback = getattr(feedback_msg, 'feedback', None)
                if feedback is None or not goal_handle.is_active:
                    return
                proxy_feedback = GoToZoneAction.Feedback()
                proxy_feedback.progress = float(getattr(feedback, 'progress', 0.0))
                proxy_feedback.status = str(getattr(feedback, 'status', '') or '')
                try:
                    goal_handle.publish_feedback(proxy_feedback)
                except Exception:
                    pass

            send_future = self.go_to_zone_client.send_goal_async(
                downstream_goal,
                feedback_callback=_forward_feedback,
            )
            downstream_goal_handle = await self._wait_future_with_timeout(send_future, 2.0)
            if downstream_goal_handle is None:
                result.success = False
                result.message = 'Timeout sending GoToZone through arbitrator'
                goal_handle.abort()
                return result
            if not bool(getattr(downstream_goal_handle, 'accepted', False)):
                result.success = False
                result.message = f'Downstream GoToZone rejected "{summary}"'
                goal_handle.abort()
                return result

            if not self._set_downstream_goal_handle(token, downstream_goal_handle):
                try:
                    cancel_future = downstream_goal_handle.cancel_goal_async()
                    cancel_response = await self._wait_future_with_timeout(cancel_future, 2.0)
                    outcome, detail = self._classify_cancel_response(cancel_response)
                    if outcome not in ('accepted', 'terminal'):
                        self.get_logger().warn(
                            f'GoToZone downstream orphan cleanup failed for "{summary}": {detail}'
                        )
                except Exception:
                    pass
                result.success = False
                result.message = 'Arbitrator request state changed during GoToZone dispatch'
                goal_handle.abort()
                return result

            result_future = downstream_goal_handle.get_result_async()
            cancel_sent = False

            while not result_future.done():
                if goal_handle.is_cancel_requested and not cancel_sent:
                    cancel_sent = True
                    cancel_future = downstream_goal_handle.cancel_goal_async()
                    cancel_response = await self._wait_future_with_timeout(cancel_future, 2.0)
                    outcome, detail = self._classify_cancel_response(cancel_response)
                    if outcome not in ('accepted', 'terminal'):
                        self.get_logger().warn(
                            f'GoToZone cancel forwarding failed for "{summary}": {detail}'
                        )
                await self._non_blocking_wait(0.05)

            wrapped = result_future.result()
            downstream_result = getattr(wrapped, 'result', None)
            status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))

            result.success = bool(getattr(downstream_result, 'success', False))
            result.message = str(getattr(downstream_result, 'message', '') or '').strip()

            if status == GoalStatus.STATUS_CANCELED:
                if not result.message:
                    result.message = 'GoToZone canceled by arbitrator'
                goal_handle.canceled()
                result.success = False
            elif status == GoalStatus.STATUS_SUCCEEDED and bool(result.success):
                goal_handle.succeed()
            else:
                if not result.message:
                    result.message = (
                        f'Downstream GoToZone finished with status {status}'
                    )
                result.success = False
                goal_handle.abort()
            return result
        finally:
            self._clear_request(token)

    async def execute_follow_path(self, goal_handle):
        result = FollowPathAction.Result()
        summary = self._follow_summary(goal_handle.request)
        ok, message, token = self._activate_request(
            kind='follow_path',
            source='action:/zone_follow_path',
            summary=summary,
            mode='action',
            upstream_goal_handle=goal_handle,
        )
        if not ok:
            result.success = False
            result.message = message
            result.completed = 0
            goal_handle.abort()
            return result

        try:
            if not self.follow_path_client.wait_for_server(timeout_sec=2.0):
                result.success = False
                result.message = 'Private FollowPath action server is unavailable'
                result.completed = 0
                goal_handle.abort()
                return result

            downstream_goal = self._build_follow_path_goal(
                waypoints=goal_handle.request.waypoints,
                zone_names=goal_handle.request.zone_names,
                shelf_checks=goal_handle.request.shelf_checks,
            )

            def _forward_feedback(feedback_msg):
                feedback = getattr(feedback_msg, 'feedback', None)
                if feedback is None or not goal_handle.is_active:
                    return
                proxy_feedback = FollowPathAction.Feedback()
                proxy_feedback.current_index = int(getattr(feedback, 'current_index', 0))
                proxy_feedback.total = int(getattr(feedback, 'total', 0))
                proxy_feedback.status = str(getattr(feedback, 'status', '') or '')
                try:
                    goal_handle.publish_feedback(proxy_feedback)
                except Exception:
                    pass

            send_future = self.follow_path_client.send_goal_async(
                downstream_goal,
                feedback_callback=_forward_feedback,
            )
            downstream_goal_handle = await self._wait_future_with_timeout(send_future, 2.0)
            if downstream_goal_handle is None:
                result.success = False
                result.completed = 0
                result.message = 'Timeout sending FollowPath through arbitrator'
                goal_handle.abort()
                return result
            if not bool(getattr(downstream_goal_handle, 'accepted', False)):
                result.success = False
                result.completed = 0
                result.message = 'Downstream FollowPath request was rejected'
                goal_handle.abort()
                return result

            if not self._set_downstream_goal_handle(token, downstream_goal_handle):
                try:
                    cancel_future = downstream_goal_handle.cancel_goal_async()
                    cancel_response = await self._wait_future_with_timeout(cancel_future, 2.0)
                    outcome, detail = self._classify_cancel_response(cancel_response)
                    if outcome not in ('accepted', 'terminal'):
                        self.get_logger().warn(
                            f'FollowPath downstream orphan cleanup failed for {summary}: {detail}'
                        )
                except Exception:
                    pass
                result.success = False
                result.completed = 0
                result.message = 'Arbitrator request state changed during FollowPath dispatch'
                goal_handle.abort()
                return result

            result_future = downstream_goal_handle.get_result_async()
            cancel_sent = False

            while not result_future.done():
                if goal_handle.is_cancel_requested and not cancel_sent:
                    cancel_sent = True
                    cancel_future = downstream_goal_handle.cancel_goal_async()
                    cancel_response = await self._wait_future_with_timeout(cancel_future, 2.0)
                    outcome, detail = self._classify_cancel_response(cancel_response)
                    if outcome not in ('accepted', 'terminal'):
                        self.get_logger().warn(
                            f'FollowPath cancel forwarding failed for {summary}: {detail}'
                        )
                await self._non_blocking_wait(0.05)

            wrapped = result_future.result()
            downstream_result = getattr(wrapped, 'result', None)
            status = int(getattr(wrapped, 'status', GoalStatus.STATUS_UNKNOWN))

            result.success = bool(getattr(downstream_result, 'success', False))
            result.message = str(getattr(downstream_result, 'message', '') or '').strip()
            result.completed = int(getattr(downstream_result, 'completed', 0) or 0)

            if status == GoalStatus.STATUS_CANCELED:
                if not result.message:
                    result.message = 'FollowPath canceled by arbitrator'
                goal_handle.canceled()
                result.success = False
            elif status == GoalStatus.STATUS_SUCCEEDED and bool(result.success):
                goal_handle.succeed()
            else:
                if not result.message:
                    result.message = (
                        f'Downstream FollowPath finished with status {status}'
                    )
                result.success = False
                goal_handle.abort()
            return result
        finally:
            self._clear_request(token)


def main(args=None):
    rclpy.init(args=args)
    node = NavigationArbitrator()
    executor = MultiThreadedExecutor(num_threads=4)
    executor.add_node(node)

    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            executor.shutdown()
        except Exception:
            pass
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
