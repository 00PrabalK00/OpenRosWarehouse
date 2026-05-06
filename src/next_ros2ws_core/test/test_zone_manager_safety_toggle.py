import asyncio
import sys
from pathlib import Path
import pytest


MODULE_DIR = Path(__file__).resolve().parents[1]
if str(MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(MODULE_DIR))

try:
    from src.zone_manager import ZoneManager  # noqa: E402
except ImportError as exc:  # pragma: no cover - depends on generated ROS interfaces in env
    pytest.skip(f'zone_manager import unavailable: {exc}', allow_module_level=True)


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(('info', message))

    def warn(self, message):
        self.messages.append(('warn', message))

    def error(self, message):
        self.messages.append(('error', message))


class _Future:
    def __init__(self, result):
        self._result = result

    def done(self):
        return True

    def result(self):
        return self._result


class _Client:
    def __init__(self, result, available=True):
        self._result = result
        self.available = available
        self.requests = []

    def wait_for_service(self, timeout_sec=0.0):
        return self.available

    def call_async(self, req):
        self.requests.append(req)
        return _Future(self._result)


def _make_result(successful=True, reason=''):
    return type(
        'SetParametersResponse',
        (),
        {
            'results': [
                type(
                    'ParamResult',
                    (),
                    {
                        'successful': successful,
                        'reason': reason,
                    },
                )()
            ]
        },
    )()


def _make_zone_manager(client):
    manager = ZoneManager.__new__(ZoneManager)
    manager.safety_set_params_client = client
    manager.safety_set_params_service = '/safety_controller/set_parameters'
    manager.get_logger = lambda: _Logger()

    async def _noop_wait(_duration):
        return None

    manager._non_blocking_wait = _noop_wait
    return manager


def test_set_safety_distance_enabled_sends_bool_parameter():
    client = _Client(_make_result(successful=True))
    manager = _make_zone_manager(client)

    ok, message = asyncio.run(
        manager._set_safety_distance_enabled(False, 'test insertion')
    )

    assert ok is True
    assert message == 'distance safety disabled'
    assert len(client.requests) == 1
    request = client.requests[0]
    assert len(request.parameters) == 1
    assert request.parameters[0].name == 'distance_safety_enabled'
    assert request.parameters[0].value.bool_value is False


def test_set_safety_distance_enabled_surfaces_rejection():
    client = _Client(_make_result(successful=False, reason='nope'))
    manager = _make_zone_manager(client)

    ok, message = asyncio.run(
        manager._set_safety_distance_enabled(True, 'cleanup')
    )

    assert ok is False
    assert 'rejected: nope' in message
