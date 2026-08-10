import asyncio
import threading

from pycti.connector.opencti_connector_helper import ListenQueue


class _NoopLogger:
    def error(self, *_args, **_kwargs):
        pass


class _Helper:
    def __init__(self):
        self.connector_logger = _NoopLogger()


class _Request:
    headers = {"Authorization": "Bearer test"}

    def __init__(self, marker):
        self.marker = marker

    async def json(self):
        return {"marker": self.marker}


def _listener(callback, worker_count=1):
    listener = object.__new__(ListenQueue)
    listener.helper = _Helper()
    listener.is_token_valid = lambda _token: True
    listener._data_handler = callback
    listener.listen_worker_count = worker_count
    listener._worker_pool = None
    return listener


def test_http_listener_runs_blocking_callbacks_off_the_event_loop():
    first_started = threading.Event()
    second_started = threading.Event()
    first_observed_overlap = []

    def callback(data):
        if data["marker"] == "a":
            first_started.set()
            first_observed_overlap.append(second_started.wait(timeout=0.5))
        else:
            first_started.wait(timeout=0.5)
            second_started.set()
        return True

    listener = _listener(callback, worker_count=2)

    async def run_requests():
        return await asyncio.gather(
            listener._http_process_callback(_Request("a")),
            listener._http_process_callback(_Request("b")),
        )

    try:
        responses = asyncio.run(run_requests())

        assert first_observed_overlap == [True]
        assert [response.status_code for response in responses] == [202, 202]
    finally:
        listener._shutdown_worker_pool()


def test_http_listener_returns_500_without_durable_outcome():
    listener = _listener(lambda _data: False)

    try:
        response = asyncio.run(listener._http_process_callback(_Request("a")))

        assert response.status_code == 500
    finally:
        listener._shutdown_worker_pool()
