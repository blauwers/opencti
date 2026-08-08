from unittest.mock import MagicMock

from src import worker as worker_module
from src.worker import BATCH_DELIVERY_PROTOCOL_MAX, Worker


class OneLoopEvent:
    def __init__(self):
        self.stopped = False

    def is_set(self):
        return self.stopped

    def wait(self, _timeout):
        self.stopped = True


def build_worker():
    worker = Worker.__new__(Worker)
    worker.exit_event = OneLoopEvent()
    worker.opencti_pool_size = 1
    worker.opencti_realtime_pool_size = 1
    worker.listen_pool_size = 1
    worker.log_level = "info"
    worker.opencti_json_logging = True
    worker.opencti_url = "http://localhost:4000"
    worker.opencti_token = "test-token"
    worker.opencti_ssl_verify = False
    worker.opencti_api_requests_timeout = 300
    worker.opencti_api_batch_requests_timeout = None
    worker.opencti_api_batch_requests_max_payload_size = None
    worker.opencti_api_custom_headers = None
    worker.objects_max_refs = 0
    worker.worker_id = "worker-1"
    worker.worker_logger = MagicMock()
    worker.consumers = {}
    worker.api = MagicMock()
    return worker


def test_worker_advertises_protocol_capability_before_opening_push_consumers(
    monkeypatch,
):
    worker = build_worker()
    call_order = []
    worker.api.connector.list.side_effect = lambda **kwargs: (
        call_order.append(("list", kwargs["worker_runtime"])),
        [
            {
                "id": "connector--1",
                "connector_priority_group": "DEFAULT",
                "config": {
                    "push": "push-queue",
                    "push_exchange": "push-exchange",
                    "listen_exchange": "listen-exchange",
                    "push_routing": "push-routing",
                    "dead_letter_routing": "dead-letter-routing",
                    "connection": {"use_ssl": False},
                },
            }
        ],
    )[1]
    monkeypatch.setattr(worker, "build_pika_parameters", lambda _config: MagicMock())

    class FakeConsumer:
        def __init__(self, *_args, **_kwargs):
            call_order.append(("consumer", None))

        def is_alive(self):
            return True

    monkeypatch.setattr(worker_module, "MessageQueueConsumer", FakeConsumer)
    monkeypatch.setattr(
        worker_module,
        "PushHandler",
        lambda *_args, **_kwargs: MagicMock(handle_message=MagicMock()),
    )

    worker.start()

    assert call_order[0] == (
        "list",
        {
            "worker_id": "worker-1",
            "batch_delivery_protocol_max": BATCH_DELIVERY_PROTOCOL_MAX,
        },
    )
    assert call_order[1][0] == "consumer"
