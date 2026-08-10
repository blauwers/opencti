import json
from unittest.mock import MagicMock

from pycti.connector.opencti_connector_helper import ListenQueue, OpenCTIConnectorHelper


class _NoopLogger:
    def error(self, *_args, **_kwargs):
        pass


class _NoopMetric:
    def inc(self, *_args, **_kwargs):
        pass


class _FakeWork:
    def __init__(self):
        self.received_calls = []
        self.processed_calls = []

    def to_received(self, *args):
        self.received_calls.append(args)

    def to_processed(self, *args):
        self.processed_calls.append(args)


class _FakeApi:
    def __init__(self):
        self.work = _FakeWork()
        self.stix2 = MagicMock()
        self.stix2.get_reader.return_value = MagicMock(
            side_effect=lambda id, withFiles: {
                "standard_id": id,
                "entity_type": "Indicator",
            }
        )
        self.stix2.generate_export.side_effect = lambda entity: entity
        self.stix2.prepare_export.side_effect = lambda entity: [
            {"id": entity["standard_id"], "type": "indicator"}
        ]
        self.submitted_results = []

    def set_draft_id(self, _draft_id):
        pass

    def set_applicant_id_header(self, _applicant_id):
        pass

    def submit_enrichment_batch_result(self, connector_id, envelope, result):
        self.submitted_results.append((connector_id, envelope, result))
        return True


def _helper():
    helper = object.__new__(OpenCTIConnectorHelper)
    helper.work_id = None
    helper.validation_mode = "draft"
    helper.force_validation = False
    helper.draft_id = None
    helper.playbook = None
    helper.enrichment_shared_organizations = None
    helper.applicant_id = "connector-applicant"
    helper.connect_type = "INTERNAL_ENRICHMENT"
    helper.connector_id = "connector--1"
    helper.api = _FakeApi()
    helper.api_impersonate = _FakeApi()
    helper.metric = _NoopMetric()
    helper.connector_logger = _NoopLogger()
    helper._apply_enrichment_shared_organizations = lambda bundle: bundle
    return helper


def _message():
    envelope = {
        "protocol_version": 1,
        "batch_id": "enrichment-batch--1",
        "item_count": 2,
        "object_count": 0,
        "group_context": {
            "connector_id": "connector--1",
            "applicant_id": None,
            "draft_id": None,
            "mode": "auto",
            "trigger": "create",
            "resolution": "deferred",
            "playbook_context": None,
            "configuration": None,
            "shared_organization_ids": ["organization--1"],
            "context_fingerprint": "fingerprint",
        },
        "items": [
            {
                "item_id": "item--1",
                "work_id": "work--1",
                "entity_id": "indicator--1",
                "entity_type": "Indicator",
                "payload_fingerprint": "payload--1",
                "stix_entity": None,
                "stix_objects": None,
            },
            {
                "item_id": "item--2",
                "work_id": "work--2",
                "entity_id": "indicator--2",
                "entity_type": "Indicator",
                "payload_fingerprint": "payload--2",
                "stix_entity": None,
                "stix_objects": None,
            },
        ],
    }
    return {
        "event": {"enrichment_batch": json.dumps(envelope)},
        "internal": {"work_id": None, "draft_id": None, "applicant_id": None},
    }


def _listen_queue(callback):
    listen_queue = object.__new__(ListenQueue)
    listen_queue.helper = _helper()
    listen_queue.callback = MagicMock()
    listen_queue.enrichment_batch_callback = callback
    listen_queue.connector_applicant_id = "connector-applicant"
    return listen_queue


def test_batch_callback_submits_one_result_and_settles_each_work():
    output_bundle = json.dumps(
        {"type": "bundle", "objects": [{"id": "indicator--result-1"}]}
    )

    def callback(batch_data):
        assert batch_data["items"][0]["enrichment_entity"]["standard_id"] == (
            "indicator--1"
        )
        return {
            "output_bundle": output_bundle,
            "results": [
                {
                    "item_id": "item--1",
                    "work_id": "work--1",
                    "status": "PROCESSED",
                    "message": "updated",
                    "output_object_ids": ["indicator--result-1"],
                },
                {
                    "item_id": "item--2",
                    "work_id": "work--2",
                    "status": "UNCHANGED",
                    "message": None,
                    "output_object_ids": [],
                },
            ],
        }

    listen_queue = _listen_queue(callback)

    assert listen_queue._data_handler(_message()) is True
    assert listen_queue.helper.api.work.received_calls == [
        ("work--1", "Connector ready to process the operation"),
        ("work--2", "Connector ready to process the operation"),
    ]
    assert listen_queue.helper.api.work.processed_calls == [
        ("work--1", "updated", False),
        ("work--2", "No changes produced by connector", False),
    ]
    assert len(listen_queue.helper.api.submitted_results) == 1
    _, _, serialized_result = listen_queue.helper.api.submitted_results[0]
    assert json.loads(serialized_result)["output_object_count"] == 1


def test_batch_callback_requeues_whole_envelope_for_retryable_item():
    listen_queue = _listen_queue(
        lambda _batch_data: {
            "output_bundle": None,
            "results": [
                {
                    "item_id": "item--1",
                    "work_id": "work--1",
                    "status": "RETRYABLE",
                    "message": "upstream timeout",
                    "output_object_ids": [],
                },
                {
                    "item_id": "item--2",
                    "work_id": "work--2",
                    "status": "UNCHANGED",
                    "message": None,
                    "output_object_ids": [],
                },
            ],
        }
    )

    assert listen_queue._data_handler(_message()) is False
    assert listen_queue.helper.api.submitted_results == []
    assert listen_queue.helper.api.work.processed_calls == []


def test_batch_callback_rejects_misrouted_connector_envelope_before_callback():
    callback = MagicMock()
    listen_queue = _listen_queue(callback)
    message = _message()
    envelope = json.loads(message["event"]["enrichment_batch"])
    envelope["group_context"]["connector_id"] = "connector--other"
    message["event"]["enrichment_batch"] = json.dumps(envelope)

    assert listen_queue._data_handler(message) is True
    callback.assert_not_called()
    assert listen_queue.helper.api.submitted_results == []
    assert listen_queue.helper.api.work.received_calls == []
    assert listen_queue.helper.api.work.processed_calls == [
        (
            "work--1",
            "Enrichment batch envelope does not belong to this connector",
            True,
        ),
        (
            "work--2",
            "Enrichment batch envelope does not belong to this connector",
            True,
        ),
    ]
