import json
from copy import deepcopy
from unittest.mock import MagicMock, call

from pycti.connector.opencti_connector_helper import ListenQueue, OpenCTIConnectorHelper


class _NoopLogger:
    def debug(self, *_args, **_kwargs):
        pass

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


def _opencti_entity(entity_id, entity_type="Indicator"):
    return {
        "id": f"internal--{entity_id}",
        "standard_id": entity_id,
        "entity_type": entity_type,
        "parent_types": [entity_type],
    }


class _FakeApi:
    def __init__(self):
        self.work = _FakeWork()
        self.stix2 = MagicMock()
        self._entities = {
            ("Indicator", "indicator--1"): _opencti_entity("indicator--1"),
            ("Indicator", "indicator--2"): _opencti_entity("indicator--2"),
            ("Domain-Name", "domain-name--1"): _opencti_entity(
                "domain-name--1", "Domain-Name"
            ),
            ("Domain-Name", "domain-name--2"): _opencti_entity(
                "domain-name--2", "Domain-Name"
            ),
        }
        self._readers = {}
        self._listers = {}
        self.stix2.get_reader.side_effect = self._get_reader
        self.stix2.get_lister.side_effect = self._get_lister
        self.stix2.prepare_id_filters_export.side_effect = lambda entity_ids: {
            "ids": list(entity_ids)
        }
        self.stix2.prepare_simple_exports.side_effect = self._prepare_simple_exports
        self.submitted_received = []
        self.submitted_failures = []
        self.submitted_results = []

    def _get_reader(self, entity_type):
        if entity_type not in self._readers:
            self._readers[entity_type] = MagicMock(
                side_effect=lambda id, resolved_type=entity_type, **_kwargs: deepcopy(
                    self._entities.get((resolved_type, id))
                )
            )
        return self._readers[entity_type]

    def _get_lister(self, entity_type):
        if entity_type not in self._listers:
            self._listers[entity_type] = MagicMock(
                side_effect=lambda filters, first, getAll, resolved_type=entity_type, **_kwargs: [
                    deepcopy(self._entities[(resolved_type, entity_id)])
                    for entity_id in filters["ids"]
                    if (resolved_type, entity_id) in self._entities
                ]
            )
        return self._listers[entity_type]

    @staticmethod
    def _prepare_simple_exports(entities):
        return [
            [{"id": entity["standard_id"], "type": entity["entity_type"].lower()}]
            for entity in entities
        ]

    def set_draft_id(self, _draft_id):
        pass

    def set_applicant_id_header(self, _applicant_id):
        pass

    def submit_enrichment_batch_result(self, connector_id, envelope, result):
        self.submitted_results.append((connector_id, envelope, result))
        return True

    def submit_enrichment_batch_received(self, connector_id, envelope):
        self.submitted_received.append((connector_id, envelope))
        return True

    def submit_enrichment_batch_failure(self, connector_id, envelope, message):
        self.submitted_failures.append((connector_id, envelope, message))
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
    helper.connect_enrichment_entity_with_files = True
    helper.connect_enrichment_entity_with_indicators = True
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


def _unchanged_result(batch_data):
    return {
        "output_bundle": None,
        "results": [
            {
                "item_id": item["item_id"],
                "work_id": item["work_id"],
                "status": "UNCHANGED",
                "message": None,
                "output_object_ids": [],
            }
            for item in batch_data["items"]
        ],
    }


def _listen_queue(callback):
    listen_queue = object.__new__(ListenQueue)
    listen_queue.helper = _helper()
    listen_queue.callback = MagicMock()
    listen_queue.enrichment_batch_callback = callback
    listen_queue.connector_applicant_id = "connector-applicant"
    return listen_queue


def test_batch_callback_submits_one_received_and_one_terminal_result():
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
    assert listen_queue.helper.api.submitted_received == [
        ("connector--1", _message()["event"]["enrichment_batch"])
    ]
    assert listen_queue.helper.api.work.received_calls == []
    assert listen_queue.helper.api.work.processed_calls == []
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
    assert len(listen_queue.helper.api.submitted_received) == 1
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
    assert listen_queue.helper.api.submitted_received == []
    assert listen_queue.helper.api.submitted_failures == []
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


def test_batch_callback_failure_submits_one_batch_failure():
    listen_queue = _listen_queue(
        lambda _batch_data: (_ for _ in ()).throw(RuntimeError("callback failed"))
    )

    assert listen_queue._data_handler(_message()) is True
    assert len(listen_queue.helper.api.submitted_received) == 1
    assert listen_queue.helper.api.submitted_results == []
    assert listen_queue.helper.api.submitted_failures == [
        ("connector--1", _message()["event"]["enrichment_batch"], "callback failed")
    ]
    assert listen_queue.helper.api.work.processed_calls == []


def test_batch_result_submission_failure_requeues_without_marking_failed():
    listen_queue = _listen_queue(_unchanged_result)
    listen_queue.helper.api.submit_enrichment_batch_result = MagicMock(
        return_value=False
    )

    assert listen_queue._data_handler(_message()) is False
    assert len(listen_queue.helper.api.submitted_received) == 1
    assert listen_queue.helper.api.submitted_failures == []
    assert listen_queue.helper.api.work.processed_calls == []


def test_batch_received_submission_failure_requeues_without_marking_failed():
    listen_queue = _listen_queue(_unchanged_result)
    listen_queue.helper.api.submit_enrichment_batch_received = MagicMock(
        return_value=False
    )

    assert listen_queue._data_handler(_message()) is False
    assert listen_queue.helper.api.submitted_results == []
    assert listen_queue.helper.api.submitted_failures == []
    assert listen_queue.helper.api.work.processed_calls == []


def test_batch_callback_prefetches_same_type_entities_and_simple_exports():
    captured_items = []
    listen_queue = _listen_queue(
        lambda batch_data: captured_items.extend(batch_data["items"])
        or _unchanged_result(batch_data)
    )

    assert listen_queue._data_handler(_message()) is True

    indicator_lister = listen_queue.helper.api._listers["Indicator"]
    indicator_lister.assert_called_once_with(
        filters={"ids": ["indicator--1", "indicator--2"]},
        first=2,
        getAll=True,
        withFiles=True,
    )
    assert listen_queue.helper.api._readers == {}
    listen_queue.helper.api.stix2.prepare_simple_exports.assert_called_once()
    assert [item["stix_entity"]["id"] for item in captured_items] == [
        "indicator--1",
        "indicator--2",
    ]


def test_batch_callback_omits_file_projection_when_connector_opts_out():
    listen_queue = _listen_queue(_unchanged_result)
    listen_queue.helper.connect_enrichment_entity_with_files = False
    listen_queue.helper.connect_enrichment_entity_with_indicators = False

    assert listen_queue._data_handler(_message()) is True

    listen_queue.helper.api._listers["Indicator"].assert_called_once_with(
        filters={"ids": ["indicator--1", "indicator--2"]},
        first=2,
        getAll=True,
        withFiles=False,
    )


def test_batch_callback_omits_indicator_projection_for_observables_when_connector_opts_out():
    message = _message()
    envelope = json.loads(message["event"]["enrichment_batch"])
    envelope["items"] = [
        {
            "item_id": "item--1",
            "work_id": "work--1",
            "entity_id": "domain-name--1",
            "entity_type": "Domain-Name",
            "payload_fingerprint": "payload--1",
            "stix_entity": None,
            "stix_objects": None,
        },
        {
            "item_id": "item--2",
            "work_id": "work--2",
            "entity_id": "domain-name--2",
            "entity_type": "Domain-Name",
            "payload_fingerprint": "payload--2",
            "stix_entity": None,
            "stix_objects": None,
        },
    ]
    envelope["item_count"] = 2
    message["event"]["enrichment_batch"] = json.dumps(envelope)
    listen_queue = _listen_queue(_unchanged_result)
    listen_queue.helper.connect_enrichment_entity_with_indicators = False

    assert listen_queue._data_handler(message) is True

    listen_queue.helper.api._listers["Domain-Name"].assert_called_once_with(
        filters={"ids": ["domain-name--1", "domain-name--2"]},
        first=2,
        getAll=True,
        withFiles=True,
        withIndicators=False,
    )


def test_batch_callback_reuses_duplicate_entity_lookup_without_sharing_items():
    message = _message()
    envelope = json.loads(message["event"]["enrichment_batch"])
    envelope["items"][1]["entity_id"] = "indicator--1"
    message["event"]["enrichment_batch"] = json.dumps(envelope)

    def callback(batch_data):
        first_item, second_item = batch_data["items"]
        first_item["enrichment_entity"]["mutated"] = True
        first_item["stix_objects"][0]["mutated"] = True
        assert "mutated" not in second_item["enrichment_entity"]
        assert "mutated" not in second_item["stix_objects"][0]
        return _unchanged_result(batch_data)

    listen_queue = _listen_queue(callback)

    assert listen_queue._data_handler(message) is True

    indicator_reader = listen_queue.helper.api._readers["Indicator"]
    indicator_reader.assert_called_once_with(id="indicator--1", withFiles=True)
    listen_queue.helper.api.stix2.prepare_simple_exports.assert_called_once()
    prepared_entities = (
        listen_queue.helper.api.stix2.prepare_simple_exports.call_args.args[0]
    )
    assert [entity["standard_id"] for entity in prepared_entities] == ["indicator--1"]


def test_batch_callback_groups_supported_types_and_falls_back_for_unlisted_types():
    message = _message()
    envelope = json.loads(message["event"]["enrichment_batch"])
    envelope["items"].append(
        {
            "item_id": "item--3",
            "work_id": "work--3",
            "entity_id": "report--1",
            "entity_type": "Report",
            "payload_fingerprint": "payload--3",
            "stix_entity": None,
            "stix_objects": None,
        }
    )
    envelope["item_count"] = 3
    message["event"]["enrichment_batch"] = json.dumps(envelope)

    listen_queue = _listen_queue(_unchanged_result)
    listen_queue.helper.api._entities[("Report", "report--1")] = _opencti_entity(
        "report--1", "Report"
    )
    original_get_lister = listen_queue.helper.api.stix2.get_lister.side_effect
    listen_queue.helper.api.stix2.get_lister.side_effect = lambda entity_type: (
        None if entity_type == "Report" else original_get_lister(entity_type)
    )

    assert listen_queue._data_handler(message) is True

    listen_queue.helper.api._listers["Indicator"].assert_called_once()
    listen_queue.helper.api._readers["Report"].assert_called_once_with(
        id="report--1", withFiles=True
    )


def test_batch_callback_falls_back_to_reads_when_group_listing_fails():
    listen_queue = _listen_queue(_unchanged_result)
    indicator_lister = listen_queue.helper.api._get_lister("Indicator")
    indicator_lister.side_effect = RuntimeError("temporary list failure")

    assert listen_queue._data_handler(_message()) is True

    indicator_reader = listen_queue.helper.api._readers["Indicator"]
    assert indicator_reader.call_args_list == [
        call(id="indicator--1", withFiles=True),
        call(id="indicator--2", withFiles=True),
    ]


def test_batch_callback_keeps_provided_stix_payloads_without_exporting():
    message = _message()
    envelope = json.loads(message["event"]["enrichment_batch"])
    for item in envelope["items"]:
        item["stix_entity"] = json.dumps({"id": item["entity_id"], "type": "indicator"})
        item["stix_objects"] = json.dumps(
            {
                "type": "bundle",
                "objects": [{"id": item["entity_id"], "type": "indicator"}],
            }
        )
    envelope["group_context"]["resolution"] = "stix_bundle"
    envelope["object_count"] = 2
    message["event"]["enrichment_batch"] = json.dumps(envelope)

    captured_items = []
    listen_queue = _listen_queue(
        lambda batch_data: captured_items.extend(batch_data["items"])
        or _unchanged_result(batch_data)
    )

    assert listen_queue._data_handler(message) is True
    listen_queue.helper.api.stix2.prepare_simple_exports.assert_not_called()
    assert [item["stix_entity"]["id"] for item in captured_items] == [
        "indicator--1",
        "indicator--2",
    ]
