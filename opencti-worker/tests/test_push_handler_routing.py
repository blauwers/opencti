import base64
import json
from unittest.mock import MagicMock

import pytest
from pycti.api.opencti_api_batch import BatchMutationPlanTooLarge
from requests import ConnectionError

from src import push_handler
from src.push_handler import (
    BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
    BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
    BATCH_DELIVERY_KIND_CHILD,
    BATCH_DELIVERY_KIND_ROOT,
    BATCH_DELIVERY_PROTOCOL_V2,
    PushHandler,
    batch_replay_count,
    build_child_delivery_id,
    build_batch_expectation_error,
    build_root_delivery_id,
    is_batch_payload_too_large_error,
    parse_batch_delivery_envelope,
    should_add_legacy_default_split_expectations,
    should_dead_letter_rejected_item,
    should_replay_intact_bundle,
    should_replay_rejected_item,
    should_report_batch_expectation,
    should_split_bundles,
)


def build_handler():
    handler = PushHandler.__new__(PushHandler)
    handler.api = MagicMock()
    handler.api.work.add_expectations.return_value = True
    handler.api.stix2.import_bundle_from_json.return_value = ([], [])
    handler.api.stix2.import_bundle_from_json_batch.return_value = ([], [])
    handler.logger = MagicMock()
    handler.connector_id = "connector--1"
    handler.push_exchange = "push-exchange"
    handler.listen_exchange = "listen-exchange"
    handler.push_routing = "push-routing"
    handler.dead_letter_routing = "dead-letter-routing"
    handler.pika_parameters = MagicMock()
    handler.bundles_global_counter = MagicMock()
    handler.bundles_processing_time_gauge = MagicMock()
    handler.objects_max_refs = 0
    return handler


def build_message(**overrides):
    content = {
        "type": "bundle",
        "id": "bundle--11111111-1111-4111-8111-111111111111",
        "objects": [{"id": "indicator--1"}, {"id": "indicator--2"}],
    }
    message = {
        "type": "bundle",
        "content": base64.b64encode(json.dumps(content).encode("utf-8")).decode(
            "utf-8"
        ),
    }
    message.update(overrides)
    return json.dumps(message)


def build_v2_message(**overrides):
    submission_id = "batch-submission--1"
    message = {
        "submission_id": submission_id,
        "delivery_id": build_root_delivery_id(submission_id),
        "parent_delivery_id": None,
        "delivery_kind": BATCH_DELIVERY_KIND_ROOT,
        "delivery_protocol_version": BATCH_DELIVERY_PROTOCOL_V2,
        "delivery_branch_kind": "ROOT",
        "delivery_branch_sequence": 0,
        "delivery_branch_ordinal": 0,
    }
    message.update(overrides)
    return build_message(**message)


def test_handler_passes_request_timeouts_to_api_client(monkeypatch):
    client = MagicMock()
    client_kwargs = {}

    def build_client(**kwargs):
        client_kwargs.update(kwargs)
        return client

    monkeypatch.setattr(push_handler, "OpenCTIApiClient", build_client)

    PushHandler(
        logger=MagicMock(),
        log_level="info",
        json_logging=True,
        opencti_url="http://localhost:4000",
        opencti_token="test-token",
        ssl_verify=False,
        connector_id="connector--1",
        push_exchange="push-exchange",
        listen_exchange="listen-exchange",
        push_routing="push-routing",
        dead_letter_routing="dead-letter-routing",
        pika_parameters=MagicMock(),
        bundles_global_counter=MagicMock(),
        bundles_processing_time_gauge=MagicMock(),
        objects_max_refs=0,
        requests_timeout=321,
        batch_requests_timeout=4200,
        batch_requests_max_payload_size=123456,
        custom_headers="x-test:value",
    )

    assert client_kwargs["requests_timeout"] == 321
    assert client_kwargs["batch_requests_timeout"] == 4200
    assert client_kwargs["batch_requests_max_payload_size"] == 123456
    assert client_kwargs["custom_headers"] == "x-test:value"


def test_bundle_transport_is_unsplit_by_default():
    content = {"objects": [{"id": "indicator--1"}, {"id": "indicator--2"}]}

    assert should_split_bundles({}, content) is False
    assert should_split_bundles({"no_split": False}, content) is False
    assert should_split_bundles({"no_split": True}, content) is False


def test_worker_parses_v1_and_v2_delivery_envelopes_without_reinterpreting_v1():
    assert (
        parse_batch_delivery_envelope({"submission_id": "batch-submission--1"}) is None
    )

    envelope = parse_batch_delivery_envelope(json.loads(build_v2_message()))
    assert envelope is not None
    assert envelope.delivery_id == build_root_delivery_id("batch-submission--1")
    assert envelope.delivery_kind == BATCH_DELIVERY_KIND_ROOT
    assert envelope.parent_delivery_id is None


def test_worker_rejects_v2_delivery_ids_that_do_not_match_lineage():
    with pytest.raises(ValueError, match="Invalid root batch delivery envelope"):
        parse_batch_delivery_envelope(
            json.loads(build_v2_message(delivery_id="batch-delivery--wrong"))
        )

    root_id = build_root_delivery_id("batch-submission--1")
    with pytest.raises(ValueError, match="Invalid child batch delivery envelope"):
        parse_batch_delivery_envelope(
            json.loads(
                build_v2_message(
                    delivery_id="batch-delivery--wrong",
                    parent_delivery_id=root_id,
                    delivery_kind=BATCH_DELIVERY_KIND_CHILD,
                    delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                    delivery_branch_sequence=0,
                    delivery_branch_ordinal=0,
                )
            )
        )


def test_split_bundles_requires_positive_opt_in():
    content = {"objects": [{"id": "indicator--1"}, {"id": "indicator--2"}]}

    assert should_split_bundles({"split_bundles": True}, content) is True
    assert should_split_bundles({"split_bundles": "true"}, content) is False
    assert (
        should_split_bundles({"split_bundles": True, "no_split": True}, content) is True
    )


def test_single_object_bundle_never_splits():
    content = {"objects": [{"id": "indicator--1"}]}

    assert should_split_bundles({"split_bundles": True}, content) is False


def test_old_multi_object_messages_get_expectations_without_splitting():
    content = {"objects": [{"id": "indicator--1"}, {"id": "indicator--2"}]}

    assert should_add_legacy_default_split_expectations({}, content) is True
    assert (
        should_add_legacy_default_split_expectations({"no_split": False}, content)
        is True
    )
    assert (
        should_add_legacy_default_split_expectations({"no_split": True}, content)
        is False
    )
    assert (
        should_add_legacy_default_split_expectations({"split_bundles": False}, content)
        is False
    )


def test_new_unsplit_messages_report_one_batch_expectation():
    assert should_report_batch_expectation({"split_bundles": False}) is True
    assert should_report_batch_expectation({"split_bundles": True}) is False
    assert should_report_batch_expectation({}) is False
    assert build_batch_expectation_error({"id": "bundle--1"}, []) is None
    assert build_batch_expectation_error(
        {"id": "bundle--1"}, [{"id": "indicator--1"}]
    ) == {
        "error": "1 element(s) failed during batch import",
        "source": "Bundle bundle--1",
    }
    assert should_dead_letter_rejected_item({"id": "indicator--1"}) is False
    assert (
        should_dead_letter_rejected_item(
            {"id": "indicator--1", "rejection_info": {"reject_reason": "MAX_RETRY"}}
        )
        is True
    )
    retryable_item = {
        "id": "relationship--1",
        "rejection_info": {
            "reject_reason": "MISSING_REFERENCE",
            "retryable": True,
        },
    }
    assert should_replay_rejected_item(retryable_item) is True
    assert should_dead_letter_rejected_item(retryable_item) is False
    retryable_lock_item = {
        "id": "relationship--2",
        "rejection_info": {
            "reject_reason": "LOCK_ERROR",
            "retryable": True,
        },
    }
    assert should_replay_rejected_item(retryable_lock_item) is True
    assert should_dead_letter_rejected_item(retryable_lock_item) is False
    assert batch_replay_count({"batch_replay_count": 2}) == 2
    assert batch_replay_count({"batch_replay_count": "2"}) == 0
    assert should_replay_intact_bundle({}, [retryable_item]) is True
    assert (
        should_replay_intact_bundle({"batch_replay_count": 4}, [retryable_item])
        is False
    )


def test_handler_imports_default_multi_object_bundle_without_requeue(monkeypatch):
    handler = build_handler()

    def fail_legacy_split(*args, **kwargs):
        raise AssertionError("default worker route must not split bundles")

    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        fail_legacy_split,
    )

    result = handler.handle_message(build_message(split_bundles=False))

    assert result == "ack"
    handler.api.stix2.import_bundle_from_json.assert_not_called()
    handler.api.stix2.import_bundle_from_json_batch.assert_called_once()
    imported_raw_content = (
        handler.api.stix2.import_bundle_from_json_batch.call_args.args[0]
    )
    assert json.loads(imported_raw_content)["id"] == (
        "bundle--11111111-1111-4111-8111-111111111111"
    )
    assert len(json.loads(imported_raw_content)["objects"]) == 2


def test_handler_processes_v2_root_messages_on_the_existing_unsplit_route():
    handler = build_handler()

    result = handler.handle_message(build_v2_message(split_bundles=False))

    assert result == "ack"
    handler.api.stix2.import_bundle_from_json.assert_not_called()
    handler.api.stix2.import_bundle_from_json_batch.assert_called_once()


def test_handler_reports_new_unsplit_bundle_once_at_batch_boundary():
    handler = build_handler()

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            batch_wait_until="COMMITTED",
        )
    )

    assert result == "ack"
    handler.api.set_batch_wait_until.assert_called_once_with("COMMITTED")
    handler.api.stix2.import_bundle_from_json.assert_not_called()
    handler.api.stix2.import_bundle_from_json_batch.assert_called_once()
    assert (
        handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs[
            "report_expectations"
        ]
        is False
    )
    assert (
        handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs["wait_until"]
        == "COMMITTED"
    )
    assert (
        handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs[
            "split_oversized_batch_plan"
        ]
        is False
    )
    handler.api.work.report_expectation.assert_called_once_with("work--1", None)


def test_batch_payload_too_large_detection_handles_typed_and_backend_errors():
    assert is_batch_payload_too_large_error(BatchMutationPlanTooLarge(2, 1)) is True
    assert (
        is_batch_payload_too_large_error(ValueError("request entity too large")) is True
    )
    assert is_batch_payload_too_large_error(ValueError("unrelated")) is False


def test_handler_requeues_durable_batch_chunks_for_oversized_batch_plan(monkeypatch):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = (
        BatchMutationPlanTooLarge(200, 100)
    )
    batch_chunks = [
        (
            {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--1"}]},
            {"version": 1, "ordered_object_ids": ["indicator--1"]},
        ),
        (
            {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--2"}]},
            {"version": 1, "ordered_object_ids": ["indicator--2"]},
        ),
    ]
    monkeypatch.setattr(
        push_handler.OpenCTIStix2,
        "build_oversized_batch_plan_chunks",
        lambda *args, **kwargs: batch_chunks,
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            batch_plan={"version": 1},
        )
    )

    assert result == "ack"
    handler.api.work.add_expectations.assert_called_once_with("work--1", 2)
    handler.api.work.report_expectation.assert_called_once_with("work--1", None)
    assert handler.send_bundle_to_specific_queue.call_count == 2
    first_chunk_data = handler.send_bundle_to_specific_queue.call_args_list[0].args[3]
    second_chunk_data = handler.send_bundle_to_specific_queue.call_args_list[1].args[3]
    assert first_chunk_data["split_bundles"] is False
    assert first_chunk_data["no_split"] is True
    assert first_chunk_data["batch_plan"] == batch_chunks[0][1]
    assert second_chunk_data["batch_plan"] == batch_chunks[1][1]


def test_handler_requeues_transient_api_connection_failures(monkeypatch):
    handler = build_handler()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = ConnectionError(
        "remote disconnected"
    )
    sleep = MagicMock()
    monkeypatch.setattr(push_handler.time, "sleep", sleep)
    monkeypatch.setattr(push_handler.random, "uniform", lambda *_: 12.34)

    result = handler.handle_message(build_message(split_bundles=False))

    assert result == "requeue"
    sleep.assert_called_once_with(12.34)
    handler.logger.error.assert_called_once_with(
        "Error executing data handling, a connection error or timeout occurred"
    )


def test_handler_republishes_intact_bundle_for_retryable_batch_failures(monkeypatch):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [],
        [
            {
                "id": "relationship--1",
                "rejection_info": {
                    "reject_reason": "MISSING_REFERENCE",
                    "retryable": True,
                },
            }
        ],
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_message(split_bundles=False, work_id="work--1")
    )

    assert result == "ack"
    handler.api.work.report_expectation.assert_not_called()
    handler.send_bundle_to_specific_queue.assert_called_once()
    replay_call = handler.send_bundle_to_specific_queue.call_args.args
    assert replay_call[0] is channel
    assert replay_call[1] == "push-exchange"
    assert replay_call[2] == "push-routing"
    assert replay_call[3]["batch_replay_count"] == 1
    assert replay_call[4]["id"] == "bundle--11111111-1111-4111-8111-111111111111"
    handler.api.set_retry_number.assert_any_call(None)


def test_handler_republishes_v2_intact_replays_with_deterministic_child_ids(
    monkeypatch,
):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [],
        [
            {
                "id": "relationship--1",
                "rejection_info": {
                    "reject_reason": "MISSING_REFERENCE",
                    "retryable": True,
                },
            }
        ],
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(build_v2_message(split_bundles=False))

    assert result == "ack"
    replay_data = handler.send_bundle_to_specific_queue.call_args.args[3]
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    assert replay_data["delivery_id"] == build_child_delivery_id(
        root_delivery_id,
        BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
        1,
        0,
    )
    assert replay_data["parent_delivery_id"] == root_delivery_id
    assert replay_data["delivery_kind"] == BATCH_DELIVERY_KIND_CHILD


def test_handler_reports_retryable_batch_failure_after_replay_budget(monkeypatch):
    handler = build_handler()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [],
        [
            {
                "id": "relationship--1",
                "rejection_info": {
                    "reject_reason": "MISSING_REFERENCE",
                    "retryable": True,
                },
            }
        ],
    )

    def fail_republish(*args, **kwargs):
        raise AssertionError("exhausted retries must not republish the bundle")

    monkeypatch.setattr(push_handler.pika, "BlockingConnection", fail_republish)

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            batch_replay_count=4,
        )
    )

    assert result == "ack"
    handler.api.work.report_expectation.assert_called_once_with(
        "work--1",
        {
            "error": "1 element(s) failed during batch import",
            "source": "Bundle bundle--11111111-1111-4111-8111-111111111111",
        },
    )
    handler.api.set_retry_number.assert_any_call(4)


def test_handler_dead_letters_nonretryable_batch_failures(monkeypatch):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [{"id": "indicator--2", "type": "indicator"}],
        [
            {
                "id": "indicator--1",
                "rejection_info": {
                    "reject_reason": "FUNCTIONAL_ERROR",
                    "retryable": False,
                },
            }
        ],
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_message(split_bundles=False, work_id="work--1")
    )

    assert result == "ack"
    handler.api.work.report_expectation.assert_called_once_with(
        "work--1",
        {
            "error": "1 element(s) failed during batch import",
            "source": "Bundle bundle--11111111-1111-4111-8111-111111111111",
        },
    )
    handler.send_bundle_to_specific_queue.assert_called_once()
    dead_letter_call = handler.send_bundle_to_specific_queue.call_args.args
    assert dead_letter_call[0] is channel
    assert dead_letter_call[1] == "listen-exchange"
    assert dead_letter_call[2] == "dead-letter-routing"
    assert dead_letter_call[4]["id"] == "indicator--1"
    assert (
        dead_letter_call[4]["rejection_info"]["original_connector_id"] == "connector--1"
    )


def test_handler_forwards_backend_batch_plan_to_batch_importer():
    handler = build_handler()
    backend_batch_plan = {
        "version": 1,
        "execution_phases": [
            {"phase": 0, "object_ids": ["indicator--1"]},
            {"phase": 1, "object_ids": ["indicator--2"]},
        ],
    }

    result = handler.handle_message(
        build_message(split_bundles=False, batch_plan=backend_batch_plan)
    )

    assert result == "ack"
    assert (
        handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs[
            "backend_batch_plan"
        ]
        == backend_batch_plan
    )
    assert (
        handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs[
            "split_oversized_batch_plan"
        ]
        is False
    )


def test_handler_requeues_child_bundles_only_for_explicit_split(monkeypatch):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()

    split_bundles = [
        {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--1"}]},
        {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--2"}]},
    ]
    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        lambda *args, **kwargs: (2, [], split_bundles),
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_message(split_bundles=True, work_id="work--1")
    )

    assert result == "ack"
    handler.api.stix2.import_bundle_from_json.assert_not_called()
    handler.api.stix2.import_bundle_from_json_batch.assert_not_called()
    handler.api.work.add_expectations.assert_called_once_with("work--1", 2)
    assert handler.send_bundle_to_specific_queue.call_count == 2


def test_handler_requeues_v2_split_children_with_stable_sibling_ids(monkeypatch):
    handler = build_handler()
    handler.send_bundle_to_specific_queue = MagicMock()

    split_bundles = [
        {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--1"}]},
        {"type": "bundle", "id": "bundle--1", "objects": [{"id": "indicator--2"}]},
    ]
    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        lambda *args, **kwargs: (2, [], split_bundles),
    )

    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_v2_message(split_bundles=True, work_id="work--1")
    )

    assert result == "ack"
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    first_child = handler.send_bundle_to_specific_queue.call_args_list[0].args[3]
    second_child = handler.send_bundle_to_specific_queue.call_args_list[1].args[3]
    assert first_child["delivery_id"] == build_child_delivery_id(
        root_delivery_id,
        BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
        0,
        0,
    )
    assert second_child["delivery_id"] == build_child_delivery_id(
        root_delivery_id,
        BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
        0,
        1,
    )
    assert first_child["delivery_id"] != second_child["delivery_id"]
