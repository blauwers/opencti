import base64
import json
from unittest.mock import MagicMock, call

import pytest
from pycti.api.opencti_api_batch import (
    BatchMutationPlanTooLarge,
    BatchMutationPlanTooManyExecutionGroups,
)
from requests import ConnectionError

from src import push_handler
from src.push_handler import (
    BATCH_DELIVERY_CANDIDATE_ID_KEY,
    BATCH_DELIVERY_CANDIDATE_PAYLOAD_FINGERPRINT_KEY,
    BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
    BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
    BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
    BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
    BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED,
    BATCH_DELIVERY_HANDOFF_NONE,
    BATCH_DELIVERY_KIND_CHILD,
    BATCH_DELIVERY_KIND_ROOT,
    BATCH_DELIVERY_PROTOCOL_V2,
    PushHandler,
    batch_expectation_work_ids,
    batch_replay_count,
    build_batch_expectation_error,
    build_child_delivery_id,
    build_root_delivery_id,
    is_batch_plan_limit_error,
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
    handler.api.batch_delivery_handoff.return_value = {
        "parent_delivery_id": "batch-delivery--unused",
        "handoff_evidence": BATCH_DELIVERY_HANDOFF_NONE,
        "child_set_fingerprint": None,
        "child_count": 0,
        "pending_children": [],
    }
    handler.api.reserve_batch_delivery_children.side_effect = (
        lambda parent_delivery_id, children: build_reserved_handoff(
            parent_delivery_id,
            [child["queue_payload"] for child in children],
        )
    )
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


def build_reserved_handoff(parent_delivery_id, queue_payloads, evidence=None):
    pending_children = []
    for queue_payload in queue_payloads:
        child = json.loads(queue_payload)
        pending_children.append(
            {
                "delivery_id": child["delivery_id"],
                "state": "READY",
                "queue_payload": queue_payload,
            }
        )
    return {
        "parent_delivery_id": parent_delivery_id,
        "handoff_evidence": evidence or BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED,
        "child_set_fingerprint": "fingerprint-1",
        "child_count": len(queue_payloads),
        "children": pending_children,
        "pending_children": pending_children,
    }


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


def build_candidate_message(**overrides):
    message = {
        BATCH_DELIVERY_CANDIDATE_ID_KEY: "batch-delivery-candidate--1",
        BATCH_DELIVERY_CANDIDATE_PAYLOAD_FINGERPRINT_KEY: "a" * 64,
    }
    message.update(overrides)
    return build_message(**message)


def decode_queue_message_content(queue_message):
    return json.loads(base64.b64decode(queue_message["content"]).decode("utf-8"))


def build_enrichment_batch_result(**overrides):
    result = {
        "results": [
            {
                "work_id": "work--1",
                "output_object_ids": ["indicator--1"],
            },
            {
                "work_id": "work--2",
                "output_object_ids": ["indicator--2"],
            },
        ]
    }
    result.update(overrides)
    return json.dumps(result)


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
        batch_requests_max_execution_groups=1024,
        custom_headers="x-test:value",
    )

    assert client_kwargs["requests_timeout"] == 321
    assert client_kwargs["batch_requests_timeout"] == 4200
    assert client_kwargs["batch_requests_max_payload_size"] == 123456
    assert client_kwargs["batch_requests_max_execution_groups"] == 1024
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
    assert batch_expectation_work_ids(
        {"additional_work_ids": ["work--2", "work--1"]}, "work--1"
    ) == ["work--1", "work--2"]
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


def test_v1_routes_do_not_use_durable_child_handoff_api(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        lambda *args, **kwargs: (
            2,
            [],
            [
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--1"}],
                },
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--2"}],
                },
            ],
        ),
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
    handler.api.batch_delivery_handoff.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_not_called()


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


def test_handler_reports_enrichment_batch_result_expectation_for_each_work():
    handler = build_handler()

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            additional_work_ids=["work--2"],
            enrichment_batch_result=build_enrichment_batch_result(),
        )
    )

    assert result == "ack"
    assert handler.api.work.report_expectation.call_args_list == [
        call("work--1", None),
        call("work--2", None),
    ]


def test_handler_reports_enrichment_batch_rejection_only_to_owning_work():
    handler = build_handler()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [{"id": "indicator--2", "type": "indicator"}],
        [
            {
                "id": "indicator--1",
                "rejection_info": {
                    "reject_reason": "MISSING_REFERENCE",
                    "retryable": True,
                },
            }
        ],
    )

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            additional_work_ids=["work--2"],
            enrichment_batch_result=build_enrichment_batch_result(),
            batch_replay_count=4,
        )
    )

    assert result == "ack"
    assert handler.api.work.report_expectation.call_args_list == [
        call(
            "work--1",
            {
                "error": "1 element(s) failed during batch import",
                "source": "Bundle bundle--11111111-1111-4111-8111-111111111111",
            },
        ),
        call("work--2", None),
    ]


def test_handler_reports_shared_enrichment_batch_rejection_to_each_owner():
    handler = build_handler()
    handler.api.stix2.import_bundle_from_json_batch.return_value = (
        [{"id": "indicator--1", "type": "indicator"}],
        [
            {
                "id": "label--shared",
                "rejection_info": {
                    "reject_reason": "MISSING_REFERENCE",
                    "retryable": True,
                },
            }
        ],
    )

    result = handler.handle_message(
        build_message(
            split_bundles=False,
            work_id="work--1",
            additional_work_ids=["work--2"],
            enrichment_batch_result=build_enrichment_batch_result(
                results=[
                    {
                        "work_id": "work--1",
                        "output_object_ids": ["indicator--1", "label--shared"],
                    },
                    {
                        "work_id": "work--2",
                        "output_object_ids": ["indicator--2", "label--shared"],
                    },
                ]
            ),
            batch_replay_count=4,
        )
    )

    expected_error = {
        "error": "1 element(s) failed during batch import",
        "source": "Bundle bundle--11111111-1111-4111-8111-111111111111",
    }
    assert result == "ack"
    assert handler.api.work.report_expectation.call_args_list == [
        call("work--1", expected_error),
        call("work--2", expected_error),
    ]


def test_handler_reports_fallback_child_expectation_for_each_enrichment_work():
    handler = build_handler()
    child_content = {
        "type": "bundle",
        "id": "bundle--child",
        "objects": [{"id": "indicator--1"}],
    }

    result = handler.handle_message(
        build_message(
            content=base64.b64encode(json.dumps(child_content).encode("utf-8")).decode(
                "utf-8"
            ),
            split_bundles=True,
            work_id="work--1",
            additional_work_ids=["work--2"],
            enrichment_batch_result=build_enrichment_batch_result(),
        )
    )

    assert result == "ack"
    assert (
        handler.api.stix2.import_bundle_from_json.call_args.kwargs[
            "report_expectations"
        ]
        is False
    )
    assert handler.api.work.report_expectation.call_args_list == [
        call("work--1", None),
        call("work--2", None),
    ]


def test_handler_forwards_v2_direct_delivery_context_to_batch_importer():
    handler = build_handler()

    result = handler.handle_message(build_v2_message(split_bundles=False))

    assert result == "ack"
    assert handler.api.stix2.import_bundle_from_json_batch.call_args.kwargs[
        "direct_delivery_context"
    ] == {
        "submission_id": "batch-submission--1",
        "delivery_id": build_root_delivery_id("batch-submission--1"),
        "parent_delivery_id": None,
        "delivery_kind": BATCH_DELIVERY_KIND_ROOT,
        "delivery_protocol_version": BATCH_DELIVERY_PROTOCOL_V2,
        "delivery_branch_kind": "ROOT",
        "delivery_branch_sequence": 0,
        "delivery_branch_ordinal": 0,
    }


def test_batch_plan_limit_detection_handles_typed_and_backend_errors():
    assert is_batch_plan_limit_error(BatchMutationPlanTooLarge(2, 1)) is True
    assert (
        is_batch_plan_limit_error(BatchMutationPlanTooManyExecutionGroups(2, 1)) is True
    )
    assert is_batch_plan_limit_error(ValueError("request entity too large")) is True
    assert is_batch_plan_limit_error(ValueError("unrelated")) is False


@pytest.mark.parametrize(
    "limit_error",
    [
        BatchMutationPlanTooLarge(200, 100),
        BatchMutationPlanTooManyExecutionGroups(3, 2),
    ],
)
def test_handler_requeues_durable_batch_chunks_for_bounded_batch_plan(
    monkeypatch, limit_error
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = limit_error
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
    assert handler.send_queue_message_to_specific_queue.call_count == 2
    first_chunk_data = handler.send_queue_message_to_specific_queue.call_args_list[
        0
    ].args[3]
    second_chunk_data = handler.send_queue_message_to_specific_queue.call_args_list[
        1
    ].args[3]
    assert first_chunk_data["split_bundles"] is False
    assert first_chunk_data["no_split"] is True
    assert first_chunk_data["batch_plan"] == batch_chunks[0][1]
    assert second_chunk_data["batch_plan"] == batch_chunks[1][1]


def test_handler_passes_execution_group_limit_to_oversized_chunk_builder(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = (
        BatchMutationPlanTooManyExecutionGroups(3, 2)
    )
    builder_kwargs = {}

    def build_chunks(*args, **kwargs):
        builder_kwargs.update(kwargs)
        return [
            (
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--1"}],
                },
                {"version": 1, "ordered_object_ids": ["indicator--1"]},
            )
        ]

    monkeypatch.setattr(
        push_handler.OpenCTIStix2,
        "build_oversized_batch_plan_chunks",
        build_chunks,
    )
    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_message(split_bundles=False, batch_plan={"version": 1})
    )

    assert result == "ack"
    assert builder_kwargs["max_execution_groups"] == 2


def test_handler_extends_all_enrichment_work_expectations_for_oversized_chunks(
    monkeypatch,
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = (
        BatchMutationPlanTooLarge(200, 100)
    )
    monkeypatch.setattr(
        push_handler.OpenCTIStix2,
        "build_oversized_batch_plan_chunks",
        lambda *args, **kwargs: [
            (
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--1"}],
                },
                {"version": 1, "ordered_object_ids": ["indicator--1"]},
            ),
            (
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--2"}],
                },
                {"version": 1, "ordered_object_ids": ["indicator--2"]},
            ),
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
        build_message(
            split_bundles=False,
            work_id="work--1",
            additional_work_ids=["work--2"],
            batch_plan={"version": 1},
        )
    )

    assert result == "ack"
    assert handler.api.work.add_expectations.call_args_list == [
        call("work--1", 2),
        call("work--2", 2),
    ]
    assert handler.api.work.report_expectation.call_args_list == [
        call("work--1", None),
        call("work--2", None),
    ]


def test_handler_requeues_oversized_chunks_without_work_tracking(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = (
        BatchMutationPlanTooLarge(200, 100)
    )
    monkeypatch.setattr(
        push_handler.OpenCTIStix2,
        "build_oversized_batch_plan_chunks",
        lambda *args, **kwargs: [
            (
                {
                    "type": "bundle",
                    "id": "bundle--1",
                    "objects": [{"id": "indicator--1"}],
                },
                {"version": 1, "ordered_object_ids": ["indicator--1"]},
            )
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
        build_message(split_bundles=False, batch_plan={"version": 1})
    )

    assert result == "ack"
    handler.send_queue_message_to_specific_queue.assert_called_once()
    handler.api.work.add_expectations.assert_not_called()
    handler.api.work.report_expectation.assert_not_called()


def test_handler_reserves_v2_oversized_chunks_before_publishing(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
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

    def reserve_children(parent_delivery_id, children):
        return build_reserved_handoff(
            parent_delivery_id,
            [child["queue_payload"] for child in children],
        )

    handler.api.reserve_batch_delivery_children.side_effect = reserve_children

    result = handler.handle_message(
        build_v2_message(
            split_bundles=False,
            work_id="work--1",
            batch_plan={"version": 1},
        )
    )

    assert result == "ack"
    reserve_call = handler.api.reserve_batch_delivery_children.call_args.args
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    assert reserve_call[0] == root_delivery_id
    assert [child["branch_kind"] for child in reserve_call[1]] == [
        BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
        BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
    ]
    assert handler.send_queue_message_to_specific_queue.call_count == 2
    handler.api.mark_batch_delivery_children_published.assert_called_once_with(
        root_delivery_id,
        [
            build_child_delivery_id(
                root_delivery_id, BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK, 0, 0
            ),
            build_child_delivery_id(
                root_delivery_id, BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK, 0, 1
            ),
        ],
    )
    handler.api.work.add_expectations.assert_not_called()
    handler.api.work.report_expectation.assert_not_called()


def test_handler_promotes_candidate_before_reserving_oversized_chunks(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
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
    promoted_data = json.loads(
        build_v2_message(
            submission_id="batch-delivery-candidate--1",
            delivery_id=build_root_delivery_id("batch-delivery-candidate--1"),
            batch_delivery_candidate_id="batch-delivery-candidate--1",
            split_bundles=False,
            work_id="work--1",
            batch_plan={"version": 1},
        )
    )
    handler.api.promote_batch_delivery_root.return_value = {
        "delivery_id": promoted_data["delivery_id"],
    }

    result = handler.handle_message(
        build_candidate_message(
            split_bundles=False,
            work_id="work--1",
            batch_plan={"version": 1},
        )
    )

    assert result == "ack"
    handler.api.promote_batch_delivery_root.assert_called_once()
    candidate_id, payload_fingerprint, work_id, additional_work_ids = (
        handler.api.promote_batch_delivery_root.call_args.args
    )
    assert candidate_id == "batch-delivery-candidate--1"
    assert payload_fingerprint == "a" * 64
    assert work_id == "work--1"
    assert additional_work_ids is None
    reserve_call = handler.api.reserve_batch_delivery_children.call_args.args
    assert reserve_call[0] == build_root_delivery_id(candidate_id)
    handler.api.work.add_expectations.assert_not_called()
    handler.api.work.report_expectation.assert_not_called()


def test_handler_requeues_candidate_promotion_connection_failures(monkeypatch):
    handler = build_handler()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = (
        BatchMutationPlanTooLarge(200, 100)
    )
    handler.api.promote_batch_delivery_root.side_effect = ConnectionError(
        "promotion unavailable"
    )
    monkeypatch.setattr(push_handler.time, "sleep", MagicMock())
    monkeypatch.setattr(push_handler.random, "uniform", lambda *_: 0)

    result = handler.handle_message(
        build_candidate_message(
            split_bundles=False,
            work_id="work--1",
            batch_plan={"version": 1},
        )
    )

    assert result == "requeue"
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.api.work.add_expectations.assert_not_called()
    handler.api.work.report_expectation.assert_not_called()


def test_handler_resumes_reserved_v2_chunks_before_reimport(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = AssertionError(
        "reserved chunk handoffs must not reimport the parent"
    )
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    chunk_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(chunk_child)],
    )
    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_v2_message(split_bundles=False, work_id="work--1")
    )

    assert result == "ack"
    handler.api.stix2.import_bundle_from_json_batch.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "push-exchange",
        "push-routing",
        chunk_child,
        False,
    )
    handler.api.work.report_expectation.assert_not_called()


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
    handler.send_queue_message_to_specific_queue = MagicMock()
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
    handler.send_queue_message_to_specific_queue.assert_called_once()
    replay_call = handler.send_queue_message_to_specific_queue.call_args.args
    assert replay_call[0] is channel
    assert replay_call[1] == "push-exchange"
    assert replay_call[2] == "push-routing"
    assert replay_call[3]["batch_replay_count"] == 1
    assert (
        decode_queue_message_content(replay_call[3])["id"]
        == "bundle--11111111-1111-4111-8111-111111111111"
    )
    handler.api.set_retry_number.assert_any_call(None)


def test_handler_republishes_v2_intact_replays_with_deterministic_child_ids(
    monkeypatch,
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
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
    replay_data = handler.send_queue_message_to_specific_queue.call_args.args[3]
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    assert replay_data["delivery_id"] == build_child_delivery_id(
        root_delivery_id,
        BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
        1,
        0,
    )
    assert replay_data["parent_delivery_id"] == root_delivery_id
    assert replay_data["delivery_kind"] == BATCH_DELIVERY_KIND_CHILD


def test_handler_reuses_reserved_v2_replay_children_and_publishes_only_missing(
    monkeypatch,
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
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
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    reserved_queue_payload = json.dumps(
        json.loads(
            build_v2_message(
                delivery_id=build_child_delivery_id(
                    root_delivery_id,
                    BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
                    1,
                    0,
                ),
                parent_delivery_id=root_delivery_id,
                delivery_kind=BATCH_DELIVERY_KIND_CHILD,
                delivery_branch_kind=BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
                delivery_branch_sequence=1,
                delivery_branch_ordinal=0,
                batch_replay_count=1,
            )
        )
    )
    handler.api.reserve_batch_delivery_children.return_value = build_reserved_handoff(
        root_delivery_id,
        [reserved_queue_payload],
    )
    handler.api.reserve_batch_delivery_children.side_effect = None

    result = handler.handle_message(build_v2_message(split_bundles=False))

    assert result == "ack"
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "push-exchange",
        "push-routing",
        json.loads(reserved_queue_payload),
        False,
    )
    handler.api.mark_batch_delivery_children_published.assert_called_once_with(
        root_delivery_id,
        [
            build_child_delivery_id(
                root_delivery_id, BATCH_DELIVERY_BRANCH_INTACT_REPLAY, 1, 0
            )
        ],
    )


def test_handler_resumes_reserved_v2_replay_children_before_reimport(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = AssertionError(
        "reserved replay child handoffs must not reimport the parent"
    )
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    replay_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
                1,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
            delivery_branch_sequence=1,
            delivery_branch_ordinal=0,
            batch_replay_count=1,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(replay_child)],
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
    handler.api.stix2.import_bundle_from_json_batch.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "push-exchange",
        "push-routing",
        replay_child,
        False,
    )


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
    handler.send_queue_message_to_specific_queue = MagicMock()
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
    handler.send_queue_message_to_specific_queue.assert_called_once()
    dead_letter_call = handler.send_queue_message_to_specific_queue.call_args.args
    assert dead_letter_call[0] is channel
    assert dead_letter_call[1] == "listen-exchange"
    assert dead_letter_call[2] == "dead-letter-routing"
    dead_letter_content = decode_queue_message_content(dead_letter_call[3])
    assert dead_letter_content["id"] == "indicator--1"
    assert (
        dead_letter_content["rejection_info"]["original_connector_id"] == "connector--1"
    )


def test_handler_resumes_reserved_v2_dead_letter_children_before_reimport(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = AssertionError(
        "reserved dead-letter handoffs must not reimport the parent"
    )
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    dead_letter_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(dead_letter_child)],
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
    handler.api.stix2.import_bundle_from_json_batch.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "listen-exchange",
        "dead-letter-routing",
        dead_letter_child,
        False,
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
    handler.send_queue_message_to_specific_queue = MagicMock()

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
    assert handler.send_queue_message_to_specific_queue.call_count == 2


def test_handler_requeues_v2_split_children_with_stable_sibling_ids(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()

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
    first_child = handler.send_queue_message_to_specific_queue.call_args_list[0].args[3]
    second_child = handler.send_queue_message_to_specific_queue.call_args_list[1].args[
        3
    ]
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
    handler.api.work.add_expectations.assert_not_called()
    handler.api.work.report_expectation.assert_not_called()


def test_handler_resumes_reserved_v2_fallback_split_children_before_reimport(
    monkeypatch,
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    handler.api.stix2.import_bundle_from_json_batch.side_effect = AssertionError(
        "reserved fallback split handoffs must not reimport the parent"
    )
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    split_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(split_child)],
    )
    channel = MagicMock()
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_v2_message(split_bundles=False, work_id="work--1")
    )

    assert result == "ack"
    handler.api.stix2.import_bundle_from_json_batch.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "push-exchange",
        "push-routing",
        split_child,
        True,
    )
    handler.api.work.report_expectation.assert_not_called()


def test_handler_recovers_only_missing_reserved_v2_split_children(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    first_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    second_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                1,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=1,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(second_child)],
    )
    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("reserved child handoff must not recompute split bundles")
        ),
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
    handler.send_queue_message_to_specific_queue.assert_called_once_with(
        channel,
        "push-exchange",
        "push-routing",
        second_child,
        True,
    )
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_called_once_with(
        root_delivery_id,
        [second_child["delivery_id"]],
    )
    assert first_child["delivery_id"] != second_child["delivery_id"]


def test_handler_finalizes_reserved_v2_split_handoff_without_republishing(
    monkeypatch,
):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    first_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    second_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                1,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=1,
        )
    )
    handler.api.batch_delivery_handoff.return_value = {
        "parent_delivery_id": root_delivery_id,
        "handoff_evidence": BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED,
        "child_set_fingerprint": "fingerprint-1",
        "child_count": 2,
        "children": [
            {
                "delivery_id": first_child["delivery_id"],
                "state": "PUBLISHED",
                "queue_payload": json.dumps(first_child),
            },
            {
                "delivery_id": second_child["delivery_id"],
                "state": "PUBLISHED",
                "queue_payload": json.dumps(second_child),
            },
        ],
        "pending_children": [],
    }

    def fail_split(*args, **kwargs):
        raise AssertionError("completed child handoffs must not be recomputed")

    monkeypatch.setattr(
        push_handler.OpenCTIStix2Splitter,
        "split_bundle_with_expectations",
        fail_split,
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
    handler.send_queue_message_to_specific_queue.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_called_once_with(
        root_delivery_id,
        [],
    )
    handler.api.work.report_expectation.assert_not_called()


def test_handler_requeues_reserved_v2_child_publish_failures(monkeypatch):
    handler = build_handler()
    monkeypatch.setattr(push_handler.time, "sleep", MagicMock())
    monkeypatch.setattr(push_handler.random, "uniform", lambda *_: 0)
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(child)],
    )
    handler.send_queue_message_to_specific_queue = MagicMock(
        side_effect=RuntimeError("broker connection lost")
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

    assert result == "requeue"
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_not_called()


def test_resume_reserved_handoff_rejects_mismatched_child_branch_route():
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    root_delivery_id = build_root_delivery_id("batch-submission--1")
    dead_letter_child = json.loads(
        build_v2_message(
            delivery_id=build_child_delivery_id(
                root_delivery_id,
                BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
                0,
                0,
            ),
            parent_delivery_id=root_delivery_id,
            delivery_kind=BATCH_DELIVERY_KIND_CHILD,
            delivery_branch_kind=BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
            delivery_branch_sequence=0,
            delivery_branch_ordinal=0,
        )
    )
    handler.api.batch_delivery_handoff.return_value = build_reserved_handoff(
        root_delivery_id,
        [json.dumps(dead_letter_child)],
    )

    resumed = handler._resume_reserved_child_handoff(
        MagicMock(),
        "push-exchange",
        "push-routing",
        json.loads(build_v2_message()),
        BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
    )

    assert resumed is False
    handler.send_queue_message_to_specific_queue.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_not_called()


def test_handler_requeues_v2_child_handoff_when_confirms_unavailable(monkeypatch):
    handler = build_handler()
    handler.send_queue_message_to_specific_queue = MagicMock()
    monkeypatch.setattr(push_handler.time, "sleep", MagicMock())
    monkeypatch.setattr(push_handler.random, "uniform", lambda *_: 0)
    channel = MagicMock()
    channel.confirm_delivery.side_effect = RuntimeError("confirms unavailable")
    connection = MagicMock()
    connection.__enter__.return_value = connection
    connection.channel.return_value.__enter__.return_value = channel
    monkeypatch.setattr(
        push_handler.pika, "BlockingConnection", lambda *args: connection
    )

    result = handler.handle_message(
        build_v2_message(split_bundles=True, work_id="work--1")
    )

    assert result == "requeue"
    handler.api.batch_delivery_handoff.assert_not_called()
    handler.api.reserve_batch_delivery_children.assert_not_called()
    handler.send_queue_message_to_specific_queue.assert_not_called()
    handler.api.mark_batch_delivery_children_published.assert_not_called()
