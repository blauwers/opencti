import base64
import json
from unittest.mock import MagicMock

from src import push_handler
from src.push_handler import (
    PushHandler,
    build_batch_expectation_error,
    should_add_legacy_default_split_expectations,
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


def test_bundle_transport_is_unsplit_by_default():
    content = {"objects": [{"id": "indicator--1"}, {"id": "indicator--2"}]}

    assert should_split_bundles({}, content) is False
    assert should_split_bundles({"no_split": False}, content) is False
    assert should_split_bundles({"no_split": True}, content) is False


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
    imported_raw_content = handler.api.stix2.import_bundle_from_json_batch.call_args.args[
        0
    ]
    assert json.loads(imported_raw_content)["id"] == (
        "bundle--11111111-1111-4111-8111-111111111111"
    )
    assert len(json.loads(imported_raw_content)["objects"]) == 2


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
    handler.api.work.report_expectation.assert_called_once_with("work--1", None)


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
