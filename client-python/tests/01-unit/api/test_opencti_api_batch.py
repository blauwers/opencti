import json
from unittest.mock import MagicMock

import pytest

from pycti.api.opencti_api_batch import (
    BatchMutationPlan,
    BatchMutationPlanTooLarge,
    BatchMutationPlanTooManyExecutionGroups,
    build_batch_result_token,
)
from pycti.api.opencti_api_client import (
    DEFAULT_BATCH_REQUESTS_TIMEOUT,
    File,
    OpenCTIApiClient,
)


def test_batch_mutation_plan_captures_mutation_shape_and_result_tokens():
    plan = BatchMutationPlan()
    query = """
        mutation IndicatorAdd($input: IndicatorAddInput!) {
            indicatorAdd(input: $input) {
                id
                standard_id
                entity_type
                parent_types
                ... on Indicator {
                    name
                }
                observables {
                    edges {
                        node {
                            id
                        }
                    }
                }
            }
        }
        """

    result = plan.capture(
        query,
        {"input": {"stix_id": "indicator--1"}},
        [],
    )

    assert result == {
        "data": {
            "indicatorAdd": {
                "id": build_batch_result_token(0, ["indicatorAdd", "id"]),
                "standard_id": build_batch_result_token(
                    0, ["indicatorAdd", "standard_id"]
                ),
                "entity_type": build_batch_result_token(
                    0, ["indicatorAdd", "entity_type"]
                ),
                "parent_types": [],
                "name": build_batch_result_token(0, ["indicatorAdd", "name"]),
                "observables": {"edges": []},
            }
        }
    }
    assert plan.operations == [
        {
            "query": query,
            "variables": '{"input": {"stix_id": "indicator--1"}}',
        }
    ]


def test_batch_mutation_plan_serializes_uploads_with_variable_paths():
    plan = BatchMutationPlan()

    plan.capture(
        "mutation Upload($input: TestInput!) { upload(input: $input) { id } }",
        {"input": {"files": [None], "file": None}},
        [
            {"key": "input.files", "file": [File("one.txt", "one")], "multiple": True},
            {"key": "input.file", "file": File("two.txt", b"two"), "multiple": False},
        ],
    )

    assert plan.operations[0]["files"] == [
        {
            "path": "input.files.0",
            "name": "one.txt",
            "mime_type": "text/plain",
            "data": "b25l",
        },
        {
            "path": "input.file",
            "name": "two.txt",
            "mime_type": "text/plain",
            "data": "dHdv",
        },
    ]


def test_batch_mutation_plan_stops_capture_when_serialized_operations_exceed_limit():
    plan = BatchMutationPlan(max_serialized_operations_size=1)

    with pytest.raises(BatchMutationPlanTooLarge) as raised:
        plan.capture(
            "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
            {"input": {"stix_id": "indicator--1"}},
            [],
        )

    assert raised.value.actual_size > raised.value.max_size
    assert plan.operations == []


def test_batch_mutation_plan_tags_operations_with_execution_group_metadata():
    plan = BatchMutationPlan()

    with plan.execution_group(2, "indicator--1"):
        plan.capture(
            "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
            {"input": {"stix_id": "indicator--1"}},
            [],
        )
        plan.capture(
            "mutation IndicatorEdit($id: ID!) { indicatorEdit(id: $id) { id } }",
            {"id": "indicator--1"},
            [],
        )

    with plan.execution_group(3, "relationship--1"):
        plan.capture(
            "mutation RelationshipAdd($input: StixCoreRelationshipAddInput!) { stixCoreRelationshipAdd(input: $input) { id } }",
            {"input": {"fromId": "indicator--1", "toId": "identity--1"}},
            [],
        )

    assert [
        (
            operation["execution_group"],
            operation["execution_phase"],
            operation["object_id"],
        )
        for operation in plan.operations
    ] == [
        (0, 2, "indicator--1"),
        (0, 2, "indicator--1"),
        (1, 3, "relationship--1"),
    ]


def test_batch_mutation_plan_stops_before_execution_group_limit_is_exceeded():
    plan = BatchMutationPlan(max_execution_groups=2)

    with plan.execution_group(0, "indicator--1"):
        plan.capture(
            "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
            {"input": {"stix_id": "indicator--1"}},
            [],
        )
    with plan.execution_group(0, "indicator--2"):
        plan.capture(
            "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
            {"input": {"stix_id": "indicator--2"}},
            [],
        )

    with pytest.raises(BatchMutationPlanTooManyExecutionGroups) as raised:
        with plan.execution_group(0, "indicator--3"):
            pass

    assert raised.value.actual_count == 3
    assert raised.value.max_count == 2
    assert len(plan.operations) == 2


def test_batch_mutation_plan_uses_batch_specific_request_timeout():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        requests_timeout=300,
    )
    client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    plan = BatchMutationPlan()
    plan.capture(
        "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
        {"input": {"stix_id": "indicator--1"}},
        [],
    )

    client.execute_batch_mutation_plan(plan)

    assert client.session_batch_requests_timeout == 3600
    assert (
        client.query.call_args.kwargs["request_timeout"]
        == DEFAULT_BATCH_REQUESTS_TIMEOUT
    )


def test_batch_mutation_plan_preserves_explicit_batch_request_timeout():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        requests_timeout=300,
        batch_requests_timeout=4200,
    )
    client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    plan = BatchMutationPlan()
    plan.capture(
        "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
        {"input": {"stix_id": "indicator--1"}},
        [],
    )

    client.execute_batch_mutation_plan(plan)

    assert client.session_batch_requests_timeout == 4200
    assert client.query.call_args.kwargs["request_timeout"] == 4200
    assert client.query.call_args.kwargs["fresh_session"] is True


def test_batch_mutation_plan_forwards_direct_delivery_context():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
    )
    client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    plan = BatchMutationPlan()
    plan.capture(
        "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
        {"input": {"stix_id": "indicator--1"}},
        [],
    )
    direct_delivery_context = {
        "submission_id": "batch-submission--1",
        "delivery_id": "batch-delivery--1",
        "parent_delivery_id": None,
        "delivery_kind": "ROOT",
        "delivery_protocol_version": 2,
        "delivery_branch_kind": "ROOT",
        "delivery_branch_sequence": 0,
        "delivery_branch_ordinal": 0,
    }

    client.execute_batch_mutation_plan(
        plan,
        execution_mode="BULK",
        wait_until="MATERIALIZED",
        direct_delivery_context=direct_delivery_context,
    )

    assert client.query.call_args.args[1]["options"]["direct_delivery_context"] == (
        direct_delivery_context
    )


def test_batch_mutation_plan_uses_one_shot_http_session(monkeypatch):
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
    )
    shared_session = MagicMock()
    client.session = shared_session
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"data": {"batchMutationsExecute": {}}}
    fresh_session = MagicMock()
    fresh_session.post.return_value = response
    monkeypatch.setattr(
        "pycti.api.opencti_api_client.requests.session", lambda: fresh_session
    )
    plan = BatchMutationPlan()
    plan.capture(
        "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
        {"input": {"stix_id": "indicator--1"}},
        [],
    )

    result = client.execute_batch_mutation_plan(plan)

    assert result == {"data": {"batchMutationsExecute": {}}}
    shared_session.post.assert_not_called()
    shared_session.close.assert_called_once()
    fresh_session.post.assert_called_once()
    fresh_session.close.assert_called_once()


def test_batch_mutation_plan_rejects_oversized_serialized_request_before_query():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        batch_requests_max_payload_size=1,
    )
    client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    plan = BatchMutationPlan()
    plan.capture(
        "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
        {"input": {"stix_id": "indicator--1"}},
        [],
    )

    with pytest.raises(BatchMutationPlanTooLarge) as raised:
        client.execute_batch_mutation_plan(plan)

    assert raised.value.actual_size > raised.value.max_size
    client.query.assert_not_called()


def test_batch_mutation_plan_reserves_request_envelope_before_capture():
    query = "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }"
    variables = {"input": {"stix_id": "indicator--1"}}
    probe_client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
    )
    probe_client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    probe_plan = BatchMutationPlan()
    probe_plan.capture(query, variables, [])
    probe_client.execute_batch_mutation_plan(
        probe_plan,
        execution_mode="BULK",
        wait_until="COMMITTED",
        backend_batch_plan={"version": 1, "execution_phases": []},
    )
    mutation, payload_variables = probe_client.query.call_args.args[:2]
    exact_request_size = len(
        json.dumps({"query": mutation, "variables": payload_variables}).encode("utf-8")
    )

    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        batch_requests_max_payload_size=exact_request_size,
    )
    client.query = MagicMock(return_value={"data": {"batchMutationsExecute": {}}})
    with client.batch_mutation_plan(
        execution_mode="BULK",
        wait_until="COMMITTED",
        backend_batch_plan={"version": 1, "execution_phases": []},
    ) as plan:
        plan.capture(query, variables, [])
    client.execute_batch_mutation_plan(
        plan,
        execution_mode="BULK",
        wait_until="COMMITTED",
        backend_batch_plan={"version": 1, "execution_phases": []},
    )
    client.query.assert_called_once()

    too_small_client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        batch_requests_max_payload_size=exact_request_size - 1,
    )
    with too_small_client.batch_mutation_plan(
        execution_mode="BULK",
        wait_until="COMMITTED",
        backend_batch_plan={"version": 1, "execution_phases": []},
    ) as plan:
        with pytest.raises(BatchMutationPlanTooLarge):
            plan.capture(query, variables, [])


def test_batch_mutation_plan_applies_client_execution_group_limit():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        batch_requests_max_execution_groups=1,
    )

    with client.batch_mutation_plan() as plan:
        with plan.execution_group(0, "indicator--1"):
            plan.capture(
                "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
                {"input": {"stix_id": "indicator--1"}},
                [],
            )
        with pytest.raises(BatchMutationPlanTooManyExecutionGroups):
            with plan.execution_group(0, "indicator--2"):
                pass


def test_send_bundle_to_api_uses_json_admission_below_payload_threshold():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        bundle_submission_max_payload_size=10_000,
    )
    client.query = MagicMock(return_value={"data": {"stixBundlePush": True}})

    client.send_bundle_to_api(
        connector_id="connector-1",
        bundle='{"type":"bundle","objects":[]}',
        work_id="work-1",
        wait_until="COMMITTED",
    )

    mutation, variables = client.query.call_args.args[:2]
    assert "stixBundlePush(" in mutation
    assert "stixBundlePushUpload(" not in mutation
    assert variables == {
        "connectorId": "connector-1",
        "bundle": '{"type":"bundle","objects":[]}',
        "work_id": "work-1",
        "split_bundles": False,
        "cleanup_inconsistent_bundle": False,
        "wait_until": "COMMITTED",
    }


def test_send_bundle_to_api_uses_multipart_admission_above_payload_threshold():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        bundle_submission_max_payload_size=1,
    )
    client.query = MagicMock(return_value={"data": {"stixBundlePushUpload": True}})

    client.send_bundle_to_api(
        connector_id="connector-1",
        bundle='{"type":"bundle","objects":[]}',
        work_id="work-1",
        split_bundles=True,
        cleanup_inconsistent_bundle=True,
        wait_until="MATERIALIZED",
    )

    mutation, variables = client.query.call_args.args[:2]
    assert "stixBundlePushUpload(" in mutation
    assert "stixBundlePush(" not in mutation
    assert variables["connectorId"] == "connector-1"
    assert variables["work_id"] == "work-1"
    assert variables["split_bundles"] is True
    assert variables["cleanup_inconsistent_bundle"] is True
    assert variables["wait_until"] == "MATERIALIZED"
    assert isinstance(variables["bundle"], File)
    assert variables["bundle"].name == "bundle.json"
    assert variables["bundle"].data == '{"type":"bundle","objects":[]}'
    assert variables["bundle"].mime == "application/json"


def test_batch_delivery_handoff_methods_forward_graphql_variables_and_results():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
    )
    client.query = MagicMock(
        side_effect=[
            {"data": {"batchDeliveryHandoff": {"handoff_evidence": "NONE"}}},
            {
                "data": {
                    "batchDeliveryPromoteRoot": {
                        "delivery_id": "batch-delivery--root",
                    }
                }
            },
            {
                "data": {
                    "batchDeliveryReserveChildren": {
                        "handoff_evidence": "CHILDREN_RESERVED"
                    }
                }
            },
            {
                "data": {
                    "batchDeliveryMarkChildrenPublished": {
                        "handoff_evidence": "CHILDREN_PUBLISHED"
                    }
                }
            },
        ]
    )
    children = [
        {
            "branch_kind": "LEGACY_SPLIT",
            "branch_sequence": 0,
            "branch_ordinal": 0,
            "queue_payload": "{}",
        }
    ]

    handoff = client.batch_delivery_handoff("batch-delivery--parent")
    promoted = client.promote_batch_delivery_root(
        "batch-delivery-candidate--1",
        "a" * 64,
        "work--1",
        ["work--2"],
    )
    reserved = client.reserve_batch_delivery_children(
        "batch-delivery--parent", children
    )
    published = client.mark_batch_delivery_children_published(
        "batch-delivery--parent", ["batch-delivery--child"]
    )

    assert handoff == {"handoff_evidence": "NONE"}
    assert promoted == {
        "delivery_id": "batch-delivery--root",
    }
    assert reserved == {"handoff_evidence": "CHILDREN_RESERVED"}
    assert published == {"handoff_evidence": "CHILDREN_PUBLISHED"}
    assert client.query.call_args_list[0].args[1] == {
        "parentDeliveryId": "batch-delivery--parent"
    }
    assert client.query.call_args_list[1].args[1] == {
        "candidateId": "batch-delivery-candidate--1",
        "payloadFingerprint": "a" * 64,
        "workId": "work--1",
        "additionalWorkIds": ["work--2"],
    }
    assert client.query.call_args_list[2].args[1] == {
        "parentDeliveryId": "batch-delivery--parent",
        "children": children,
    }
    assert client.query.call_args_list[3].args[1] == {
        "parentDeliveryId": "batch-delivery--parent",
        "childDeliveryIds": ["batch-delivery--child"],
    }


def test_batch_delivery_reserve_children_uses_multipart_for_oversized_manifest():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
        bundle_submission_max_payload_size=1,
    )
    client.query = MagicMock(
        return_value={
            "data": {
                "batchDeliveryReserveChildrenUpload": {
                    "handoff_evidence": "CHILDREN_RESERVED"
                }
            }
        }
    )
    children = [
        {
            "branch_kind": "LEGACY_SPLIT",
            "branch_sequence": 0,
            "branch_ordinal": 0,
            "queue_payload": '{"content":"payload"}',
        }
    ]

    reserved = client.reserve_batch_delivery_children(
        "batch-delivery--parent", children
    )

    assert reserved == {"handoff_evidence": "CHILDREN_RESERVED"}
    mutation, variables = client.query.call_args.args
    assert "batchDeliveryReserveChildrenUpload" in mutation
    assert variables["parentDeliveryId"] == "batch-delivery--parent"
    assert isinstance(variables["children"], File)
    assert variables["children"].name == "batch-delivery-children.json"
    assert json.loads(variables["children"].data) == children


def test_enrichment_batch_result_submit_forwards_graphql_variables_and_result():
    client = OpenCTIApiClient(
        url="http://localhost:4000",
        token="test-token",
        perform_health_check=False,
    )
    client.query = MagicMock(
        return_value={"data": {"enrichmentBatchResultSubmit": True}}
    )

    result = client.submit_enrichment_batch_result(
        "connector--1",
        '{"batch_id":"enrichment-batch--1"}',
        '{"result_count":1}',
    )

    assert result is True
    assert client.query.call_args.args[1] == {
        "connectorId": "connector--1",
        "envelope": '{"batch_id":"enrichment-batch--1"}',
        "result": '{"result_count":1}',
    }
