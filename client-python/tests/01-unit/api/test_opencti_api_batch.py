from unittest.mock import MagicMock

import pytest

from pycti.api.opencti_api_batch import (
    BatchMutationPlan,
    BatchMutationPlanTooLarge,
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
