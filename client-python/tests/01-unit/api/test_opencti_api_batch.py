from pycti.api.opencti_api_batch import BatchMutationPlan, build_batch_result_token
from pycti.api.opencti_api_client import File


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
