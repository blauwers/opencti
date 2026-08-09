from types import SimpleNamespace

from pycti.entities.opencti_stix_nested_ref_relationship import (
    StixNestedRefRelationship,
)


class _NestedRefOpenCTI:
    def __init__(self):
        self.app_logger = SimpleNamespace(info=lambda *_args, **_kwargs: None)
        self.query_calls = []

    def query(self, query, variables):
        self.query_calls.append((query, variables))
        return {
            "data": {
                "stixCoreObjectEdit": {
                    "relationsAdd": {"id": "observable--1"},
                }
            }
        }

    @staticmethod
    def process_multiple_fields(data):
        return data


def test_add_many_to_stix_core_object_normalizes_relationship_type():
    opencti = _NestedRefOpenCTI()
    nested_ref_relationship = StixNestedRefRelationship(opencti)

    result = nested_ref_relationship.add_many_to_stix_core_object(
        "observable--1",
        ["ipv4-addr--1", "ipv4-addr--2"],
        "resolves-to",
    )

    assert result == {"id": "observable--1"}
    assert opencti.query_calls[0][1] == {
        "id": "observable--1",
        "input": {
            "toIds": ["ipv4-addr--1", "ipv4-addr--2"],
            "relationship_type": "obs_resolves-to",
        },
    }
