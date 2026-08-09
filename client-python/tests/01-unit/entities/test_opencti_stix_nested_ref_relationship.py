from types import SimpleNamespace

from pycti.entities.opencti_stix_nested_ref_relationship import (
    StixNestedRefRelationship,
)


class _NestedRefOpenCTI:
    def __init__(self):
        self.app_logger = SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            debug=lambda *_args, **_kwargs: None,
        )
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


class _PagedNestedRefOpenCTI:
    def __init__(self):
        self.app_logger = SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            debug=lambda *_args, **_kwargs: None,
        )
        self.query_calls = []
        self.pages = [
            {
                "items": [{"id": "relationship--1"}, {"id": "relationship--2"}],
                "pageInfo": {"endCursor": "0", "hasNextPage": True},
            },
            {
                "items": [{"id": "relationship--3"}],
                "pageInfo": {"endCursor": "1", "hasNextPage": False},
            },
        ]

    def query(self, query, variables):
        self.query_calls.append((query, variables))
        after = variables["after"]
        page_index = 0 if after is None else int(after) + 1
        return {"data": {"stixNestedRefRelationships": self.pages[page_index]}}

    @staticmethod
    def process_multiple(page, with_pagination=False):
        return page if with_pagination else page["items"]


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


def test_list_get_all_preserves_page_order():
    opencti = _PagedNestedRefOpenCTI()
    nested_ref_relationship = StixNestedRefRelationship(opencti)

    result = nested_ref_relationship.list(getAll=True)

    assert [item["id"] for item in result] == [
        "relationship--1",
        "relationship--2",
        "relationship--3",
    ]
    assert [variables["after"] for _, variables in opencti.query_calls] == [None, "0"]


def test_list_without_get_all_preserves_pagination_shape():
    opencti = _PagedNestedRefOpenCTI()
    nested_ref_relationship = StixNestedRefRelationship(opencti)

    result = nested_ref_relationship.list(withPagination=True)

    assert result == opencti.pages[0]
    assert len(opencti.query_calls) == 1
