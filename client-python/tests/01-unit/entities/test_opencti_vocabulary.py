from types import SimpleNamespace

from pycti.entities.opencti_vocabulary import Vocabulary


class _VocabularyListOpenCTI:
    def __init__(self):
        self.app_logger = SimpleNamespace(
            info=lambda *_args, **_kwargs: None,
            debug=lambda *_args, **_kwargs: None,
        )
        self.query_calls = []
        self.results = [
            {
                "data": {
                    "vocabularies": {
                        "edges": [{"node": {"id": "vocabulary--1", "name": "one"}}],
                        "pageInfo": {
                            "endCursor": "cursor-1",
                            "hasNextPage": True,
                        },
                    }
                }
            },
            {
                "data": {
                    "vocabularies": {
                        "edges": [{"node": {"id": "vocabulary--2", "name": "two"}}],
                        "pageInfo": {
                            "endCursor": "cursor-2",
                            "hasNextPage": False,
                        },
                    }
                }
            },
        ]

    def query(self, query, variables):
        self.query_calls.append((query, variables))
        return self.results.pop(0)

    @staticmethod
    def process_multiple(data, with_pagination=False):
        entities = [edge["node"] for edge in data["edges"]]
        if with_pagination:
            return {"entities": entities, "pagination": data["pageInfo"]}
        return entities


def test_list_get_all_paginates_vocabulary_results():
    opencti = _VocabularyListOpenCTI()
    vocabulary = Vocabulary(opencti)
    filters = {
        "mode": "and",
        "filters": [{"key": "name", "values": ["one", "two"]}],
        "filterGroups": [],
    }

    result = vocabulary.list(filters=filters, first=1, getAll=True)

    assert result == [
        {"id": "vocabulary--1", "name": "one"},
        {"id": "vocabulary--2", "name": "two"},
    ]
    assert opencti.query_calls[0][1] == {
        "filters": filters,
        "first": 1,
        "after": None,
    }
    assert opencti.query_calls[1][1] == {
        "filters": filters,
        "first": 1,
        "after": "cursor-1",
    }
    assert opencti.query_calls[0][0].count("{") == opencti.query_calls[0][0].count("}")
