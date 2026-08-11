from pycti.entities.opencti_indicator import Indicator


class _NullLogger:
    def info(self, *_args, **_kwargs):
        pass

    def error(self, *_args, **_kwargs):
        raise AssertionError("unexpected error log")


class _ProjectionClient:
    def __init__(self):
        self.app_logger = _NullLogger()
        self.queries = []

    def query(self, query, variables):
        self.queries.append((query, variables))
        if "query Indicators" in query:
            return {
                "data": {
                    "indicators": {
                        "edges": [
                            {
                                "node": {
                                    "id": "indicator--1",
                                    "standard_id": "indicator--1",
                                    "entity_type": "Indicator",
                                }
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }
        return {
            "data": {
                "indicator": {
                    "id": "indicator--1",
                    "standard_id": "indicator--1",
                    "entity_type": "Indicator",
                }
            }
        }

    @staticmethod
    def process_multiple_fields(data):
        return data

    @staticmethod
    def process_multiple(data, *_args):
        return [edge["node"] for edge in data["edges"]]


def test_list_keeps_external_references_in_default_projection():
    client = _ProjectionClient()

    Indicator(client).list()

    assert "    externalReferences {" in client.queries[0][0]


def test_read_can_omit_external_references_without_changing_default_projection():
    client = _ProjectionClient()

    Indicator(client).read(id="indicator--1", withExternalReferences=False)

    assert "    externalReferences {" not in client.queries[0][0]
    assert "    importFiles {" not in client.queries[0][0]


def test_read_can_omit_external_references_while_including_files():
    client = _ProjectionClient()

    Indicator(client).read(
        id="indicator--1",
        withFiles=True,
        withExternalReferences=False,
    )

    assert "    externalReferences {" not in client.queries[0][0]
    assert "    importFiles {" in client.queries[0][0]


def test_filtered_read_preserves_external_reference_projection_choice():
    client = _ProjectionClient()

    Indicator(client).read(
        filters={"mode": "and", "filters": [], "filterGroups": []},
        withExternalReferences=False,
    )

    assert "    externalReferences {" not in client.queries[0][0]


def test_filtered_read_preserves_file_projection_choice():
    client = _ProjectionClient()

    Indicator(client).read(
        filters={"mode": "and", "filters": [], "filterGroups": []},
        withFiles=True,
        withExternalReferences=False,
    )

    assert "    externalReferences {" not in client.queries[0][0]
    assert "    importFiles {" in client.queries[0][0]
