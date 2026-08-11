from pycti.entities.opencti_stix_cyber_observable import StixCyberObservable


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
        if "query StixCyberObservables" in query:
            return {
                "data": {
                    "stixCyberObservables": {
                        "edges": [
                            {
                                "node": {
                                    "id": "domain-name--1",
                                    "standard_id": "domain-name--1",
                                    "entity_type": "Domain-Name",
                                    "parent_types": ["Stix-Cyber-Observable"],
                                }
                            }
                        ],
                        "pageInfo": {"hasNextPage": False},
                    }
                }
            }
        return {
            "data": {
                "stixCyberObservable": {
                    "id": "domain-name--1",
                    "standard_id": "domain-name--1",
                    "entity_type": "Domain-Name",
                    "parent_types": ["Stix-Cyber-Observable"],
                }
            }
        }

    @staticmethod
    def process_multiple_fields(data):
        return data

    @staticmethod
    def process_multiple(data, *_args):
        return [edge["node"] for edge in data["edges"]]


def test_read_keeps_related_indicators_in_default_projection():
    client = _ProjectionClient()

    StixCyberObservable(client).read(id="domain-name--1")

    assert "    indicators {" in client.queries[0][0]


def test_read_can_omit_related_indicators_without_changing_default_projection():
    client = _ProjectionClient()

    StixCyberObservable(client).read(id="domain-name--1", withIndicators=False)

    assert "    indicators {" not in client.queries[0][0]


def test_read_can_omit_external_references_without_changing_default_projection():
    client = _ProjectionClient()

    StixCyberObservable(client).read(
        id="domain-name--1", withExternalReferences=False
    )

    assert "    externalReferences {" not in client.queries[0][0]
    assert "    indicators {" in client.queries[0][0]


def test_read_can_omit_external_references_with_files_and_indicators():
    client = _ProjectionClient()

    StixCyberObservable(client).read(
        id="domain-name--1",
        withFiles=True,
        withIndicators=False,
        withExternalReferences=False,
    )

    assert "    externalReferences {" not in client.queries[0][0]
    assert "    indicators {" not in client.queries[0][0]
    assert "    importFiles {" in client.queries[0][0]


def test_filtered_read_preserves_indicator_projection_choice():
    client = _ProjectionClient()

    StixCyberObservable(client).read(
        filters={"mode": "and", "filters": [], "filterGroups": []},
        withIndicators=False,
    )

    assert "    indicators {" not in client.queries[0][0]


def test_filtered_read_preserves_external_reference_projection_choice():
    client = _ProjectionClient()

    StixCyberObservable(client).read(
        filters={"mode": "and", "filters": [], "filterGroups": []},
        withExternalReferences=False,
    )

    assert "    externalReferences {" not in client.queries[0][0]
