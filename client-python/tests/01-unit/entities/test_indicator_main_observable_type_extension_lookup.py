from collections import Counter
from types import SimpleNamespace

from pycti.api.opencti_api_client import OpenCTIApiClient
from pycti.entities.opencti_indicator import Indicator

_OPENCTI_EXTENSION = "extension-definition--ea279b3e-5c71-4632-ac08-831c66a786ba"


class _OpenCTI:
    def __init__(self):
        self.extension_lookup_counts = Counter()
        self.mitre_extension_lookup_counts = Counter()
        self.bulk_lookup_keys = []

    def get_attribute_in_extension(self, key, stix_object):
        self.extension_lookup_counts[key] += 1
        return OpenCTIApiClient.get_attribute_in_extension(key, stix_object)

    def get_attribute_in_mitre_extension(self, key, stix_object):
        self.mitre_extension_lookup_counts[key] += 1
        return OpenCTIApiClient.get_attribute_in_mitre_extension(key, stix_object)

    def copy_attributes_from_extension(self, attribute_map, stix_object):
        self.bulk_lookup_keys.append(tuple(attribute_map))
        OpenCTIApiClient.copy_attributes_from_extension(attribute_map, stix_object)


class _QueryOpenCTI:
    def __init__(self):
        self.queries = []
        self.app_logger = SimpleNamespace(
            error=lambda *_args, **_kwargs: None,
            info=lambda *_args, **_kwargs: None,
        )

    def query(self, query, _variables):
        self.queries.append(query)
        return {
            "data": {
                "indicatorsAdd": [
                    {
                        "id": "indicator--1",
                        "standard_id": "indicator--1",
                        "entity_type": "Indicator",
                        "parent_types": ["Stix-Domain-Object"],
                    }
                ]
            }
        }

    @staticmethod
    def process_multiple_fields(indicator):
        return indicator


def _bulk_create_item():
    return {
        "name": "Indicator",
        "pattern_type": "stix",
        "pattern": "[ipv4-addr:value = '192.0.2.1']",
        "x_opencti_main_observable_type": "IPv4-Addr",
    }


def test_indicator_main_observable_type_extension_is_read_once():
    opencti = _OpenCTI()
    indicator = Indicator(opencti)
    indicator.create = lambda **kwargs: kwargs
    stix_object = {
        "id": "indicator--1",
        "type": "indicator",
        "pattern": "[ipv4-addr:value = '192.0.2.1']",
        "extensions": {
            _OPENCTI_EXTENSION: {"main_observable_type": "IPv4-Addr"},
        },
    }

    result = indicator.import_from_stix2(stixObject=stix_object)

    assert result["x_opencti_main_observable_type"] == "IPv4-Addr"
    assert opencti.extension_lookup_counts["main_observable_type"] == 1


def test_indicator_import_bulk_copies_ordinary_extension_fields():
    opencti = _OpenCTI()
    indicator = Indicator(opencti)
    indicator.create = lambda **kwargs: kwargs
    stix_object = {
        "id": "indicator--2",
        "type": "indicator",
        "pattern": "[ipv4-addr:value = '192.0.2.2']",
        "x_opencti_score": 99,
        "extensions": {
            _OPENCTI_EXTENSION: {
                "score": 50,
                "detection": True,
                "create_observables": True,
                "workflow_id": "workflow--indicator",
            },
        },
    }

    result = indicator.import_from_stix2(stixObject=stix_object)

    assert result["x_opencti_score"] == 99
    assert result["x_opencti_detection"] is True
    assert result["x_opencti_create_observables"] is True
    assert result["x_opencti_workflow_id"] == "workflow--indicator"
    assert len(opencti.bulk_lookup_keys) == 1
    assert ("x_opencti_score", "score") in opencti.bulk_lookup_keys[0]


def test_indicator_import_many_builds_inputs_for_each_stix_object():
    opencti = _OpenCTI()
    indicator = Indicator(opencti)
    captured = {}

    def create_many(items, with_observables=True):
        captured["with_observables"] = with_observables
        return items

    indicator.create_many = create_many
    stix_objects = [
        {
            "id": "indicator--1",
            "type": "indicator",
            "pattern": "[ipv4-addr:value = '192.0.2.1']",
            "pattern_type": "stix",
            "extensions": {
                _OPENCTI_EXTENSION: {"main_observable_type": "IPv4-Addr"},
            },
        },
        {
            "id": "indicator--2",
            "type": "indicator",
            "pattern": "[domain-name:value = 'example.test']",
            "pattern_type": "stix",
            "x_opencti_main_observable_type": "Domain-Name",
        },
    ]

    result = indicator.import_many_from_stix2(
        stix_objects,
        [{"object_marking_ids": ["marking--1"]}, {}],
        update=True,
    )

    assert [item["stix_id"] for item in result] == ["indicator--1", "indicator--2"]
    assert result[0]["x_opencti_main_observable_type"] == "IPv4-Addr"
    assert result[0]["objectMarking"] == ["marking--1"]
    assert result[1]["x_opencti_main_observable_type"] == "Domain-Name"
    assert all(item["update"] is True for item in result)
    assert captured["with_observables"] is False


def test_indicator_create_many_keeps_observables_by_default():
    opencti = _QueryOpenCTI()
    indicator = Indicator(opencti)

    indicator.create_many([_bulk_create_item()])

    assert "observables {" in opencti.queries[0]


def test_indicator_create_many_can_skip_observables():
    opencti = _QueryOpenCTI()
    indicator = Indicator(opencti)

    indicator.create_many([_bulk_create_item()], with_observables=False)

    assert "observables {" not in opencti.queries[0]
