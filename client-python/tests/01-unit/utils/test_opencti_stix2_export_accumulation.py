from types import SimpleNamespace

from pycti.utils.opencti_stix2 import OpenCTIStix2


class _StaticCollection:
    def __init__(self, items):
        self.items = items

    def list(self, **_kwargs):
        return self.items


def _helper(entities=None):
    helper = OpenCTIStix2.__new__(OpenCTIStix2)
    helper.generate_export = lambda entity: entity
    helper.prepare_export = lambda entity, mode, access_filter: [entity]
    if entities is not None:
        helper.export_entities_list = lambda **_kwargs: entities
    return helper


def _relationship(identifier):
    return {
        "id": identifier,
        "type": "uses",
        "x_opencti_id": f"internal-{identifier}",
        "from": {
            "id": "root",
            "standard_id": "indicator--root",
            "entity_type": "Indicator",
            "parent_types": ["Stix-Domain-Object"],
        },
        "to": {
            "id": f"target-{identifier}",
            "standard_id": f"malware--{identifier}",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        },
    }


def _full_helper(relationships):
    helper = OpenCTIStix2.__new__(OpenCTIStix2)
    helper.opencti = SimpleNamespace(
        stix_nested_ref_relationship=_StaticCollection([]),
        stix_core_relationship=_StaticCollection(relationships),
        stix_sighting_relationship=_StaticCollection([]),
        opencti_stix_object_or_stix_relationship=_StaticCollection([{}]),
    )
    helper.generate_export = lambda entity: entity
    helper.prepare_id_filters_export = lambda entity_id, access_filter: None
    helper.get_reader = lambda resolve_type: lambda filters: None
    return helper


def test_export_selected_deduplicates_and_rewrites_bundle_once():
    helper = _helper()
    rewrite_sizes = []
    helper._rewrite_embedded_image_uris_in_bundle_for_export = (
        lambda bundle: rewrite_sizes.append(len(bundle["objects"]))
    )
    entities = [
        {"id": "indicator--1", "type": "indicator"},
        {"id": "indicator--2", "type": "indicator"},
        {"id": "indicator--1", "type": "indicator"},
    ]

    bundle = helper.export_selected(entities)

    assert [item["id"] for item in bundle["objects"]] == [
        "indicator--1",
        "indicator--2",
    ]
    assert rewrite_sizes == [2]


def test_prepare_export_full_deduplicates_relationship_bundles():
    helper = _full_helper(
        [
            _relationship("relationship--1"),
            _relationship("relationship--2"),
            _relationship("relationship--1"),
        ]
    )
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    result = helper.prepare_export(entity=entity, mode="full")

    assert [item["id"] for item in result] == [
        "indicator--root",
        "relationship--1",
        "relationship--2",
    ]


def test_export_list_deduplicates_objects_and_rewrites_bundle_once():
    entities = [
        {"id": "indicator--1", "type": "indicator"},
        {"id": "indicator--2", "type": "indicator"},
        {"id": "indicator--1", "type": "indicator"},
    ]
    helper = _helper(entities)
    rewrite_sizes = []
    helper._rewrite_embedded_image_uris_in_bundle_for_export = (
        lambda bundle: rewrite_sizes.append(len(bundle["objects"]))
    )

    bundle = helper.export_list(entity_type="Indicator")

    assert [item["id"] for item in bundle["objects"]] == [
        "indicator--1",
        "indicator--2",
    ]
    assert rewrite_sizes == [2]
